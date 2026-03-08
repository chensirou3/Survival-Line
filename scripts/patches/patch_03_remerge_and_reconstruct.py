"""
Patch 03: Re-merge panel using patched electricity + min wage, reconstruct survival line.
Uses _patched.csv files where available, falls back to original _clean.csv.
Outputs to data_clean/merged/panel_merged_patched.csv and data_final/ _patched files.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root, coverage_audit
import pandas as pd
import numpy as np

def main():
    root = get_project_root()
    logger = setup_logger("patch_03_remerge")
    logger.info("=== Patch 03: Re-merge and Reconstruct ===")

    merged_dir = root / "data_clean" / "merged"

    # 1. Load skeleton
    skeleton = pd.read_csv(root / "data_raw" / "panel_skeleton" / "panel_skeleton.csv",
                           dtype={"state_fips": str})
    logger.info(f"Skeleton: {skeleton.shape}")

    # 2. Load modules (patched where available)
    def load_module(name, patched_path, original_path):
        if patched_path.exists():
            logger.info(f"  {name}: using PATCHED ({patched_path.name})")
            return pd.read_csv(patched_path, dtype={"state_fips": str})
        elif original_path.exists():
            logger.info(f"  {name}: using ORIGINAL ({original_path.name})")
            return pd.read_csv(original_path, dtype={"state_fips": str})
        else:
            logger.warning(f"  {name}: NOT FOUND")
            return None

    cd = root / "data_clean"
    modules = {}

    # Min wage (patched has 2023)
    mw = load_module("min_wage",
                      cd / "min_wage" / "mw_clean_patched.csv",
                      cd / "min_wage" / "mw_clean.csv")
    if mw is not None: modules["min_wage"] = mw

    # Housing (no patch)
    housing = load_module("housing",
                          cd / "housing" / "housing_clean_patched.csv",
                          cd / "housing" / "housing_clean.csv")
    if housing is not None: modules["housing"] = housing

    # Food (no patch)
    food = load_module("food",
                       cd / "food" / "food_clean_patched.csv",
                       cd / "food" / "food_clean.csv")
    if food is not None: modules["food"] = food

    # Utilities: use patched electricity + original gas, merge them
    elec = load_module("electricity",
                       cd / "utilities" / "electricity_clean_patched.csv",
                       cd / "utilities" / "electricity_clean.csv")
    gas = load_module("gas",
                      cd / "utilities" / "gas_clean_patched.csv",
                      cd / "utilities" / "gas_clean.csv")
    if elec is not None and gas is not None:
        # Drop overlapping columns before merging
        gas_cols_to_drop = [c for c in gas.columns if c in elec.columns
                            and c not in ["state_fips", "year", "state_abbr"]]
        if gas_cols_to_drop:
            gas = gas.drop(columns=gas_cols_to_drop)
        utilities = elec.merge(gas, on=["state_fips", "year"], how="outer", suffixes=('', '_gas'))
        # Clean up suffixed state_abbr
        if 'state_abbr_gas' in utilities.columns:
            utilities['state_abbr'] = utilities['state_abbr'].fillna(utilities['state_abbr_gas'])
            utilities.drop(columns=['state_abbr_gas'], inplace=True)
        modules["utilities"] = utilities
    elif elec is not None:
        modules["utilities"] = elec

    # Controls (no patch)
    controls = load_module("controls",
                           cd / "controls" / "controls_clean_patched.csv",
                           cd / "controls" / "controls_clean.csv")
    if controls is not None: modules["controls"] = controls

    # 3. Merge
    panel = skeleton.copy()
    for name, df in modules.items():
        common = [c for c in df.columns if c in panel.columns and c not in ["state_fips", "year"]]
        if common:
            logger.warning(f"  Dropping overlapping columns from {name}: {common}")
            df = df.drop(columns=common)
        panel = panel.merge(df, on=["state_fips", "year"], how="left")
        logger.info(f"  After merging {name}: {panel.shape}")

    # 4. Save merged
    panel.to_csv(merged_dir / "panel_merged_patched.csv", index=False)
    logger.info(f"Merged panel saved: {panel.shape}")

    # 5. Construct Survival Line (same formula as Module 7)
    if "contract_rent_annual" not in panel.columns and "contract_rent_monthly" in panel.columns:
        panel["contract_rent_annual"] = panel["contract_rent_monthly"] * 12
    if "electric_bill_annual" not in panel.columns and "electric_bill_monthly" in panel.columns:
        panel["electric_bill_annual"] = panel["electric_bill_monthly"] * 12
    if "gas_bill_annual" not in panel.columns and "gas_bill_monthly" in panel.columns:
        panel["gas_bill_annual"] = panel["gas_bill_monthly"] * 12

    housing_col = panel.get("contract_rent_annual", pd.Series(np.nan, index=panel.index))
    food_col = panel.get("food_reconstructed_annual", pd.Series(np.nan, index=panel.index))
    elec_col = panel.get("electric_bill_annual", pd.Series(np.nan, index=panel.index))
    gas_col = panel.get("gas_bill_annual", pd.Series(np.nan, index=panel.index))

    panel["survival_line_nominal_main"] = housing_col + food_col + elec_col + gas_col
    if "gross_rent_annual" in panel.columns:
        panel["survival_line_nominal_grossrent"] = panel["gross_rent_annual"] + food_col
    else:
        panel["survival_line_nominal_grossrent"] = np.nan
    panel["survival_line_nominal_no_gas"] = housing_col + food_col + elec_col

    if "annualized_mw_income" in panel.columns:
        for ver in ["main", "grossrent", "no_gas"]:
            sl_col = f"survival_line_nominal_{ver}"
            if sl_col in panel.columns:
                panel[f"mw_survival_gap_{ver}"] = panel["annualized_mw_income"] - panel[sl_col]
                panel[f"mw_survival_ratio_{ver}"] = panel["annualized_mw_income"] / panel[sl_col]

    panel["quality_flag"] = "ok"
    if "gas_bill_monthly" in panel.columns:
        panel.loc[panel["gas_bill_monthly"].isna(), "quality_flag"] = "gas_missing"
    panel.loc[panel["year"] == 2020, "quality_flag"] = "acs_2020_missing"

    # 6. Save final
    main_dir = root / "data_final" / "survival_main"
    export_dir = root / "data_final" / "export"

    panel.to_csv(main_dir / "survival_line_all_versions_patched.csv", index=False)

    main_cols = [
        "state_fips", "state_abbr", "state_name", "year",
        "census_region", "census_division",
        "contract_rent_monthly", "contract_rent_annual",
        "electric_bill_monthly", "electric_bill_annual",
        "gas_bill_monthly", "gas_bill_annual",
        "tfp_national_annual", "rpp_goods_index",
        "food_reconstructed_annual", "construction_flag_food",
        "survival_line_nominal_main",
        "min_wage_nominal", "binding_min_wage", "annualized_mw_income",
        "mw_survival_gap_main", "mw_survival_ratio_main",
        "quality_flag",
    ]
    existing_cols = [c for c in main_cols if c in panel.columns]
    panel[existing_cols].to_csv(export_dir / "survival_line_main_patched.csv", index=False)

    # Stats
    for ver in ["main", "no_gas"]:
        col = f"survival_line_nominal_{ver}"
        n = panel[col].notna().sum()
        logger.info(f"{ver}: {n}/{len(panel)} non-null ({n/len(panel)*100:.1f}%)")
        if panel[col].notna().any():
            logger.info(f"  {panel[col].describe()}")

    logger.info("=== Patch 03 COMPLETE ===")

if __name__ == "__main__":
    main()

