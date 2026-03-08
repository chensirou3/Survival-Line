"""
Module 7: Construct Survival Line Versions
Main: 12*ContractRent + Food_reconstructed + 12*ElecBill + 12*GasBill
Robustness: GrossRent version, no-gas version
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root
import pandas as pd
import numpy as np

def main():
    root = get_project_root()
    logger = setup_logger("construct")
    logger.info("=== Module 7: Construct Survival Line ===")

    # Load merged panel
    merged_path = root / "data_clean" / "merged" / "panel_merged.csv"
    if not merged_path.exists():
        logger.error("Merged panel not found. Run Module 6 first.")
        return

    df = pd.read_csv(merged_path, dtype={"state_fips": str})
    logger.info(f"Loaded merged panel: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")

    # === Main Version ===
    # SurvivalLine = 12*ContractRent + Food_reconstructed + 12*ElecBill + 12*GasBill
    if "contract_rent_annual" not in df.columns and "contract_rent_monthly" in df.columns:
        df["contract_rent_annual"] = df["contract_rent_monthly"] * 12
    if "electric_bill_annual" not in df.columns and "electric_bill_monthly" in df.columns:
        df["electric_bill_annual"] = df["electric_bill_monthly"] * 12
    if "gas_bill_annual" not in df.columns and "gas_bill_monthly" in df.columns:
        df["gas_bill_annual"] = df["gas_bill_monthly"] * 12

    # Main specification
    housing = df.get("contract_rent_annual", pd.Series(np.nan, index=df.index))
    food = df.get("food_reconstructed_annual", pd.Series(np.nan, index=df.index))
    elec = df.get("electric_bill_annual", pd.Series(np.nan, index=df.index))
    gas = df.get("gas_bill_annual", pd.Series(np.nan, index=df.index))

    df["survival_line_nominal_main"] = housing + food + elec + gas

    # === Robustness: Gross Rent version (no independent utilities) ===
    if "gross_rent_annual" in df.columns:
        df["survival_line_nominal_grossrent"] = df["gross_rent_annual"] + food
    else:
        df["survival_line_nominal_grossrent"] = np.nan

    # === Robustness: No-gas version ===
    df["survival_line_nominal_no_gas"] = housing + food + elec

    # === MW gap and ratio ===
    if "annualized_mw_income" in df.columns:
        for ver in ["main", "grossrent", "no_gas"]:
            sl_col = f"survival_line_nominal_{ver}"
            if sl_col in df.columns:
                df[f"mw_survival_gap_{ver}"] = df["annualized_mw_income"] - df[sl_col]
                df[f"mw_survival_ratio_{ver}"] = df["annualized_mw_income"] / df[sl_col]

    # === Quality flags ===
    df["quality_flag"] = "ok"
    # Flag where gas is missing
    if "gas_bill_monthly" in df.columns:
        df.loc[df["gas_bill_monthly"].isna(), "quality_flag"] = "gas_missing"
    # Flag 2020 (ACS not released)
    df.loc[df["year"] == 2020, "quality_flag"] = "acs_2020_missing"

    # === Save ===
    main_dir = root / "data_final" / "survival_main"
    rob_dir = root / "data_final" / "survival_robustness"
    export_dir = root / "data_final" / "export"

    df.to_csv(main_dir / "survival_line_all_versions.csv", index=False)
    df.to_parquet(main_dir / "survival_line_all_versions.parquet", index=False)

    # Export a clean main-version-only file
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
    existing_cols = [c for c in main_cols if c in df.columns]
    df_main = df[existing_cols].copy()
    df_main.to_csv(export_dir / "survival_line_main.csv", index=False)
    df_main.to_parquet(export_dir / "survival_line_main.parquet", index=False)

    logger.info(f"Main version non-null: {df['survival_line_nominal_main'].notna().sum()}/{len(df)}")
    logger.info(f"GrossRent version non-null: {df['survival_line_nominal_grossrent'].notna().sum()}/{len(df)}")
    logger.info(f"No-gas version non-null: {df['survival_line_nominal_no_gas'].notna().sum()}/{len(df)}")

    # Summary stats
    for ver in ["main", "grossrent", "no_gas"]:
        col = f"survival_line_nominal_{ver}"
        if col in df.columns and df[col].notna().any():
            logger.info(f"\n{ver} summary:\n{df[col].describe()}")

    logger.info("=== Module 7 COMPLETE ===")

if __name__ == "__main__":
    main()

