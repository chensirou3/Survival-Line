"""
Generate Sample Audit: Produces sample tables, coverage audits, and quality checks
for all 6 data modules of the Survival Line project.
Outputs to docs/audit_reports/
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '00_setup'))
from utils import get_project_root, FIPS_STATES
import pandas as pd
import numpy as np

root = get_project_root()
OUT = root / "docs" / "audit_reports"
OUT.mkdir(parents=True, exist_ok=True)

SAMPLE_FIPS = ["06", "36", "48", "12", "17"]  # CA, NY, TX, FL, IL
SAMPLE_YEARS = [2008, 2012, 2016, 2020, 2023]
ALL_FIPS = set(FIPS_STATES.keys())
ALL_YEARS = set(range(2008, 2024))

def coverage(df, name, extra_cols=None):
    """Generate coverage audit for a module."""
    fips_present = set(df["state_fips"].unique())
    years_present = set(df["year"].unique())
    missing_fips = ALL_FIPS - fips_present
    missing_years = ALL_YEARS - years_present
    n = len(df)
    n_expected = 51 * 16
    # Per-year coverage
    year_cov = df.groupby("year")["state_fips"].nunique().reset_index()
    year_cov.columns = ["year", "states_present"]
    year_cov["states_expected"] = 51
    year_cov["complete"] = year_cov["states_present"] == 51

    report = {
        "module": name,
        "rows": n,
        "expected_rows": n_expected,
        "coverage_pct": round(n / n_expected * 100, 1),
        "states_present": len(fips_present),
        "states_expected": 51,
        "missing_states": sorted(missing_fips) if missing_fips else "none",
        "years_present": sorted(years_present),
        "missing_years": sorted(missing_years) if missing_years else "none",
    }
    # Column missingness
    if extra_cols:
        for col in extra_cols:
            if col in df.columns:
                miss = df[col].isna().sum()
                report[f"missing_{col}"] = f"{miss}/{n} ({round(miss/n*100,1)}%)"
            else:
                report[f"missing_{col}"] = "COLUMN_NOT_FOUND"
    return report, year_cov

def sample(df, name, cols=None):
    """Extract sample rows for human inspection."""
    mask = df["state_fips"].isin(SAMPLE_FIPS) & df["year"].isin(SAMPLE_YEARS)
    s = df[mask].copy()
    if cols:
        existing = [c for c in cols if c in df.columns]
        s = s[existing]
    return s.sort_values(["state_fips", "year"]).reset_index(drop=True)

def quality_check(df, name, value_cols):
    """Basic quality checks: duplicates, outliers, NaN patterns."""
    checks = []
    # Duplicate keys
    dups = df.duplicated(subset=["state_fips", "year"], keep=False).sum()
    checks.append({"check": "duplicate_keys", "result": dups, "status": "PASS" if dups == 0 else "FAIL"})
    # Value checks
    for col in value_cols:
        if col not in df.columns:
            checks.append({"check": f"exists_{col}", "result": "MISSING", "status": "FAIL"})
            continue
        vals = df[col].dropna()
        if len(vals) == 0:
            checks.append({"check": f"has_data_{col}", "result": 0, "status": "FAIL"})
            continue
        q1, q3 = vals.quantile(0.25), vals.quantile(0.75)
        iqr = q3 - q1
        outliers = ((vals < q1 - 3*iqr) | (vals > q3 + 3*iqr)).sum()
        checks.append({"check": f"outliers_{col}", "result": f"{outliers} (IQR×3)", 
                       "status": "PASS" if outliers <= 5 else "WARN"})
        checks.append({"check": f"range_{col}", "result": f"{vals.min():.2f} – {vals.max():.2f}", "status": "INFO"})
        checks.append({"check": f"mean_{col}", "result": f"{vals.mean():.2f}", "status": "INFO"})
    return pd.DataFrame(checks)

print("=" * 70)
print("SURVIVAL LINE DATA SAMPLE AUDIT")
print("=" * 70)

# ============================================================
# MODULE 0: PANEL SKELETON
# ============================================================
print("\n>>> Module 0: Panel Skeleton")
skel = pd.read_csv(root / "data_raw/panel_skeleton/panel_skeleton.csv", dtype={"state_fips": str})
s0 = sample(skel, "skeleton", ["state_fips", "state_abbr", "state_name", "year", "census_region", "census_division"])
c0, yc0 = coverage(skel, "skeleton")
q0 = quality_check(skel, "skeleton", [])
s0.to_csv(OUT / "sample_panel_skeleton.csv", index=False)
pd.DataFrame([c0]).to_csv(OUT / "coverage_panel_skeleton.csv", index=False)
print(f"  Rows: {c0['rows']}, States: {c0['states_present']}, Coverage: {c0['coverage_pct']}%")
print(f"  Sample:\n{s0.head(10).to_string(index=False)}")

# ============================================================
# MODULE 1: MINIMUM WAGE
# ============================================================
print("\n>>> Module 1: Minimum Wage")
mw_path = root / "data_clean/min_wage/mw_clean_patched.csv"
if not mw_path.exists():
    mw_path = root / "data_clean/min_wage/mw_clean.csv"
mw = pd.read_csv(mw_path, dtype={"state_fips": str})
mw_cols = ["state_fips", "state_abbr", "year", "min_wage_nominal", "federal_min_wage", "binding_min_wage", "annualized_mw_income"]
s1 = sample(mw, "min_wage", mw_cols)
c1, yc1 = coverage(mw, "min_wage", ["min_wage_nominal", "binding_min_wage", "annualized_mw_income"])
q1 = quality_check(mw, "min_wage", ["min_wage_nominal", "binding_min_wage", "annualized_mw_income"])
s1.to_csv(OUT / "sample_min_wage.csv", index=False)
pd.DataFrame([c1]).to_csv(OUT / "coverage_min_wage.csv", index=False)
q1.to_csv(OUT / "quality_min_wage.csv", index=False)
yc1.to_csv(OUT / "coverage_min_wage_yearly.csv", index=False)
print(f"  Rows: {c1['rows']}, Coverage: {c1['coverage_pct']}%, Missing years: {c1['missing_years']}")
print(f"  Sample:\n{s1.to_string(index=False)}")

# ============================================================
# MODULE 2: HOUSING
# ============================================================
print("\n>>> Module 2: Housing")
hs = pd.read_csv(root / "data_clean/housing/housing_clean.csv", dtype={"state_fips": str})
hs_cols = ["state_fips", "state_abbr", "year", "contract_rent_monthly", "contract_rent_annual", "gross_rent_monthly", "gross_rent_annual"]
s2 = sample(hs, "housing", hs_cols)
c2, yc2 = coverage(hs, "housing", ["contract_rent_monthly", "gross_rent_monthly"])
q2 = quality_check(hs, "housing", ["contract_rent_monthly", "gross_rent_monthly"])
s2.to_csv(OUT / "sample_housing.csv", index=False)
pd.DataFrame([c2]).to_csv(OUT / "coverage_housing.csv", index=False)
q2.to_csv(OUT / "quality_housing.csv", index=False)
yc2.to_csv(OUT / "coverage_housing_yearly.csv", index=False)
# Check 2020
has_2020 = 2020 in hs["year"].values
print(f"  Rows: {c2['rows']}, Coverage: {c2['coverage_pct']}%, Missing years: {c2['missing_years']}")
print(f"  ACS 2020 present: {has_2020} (expected: False)")
print(f"  Sample:\n{s2.to_string(index=False)}")

# ============================================================
# MODULE 3: UTILITIES
# ============================================================
print("\n>>> Module 3: Utilities")
elec_path = root / "data_clean/utilities/electricity_clean_patched.csv"
if not elec_path.exists():
    elec_path = root / "data_clean/utilities/electricity_clean.csv"
elec = pd.read_csv(elec_path, dtype={"state_fips": str})
gas = pd.read_csv(root / "data_clean/utilities/gas_clean.csv", dtype={"state_fips": str})
# Merge electricity + gas
util = elec.merge(gas[["state_fips", "year", "gas_price_per_mcf", "gas_bill_monthly", "gas_bill_annual", "construction_flag_gas"]],
                  on=["state_fips", "year"], how="outer")
util_cols = ["state_fips", "state_abbr", "year", "electric_bill_monthly", "electric_bill_annual",
             "gas_bill_monthly", "gas_bill_annual", "construction_flag_gas"]
if "construction_flag_electric" in util.columns:
    util_cols.insert(5, "construction_flag_electric")
s3 = sample(util, "utilities", util_cols)
c3, yc3 = coverage(util, "utilities", ["electric_bill_monthly", "gas_bill_monthly"])
q3 = quality_check(util, "utilities", ["electric_bill_monthly", "gas_bill_monthly", "gas_bill_annual"])
s3.to_csv(OUT / "sample_utilities.csv", index=False)
pd.DataFrame([c3]).to_csv(OUT / "coverage_utilities.csv", index=False)
q3.to_csv(OUT / "quality_utilities.csv", index=False)
yc3.to_csv(OUT / "coverage_utilities_yearly.csv", index=False)
# AK/HI special check
for fips, label in [("02", "Alaska"), ("15", "Hawaii")]:
    sub = util[util["state_fips"] == fips]
    e_miss = sub["electric_bill_monthly"].isna().sum()
    g_miss = sub["gas_bill_monthly"].isna().sum()
    print(f"  {label}: {len(sub)} rows, elec_missing={e_miss}, gas_missing={g_miss}")
print(f"  Rows: {c3['rows']}, Coverage: {c3['coverage_pct']}%, Missing years: {c3['missing_years']}")
print(f"  Sample:\n{s3.to_string(index=False)}")

# ============================================================
# MODULE 4: FOOD
# ============================================================
print("\n>>> Module 4: Food")
food = pd.read_csv(root / "data_clean/food/food_clean.csv", dtype={"state_fips": str})
food_cols = ["state_fips", "state_abbr", "year", "tfp_national_monthly", "tfp_national_annual",
             "rpp_goods_index", "food_reconstructed_annual", "construction_flag_food"]
s4 = sample(food, "food", food_cols)
c4, yc4 = coverage(food, "food", ["tfp_national_annual", "rpp_goods_index", "food_reconstructed_annual"])
q4 = quality_check(food, "food", ["tfp_national_annual", "rpp_goods_index", "food_reconstructed_annual"])
s4.to_csv(OUT / "sample_food.csv", index=False)
pd.DataFrame([c4]).to_csv(OUT / "coverage_food.csv", index=False)
q4.to_csv(OUT / "quality_food.csv", index=False)
yc4.to_csv(OUT / "coverage_food_yearly.csv", index=False)
# AK/HI food flag check
for fips, label in [("02", "Alaska"), ("15", "Hawaii")]:
    sub = food[food["state_fips"] == fips]
    flags = sub["construction_flag_food"].unique()
    print(f"  {label} food flags: {flags}")
print(f"  Rows: {c4['rows']}, Coverage: {c4['coverage_pct']}%, Missing years: {c4['missing_years']}")
print(f"  Sample:\n{s4.to_string(index=False)}")

# ============================================================
# MODULE 5: CONTROLS
# ============================================================
print("\n>>> Module 5: Controls")
ctrl = pd.read_csv(root / "data_clean/controls/controls_clean.csv", dtype={"state_fips": str})
ctrl_cols = ["state_fips", "year", "poverty_rate", "median_hh_income", "rpp_all", "rpp_rents"]
s5 = sample(ctrl, "controls", ctrl_cols)
c5, yc5 = coverage(ctrl, "controls", ["poverty_rate", "median_hh_income", "rpp_all", "rpp_rents"])
q5 = quality_check(ctrl, "controls", ["poverty_rate", "median_hh_income", "rpp_all", "rpp_rents"])
s5.to_csv(OUT / "sample_controls.csv", index=False)
pd.DataFrame([c5]).to_csv(OUT / "coverage_controls.csv", index=False)
q5.to_csv(OUT / "quality_controls.csv", index=False)
yc5.to_csv(OUT / "coverage_controls_yearly.csv", index=False)
print(f"  Rows: {c5['rows']}, Coverage: {c5['coverage_pct']}%, Missing years: {c5['missing_years']}")
print(f"  Sample:\n{s5.to_string(index=False)}")

# ============================================================
# FINAL MERGED PANEL CHECK
# ============================================================
print("\n>>> Final Merged Panel (patched)")
merged_path = root / "data_final/export/survival_line_main_patched.csv"
if merged_path.exists():
    final = pd.read_csv(merged_path, dtype={"state_fips": str})
    final_val_cols = ["survival_line_nominal_main", "contract_rent_annual", "electric_bill_annual",
                      "gas_bill_annual", "food_reconstructed_annual", "binding_min_wage"]
    cf, ycf = coverage(final, "final_panel", [c for c in final_val_cols if c in final.columns])
    qf = quality_check(final, "final_panel", [c for c in final_val_cols if c in final.columns])
    sf = sample(final, "final_panel", ["state_fips", "state_abbr", "year"] +
                [c for c in final_val_cols if c in final.columns])
    sf.to_csv(OUT / "sample_final_panel.csv", index=False)
    pd.DataFrame([cf]).to_csv(OUT / "coverage_final_panel.csv", index=False)
    qf.to_csv(OUT / "quality_final_panel.csv", index=False)
    print(f"  Rows: {cf['rows']}, Coverage: {cf['coverage_pct']}%")
    print(f"  SL valid: {final['survival_line_nominal_main'].notna().sum()}/{len(final)}")
    print(f"  Sample:\n{sf.to_string(index=False)}")
else:
    print("  WARNING: survival_line_main_patched.csv not found!")

# ============================================================
# EXECUTIVE SUMMARY
# ============================================================
print("\n" + "=" * 70)
print("EXECUTIVE SUMMARY")
print("=" * 70)
modules = [
    ("Panel Skeleton", c0), ("Minimum Wage", c1), ("Housing", c2),
    ("Utilities", c3), ("Food", c4), ("Controls", c5)
]
summary_rows = []
for name, c in modules:
    summary_rows.append({
        "Module": name,
        "Rows": c["rows"],
        "Expected": c["expected_rows"],
        "Coverage%": c["coverage_pct"],
        "States": c["states_present"],
        "Missing_Years": str(c["missing_years"]),
        "Merge_Ready": "YES" if c["states_present"] == 51 else "NO"
    })
summary_df = pd.DataFrame(summary_rows)
summary_df.to_csv(OUT / "executive_summary.csv", index=False)
print(summary_df.to_string(index=False))

print(f"\n✅ All audit files written to: {OUT}")
print(f"   Files generated: {len(list(OUT.glob('*.csv')))}")
