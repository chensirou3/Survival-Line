"""
Module 8: Quality Control & Coverage Audit
Checks: coverage, missingness, outliers, double-counting, AK/HI special cases
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root, FIPS_STATES
import pandas as pd
import numpy as np

def check_coverage(df, logger):
    """Check state × year coverage."""
    expected_states = 51
    expected_years = set(range(2008, 2024))
    states = df['state_fips'].nunique()
    years = set(df['year'].unique())
    missing_years = expected_years - years
    logger.info(f"States: {states}/{expected_states}")
    logger.info(f"Years: {sorted(years)}")
    if missing_years:
        logger.warning(f"Missing years: {sorted(missing_years)}")
    # Per-state coverage
    state_counts = df.groupby('state_fips')['year'].nunique()
    incomplete = state_counts[state_counts < len(expected_years)]
    if len(incomplete) > 0:
        logger.warning(f"States with incomplete year coverage:\n{incomplete}")
    return states, years

def check_missingness(df, logger):
    """Check column-level missingness."""
    key_cols = [
        'contract_rent_monthly', 'electric_bill_monthly', 'gas_bill_monthly',
        'food_reconstructed_annual', 'survival_line_nominal_main',
        'binding_min_wage', 'annualized_mw_income'
    ]
    report = []
    for col in key_cols:
        if col in df.columns:
            n_miss = df[col].isna().sum()
            pct = round(n_miss / len(df) * 100, 2)
            report.append({"variable": col, "missing": n_miss, "pct": pct})
            if n_miss > 0:
                logger.warning(f"  {col}: {n_miss} missing ({pct}%)")
        else:
            report.append({"variable": col, "missing": "COLUMN_NOT_FOUND", "pct": 100})
            logger.error(f"  {col}: COLUMN NOT FOUND")
    return pd.DataFrame(report)

def check_outliers(df, logger):
    """Flag extreme values using IQR method."""
    numeric_cols = ['contract_rent_monthly', 'electric_bill_monthly',
                    'food_reconstructed_annual', 'survival_line_nominal_main']
    flags = []
    for col in numeric_cols:
        if col not in df.columns:
            continue
        vals = df[col].dropna()
        if len(vals) == 0:
            continue
        q1, q3 = vals.quantile(0.25), vals.quantile(0.75)
        iqr = q3 - q1
        lower, upper = q1 - 3 * iqr, q3 + 3 * iqr
        outliers = df[(df[col] < lower) | (df[col] > upper)]
        if len(outliers) > 0:
            logger.warning(f"  {col}: {len(outliers)} outliers (< {lower:.0f} or > {upper:.0f})")
            for _, row in outliers.iterrows():
                flags.append({
                    "state_fips": row.get("state_fips"), "year": row.get("year"),
                    "variable": col, "value": row[col],
                    "lower_bound": lower, "upper_bound": upper
                })
    return pd.DataFrame(flags) if flags else pd.DataFrame()

def check_double_counting(df, logger):
    """Verify Contract Rent vs Gross Rent usage consistency."""
    logger.info("Checking double-counting risk...")
    if 'gross_rent_monthly' in df.columns and 'contract_rent_monthly' in df.columns:
        both = df[df['gross_rent_monthly'].notna() & df['contract_rent_monthly'].notna()]
        diff = both['gross_rent_monthly'] - both['contract_rent_monthly']
        logger.info(f"  Gross - Contract rent diff: mean={diff.mean():.0f}, "
                    f"min={diff.min():.0f}, max={diff.max():.0f}")
        if (diff < 0).any():
            logger.error("  ALERT: Some Gross Rent < Contract Rent — data inconsistency")

def check_ak_hi(df, logger):
    """Check Alaska and Hawaii special handling."""
    for fips, name in [("02", "Alaska"), ("15", "Hawaii")]:
        sub = df[df['state_fips'] == fips]
        if sub.empty:
            logger.warning(f"  {name}: NO DATA")
            continue
        logger.info(f"  {name}: {len(sub)} rows")
        if 'construction_flag_food' in sub.columns:
            flags = sub['construction_flag_food'].unique()
            logger.info(f"    Food flags: {flags}")
        if 'survival_line_nominal_main' in sub.columns:
            sl = sub['survival_line_nominal_main']
            logger.info(f"    Survival line range: {sl.min():.0f} – {sl.max():.0f}")

def main():
    root = get_project_root()
    logger = setup_logger("qc", root / "logs" / "qc_logs")
    logger.info("=== Module 8: Quality Control ===")

    # Load final panel
    panel_path = root / "data_final" / "survival_main" / "survival_line_all_versions.csv"
    if not panel_path.exists():
        # Try merged panel
        panel_path = root / "data_clean" / "merged" / "panel_merged.csv"
    if not panel_path.exists():
        logger.error("No panel found. Run previous modules first.")
        return

    df = pd.read_csv(panel_path, dtype={"state_fips": str})
    logger.info(f"Loaded panel: {df.shape}")

    qc_dir = root / "qc"

    # 1. Coverage
    logger.info("\n--- Coverage Check ---")
    check_coverage(df, logger)

    # 2. Missingness
    logger.info("\n--- Missingness Check ---")
    miss_report = check_missingness(df, logger)
    miss_report.to_csv(qc_dir / "missingness" / "variable_missingness.csv", index=False)

    # 3. Outliers
    logger.info("\n--- Outlier Check ---")
    outlier_report = check_outliers(df, logger)
    if not outlier_report.empty:
        outlier_report.to_csv(qc_dir / "outliers" / "outlier_flags.csv", index=False)

    # 4. Double counting
    logger.info("\n--- Double Counting Check ---")
    check_double_counting(df, logger)

    # 5. AK/HI
    logger.info("\n--- Alaska/Hawaii Check ---")
    check_ak_hi(df, logger)

    logger.info("\n=== Module 8 QC COMPLETE ===")

if __name__ == "__main__":
    main()

