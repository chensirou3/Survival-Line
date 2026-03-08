"""
Module 6: Merge all modules into unified panel
Merges: skeleton + min_wage + housing + utilities + food + controls
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root, coverage_audit
import pandas as pd

def main():
    root = get_project_root()
    logger = setup_logger("merge")
    logger.info("=== Module 6: Merge Panel ===")

    merged_dir = root / "data_clean" / "merged"

    # 1. Load skeleton
    skeleton = pd.read_csv(root / "data_raw" / "panel_skeleton" / "panel_skeleton.csv",
                           dtype={"state_fips": str})
    logger.info(f"Skeleton: {skeleton.shape}")

    # 2. Load each module
    modules = {
        "min_wage": root / "data_clean" / "min_wage" / "mw_clean.csv",
        "housing": root / "data_clean" / "housing" / "housing_clean.csv",
        "food": root / "data_clean" / "food" / "food_clean.csv",
    }

    # Utilities — may have separate files
    util_path = root / "data_clean" / "utilities" / "utilities_clean.csv"
    if util_path.exists():
        modules["utilities"] = util_path

    controls_path = root / "data_clean" / "controls" / "controls_clean.csv"
    if controls_path.exists():
        modules["controls"] = controls_path

    panel = skeleton.copy()
    for name, path in modules.items():
        if not path.exists():
            logger.warning(f"  {name} not found at {path}, skipping")
            continue
        df = pd.read_csv(path, dtype={"state_fips": str})
        # Drop duplicate columns before merge
        common = [c for c in df.columns if c in panel.columns and c not in ["state_fips", "year"]]
        if common:
            logger.warning(f"  Dropping overlapping columns from {name}: {common}")
            df = df.drop(columns=common)
        panel = panel.merge(df, on=["state_fips", "year"], how="left")
        logger.info(f"  After merging {name}: {panel.shape}")

    # 3. Save merged
    panel.to_csv(merged_dir / "panel_merged.csv", index=False)
    panel.to_parquet(merged_dir / "panel_merged.parquet", index=False)
    logger.info(f"Merged panel saved: {panel.shape}")

    # 4. Coverage audit
    report = coverage_audit(panel, "merged_panel")
    logger.info(f"Coverage: {report}")

    # 5. Quick missingness report
    miss = panel.isnull().sum()
    miss_pct = (panel.isnull().sum() / len(panel) * 100).round(2)
    miss_report = pd.DataFrame({"missing_count": miss, "missing_pct": miss_pct})
    miss_report.to_csv(root / "qc" / "missingness" / "merged_missingness.csv")
    logger.info(f"Missingness:\n{miss_report[miss_report['missing_count'] > 0]}")

    logger.info("=== Module 6 COMPLETE ===")

if __name__ == "__main__":
    main()

