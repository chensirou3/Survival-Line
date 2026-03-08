"""
Module 0: Panel Skeleton Builder
构建 50 states + DC × 2008–2023 的 state-year 骨架面板
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import build_state_lookup, setup_logger, get_project_root, coverage_audit
import pandas as pd
import itertools

def main():
    root = get_project_root()
    logger = setup_logger("skeleton")
    logger.info("=== Module 0: Building Panel Skeleton ===")

    # 1. Build state lookup
    states = build_state_lookup()
    logger.info(f"State lookup: {len(states)} units (50 states + DC)")

    # 2. Build year range
    years = list(range(2008, 2024))  # 2008–2023
    logger.info(f"Year range: {years[0]}–{years[-1]} ({len(years)} years)")

    # 3. Cross-join state × year
    skeleton = pd.DataFrame(
        list(itertools.product(states["state_fips"], years)),
        columns=["state_fips", "year"]
    )
    skeleton = skeleton.merge(states, on="state_fips", how="left")

    # 4. Sort
    skeleton = skeleton.sort_values(["state_fips", "year"]).reset_index(drop=True)

    # 5. Verify
    assert len(skeleton) == 51 * 16, f"Expected {51*16}, got {len(skeleton)}"
    assert skeleton["state_fips"].nunique() == 51
    assert skeleton["year"].nunique() == 16
    logger.info(f"Skeleton shape: {skeleton.shape}")

    # 6. Save
    out_dir = root / "data_raw" / "panel_skeleton"
    skeleton.to_csv(out_dir / "panel_skeleton.csv", index=False)
    skeleton.to_parquet(out_dir / "panel_skeleton.parquet", index=False)
    logger.info(f"Saved to {out_dir}")

    # 7. Also save state lookup for reference
    states.to_csv(out_dir / "state_lookup.csv", index=False)

    # 8. Coverage audit
    report = coverage_audit(skeleton, "skeleton")
    logger.info(f"Coverage audit: {report}")
    logger.info("=== Module 0 COMPLETE ===")

if __name__ == "__main__":
    main()

