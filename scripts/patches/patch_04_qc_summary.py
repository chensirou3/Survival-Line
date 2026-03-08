"""
Patch 04: QC Summary — compare pre-patch vs post-patch coverage and data quality.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root
import pandas as pd
import numpy as np

def main():
    root = get_project_root()
    logger = setup_logger("patch_04_qc")
    logger.info("=== Patch 04: QC Summary ===")

    main_dir = root / "data_final" / "survival_main"
    qc_dir = root / "qc" / "patch_01"
    qc_dir.mkdir(parents=True, exist_ok=True)

    # Load pre-patch and post-patch
    pre = pd.read_csv(main_dir / "survival_line_all_versions.csv", dtype={"state_fips": str})
    post = pd.read_csv(main_dir / "survival_line_all_versions_patched.csv", dtype={"state_fips": str})
    logger.info(f"Pre-patch: {pre.shape}, Post-patch: {post.shape}")

    # 1. Coverage comparison
    key_cols = ['survival_line_nominal_main', 'survival_line_nominal_no_gas',
                'electric_bill_monthly', 'min_wage_nominal', 'binding_min_wage',
                'contract_rent_monthly', 'food_reconstructed_annual',
                'gas_bill_monthly', 'annualized_mw_income']

    coverage = []
    for col in key_cols:
        pre_n = pre[col].notna().sum() if col in pre.columns else 0
        post_n = post[col].notna().sum() if col in post.columns else 0
        coverage.append({
            'variable': col,
            'pre_patch_nonmissing': pre_n,
            'post_patch_nonmissing': post_n,
            'gained': post_n - pre_n,
            'pre_pct': round(pre_n / len(pre) * 100, 1),
            'post_pct': round(post_n / len(post) * 100, 1),
        })

    cov_df = pd.DataFrame(coverage)
    cov_df.to_csv(qc_dir / "coverage_comparison.csv", index=False)
    logger.info(f"\n=== Coverage Comparison ===\n{cov_df.to_string(index=False)}")

    # 2. Year-level coverage for survival_line_nominal_main
    logger.info("\n=== Year-level Coverage (survival_line_nominal_main) ===")
    for year in range(2008, 2024):
        pre_n = pre.loc[(pre['year']==year) & pre['survival_line_nominal_main'].notna()].shape[0]
        post_n = post.loc[(post['year']==year) & post['survival_line_nominal_main'].notna()].shape[0]
        status = "✓" if post_n == 51 else f"({post_n}/51)"
        delta = f"+{post_n - pre_n}" if post_n > pre_n else ""
        logger.info(f"  {year}: pre={pre_n}/51, post={post_n}/51 {status} {delta}")

    # 3. Remaining missingness in post-patch
    logger.info("\n=== Remaining Missingness (post-patch) ===")
    miss = post.isnull().sum()
    miss_pct = (post.isnull().sum() / len(post) * 100).round(2)
    miss_df = pd.DataFrame({"missing_count": miss, "missing_pct": miss_pct})
    miss_df = miss_df[miss_df['missing_count'] > 0].sort_values('missing_count', ascending=False)
    miss_df.to_csv(qc_dir / "post_patch_missingness.csv")
    logger.info(f"\n{miss_df.to_string()}")

    # 4. Sanity checks on patched electricity data
    logger.info("\n=== Electricity Patch Sanity Checks ===")
    elec_post = post[post['year'].isin([2021, 2022, 2023]) & post['electric_bill_monthly'].notna()]
    if len(elec_post) > 0:
        stats = elec_post.groupby('year')['electric_bill_monthly'].agg(['mean', 'min', 'max', 'count'])
        logger.info(f"\n{stats.to_string()}")
        # Check for outliers
        for year in [2021, 2022, 2023]:
            yr_data = elec_post[elec_post['year'] == year]['electric_bill_monthly']
            if len(yr_data) > 0:
                low = yr_data.quantile(0.01)
                high = yr_data.quantile(0.99)
                outliers = yr_data[(yr_data < low) | (yr_data > high)]
                if len(outliers) > 0:
                    logger.warning(f"  {year} outliers: {len(outliers)} states outside [{low:.0f}, {high:.0f}]")

    # 5. MW 2023 sanity check
    logger.info("\n=== MW 2023 Sanity Check ===")
    mw_2023 = post[post['year'] == 2023]
    if 'binding_min_wage' in mw_2023.columns:
        logger.info(f"  States with MW data: {mw_2023['binding_min_wage'].notna().sum()}")
        logger.info(f"  Mean binding MW: ${mw_2023['binding_min_wage'].mean():.2f}")
        logger.info(f"  Range: ${mw_2023['binding_min_wage'].min():.2f} - ${mw_2023['binding_min_wage'].max():.2f}")
        # States at federal floor
        at_floor = mw_2023[mw_2023['binding_min_wage'] == 7.25]
        logger.info(f"  States at federal floor ($7.25): {len(at_floor)}")

    # 6. Continuity check: compare 2020 vs 2021 electric bills
    logger.info("\n=== Continuity Check: Electric Bill 2020→2021 ===")
    e2020 = post[post['year']==2020][['state_fips', 'electric_bill_monthly']].rename(
        columns={'electric_bill_monthly': 'bill_2020'})
    e2021 = post[post['year']==2021][['state_fips', 'electric_bill_monthly']].rename(
        columns={'electric_bill_monthly': 'bill_2021'})
    if len(e2020) > 0 and len(e2021) > 0:
        cont = e2020.merge(e2021, on='state_fips')
        cont['pct_change'] = (cont['bill_2021'] / cont['bill_2020'] - 1) * 100
        logger.info(f"  Mean pct change: {cont['pct_change'].mean():.1f}%")
        logger.info(f"  Max increase: {cont['pct_change'].max():.1f}%")
        logger.info(f"  Max decrease: {cont['pct_change'].min():.1f}%")
        big_jumps = cont[cont['pct_change'].abs() > 20]
        if len(big_jumps) > 0:
            logger.warning(f"  Large jumps (>20%): {len(big_jumps)} states")

    # 7. Final summary
    sl_main = post['survival_line_nominal_main']
    logger.info(f"\n{'='*60}")
    logger.info(f"FINAL SUMMARY (Post-Patch)")
    logger.info(f"  Total obs: {len(post)}")
    logger.info(f"  Survival Line (main) coverage: {sl_main.notna().sum()}/{len(post)} "
               f"({sl_main.notna().sum()/len(post)*100:.1f}%)")
    logger.info(f"  Missing = ACS 2020 year: {(post['year']==2020).sum()} rows")
    logger.info(f"  Mean SL: ${sl_main.mean():.0f}")
    logger.info(f"  Improvement: {sl_main.notna().sum() - pre['survival_line_nominal_main'].notna().sum()} "
               f"additional obs vs pre-patch")
    logger.info(f"{'='*60}")
    logger.info("=== Patch 04 COMPLETE ===")

if __name__ == "__main__":
    main()

