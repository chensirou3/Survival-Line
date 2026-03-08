"""
Patch 01: Update EIA Electricity Bill Data (2021-2023)
Source: EIA HS861 2010-.xlsx (Total Electric Industry, Residential)
  - Uses Revenue / (Customers * 12) = Avg Monthly Bill
  - Cross-validates against table_5a.xlsx from Wayback Machine (2022, 2023) and current (2024)
Also downloads table_5a snapshots for direct Average Monthly Bill where available.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root, FIPS_STATES
import pandas as pd
import numpy as np
import requests

ABBR_TO_FIPS = {v[0]: k for k, v in FIPS_STATES.items()}
STATE_NAME_TO_ABBR = {v[1].upper(): v[0] for k, v in FIPS_STATES.items()}

def download_hs861(raw_dir, logger):
    """Download HS861 2010-.xlsx from EIA."""
    url = "https://www.eia.gov/electricity/data/state/xls/861/HS861 2010-.xlsx"
    fpath = raw_dir / "HS861_2010.xlsx"
    if fpath.exists():
        logger.info(f"HS861 already downloaded: {fpath}")
        return fpath
    logger.info(f"Downloading HS861 from {url}")
    resp = requests.get(url, timeout=180, allow_redirects=True)
    resp.raise_for_status()
    with open(fpath, "wb") as f:
        f.write(resp.content)
    logger.info(f"Saved HS861: {len(resp.content)} bytes")
    return fpath

def download_table5a_snapshots(raw_dir, logger):
    """Download table_5a from Wayback (2022, 2023) and current (2024)."""
    snapshots = {
        2022: "https://web.archive.org/web/20240423060340/https://www.eia.gov/electricity/sales_revenue_price/xls/table_5A.xlsx",
        2023: "https://web.archive.org/web/20241112165955/https://www.eia.gov/electricity/sales_revenue_price/xls/table_5A.xlsx",
        2024: "https://www.eia.gov/electricity/sales_revenue_price/xls/table_5A.xlsx",
    }
    paths = {}
    for year, url in snapshots.items():
        fpath = raw_dir / f"table_5a_{year}.xlsx"
        if fpath.exists():
            logger.info(f"table_5a {year} already downloaded")
            paths[year] = fpath
            continue
        try:
            resp = requests.get(url, timeout=120, allow_redirects=True)
            resp.raise_for_status()
            with open(fpath, "wb") as f:
                f.write(resp.content)
            paths[year] = fpath
            logger.info(f"table_5a {year}: {len(resp.content)} bytes")
        except Exception as e:
            logger.warning(f"table_5a {year} download failed: {e}")
    return paths

def parse_hs861(fpath, logger):
    """Parse HS861 Total Electric Industry sheet for residential data."""
    logger.info("Parsing HS861 Total Electric Industry...")
    df = pd.read_excel(fpath, sheet_name="Total Electric Industry", header=2)
    # Columns: Year, STATE, then RESIDENTIAL group: Revenues, Sales, Customers, Price, ...
    # Get residential columns
    cols = list(df.columns)
    logger.info(f"HS861 columns: {cols}")
    # Rename to clear names
    df = df.rename(columns={
        cols[0]: 'Year', cols[1]: 'State',
        cols[2]: 'res_revenue',   # Thousand Dollars
        cols[3]: 'res_sales',     # Megawatthours
        cols[4]: 'res_customers', # Count
        cols[5]: 'res_price',     # Cents/kWh
    })
    df = df[['Year', 'State', 'res_revenue', 'res_customers', 'res_price', 'res_sales']].copy()
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    df = df.dropna(subset=['Year'])
    df['Year'] = df['Year'].astype(int)
    # Map state abbreviations
    df['state_abbr'] = df['State'].str.strip().str.upper()
    df['state_fips'] = df['state_abbr'].map(ABBR_TO_FIPS)
    df = df.dropna(subset=['state_fips'])
    # Compute monthly bill = Revenue($k) * 1000 / Customers / 12
    df['res_revenue'] = pd.to_numeric(df['res_revenue'], errors='coerce')
    df['res_customers'] = pd.to_numeric(df['res_customers'], errors='coerce')
    df['res_sales'] = pd.to_numeric(df['res_sales'], errors='coerce')
    df['electric_bill_monthly'] = df['res_revenue'] * 1000 / df['res_customers'] / 12
    # Also compute from price * consumption
    df['avg_monthly_kwh'] = df['res_sales'] / df['res_customers'] / 12
    df['bill_from_price'] = df['res_price'] * df['avg_monthly_kwh'] / 100
    logger.info(f"HS861 parsed: {df.shape}, years: {sorted(df['Year'].unique())}")
    return df

def parse_table5a(fpath, year, logger):
    """Parse table_5a for direct Average Monthly Bill by state."""
    df = pd.read_excel(fpath, header=2)
    df.columns = ['State', 'Customers', 'AvgMonthlyConsumption', 'AvgPrice', 'AvgMonthlyBill']
    # Filter to actual state rows (skip regions, totals)
    df = df[df['State'].apply(lambda x: isinstance(x, str) and len(x.strip()) > 1)].copy()
    df['state_abbr_raw'] = df['State'].str.strip().str.upper()
    # Map state names to abbreviations
    df['state_abbr'] = df['state_abbr_raw'].map(
        lambda x: STATE_NAME_TO_ABBR.get(x, ABBR_TO_FIPS.get(x))  # try name first, then abbr
    )
    # Also try matching abbreviations directly
    for idx, row in df.iterrows():
        if pd.isna(row['state_abbr']):
            raw = row['state_abbr_raw']
            if raw in ABBR_TO_FIPS:
                df.at[idx, 'state_abbr'] = raw
    df['state_fips'] = df['state_abbr'].map(lambda x: ABBR_TO_FIPS.get(x) if pd.notna(x) else None)
    df = df.dropna(subset=['state_fips'])
    df['year'] = year
    logger.info(f"table_5a {year}: {len(df)} states parsed")
    return df[['state_fips', 'state_abbr', 'year', 'AvgMonthlyBill', 'AvgPrice', 'AvgMonthlyConsumption']]

def main():
    root = get_project_root()
    logger = setup_logger("patch_01_electricity")
    logger.info("=== Patch 01: Update Electricity 2021-2023 ===")

    raw_dir = root / "data_raw" / "patch_electricity_2021_2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    clean_dir = root / "data_clean" / "utilities"

    # 1. Download sources
    hs861_path = download_hs861(raw_dir, logger)
    t5a_paths = download_table5a_snapshots(raw_dir, logger)

    # 2. Parse HS861 for all years
    hs861 = parse_hs861(hs861_path, logger)

    # 3. Parse table_5a snapshots for cross-validation
    t5a_frames = []
    for year, fpath in t5a_paths.items():
        try:
            t5a_df = parse_table5a(fpath, year, logger)
            t5a_frames.append(t5a_df)
        except Exception as e:
            logger.warning(f"table_5a {year} parse failed: {e}")

    # 4. Cross-validate HS861 vs table_5a for 2022-2024
    if t5a_frames:
        t5a_all = pd.concat(t5a_frames, ignore_index=True)
        for year in [2022, 2023, 2024]:
            t5a_yr = t5a_all[t5a_all['year'] == year]
            hs_yr = hs861[hs861['Year'] == year]
            if len(t5a_yr) > 0 and len(hs_yr) > 0:
                merged_cv = t5a_yr.merge(hs_yr[['state_fips', 'electric_bill_monthly']],
                                         on='state_fips', how='inner')
                if len(merged_cv) > 0:
                    corr = merged_cv['AvgMonthlyBill'].corr(merged_cv['electric_bill_monthly'])
                    diff = (merged_cv['AvgMonthlyBill'] - merged_cv['electric_bill_monthly']).abs()
                    logger.info(f"Cross-validation {year}: corr={corr:.4f}, "
                               f"mean_abs_diff=${diff.mean():.2f}, max_diff=${diff.max():.2f}")

    # 5. Build patch electricity data (2021-2023 from HS861)
    patch_years = [2021, 2022, 2023]
    patch_data = hs861[hs861['Year'].isin(patch_years)].copy()
    patch_data = patch_data.rename(columns={'Year': 'year'})
    patch_data['electric_bill_annual'] = patch_data['electric_bill_monthly'] * 12
    patch_data['construction_flag_electric'] = 'reconstructed_revenue_customers'
    patch_data['source_electric'] = 'EIA_HS861_2010'

    patch_out = patch_data[['state_fips', 'state_abbr', 'year',
                            'electric_bill_monthly', 'electric_bill_annual',
                            'construction_flag_electric', 'source_electric']].copy()
    patch_out = patch_out.sort_values(['state_fips', 'year']).reset_index(drop=True)

    # 6. Save patch file
    patch_out.to_csv(clean_dir / "electricity_patch_2021_2023.csv", index=False)
    logger.info(f"Patch electricity saved: {patch_out.shape}")
    logger.info(f"States: {patch_out['state_fips'].nunique()}, Years: {sorted(patch_out['year'].unique())}")

    # 7. Merge with existing electricity_clean.csv to create patched version
    existing = pd.read_csv(clean_dir / "electricity_clean.csv", dtype={"state_fips": str})
    logger.info(f"Existing electricity: {existing.shape}, years: {sorted(existing['year'].unique())}")

    # Add construction flag to existing data
    existing['construction_flag_electric'] = 'reconstructed_revenue_customers'
    existing['source_electric'] = 'EIA_revenue_customers_annual'

    # Append patch (only for years not in existing)
    existing_keys = set(zip(existing['state_fips'], existing['year']))
    new_rows = patch_out[~patch_out.apply(
        lambda r: (r['state_fips'], r['year']) in existing_keys, axis=1)]
    logger.info(f"New rows to append: {len(new_rows)}")

    patched = pd.concat([existing, new_rows], ignore_index=True)
    patched = patched.sort_values(['state_fips', 'year']).reset_index(drop=True)
    patched.to_csv(clean_dir / "electricity_clean_patched.csv", index=False)
    logger.info(f"Patched electricity: {patched.shape}, years: {sorted(patched['year'].unique())}")

    # 8. Save 2024 as future-ready (not in main sample)
    if 2024 in hs861['Year'].values:
        future = hs861[hs861['Year'] == 2024].copy()
        future = future.rename(columns={'Year': 'year'})
        future['electric_bill_annual'] = future['electric_bill_monthly'] * 12
        future_out = future[['state_fips', 'state_abbr', 'year',
                             'electric_bill_monthly', 'electric_bill_annual']].copy()
        future_out.to_csv(raw_dir / "electricity_2024_future_ready.csv", index=False)
        logger.info(f"2024 future-ready saved: {future_out.shape}")

    logger.info("=== Patch 01 COMPLETE ===")

if __name__ == "__main__":
    main()

