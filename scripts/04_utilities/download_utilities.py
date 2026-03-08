"""
Module 3: Utilities Data Download & Clean
- Electricity: Revenue / Customers / 12 = avg monthly bill (EIA)
- Natural Gas: Price per MCF from EIA (wide-format → long),
  converted to estimated monthly bill via avg residential consumption.
Source: EIA direct xlsx/xls downloads
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root, FIPS_STATES, coverage_audit
import pandas as pd
import numpy as np
import requests

ABBR_TO_FIPS = {v[0]: k for k, v in FIPS_STATES.items()}
# Reverse: state name → abbreviation (for gas data which uses full state names)
STATE_NAME_TO_ABBR = {v[1]: v[0] for k, v in FIPS_STATES.items()}

def download_file(url, save_path, logger):
    """Download a file and save to disk."""
    logger.info(f"Downloading {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    with open(save_path, "wb") as f:
        f.write(resp.content)
    logger.info(f"Saved to {save_path} ({len(resp.content)} bytes)")
    return save_path

def download_all(logger):
    """Download all EIA data files."""
    root = get_project_root()
    raw_dir = root / "data_raw" / "utilities"
    files = {}
    # Electricity: revenue and customers to compute avg monthly bill
    for fname in ["revenue_annual.xlsx", "customers_annual.xlsx"]:
        url = f"https://www.eia.gov/electricity/data/state/{fname}"
        files[fname] = download_file(url, raw_dir / fname, logger)
    # Natural gas residential price
    url = "https://www.eia.gov/dnav/ng/xls/NG_PRI_SUM_A_EPG0_PRS_DMCF_A.xls"
    files["gas_price"] = download_file(url, raw_dir / "eia_natgas_residential_price.xls", logger)
    return files

def clean_electricity(files, logger):
    """Compute avg monthly electric bill = Revenue($k) * 1000 / Customers / 12."""
    logger.info("Cleaning electricity data...")
    # Read revenue (header row 1, row 0 is title)
    df_rev = pd.read_excel(files["revenue_annual.xlsx"], header=1)
    df_cust = pd.read_excel(files["customers_annual.xlsx"], header=1)
    logger.info(f"Revenue columns: {list(df_rev.columns)}")
    logger.info(f"Revenue shape: {df_rev.shape}")

    # Both files: Year, State, Industry Sector Category, Residential, ...
    # Filter to "Total Electric Industry" sector
    for df in [df_rev, df_cust]:
        df.columns = [c.strip() for c in df.columns]

    rev = df_rev[df_rev['Industry Sector Category'] == 'Total Electric Industry'][
        ['Year', 'State', 'Residential']].copy()
    rev.rename(columns={'Residential': 'revenue_residential'}, inplace=True)

    cust = df_cust[df_cust['Industry Sector Category'] == 'Total Electric Industry'][
        ['Year', 'State', 'Residential']].copy()
    cust.rename(columns={'Residential': 'customers_residential'}, inplace=True)

    merged = rev.merge(cust, on=['Year', 'State'], how='inner')
    merged['revenue_residential'] = pd.to_numeric(merged['revenue_residential'], errors='coerce')
    merged['customers_residential'] = pd.to_numeric(merged['customers_residential'], errors='coerce')

    # Revenue is in thousands of dollars
    merged['electric_bill_monthly'] = (
        merged['revenue_residential'] * 1000 / merged['customers_residential'] / 12
    )
    merged['year'] = merged['Year'].astype(int)
    merged['state_abbr'] = merged['State'].str.strip().str.upper()
    merged['state_fips'] = merged['state_abbr'].map(ABBR_TO_FIPS)

    # Filter
    merged = merged[(merged['year'] >= 2008) & (merged['year'] <= 2023)]
    merged = merged.dropna(subset=['state_fips', 'electric_bill_monthly'])
    merged['electric_bill_annual'] = merged['electric_bill_monthly'] * 12

    result = merged[['state_fips', 'state_abbr', 'year',
                     'electric_bill_monthly', 'electric_bill_annual']].copy()
    result = result.sort_values(['state_fips', 'year']).reset_index(drop=True)
    logger.info(f"Electricity clean: {result.shape}, states={result['state_fips'].nunique()}")
    return result

def clean_gas(files, logger):
    """Parse EIA natural gas price data (wide → long).
    Data is $/MCF. We estimate monthly bill using national avg residential consumption.
    National avg residential gas consumption ≈ 60 MCF/year (EIA RECS).
    Monthly bill ≈ price_per_MCF * 60 / 12 = price_per_MCF * 5
    """
    logger.info("Cleaning natural gas data...")
    df = pd.read_excel(files["gas_price"], sheet_name="Data 1", header=2)
    logger.info(f"Gas shape: {df.shape}, columns: {len(df.columns)}")

    # Extract year from Date column
    df['year'] = pd.to_datetime(df['Date']).dt.year

    # Melt wide → long (each column is a state)
    state_cols = [c for c in df.columns if c not in ['Date', 'year']
                  and 'U.S.' not in c]

    records = []
    for col in state_cols:
        # Extract state name from column like "Alabama Price of Natural Gas..."
        state_name = col.split(' Price of')[0].strip()
        abbr = STATE_NAME_TO_ABBR.get(state_name)
        if abbr is None:
            # Try matching
            for sn, sa in STATE_NAME_TO_ABBR.items():
                if state_name.lower() in sn.lower() or sn.lower() in state_name.lower():
                    abbr = sa
                    break
        if abbr is None:
            continue
        fips = ABBR_TO_FIPS.get(abbr)
        if fips is None:
            continue
        for _, row in df.iterrows():
            val = pd.to_numeric(row[col], errors='coerce')
            if pd.notna(val) and 2008 <= row['year'] <= 2023:
                records.append({
                    'state_fips': fips,
                    'state_abbr': abbr,
                    'year': int(row['year']),
                    'gas_price_per_mcf': val,
                    # Estimate monthly bill: avg 60 MCF/yr → 5 MCF/month
                    'gas_bill_monthly': val * 5,
                    'gas_bill_annual': val * 60,
                    'construction_flag_gas': 'estimated_from_price',
                })

    if not records:
        logger.warning("No gas records extracted")
        return pd.DataFrame()

    result = pd.DataFrame(records)
    result = result.sort_values(['state_fips', 'year']).reset_index(drop=True)
    logger.info(f"Gas clean: {result.shape}, states={result['state_fips'].nunique()}")
    return result

def main():
    root = get_project_root()
    logger = setup_logger("utilities")
    logger.info("=== Module 3: Utilities ===")
    clean_dir = root / "data_clean" / "utilities"

    files = download_all(logger)
    elec = clean_electricity(files, logger)
    gas = clean_gas(files, logger)

    if not elec.empty:
        elec.to_csv(clean_dir / "electricity_clean.csv", index=False)
    if not gas.empty:
        gas.to_csv(clean_dir / "gas_clean.csv", index=False)

    # Merge
    if not elec.empty and not gas.empty:
        merged = elec.merge(gas, on=['state_fips', 'state_abbr', 'year'], how='outer')
    elif not elec.empty:
        merged = elec
        merged['gas_bill_monthly'] = np.nan
        merged['gas_bill_annual'] = np.nan
        merged['construction_flag_gas'] = 'missing'
    else:
        merged = gas

    if not merged.empty:
        merged.to_csv(clean_dir / "utilities_clean.csv", index=False)
        report = coverage_audit(merged, "utilities")
        logger.info(f"Coverage: {report}")

    logger.info("=== Module 3 COMPLETE ===")

if __name__ == "__main__":
    main()

