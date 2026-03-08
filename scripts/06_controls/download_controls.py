"""
Module 5: Control Variables Download & Clean
- BEA RPP All Items, Rents, Utilities
- ACS poverty rate (S1701), median income (B19013)
- BLS CPI-U annual average
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root, FIPS_STATES, coverage_audit
import pandas as pd
import requests
import time

CENSUS_API = "https://api.census.gov/data/{year}/acs/acs1"
ABBR_TO_FIPS = {v[0]: k for k, v in FIPS_STATES.items()}

def fetch_acs_subject(year, table_var, logger):
    """Fetch ACS subject table variable for all states."""
    # For subject tables (S-tables), use /subject endpoint
    if table_var.startswith("S"):
        url = f"https://api.census.gov/data/{year}/acs/acs1/subject"
    else:
        url = CENSUS_API.format(year=year)
    params = {"get": f"NAME,{table_var}", "for": "state:*"}
    try:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data[1:], columns=data[0])
        df["year"] = year
        return df
    except Exception as e:
        logger.warning(f"  Failed {table_var} {year}: {e}")
        return None

def download_acs_controls(logger):
    """Download poverty rate and median income from ACS."""
    poverty_dfs = []
    income_dfs = []

    for year in range(2008, 2024):
        if year == 2020:
            logger.warning(f"  Skipping {year} (ACS 1-year not released)")
            continue
        # Poverty rate - S1701_C03_001E = percent below poverty
        df_p = fetch_acs_subject(year, "S1701_C03_001E", logger)
        if df_p is not None:
            poverty_dfs.append(df_p)
        # Median household income - B19013_001E
        df_i = fetch_acs_subject(year, "B19013_001E", logger)
        if df_i is not None:
            income_dfs.append(df_i)
        time.sleep(0.5)

    poverty = pd.concat(poverty_dfs, ignore_index=True) if poverty_dfs else pd.DataFrame()
    income = pd.concat(income_dfs, ignore_index=True) if income_dfs else pd.DataFrame()
    return poverty, income

def download_rpp_all_items(logger):
    """RPP All Items and Rents are in the same SARPP zip already downloaded by food module."""
    root = get_project_root()
    # Try to read from already-downloaded RPP data
    food_raw = root / "data_raw" / "food"
    csv_files = list(food_raw.glob("SARPP*.csv"))
    if not csv_files:
        logger.warning("RPP data not found. Run Module 4 (food) first.")
        return pd.DataFrame()

    df = pd.read_csv(csv_files[0], encoding='latin-1')
    logger.info(f"Read RPP data: {df.shape}, LineCode values: {sorted(df['LineCode'].dropna().unique())}")
    return df

def clean_rpp_controls(df_rpp, logger):
    """Extract RPP All Items (LineCode=1), Rents (3), Services (4) from RPP data."""
    if df_rpp.empty:
        return pd.DataFrame()

    cols = list(df_rpp.columns)
    lc_col = [c for c in cols if 'linecode' in c.lower().replace(' ', '')]
    lc_col = lc_col[0] if lc_col else [c for c in cols if 'line' in c.lower()][0]
    geo_col = [c for c in cols if 'geofips' in c.lower().replace(' ', '')]
    geo_col = geo_col[0] if geo_col else cols[0]

    df_rpp[lc_col] = pd.to_numeric(df_rpp[lc_col], errors='coerce')
    valid_fips = set(FIPS_STATES.keys())

    results = []
    for lc, name in [(1, 'rpp_all'), (3, 'rpp_rents')]:
        sub = df_rpp[df_rpp[lc_col] == lc].copy()
        sub[geo_col] = sub[geo_col].astype(str).str.strip().str.strip('"')
        sub['state_fips'] = sub[geo_col].str[:2]
        sub = sub[sub['state_fips'].isin(valid_fips)]

        year_cols = [c for c in cols if str(c).strip().replace('"','').isdigit()
                     and 2000 <= int(str(c).strip().replace('"','')) <= 2030]
        melted = sub[['state_fips'] + year_cols].melt(
            id_vars='state_fips', var_name='year_str', value_name=name
        )
        melted['year'] = pd.to_numeric(melted['year_str'].str.strip().str.replace('"',''), errors='coerce').astype(int)
        melted[name] = pd.to_numeric(melted[name], errors='coerce')
        melted = melted[(melted['year'] >= 2008) & (melted['year'] <= 2023)]
        results.append(melted[['state_fips', 'year', name]])

    merged = results[0]
    for r in results[1:]:
        merged = merged.merge(r, on=['state_fips', 'year'], how='outer')
    return merged.sort_values(['state_fips', 'year']).reset_index(drop=True)

def clean_acs_controls(poverty, income, logger):
    """Clean ACS poverty and income data."""
    valid_fips = set(FIPS_STATES.keys())
    dfs = []
    for df, var, col_name in [(poverty, 'S1701_C03_001E', 'poverty_rate'),
                               (income, 'B19013_001E', 'median_hh_income')]:
        if df.empty:
            continue
        d = df.copy()
        d['state_fips'] = d['state'].str.zfill(2)
        d = d[d['state_fips'].isin(valid_fips)]
        d[col_name] = pd.to_numeric(d[var], errors='coerce')
        d['year'] = d['year'].astype(int)
        dfs.append(d[['state_fips', 'year', col_name]])

    if not dfs:
        return pd.DataFrame()
    result = dfs[0]
    for d in dfs[1:]:
        result = result.merge(d, on=['state_fips', 'year'], how='outer')
    return result.sort_values(['state_fips', 'year']).reset_index(drop=True)

def main():
    root = get_project_root()
    logger = setup_logger("controls")
    logger.info("=== Module 5: Controls ===")
    clean_dir = root / "data_clean" / "controls"

    poverty, income = download_acs_controls(logger)
    if not poverty.empty:
        poverty.to_csv(root / "data_raw" / "controls" / "poverty_raw.csv", index=False)
    if not income.empty:
        income.to_csv(root / "data_raw" / "controls" / "income_raw.csv", index=False)

    acs_clean = clean_acs_controls(poverty, income, logger)
    df_rpp = download_rpp_all_items(logger)
    rpp_clean = clean_rpp_controls(df_rpp, logger)

    if not acs_clean.empty and not rpp_clean.empty:
        controls = acs_clean.merge(rpp_clean, on=['state_fips', 'year'], how='outer')
    elif not acs_clean.empty:
        controls = acs_clean
    else:
        controls = rpp_clean

    if not controls.empty:
        controls.to_csv(clean_dir / "controls_clean.csv", index=False)
        report = coverage_audit(controls, "controls")
        logger.info(f"Coverage: {report}")

    logger.info("=== Module 5 COMPLETE ===")

if __name__ == "__main__":
    main()

