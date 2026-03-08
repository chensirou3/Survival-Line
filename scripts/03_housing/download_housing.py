"""
Module 2: Housing Data Download & Clean
Primary: ACS 1-Year B25058 (Median Contract Rent) + B25064 (Median Gross Rent)
Source: Census Bureau API
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root, FIPS_STATES, coverage_audit
import pandas as pd
import requests
import time

CENSUS_API_BASE = "https://api.census.gov/data/{year}/acs/acs1"

def fetch_acs_variable(year, variable, logger):
    """Fetch a single ACS variable for all states."""
    url = CENSUS_API_BASE.format(year=year)
    params = {
        "get": f"NAME,{variable}",
        "for": "state:*",
    }
    logger.info(f"  Fetching {variable} for {year}...")
    try:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        header = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=header)
        df["year"] = year
        return df
    except Exception as e:
        logger.warning(f"  Failed {variable} {year}: {e}")
        return None

def download_housing(logger):
    """Download Contract Rent (B25058) and Gross Rent (B25064) for 2008-2023."""
    all_contract = []
    all_gross = []

    for year in range(2008, 2024):
        # Skip 2020 - ACS 1-year was not released due to COVID
        if year == 2020:
            logger.warning(f"  Skipping {year} — ACS 1-year not released (COVID)")
            continue

        df_c = fetch_acs_variable(year, "B25058_001E", logger)
        if df_c is not None:
            all_contract.append(df_c)

        df_g = fetch_acs_variable(year, "B25064_001E", logger)
        if df_g is not None:
            all_gross.append(df_g)

        time.sleep(0.5)  # Rate limiting

    contract = pd.concat(all_contract, ignore_index=True) if all_contract else pd.DataFrame()
    gross = pd.concat(all_gross, ignore_index=True) if all_gross else pd.DataFrame()
    return contract, gross

def clean_housing(df_contract, df_gross, logger):
    """Clean and standardize housing data."""
    logger.info("Cleaning housing data...")

    def process_rent(df, var_name, col_name):
        if df.empty:
            return pd.DataFrame()
        df = df.copy()
        df["state_fips"] = df["state"].str.zfill(2)
        df[col_name] = pd.to_numeric(df[var_name], errors="coerce")
        # Filter to 50 states + DC
        valid_fips = set(FIPS_STATES.keys())
        df = df[df["state_fips"].isin(valid_fips)]
        return df[["state_fips", "year", col_name]].copy()

    dc = process_rent(df_contract, "B25058_001E", "contract_rent_monthly")
    dg = process_rent(df_gross, "B25064_001E", "gross_rent_monthly")

    if dc.empty and dg.empty:
        logger.error("No housing data obtained")
        return pd.DataFrame()

    if not dc.empty and not dg.empty:
        merged = dc.merge(dg, on=["state_fips", "year"], how="outer")
    elif not dc.empty:
        merged = dc
    else:
        merged = dg

    # Annualize
    if "contract_rent_monthly" in merged.columns:
        merged["contract_rent_annual"] = merged["contract_rent_monthly"] * 12
    if "gross_rent_monthly" in merged.columns:
        merged["gross_rent_annual"] = merged["gross_rent_monthly"] * 12

    merged["year"] = merged["year"].astype(int)
    merged = merged.sort_values(["state_fips", "year"]).reset_index(drop=True)

    # Add state_abbr
    fips_to_abbr = {k: v[0] for k, v in FIPS_STATES.items()}
    merged["state_abbr"] = merged["state_fips"].map(fips_to_abbr)

    logger.info(f"Cleaned housing data: {merged.shape}")
    return merged

def main():
    root = get_project_root()
    logger = setup_logger("housing")
    logger.info("=== Module 2: Housing ===")

    raw_dir = root / "data_raw" / "housing"
    clean_dir = root / "data_clean" / "housing"

    # Download
    df_contract, df_gross = download_housing(logger)

    if not df_contract.empty:
        df_contract.to_csv(raw_dir / "housing_contract_raw.csv", index=False)
    if not df_gross.empty:
        df_gross.to_csv(raw_dir / "housing_gross_raw.csv", index=False)
    logger.info("Raw housing data saved.")

    # Clean
    df_clean = clean_housing(df_contract, df_gross, logger)
    df_clean.to_csv(clean_dir / "housing_clean.csv", index=False)

    # Coverage
    report = coverage_audit(df_clean, "housing")
    logger.info(f"Coverage: {report}")
    logger.info("=== Module 2 COMPLETE ===")

if __name__ == "__main__":
    main()

