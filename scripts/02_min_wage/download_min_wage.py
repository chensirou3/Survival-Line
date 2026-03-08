"""
Module 1: Minimum Wage Data Download & Clean
Primary: Vaghul & Zipperer (benzipperer/historicalminwage on GitHub)
  - Data comes from GitHub Releases v1.4.0 as Excel zip
Supplement: Manual federal MW table for binding_min_wage calculation
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root, FIPS_STATES, coverage_audit
import pandas as pd
import requests
import io
import zipfile
import tempfile
from pathlib import Path

# Federal minimum wage history (effective date -> rate)
FEDERAL_MW = {
    1997: 5.15, 1998: 5.15, 1999: 5.15, 2000: 5.15, 2001: 5.15,
    2002: 5.15, 2003: 5.15, 2004: 5.15, 2005: 5.15, 2006: 5.15,
    2007: 5.85, 2008: 6.55, 2009: 7.25, 2010: 7.25, 2011: 7.25,
    2012: 7.25, 2013: 7.25, 2014: 7.25, 2015: 7.25, 2016: 7.25,
    2017: 7.25, 2018: 7.25, 2019: 7.25, 2020: 7.25, 2021: 7.25,
    2022: 7.25, 2023: 7.25,
}

def download_vaghul_zipperer(logger):
    """Download state minimum wage data from GitHub Releases (Excel zip)."""
    url = "https://github.com/benzipperer/historicalminwage/releases/download/v1.4.0/mw_state_excel.zip"
    logger.info(f"Downloading from {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    logger.info(f"Downloaded {len(resp.content)} bytes")

    # Extract xlsx files from zip
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        file_list = zf.namelist()
        logger.info(f"Files in zip: {file_list}")

        # Look for annual state-level file first
        annual_file = None
        for f in file_list:
            fl = f.lower()
            if 'annual' in fl and 'state' in fl and fl.endswith('.xlsx'):
                annual_file = f
                break
        # Fallback: any xlsx with 'annual' or 'year'
        if annual_file is None:
            for f in file_list:
                fl = f.lower()
                if ('annual' in fl or 'year' in fl) and fl.endswith('.xlsx'):
                    annual_file = f
                    break
        # Fallback: first xlsx
        if annual_file is None:
            xlsx_files = [f for f in file_list if f.endswith('.xlsx')]
            if xlsx_files:
                annual_file = xlsx_files[0]
                logger.warning(f"No annual file found, using first xlsx: {annual_file}")

        if annual_file is None:
            raise FileNotFoundError(f"No xlsx files found in zip. Contents: {file_list}")

        logger.info(f"Reading: {annual_file}")
        with zf.open(annual_file) as xlf:
            df = pd.read_excel(xlf, engine='openpyxl')

    logger.info(f"Downloaded {len(df)} rows, columns: {list(df.columns)}")
    return df

def clean_min_wage(df_raw, logger):
    """Clean and standardize minimum wage data.

    Expected columns from mw_state_annual.xlsx:
    - State FIPS Code, Name, State Abbreviation, Year
    - Annual Federal Minimum/Average/Maximum
    - Annual State Minimum/Average/Maximum

    We use 'Annual State Maximum' as the year-end state MW (highest rate in that year).
    """
    logger.info("Cleaning minimum wage data...")
    logger.info(f"Raw columns: {list(df_raw.columns)}")
    logger.info(f"Raw shape: {df_raw.shape}, sample:\n{df_raw.head(3)}")

    # Standardize column names
    col_map = {
        'State FIPS Code': 'state_fips',
        'Name': 'state_name',
        'State Abbreviation': 'state_abbr',
        'Year': 'year',
        'Annual State Maximum': 'state_mw_max',
        'Annual State Average': 'state_mw_avg',
        'Annual State Minimum': 'state_mw_min',
        'Annual Federal Maximum': 'fed_mw_max',
        'Annual Federal Average': 'fed_mw_avg',
        'Annual Federal Minimum': 'fed_mw_min',
    }
    df = df_raw.rename(columns=col_map)

    # Ensure state_fips is zero-padded 2-digit string
    df['state_fips'] = df['state_fips'].astype(int).astype(str).str.zfill(2)

    # Use state MW max as the year-end nominal rate (highest rate effective that year)
    df['min_wage_nominal'] = df['state_mw_max']
    df['min_wage_annual_avg'] = df['state_mw_avg']

    # Filter to 2008-2023
    df = df[(df['year'] >= 2008) & (df['year'] <= 2023)].copy()
    logger.info(f"After year filter: {len(df)} rows")

    # Keep only states in our FIPS lookup (50 states + DC)
    valid_fips = set(FIPS_STATES.keys())
    df = df[df['state_fips'].isin(valid_fips)].copy()
    logger.info(f"After FIPS filter: {len(df)} rows, {df['state_fips'].nunique()} states")

    # Add federal MW and compute binding MW
    df['federal_min_wage'] = df['year'].map(FEDERAL_MW)
    df['binding_min_wage'] = df[['min_wage_nominal', 'federal_min_wage']].max(axis=1)
    df['annualized_mw_income'] = df['binding_min_wage'] * 2080

    # Select output columns
    result = df[['state_fips', 'state_abbr', 'year', 'min_wage_nominal',
                 'min_wage_annual_avg', 'federal_min_wage', 'binding_min_wage',
                 'annualized_mw_income']].copy()
    result = result.sort_values(['state_fips', 'year']).reset_index(drop=True)

    cols_out = ['state_fips', 'state_abbr', 'year', 'min_wage_nominal',
                'min_wage_annual_avg', 'federal_min_wage', 'binding_min_wage',
                'annualized_mw_income']
    result = result[cols_out].sort_values(['state_fips', 'year']).reset_index(drop=True)
    logger.info(f"Cleaned MW data: {result.shape}")
    return result

def main():
    root = get_project_root()
    logger = setup_logger("min_wage")
    logger.info("=== Module 1: Minimum Wage ===")

    # Download
    raw_dir = root / "data_raw" / "min_wage"
    clean_dir = root / "data_clean" / "min_wage"

    try:
        df_raw = download_vaghul_zipperer(logger)
        df_raw.to_csv(raw_dir / "mw_raw_vaghul_zipperer.csv", index=False)
        logger.info("Raw data saved.")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise

    # Clean
    df_clean = clean_min_wage(df_raw, logger)
    df_clean.to_csv(clean_dir / "mw_clean.csv", index=False)

    # Coverage audit
    report = coverage_audit(df_clean, "min_wage")
    logger.info(f"Coverage: {report}")
    logger.info("=== Module 1 COMPLETE ===")

if __name__ == "__main__":
    main()

