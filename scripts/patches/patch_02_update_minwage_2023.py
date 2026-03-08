"""
Patch 02: Add 2023 Minimum Wage Data
Strategy:
  1. Try Vaghul & Zipperer v1.5.0 (may have 2023)
  2. Fallback: Use DOL/EPI 2023 state minimum wage rates (manually compiled)
     from publicly available tables
  3. Apply same logic: binding_mw = max(state_mw, federal_mw)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root, FIPS_STATES
import pandas as pd
import requests
import io
import zipfile

ABBR_TO_FIPS = {v[0]: k for k, v in FIPS_STATES.items()}
FEDERAL_MW_2023 = 7.25

# 2023 state minimum wages (effective Jan 1 2023 or later in 2023)
# Source: DOL Wage and Hour Division, EPI compilations
# Using the highest rate effective in 2023 for each state (consistent with V&Z "Annual State Maximum")
MW_2023 = {
    "AL": 7.25, "AK": 10.85, "AZ": 13.85, "AR": 11.00, "CA": 15.50,
    "CO": 13.65, "CT": 15.00, "DE": 11.75, "DC": 17.00, "FL": 12.00,
    "GA": 7.25, "HI": 14.00, "ID": 7.25, "IL": 13.00, "IN": 7.25,
    "IA": 7.25, "KS": 7.25, "KY": 7.25, "LA": 7.25, "ME": 13.80,
    "MD": 13.25, "MA": 15.00, "MI": 10.10, "MN": 10.59, "MS": 7.25,
    "MO": 12.00, "MT": 9.95, "NE": 10.50, "NV": 10.50, "NH": 7.25,
    "NJ": 14.13, "NM": 12.00, "NY": 14.20, "NC": 7.25, "ND": 7.25,
    "OH": 10.10, "OK": 7.25, "OR": 13.50, "PA": 7.25, "RI": 13.00,
    "SC": 7.25, "SD": 10.80, "TN": 7.25, "TX": 7.25, "UT": 7.25,
    "VT": 13.18, "VA": 12.00, "WA": 15.74, "WV": 8.75, "WI": 7.25,
    "WY": 7.25,
}

def try_vz_v150(logger):
    """Try downloading Vaghul & Zipperer v1.5.0 which may include 2023."""
    url = "https://github.com/benzipperer/historicalminwage/releases/download/v1.5.0/mw_state_excel.zip"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            logger.info(f"V&Z v1.5.0 found! ({len(resp.content)} bytes)")
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                xlsx_files = [f for f in zf.namelist() if f.endswith('.xlsx')]
                for f in xlsx_files:
                    if 'annual' in f.lower():
                        with zf.open(f) as xlf:
                            df = pd.read_excel(xlf, engine='openpyxl')
                        max_year = df['Year'].max() if 'Year' in df.columns else None
                        logger.info(f"V&Z v1.5.0 max year: {max_year}")
                        if max_year and max_year >= 2023:
                            return df
            logger.info("V&Z v1.5.0 doesn't have 2023")
        else:
            logger.info(f"V&Z v1.5.0 not available (status {resp.status_code})")
    except Exception as e:
        logger.info(f"V&Z v1.5.0 check failed: {e}")
    return None

def build_2023_from_manual(logger):
    """Build 2023 MW data from manually compiled state rates."""
    records = []
    for abbr, mw in MW_2023.items():
        fips = ABBR_TO_FIPS.get(abbr)
        if fips is None:
            continue
        binding = max(mw, FEDERAL_MW_2023)
        records.append({
            'state_fips': fips,
            'state_abbr': abbr,
            'year': 2023,
            'min_wage_nominal': mw,
            'min_wage_annual_avg': mw,  # Simplified: use max as avg for 2023
            'federal_min_wage': FEDERAL_MW_2023,
            'binding_min_wage': binding,
            'annualized_mw_income': binding * 2080,
        })
    df = pd.DataFrame(records)
    logger.info(f"Manual 2023 MW data: {len(df)} states")
    return df

def main():
    root = get_project_root()
    logger = setup_logger("patch_02_minwage")
    logger.info("=== Patch 02: Update Minimum Wage 2023 ===")

    clean_dir = root / "data_clean" / "min_wage"

    # 1. Try V&Z v1.5.0
    vz_df = try_vz_v150(logger)
    if vz_df is not None:
        logger.info("Using V&Z v1.5.0 for 2023 data")
        # Process same as original pipeline
        col_map = {
            'State FIPS Code': 'state_fips', 'State Abbreviation': 'state_abbr',
            'Year': 'year', 'Annual State Maximum': 'min_wage_nominal',
            'Annual State Average': 'min_wage_annual_avg',
            'Annual Federal Maximum': 'federal_min_wage',
        }
        vz_2023 = vz_df[vz_df['Year'] == 2023].rename(columns=col_map)
        vz_2023['state_fips'] = vz_2023['state_fips'].astype(int).astype(str).str.zfill(2)
        vz_2023['binding_min_wage'] = vz_2023[['min_wage_nominal', 'federal_min_wage']].max(axis=1)
        vz_2023['annualized_mw_income'] = vz_2023['binding_min_wage'] * 2080
        mw_2023 = vz_2023[['state_fips', 'state_abbr', 'year', 'min_wage_nominal',
                            'min_wage_annual_avg', 'federal_min_wage', 'binding_min_wage',
                            'annualized_mw_income']].copy()
    else:
        logger.info("Using manually compiled 2023 MW rates (DOL/EPI)")
        mw_2023 = build_2023_from_manual(logger)

    # 2. Load existing and append
    existing = pd.read_csv(clean_dir / "mw_clean.csv", dtype={"state_fips": str})
    logger.info(f"Existing MW: {existing.shape}, years: {sorted(existing['year'].unique())}")

    # Remove any existing 2023 rows
    existing = existing[existing['year'] != 2023]

    patched = pd.concat([existing, mw_2023], ignore_index=True)
    patched = patched.sort_values(['state_fips', 'year']).reset_index(drop=True)

    patched.to_csv(clean_dir / "mw_clean_patched.csv", index=False)
    logger.info(f"Patched MW: {patched.shape}, years: {sorted(patched['year'].unique())}")
    logger.info(f"States in 2023: {patched[patched['year']==2023]['state_fips'].nunique()}")

    # Sanity check
    sample = patched[patched['year'] == 2023].nlargest(5, 'binding_min_wage')
    logger.info(f"Top 5 MW states 2023:\n{sample[['state_abbr', 'binding_min_wage', 'annualized_mw_income']].to_string()}")

    logger.info("=== Patch 02 COMPLETE ===")

if __name__ == "__main__":
    main()

