"""
Module 4: Food Cost Reconstruction
Food_{s,t} = TFP_national_t * (RPP_goods_{s,t} / 100)
Sources:
  - USDA Thrifty Food Plan monthly cost reports (national)
  - BEA Regional Price Parities (Goods component, state-level)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '00_setup'))
from utils import setup_logger, get_project_root, FIPS_STATES, coverage_audit
import pandas as pd
import requests
import io

# ---- TFP National Annual Values ----
# Source: USDA FNS Cost of Food Reports
# Reference family: family of 4 (2 adults 19-50, 2 children 6-8 and 9-11)
# Values are monthly cost in dollars for Thrifty Food Plan
# These must be manually compiled from USDA reports
TFP_MONTHLY = {
    2008: 512.60, 2009: 530.30, 2010: 535.60, 2011: 556.80,
    2012: 567.60, 2013: 570.60, 2014: 567.80, 2015: 560.40,
    2016: 556.00, 2017: 563.80, 2018: 570.00, 2019: 577.00,
    2020: 602.00, 2021: 835.57, 2022: 939.40, 2023: 975.50,
}
# Note: 2021+ values reflect the October 2021 TFP re-evaluation
# These are approximate annual averages; exact values should be verified

BEA_RPP_URL = "https://apps.bea.gov/regional/zip/SARPP.zip"

def download_rpp(logger):
    """Download BEA Regional Price Parities data."""
    logger.info(f"Downloading BEA RPP data from {BEA_RPP_URL}")
    try:
        resp = requests.get(BEA_RPP_URL, timeout=120)
        resp.raise_for_status()
        root = get_project_root()
        zip_path = root / "data_raw" / "food" / "SARPP.zip"
        with open(zip_path, "wb") as f:
            f.write(resp.content)
        logger.info(f"Saved RPP zip to {zip_path}")

        import zipfile
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(root / "data_raw" / "food")
            logger.info(f"Extracted files: {z.namelist()}")

        # Read the SARPP CSV (Regional Price Parities)
        csv_files = [f for f in z.namelist() if f.startswith('SARPP') and f.endswith('.csv')]
        if csv_files:
            df = pd.read_csv(root / "data_raw" / "food" / csv_files[0], encoding='latin-1')
            logger.info(f"RPP data shape: {df.shape}, columns: {list(df.columns)[:10]}")
            return df
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"RPP download failed: {e}")
        return pd.DataFrame()

def clean_rpp_goods(df_rpp, logger):
    """Extract RPP Goods index (LineCode=2) by state and year."""
    logger.info("Cleaning RPP data for Goods component...")
    if df_rpp.empty:
        return pd.DataFrame()

    logger.info(f"RPP columns: {list(df_rpp.columns)}")
    logger.info(f"RPP sample:\n{df_rpp.head()}")

    # BEA format: GeoFIPS, GeoName, LineCode, Description, year columns
    # LineCode 1 = All items, 2 = Goods, 3 = Rents, etc.
    cols = list(df_rpp.columns)
    # Find LineCode column
    lc_col = [c for c in cols if 'line' in c.lower() and 'code' in c.lower()]
    if not lc_col:
        lc_col = [c for c in cols if 'linecode' in c.lower().replace(' ', '')]
    lc_col = lc_col[0] if lc_col else None

    geo_col = [c for c in cols if 'geofips' in c.lower().replace(' ', '')][0] if \
        [c for c in cols if 'geofips' in c.lower().replace(' ', '')] else cols[0]

    if lc_col is None:
        logger.error("Cannot find LineCode column")
        return pd.DataFrame()

    df_rpp[lc_col] = pd.to_numeric(df_rpp[lc_col], errors='coerce')
    # Filter to Goods (LineCode=2)
    df_goods = df_rpp[df_rpp[lc_col] == 2].copy()
    logger.info(f"Goods rows: {len(df_goods)}")

    # Clean GeoFIPS — state FIPS are 5-digit with trailing zeros (e.g., "01000")
    df_goods[geo_col] = df_goods[geo_col].astype(str).str.strip().str.strip('"')
    df_goods['state_fips'] = df_goods[geo_col].str[:2]
    valid_fips = set(FIPS_STATES.keys())
    df_goods = df_goods[df_goods['state_fips'].isin(valid_fips)]

    # Melt year columns
    year_cols = [c for c in cols if c.strip().isdigit() and 2000 <= int(c.strip()) <= 2030]
    if not year_cols:
        # Try columns that look like years
        year_cols = [c for c in cols if str(c).strip().replace('"', '').isdigit()]
        year_cols = [c for c in year_cols if 2000 <= int(str(c).strip().replace('"', '')) <= 2030]

    logger.info(f"Year columns found: {year_cols[:5]}...")

    id_vars = ['state_fips']
    melted = df_goods[['state_fips'] + year_cols].melt(
        id_vars='state_fips', var_name='year_str', value_name='rpp_goods_index'
    )
    melted['year'] = pd.to_numeric(melted['year_str'].astype(str).str.strip().str.replace('"', ''), errors='coerce')
    melted['rpp_goods_index'] = pd.to_numeric(melted['rpp_goods_index'], errors='coerce')
    melted = melted[(melted['year'] >= 2008) & (melted['year'] <= 2023)]
    melted = melted.dropna(subset=['rpp_goods_index'])
    melted['year'] = melted['year'].astype(int)

    return melted[['state_fips', 'year', 'rpp_goods_index']].sort_values(
        ['state_fips', 'year']).reset_index(drop=True)

def construct_food(df_rpp_goods, logger):
    """Construct Food_{s,t} = TFP_national_annual * (RPP_goods / 100)."""
    logger.info("Constructing food cost...")
    tfp_df = pd.DataFrame([
        {"year": y, "tfp_national_monthly": v, "tfp_national_annual": v * 12}
        for y, v in TFP_MONTHLY.items()
    ])

    merged = df_rpp_goods.merge(tfp_df, on="year", how="left")
    merged["food_reconstructed_annual"] = merged["tfp_national_annual"] * (merged["rpp_goods_index"] / 100)
    merged["construction_flag_food"] = "reconstructed"

    # AK/HI flag
    merged.loc[merged["state_fips"].isin(["02", "15"]), "construction_flag_food"] = "reconstructed_AK_HI"

    fips_to_abbr = {k: v[0] for k, v in FIPS_STATES.items()}
    merged["state_abbr"] = merged["state_fips"].map(fips_to_abbr)

    cols = ['state_fips', 'state_abbr', 'year', 'tfp_national_monthly',
            'tfp_national_annual', 'rpp_goods_index', 'food_reconstructed_annual',
            'construction_flag_food']
    logger.info(f"Food data: {merged.shape}")
    return merged[cols].sort_values(['state_fips', 'year']).reset_index(drop=True)

def main():
    root = get_project_root()
    logger = setup_logger("food")
    logger.info("=== Module 4: Food ===")

    clean_dir = root / "data_clean" / "food"

    df_rpp = download_rpp(logger)
    rpp_goods = clean_rpp_goods(df_rpp, logger)

    if rpp_goods.empty:
        logger.error("No RPP Goods data — cannot construct food cost")
        return

    food = construct_food(rpp_goods, logger)
    food.to_csv(clean_dir / "food_clean.csv", index=False)

    report = coverage_audit(food, "food")
    logger.info(f"Coverage: {report}")
    logger.info("=== Module 4 COMPLETE ===")

if __name__ == "__main__":
    main()

