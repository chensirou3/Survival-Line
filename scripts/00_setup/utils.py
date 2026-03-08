"""
utils.py — 公共工具函数
"""
import os, sys, logging, yaml, datetime
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

def get_project_root():
    return PROJECT_ROOT

def load_config(name="paths"):
    cfg_path = PROJECT_ROOT / "config" / f"{name}.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def resolve_path(relative_path):
    return PROJECT_ROOT / relative_path

def setup_logger(module_name, log_dir=None):
    if log_dir is None:
        log_dir = PROJECT_ROOT / "logs" / "download_logs"
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{module_name}_{ts}.log"
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger

# FIPS lookup table for 50 states + DC
FIPS_STATES = {
    "01": ("AL", "Alabama"), "02": ("AK", "Alaska"), "04": ("AZ", "Arizona"),
    "05": ("AR", "Arkansas"), "06": ("CA", "California"), "08": ("CO", "Colorado"),
    "09": ("CT", "Connecticut"), "10": ("DE", "Delaware"), "11": ("DC", "District of Columbia"),
    "12": ("FL", "Florida"), "13": ("GA", "Georgia"), "15": ("HI", "Hawaii"),
    "16": ("ID", "Idaho"), "17": ("IL", "Illinois"), "18": ("IN", "Indiana"),
    "19": ("IA", "Iowa"), "20": ("KS", "Kansas"), "21": ("KY", "Kentucky"),
    "22": ("LA", "Louisiana"), "23": ("ME", "Maine"), "24": ("MD", "Maryland"),
    "25": ("MA", "Massachusetts"), "26": ("MI", "Michigan"), "27": ("MN", "Minnesota"),
    "28": ("MS", "Mississippi"), "29": ("MO", "Missouri"), "30": ("MT", "Montana"),
    "31": ("NE", "Nebraska"), "32": ("NV", "Nevada"), "33": ("NH", "New Hampshire"),
    "34": ("NJ", "New Jersey"), "35": ("NM", "New Mexico"), "36": ("NY", "New York"),
    "37": ("NC", "North Carolina"), "38": ("ND", "North Dakota"), "39": ("OH", "Ohio"),
    "40": ("OK", "Oklahoma"), "41": ("OR", "Oregon"), "42": ("PA", "Pennsylvania"),
    "44": ("RI", "Rhode Island"), "45": ("SC", "South Carolina"), "46": ("SD", "South Dakota"),
    "47": ("TN", "Tennessee"), "48": ("TX", "Texas"), "49": ("UT", "Utah"),
    "50": ("VT", "Vermont"), "51": ("VA", "Virginia"), "53": ("WA", "Washington"),
    "54": ("WV", "West Virginia"), "55": ("WI", "Wisconsin"), "56": ("WY", "Wyoming"),
}

CENSUS_REGIONS = {
    "AL":"South","AK":"West","AZ":"West","AR":"South","CA":"West","CO":"West",
    "CT":"Northeast","DE":"South","DC":"South","FL":"South","GA":"South","HI":"West",
    "ID":"West","IL":"Midwest","IN":"Midwest","IA":"Midwest","KS":"Midwest","KY":"South",
    "LA":"South","ME":"Northeast","MD":"South","MA":"Northeast","MI":"Midwest","MN":"Midwest",
    "MS":"South","MO":"Midwest","MT":"West","NE":"Midwest","NV":"West","NH":"Northeast",
    "NJ":"Northeast","NM":"West","NY":"Northeast","NC":"South","ND":"Midwest","OH":"Midwest",
    "OK":"South","OR":"West","PA":"Northeast","RI":"Northeast","SC":"South","SD":"Midwest",
    "TN":"South","TX":"South","UT":"West","VT":"Northeast","VA":"South","WA":"West",
    "WV":"South","WI":"Midwest","WY":"West"
}

CENSUS_DIVISIONS = {
    "AL":"East South Central","AK":"Pacific","AZ":"Mountain","AR":"West South Central",
    "CA":"Pacific","CO":"Mountain","CT":"New England","DE":"South Atlantic",
    "DC":"South Atlantic","FL":"South Atlantic","GA":"South Atlantic","HI":"Pacific",
    "ID":"Mountain","IL":"East North Central","IN":"East North Central","IA":"West North Central",
    "KS":"West North Central","KY":"East South Central","LA":"West South Central",
    "ME":"New England","MD":"South Atlantic","MA":"New England","MI":"East North Central",
    "MN":"West North Central","MS":"East South Central","MO":"West North Central",
    "MT":"Mountain","NE":"West North Central","NV":"Mountain","NH":"New England",
    "NJ":"Middle Atlantic","NM":"Mountain","NY":"Middle Atlantic","NC":"South Atlantic",
    "ND":"West North Central","OH":"East North Central","OK":"West South Central",
    "OR":"Pacific","PA":"Middle Atlantic","RI":"New England","SC":"South Atlantic",
    "SD":"West North Central","TN":"East South Central","TX":"West South Central",
    "UT":"Mountain","VT":"New England","VA":"South Atlantic","WA":"Pacific",
    "WV":"South Atlantic","WI":"East North Central","WY":"Mountain"
}

def build_state_lookup():
    rows = []
    for fips, (abbr, name) in FIPS_STATES.items():
        rows.append({
            "state_fips": fips,
            "state_abbr": abbr,
            "state_name": name,
            "census_region": CENSUS_REGIONS[abbr],
            "census_division": CENSUS_DIVISIONS[abbr],
        })
    return pd.DataFrame(rows)

def coverage_audit(df, module_name, expected_states=51, start_year=2008, end_year=2023, output_dir=None):
    if output_dir is None:
        output_dir = PROJECT_ROOT / "qc" / "coverage"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    expected_years = list(range(start_year, end_year + 1))
    n_states = df["state_fips"].nunique()
    years = sorted(df["year"].unique())
    missing_years = [y for y in expected_years if y not in years]
    total_expected = expected_states * len(expected_years)
    total_actual = len(df)
    missing_pct = (total_expected - total_actual) / total_expected * 100 if total_expected > 0 else 0
    report = {
        "module": module_name,
        "n_states": n_states,
        "expected_states": expected_states,
        "years_covered": f"{min(years)}-{max(years)}" if years else "none",
        "missing_years": missing_years,
        "total_expected": total_expected,
        "total_actual": total_actual,
        "missing_pct": round(missing_pct, 2),
    }
    pd.DataFrame([report]).to_csv(output_dir / f"{module_name}_coverage.csv", index=False)
    return report

