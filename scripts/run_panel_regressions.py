"""
Panel regression analysis for MW-Survival Line research question.
Outputs: descriptive stats, baseline TWFE regressions, robustness checks.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '00_setup'))
from utils import get_project_root
import pandas as pd
import numpy as np
from linearmodels.panel import PanelOLS, PooledOLS
from linearmodels.panel.results import compare
import statsmodels.api as sm

root = get_project_root()
OUT = root / "final_delivery" / "regressions"
OUT.mkdir(parents=True, exist_ok=True)

# ============================================================
# LOAD DATA
# ============================================================
df = pd.read_csv(root / "final_delivery/csv/survival_line_main_submission.csv",
                 dtype={"state_fips": str})
# Bring gross_rent for robustness
merged = pd.read_csv(root / "data_clean/merged/panel_merged_patched.csv",
                     dtype={"state_fips": str})
df = df.merge(merged[["state_fips", "year", "gross_rent_annual"]],
              on=["state_fips", "year"], how="left")
# Construct gross-rent survival line and gap
df["survival_line_grossrent"] = (df["gross_rent_annual"]
                                 + df["food_reconstructed_annual"]
                                 + df["electric_bill_annual"]
                                 + df["gas_bill_annual"])
df["mw_survival_gap_grossrent"] = df["annualized_mw_income"] - df["survival_line_grossrent"]

# Analysis sample: drop obs without main gap (i.e. drop 2020 housing gap)
an = df.dropna(subset=["mw_survival_gap_main", "binding_min_wage",
                       "poverty_rate", "median_hh_income", "rpp_all"]).copy()
an = an.set_index(["state_fips", "year"])

# ============================================================
# PART 1. DESCRIPTIVE STATISTICS
# ============================================================
desc_vars = ["binding_min_wage", "annualized_mw_income",
             "survival_line_nominal_main", "mw_survival_gap_main",
             "mw_survival_ratio_main", "contract_rent_annual",
             "food_reconstructed_annual", "electric_bill_annual",
             "gas_bill_annual", "poverty_rate", "median_hh_income", "rpp_all"]
desc = df[desc_vars].describe(percentiles=[.1, .5, .9]).T.round(2)
desc.to_csv(OUT / "01_descriptive_stats.csv")

# Year-by-year means
yearly = df.groupby("year").agg(
    mw=("binding_min_wage", "mean"),
    mw_income=("annualized_mw_income", "mean"),
    sl=("survival_line_nominal_main", "mean"),
    gap=("mw_survival_gap_main", "mean"),
    ratio=("mw_survival_ratio_main", "mean"),
    n_obs=("mw_survival_gap_main", lambda s: s.notna().sum()),
).round(3)
yearly.to_csv(OUT / "02_yearly_means.csv")

# Share of state-years with negative gap (insufficient)
neg_share = (df["mw_survival_gap_main"] < 0).sum() / df["mw_survival_gap_main"].notna().sum()
pos_share = (df["mw_survival_gap_main"] >= 0).sum() / df["mw_survival_gap_main"].notna().sum()
with open(OUT / "03_gap_sign_share.txt", "w") as f:
    f.write(f"Valid obs: {df['mw_survival_gap_main'].notna().sum()}\n")
    f.write(f"Share negative (insufficient): {neg_share:.4f}\n")
    f.write(f"Share non-negative (sufficient): {pos_share:.4f}\n")

# CAGR of mean MW income vs mean SL, 2008->2023 (excl 2020)
sub = yearly.dropna()
first, last = sub.index.min(), sub.index.max()
yr_span = last - first
def cagr(v0, v1, n): return (v1 / v0) ** (1 / n) - 1
cagr_mw = cagr(sub.loc[first, "mw_income"], sub.loc[last, "mw_income"], yr_span)
cagr_sl = cagr(sub.loc[first, "sl"], sub.loc[last, "sl"], yr_span)
with open(OUT / "04_cagr.txt", "w") as f:
    f.write(f"Span: {first}->{last} ({yr_span} yrs)\n")
    f.write(f"Mean MW income {first}: ${sub.loc[first,'mw_income']:,.0f}; {last}: ${sub.loc[last,'mw_income']:,.0f}\n")
    f.write(f"Mean SL {first}: ${sub.loc[first,'sl']:,.0f}; {last}: ${sub.loc[last,'sl']:,.0f}\n")
    f.write(f"MW income CAGR: {cagr_mw*100:.3f}%/yr\n")
    f.write(f"SL CAGR:        {cagr_sl*100:.3f}%/yr\n")

# ============================================================
# PART 2. BASELINE REGRESSIONS (DV = mw_survival_gap_main)
# ============================================================
y = an["mw_survival_gap_main"]
X_noctrl = an[["binding_min_wage"]]
ctrls = ["binding_min_wage", "poverty_rate", "median_hh_income", "rpp_all"]
X_ctrl = an[ctrls]

# (1) Pooled OLS, no controls
m1 = PooledOLS(y, sm.add_constant(X_noctrl)).fit(cov_type="clustered", cluster_entity=True)
# (2) Pooled OLS + controls
m2 = PooledOLS(y, sm.add_constant(X_ctrl)).fit(cov_type="clustered", cluster_entity=True)
# (3) TWFE, no controls
m3 = PanelOLS(y, X_noctrl, entity_effects=True, time_effects=True).fit(cov_type="clustered", cluster_entity=True)
# (4) TWFE + controls (MAIN)
m4 = PanelOLS(y, X_ctrl, entity_effects=True, time_effects=True).fit(cov_type="clustered", cluster_entity=True)

cmp_base = compare({"(1) Pooled": m1, "(2) Pooled+X": m2,
                    "(3) TWFE": m3, "(4) TWFE+X": m4}, stars=True)
with open(OUT / "05_baseline_regressions.txt", "w", encoding="utf-8") as f:
    f.write(str(cmp_base))

# ============================================================
# PART 3. ROBUSTNESS
# ============================================================
# (R1) DV = gross-rent gap, same spec as (4)
an_gr = df.dropna(subset=["mw_survival_gap_grossrent"] + ctrls).copy().set_index(["state_fips", "year"])
mR1 = PanelOLS(an_gr["mw_survival_gap_grossrent"], an_gr[ctrls],
               entity_effects=True, time_effects=True).fit(cov_type="clustered", cluster_entity=True)

# (R2) Drop states that are always bound by the federal minimum (never exceed it)
ever_above = df.groupby("state_fips").apply(
    lambda g: (g["min_wage_nominal"] > g["binding_min_wage"] - 1e-9).any()
    and (g["min_wage_nominal"] > 7.25 + 1e-9).any()
)
state_above = ever_above[ever_above].index
an_ab = an.reset_index()
an_ab = an_ab[an_ab["state_fips"].isin(state_above)].set_index(["state_fips", "year"])
mR2 = PanelOLS(an_ab["mw_survival_gap_main"], an_ab[ctrls],
               entity_effects=True, time_effects=True).fit(cov_type="clustered", cluster_entity=True)

# (R3) Log(binding MW) as regressor; DV unchanged (linear gap)
an_ln = an.copy()
an_ln["log_binding_mw"] = np.log(an_ln["binding_min_wage"])
ctrls_ln = ["log_binding_mw", "poverty_rate", "median_hh_income", "rpp_all"]
mR3 = PanelOLS(an_ln["mw_survival_gap_main"], an_ln[ctrls_ln],
               entity_effects=True, time_effects=True).fit(cov_type="clustered", cluster_entity=True)

cmp_rob = compare({"(4) Main TWFE+X": m4,
                   "(R1) GrossRent DV": mR1,
                   "(R2) Above-Fed States": mR2,
                   "(R3) log(MW)": mR3}, stars=True)
with open(OUT / "06_robustness.txt", "w", encoding="utf-8") as f:
    f.write(str(cmp_rob))

# Coefficient summary CSV
rows = []
for name, m in [("(1) Pooled", m1), ("(2) Pooled+X", m2), ("(3) TWFE", m3),
                ("(4) TWFE+X [MAIN]", m4), ("(R1) GrossRent DV", mR1),
                ("(R2) Above-Fed States", mR2), ("(R3) log(MW)", mR3)]:
    key = "log_binding_mw" if "log" in name else "binding_min_wage"
    rows.append({
        "spec": name,
        "coef_MW": m.params.get(key, np.nan),
        "se": m.std_errors.get(key, np.nan),
        "tstat": m.tstats.get(key, np.nan),
        "pval": m.pvalues.get(key, np.nan),
        "n_obs": int(m.nobs),
        "r2_within": getattr(m, "rsquared_within", m.rsquared),
    })
pd.DataFrame(rows).to_csv(OUT / "07_coef_summary.csv", index=False)

print("Done. Outputs:")
for p in sorted(OUT.glob("*")):
    print(" ", p.name)
