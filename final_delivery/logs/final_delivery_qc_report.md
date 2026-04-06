# Final Delivery QC Report

Generated: 2026-03-30 20:55


## 1. Unique Key Check (state_fips + year)
- Duplicate rows: **0**
- Status: **PASS**

## 2. ACS 2020 Gap Preservation
- Survival Line 2020 NaN count: **51/51**
- Contract Rent 2020 NaN count: **51/51**
- Status: **PASS**

## 3. Survival Line Missingness Check
- Total missing: **51/816**
- Missing only in years: **[2020]**
- Status: **PASS**

## 4. Figure Generation Check
- State figures: **51** (expected: 51)
- Summary figures: **6** (expected: 6)
- Status: **PASS**

## 5. Reconstructed Variable Flags
- construction_flag_food values: **{'reconstructed': 784, 'reconstructed_AK_HI': 32}**
- Status: **PASS** (all food marked as reconstructed)

## 6. Summary Statistics
- Valid SL observations: **765/816**
- SL Mean: **$19,325**
- SL Min: **$12,467**
- SL Max: **$39,818**
- Final CSV shape: **816 rows × 27 columns**

## 7. Final Deliverables Manifest
| File | Path |
|------|------|
| Submission CSV | `final_delivery/csv/survival_line_main_submission.csv` |
| State Figures | `final_delivery/figures/states/` (51 PNGs) |
| Summary Figures | `final_delivery/figures/summary/` (6 PNGs) |
| Source Registry | `final_delivery/sources/data_sources_registry.csv` |
| Source Notes | `final_delivery/sources/data_sources_notes.md` |
| QC Report | `final_delivery/logs/final_delivery_qc_report.md` |