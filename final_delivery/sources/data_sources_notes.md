# Data Sources Notes

## Overview
This document describes the data sources used to construct the U.S. state-year
Survival Line panel dataset (50 states + DC, 2008–2023).

## Main Formula
```
SurvivalLine(s,t) = 12 × ContractRent(s,t) + Food_reconstructed(s,t)
                    + 12 × ElectricBill(s,t) + 12 × GasBill(s,t)
```

Where: `Food_reconstructed(s,t) = TFP_national(t) × RPP_goods(s,t) / 100`

## Key Distinctions

### Contract Rent vs. Gross Rent
- **Contract Rent** (ACS Table B25058): Excludes utilities. Used in main specification.
- **Gross Rent** (ACS Table B25064): Includes some utilities. Used only for robustness.
- These must NEVER be confused. The main Survival Line uses Contract Rent + independent utility bills.

### Direct vs. Reconstructed Variables
- **Direct**: Obtained directly from source (e.g., contract_rent, min_wage, RPP indices)
- **Reconstructed**: Computed from multiple sources (e.g., food cost = TFP × RPP; electric bill = Revenue/Customers/12)
- All reconstructed variables are flagged with `construction_flag_*` columns.

### ACS 2020 Gap
- The Census Bureau did not release standard ACS 1-Year estimates for 2020 due to COVID-19.
- Experimental estimates exist but are NOT used in the main specification.
- Housing variables (contract_rent, gross_rent) and ACS-based controls (poverty_rate) are missing for 2020.
- The main analysis sample is: 2008–2019, 2021–2023 (15 years × 51 units = 765 valid observations).

## Patch History
1. **Patch 01**: Extended electricity data from 2020 to 2023 using EIA Form HS861 historical files.
2. **Patch 02**: Added 2023 minimum wage data from DOL/EPI sources.
3. **Patch 03**: Re-merged all modules with patched data.
4. **Patch 04**: QC verification of patched panel.

## Coverage Summary
| Component | Coverage | Notes |
|-----------|----------|-------|
| Panel Skeleton | 816/816 (100%) | 51 states × 16 years |
| Minimum Wage | 816/816 (100%) | After Patch 02 |
| Contract Rent | 765/816 (93.8%) | Missing: 2020 (51 obs) |
| Electric Bill | 816/816 (100%) | After Patch 01 |
| Gas Bill | 816/816 (100%) | Estimated from price |
| Food Reconstructed | 816/816 (100%) | TFP × RPP |
| Survival Line (main) | 765/816 (93.8%) | Missing: 2020 (51 obs) |

## Citation
See `data_sources_registry.csv` for per-variable source details.
