$dirs = @(
    "config",
    "data_raw\panel_skeleton", "data_raw\min_wage", "data_raw\housing",
    "data_raw\utilities", "data_raw\food", "data_raw\price_adjustment", "data_raw\controls",
    "data_clean\min_wage", "data_clean\housing", "data_clean\utilities",
    "data_clean\food", "data_clean\controls", "data_clean\merged",
    "data_final\survival_main", "data_final\survival_robustness", "data_final\export",
    "docs\source_notes", "docs\method_notes", "docs\variable_dictionary", "docs\audit_reports",
    "logs\download_logs", "logs\cleaning_logs", "logs\qc_logs",
    "qc\coverage", "qc\missingness", "qc\outliers", "qc\consistency",
    "scripts\00_setup", "scripts\01_skeleton", "scripts\02_min_wage",
    "scripts\03_housing", "scripts\04_utilities", "scripts\05_food",
    "scripts\06_controls", "scripts\07_merge", "scripts\08_construct", "scripts\09_qc"
)
foreach ($d in $dirs) {
    New-Item -ItemType Directory -Path $d -Force | Out-Null
}
Write-Output "All directories created successfully."

