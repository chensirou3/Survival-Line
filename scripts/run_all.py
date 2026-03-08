"""
Master runner — execute all modules in order.
Usage: python scripts/run_all.py
"""
import subprocess
import sys
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = [
    ("Module 0: Skeleton",   ROOT / "scripts" / "01_skeleton" / "build_skeleton.py"),
    ("Module 1: Min Wage",   ROOT / "scripts" / "02_min_wage" / "download_min_wage.py"),
    ("Module 2: Housing",    ROOT / "scripts" / "03_housing"  / "download_housing.py"),
    ("Module 3: Utilities",  ROOT / "scripts" / "04_utilities"/ "download_utilities.py"),
    ("Module 4: Food",       ROOT / "scripts" / "05_food"     / "download_food.py"),
    ("Module 5: Controls",   ROOT / "scripts" / "06_controls" / "download_controls.py"),
    ("Module 6: Merge",      ROOT / "scripts" / "07_merge"    / "merge_panel.py"),
    ("Module 7: Construct",  ROOT / "scripts" / "08_construct"/ "construct_survival_line.py"),
    ("Module 8: QC",         ROOT / "scripts" / "09_qc"       / "run_qc.py"),
]

def main():
    print("=" * 60)
    print("Survival Line Data Pipeline — Full Run")
    print("=" * 60)
    failed = []
    for name, script in SCRIPTS:
        print(f"\n{'='*60}")
        print(f">>> Running {name}: {script.name}")
        print(f"{'='*60}")
        result = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(ROOT),
            capture_output=False,
        )
        if result.returncode != 0:
            print(f"*** {name} FAILED (exit code {result.returncode}) ***")
            failed.append(name)
            # Continue with remaining modules
        else:
            print(f">>> {name} completed successfully")

    print(f"\n{'='*60}")
    if failed:
        print(f"FAILED modules: {failed}")
    else:
        print("All modules completed successfully.")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()

