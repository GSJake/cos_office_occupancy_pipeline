#!/usr/bin/env python3
"""
Pipeline Orchestrator for COS Office Occupancy

Runs the end-to-end pipeline in ordered stages:
1) Convert XLSX -> CSV
2) Combine CSVs by dataset
3) Clean Occupancy
4) Clean Deskcount
5) Create DimDate
6) Create DimLocation
7) Create DimLineOfBusiness
8) Create FactOccupancy (by LOB)
9) Create FactOccupancyAggregated (all LOBs)

Usage examples:
  python run_pipeline.py                 # run all stages
  python run_pipeline.py --from 3 --to 7 # run a subset of stages
  python run_pipeline.py --only 5 6      # run specific stages
  python run_pipeline.py --dry-run       # print what would run
"""

from __future__ import annotations

import argparse
import sys
import os
from pathlib import Path
from typing import Callable, Dict, List, Tuple


# Import stage functions from existing scripts
from convert_xlsx_to_csv import convert_xlsx_to_csv
from combine_csv_files import combine_csv_files
from clean_occupancy_data import clean_occupancy_data
from clean_deskcount_data import clean_deskcount_data
from create_dim_date import create_dim_date
from create_dim_location import create_dim_location
from create_dim_line_of_business import create_dim_line_of_business
from create_fact_occupancy import create_fact_occupancy
from create_fact_occupancy_aggregated import create_fact_occupancy_aggregated


def log(msg: str) -> None:
    print(f"[pipeline] {msg}")


ROOT = Path(__file__).resolve().parent


def _inputs_dir() -> Path:
    # Use explicit 'Inputs/' path for phase 1 checks to match Databricks usage
    return Path('Inputs')


def ensure_inputs() -> Tuple[bool, str]:
    inputs = _inputs_dir()
    if not inputs.exists():
        return False, "Inputs/ directory not found. Place raw Excel files under Inputs/<Type>/YYYY_MM_<type>.xlsx"
    desk = list((inputs / "Deskcount").glob("**/*.xlsx"))
    occ = list((inputs / "Occupancy").glob("**/*.xlsx"))
    if not desk:
        return False, "No Deskcount Excel files found under Inputs/Deskcount. Expected files like 2025_01_deskcount.xlsx"
    if not occ:
        return False, "No Occupancy Excel files found under Inputs/Occupancy. Expected files like 2025_01_occupancy.xlsx"
    return True, ""


def _has_csvs(path: Path) -> bool:
    return path.exists() and any(path.glob("*.csv"))


def stage_checks() -> Dict[int, Callable[[], Tuple[bool, str]]]:
    return {
        1: lambda: ensure_inputs(),
        2: lambda: (
            _has_csvs(Path("converted_data/Deskcount")) and _has_csvs(Path("converted_data/Occupancy")),
            "converted_data subfolders missing or empty. Run stage 1 successfully first.",
        ),
        3: lambda: (Path("combined_data/Occupancy.csv").exists(), "combined_data/Occupancy.csv missing. Run stages 1-2."),
        4: lambda: (Path("combined_data/Deskcount.csv").exists(), "combined_data/Deskcount.csv missing. Run stages 1-2."),
        5: lambda: (True, ""),  # synthetic
        6: lambda: (Path("cleaned_data/Occupancy_cleaned.csv").exists(), "cleaned_data/Occupancy_cleaned.csv missing. Run stage 3."),
        7: lambda: (Path("cleaned_data/Occupancy_cleaned.csv").exists(), "cleaned_data/Occupancy_cleaned.csv missing. Run stage 3."),
        8: lambda: (
            Path("dimensions/DimDate.csv").exists()
            and Path("dimensions/DimLocation.csv").exists()
            and Path("dimensions/DimLineOfBusiness.csv").exists()
            and Path("cleaned_data/Occupancy_cleaned.csv").exists()
            and Path("cleaned_data/Deskcount_cleaned.csv").exists(),
            "Required dims or cleaned data missing. Run stages 3-7.",
        ),
        9: lambda: (
            Path("dimensions/DimDate.csv").exists()
            and Path("dimensions/DimLocation.csv").exists()
            and Path("cleaned_data/Occupancy_cleaned.csv").exists()
            and Path("cleaned_data/Deskcount_cleaned.csv").exists(),
            "Required dims or cleaned data missing. Run stages 3,4,5,6.",
        ),
    }


def define_stages() -> List[Tuple[int, str, Callable[[], None]]]:
    return [
        (1, "convert_xlsx_to_csv", convert_xlsx_to_csv),
        (2, "combine_csv_files", combine_csv_files),
        (3, "clean_occupancy_data", clean_occupancy_data),
        (4, "clean_deskcount_data", clean_deskcount_data),
        (5, "create_dim_date", create_dim_date),
        (6, "create_dim_location", create_dim_location),
        (7, "create_dim_line_of_business", create_dim_line_of_business),
        (8, "create_fact_occupancy", create_fact_occupancy),
        (9, "create_fact_occupancy_aggregated", create_fact_occupancy_aggregated),
    ]


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run office occupancy pipeline")
    parser.add_argument("--from", dest="from_stage", type=int, default=1, help="First stage number to run (default: 1)")
    parser.add_argument("--to", dest="to_stage", type=int, default=9, help="Last stage number to run (default: 9)")
    parser.add_argument(
        "--only", dest="only", type=int, nargs="+", help="Run only these stage numbers (space-separated)")
    parser.add_argument("--skip", dest="skip", type=int, nargs="+", default=[], help="Skip these stage numbers")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without running anything")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    stages = define_stages()
    checks = stage_checks()

    stage_map = {num: (name, fn) for num, name, fn in stages}

    if args.only:
        plan = [s for s in args.only if s in stage_map]
    else:
        plan = [num for num, _, _ in stages if args.from_stage <= num <= args.to_stage]
        plan = [num for num in plan if num not in (args.skip or [])]

    if not plan:
        log("No stages selected. Nothing to do.")
        return 0

    log("Execution plan:")
    for num in plan:
        log(f"  {num}) {stage_map[num][0]}")

    if args.dry_run:
        return 0

    for num in plan:
        name, fn = stage_map[num]
        ok, msg = checks[num]()
        if not ok:
            log(f"Prerequisite check failed for stage {num} ({name}): {msg}")
            return 2
        log(f"\n=== Running stage {num}: {name} ===")
        fn()
        log(f"=== Completed stage {num}: {name} ===\n")

    log("Pipeline complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
