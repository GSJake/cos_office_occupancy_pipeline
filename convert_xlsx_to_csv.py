#!/usr/bin/env python3
"""
Simple script to convert Excel files to CSV format.
Converts all .xlsx files from Inputs/ directory structure to CSV.

New input convention (preferred):
- Deskcount: `Inputs/Deskcount/YYYY_MM_deskcount.xlsx` (e.g., `2025_01_deskcount.xlsx`)
- Occupancy: `Inputs/Occupancy/YYYY_MM_occupancy.xlsx`

Backward compatibility:
- Also scans nested `Inputs/<Type>/<Year>/*.xlsx` and attempts to infer year+month from filename or sheet content.

Guardrails:
- Requires openpyxl; fails fast with clear instructions (Databricks: use %pip install -r requirements.txt).
- Output filenames use `YYYY-MM_Deskcount.csv` and `YYYY-MM_Occupancy.csv` to avoid overwrites.
- Reports failures and exits non-zero if any file fails to convert.
"""

import pandas as pd
import os
from pathlib import Path
from typing import List, Optional, Tuple

def _require_openpyxl():
    try:
        import openpyxl  # noqa: F401
    except Exception:
        msg = (
            "Missing dependency 'openpyxl'. Install requirements then rerun.\n"
            "Databricks tip: use `%pip install -r requirements.txt` in a cell, then re-run."
        )
        raise SystemExit(msg)


_MONTH_MAP = {
    'january': '01', 'jan': '01',
    'february': '02', 'feb': '02',
    'march': '03', 'mar': '03',
    'april': '04', 'apr': '04',
    'may': '05',
    'june': '06', 'jun': '06',
    'july': '07', 'jul': '07',
    'august': '08', 'aug': '08',
    'september': '09', 'sep': '09', 'sept': '09',
    'october': '10', 'oct': '10',
    'november': '11', 'nov': '11',
    'december': '12', 'dec': '12',
}


def _infer_deskcount_year_month(year: str, stem: str, df: pd.DataFrame) -> Optional[str]:
    """Infer YYYY-MM for deskcount snapshot.

    Prefer the 'Date' column in the sheet; fallback to parsing month from filename stem.
    Returns YYYY-MM or None if not inferable.
    """
    # Try from 'Date' column
    if 'Date' in df.columns:
        dates = pd.to_datetime(df['Date'], errors='coerce').dropna()
        if not dates.empty:
            snap = dates.max()
            return snap.strftime('%Y-%m')

    # Fallback: parse month from filename
    s = stem.lower()
    for key, mm in _MONTH_MAP.items():
        if key in s:
            return f"{year}-{mm}"
    return None


def _infer_occupancy_year_month(stem: str, df: pd.DataFrame) -> Optional[str]:
    """Infer YYYY-MM for occupancy file.

    Prefer 'LogonDate' column if present; fallback to filename pattern YYYY[_-]MM.
    """
    # Try from 'LogonDate'
    col = None
    for c in df.columns:
        if str(c).lower() == 'logondate':
            col = c
            break
    if col is not None:
        dates = pd.to_datetime(df[col], errors='coerce').dropna()
        if not dates.empty:
            snap = dates.max()
            return snap.strftime('%Y-%m')

    # Fallback: parse from filename
    import re as _re
    m = _re.search(r'(20\d{2})[\-_](0[1-9]|1[0-2])', stem)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


def _parse_year_month_from_name(stem: str) -> Optional[Tuple[str, str]]:
    """Parse YYYY and MM from a filename stem like '2025_01_deskcount'."""
    import re as _re
    m = _re.search(r'(20\d{2})[\-_](0[1-9]|1[0-2])', stem)
    if m:
        return m.group(1), m.group(2)
    return None

def convert_xlsx_to_csv():
    """Convert all Excel files in Inputs directory to CSV format."""
    
    # Ensure dependency is present
    _require_openpyxl()

    # Create output directory
    output_dir = Path("converted_data")
    output_dir.mkdir(exist_ok=True)
    
    # Create subdirectories for each data type
    for data_type in ["Occupancy", "Deskcount"]:
        (output_dir / data_type).mkdir(exist_ok=True)
    
    inputs_dir = Path("Inputs")
    
    total = 0
    failures: List[str] = []

    # Process each data type directory (Deskcount, Occupancy)
    for data_type_dir in inputs_dir.iterdir():
        if not data_type_dir.is_dir():
            continue
        data_type = data_type_dir.name
        print(f"\nProcessing {data_type} files...")

        # Gather both new-style files in the root and old-style in subfolders
        root_files = list(data_type_dir.glob("*.xlsx"))
        nested_files = [p for sub in data_type_dir.iterdir() if sub.is_dir() for p in sub.glob("*.xlsx")]
        files = root_files + nested_files
        if not files:
            msg = f"no .xlsx files found under {data_type_dir}"
            print(f"  {msg}")
            failures.append(f"{data_type}: {msg}")
            continue

        print(f"  Found {len(files)} input files")

        for excel_file in sorted(files):
            try:
                print(f"  Converting {excel_file.name}...")
                df = pd.read_excel(excel_file, engine='openpyxl')

                # Decide output filename
                stem = excel_file.stem
                if data_type.lower() == 'deskcount':
                    # Prefer filename pattern YYYY_MM, else infer from sheet Date, else fallback to old path-derived year
                    ym = None
                    parsed = _parse_year_month_from_name(stem)
                    if parsed:
                        ym = f"{parsed[0]}-{parsed[1]}"
                    else:
                        # Try infer from content or legacy dirname (year)
                        parent_year = excel_file.parent.name if excel_file.parent.name.isdigit() else ''
                        ym = _infer_deskcount_year_month(parent_year, stem, df)
                    if not ym:
                        raise ValueError("Unable to infer year-month for Deskcount file")
                    csv_filename = f"{ym}_Deskcount.csv"
                else:  # Occupancy
                    ym = None
                    parsed = _parse_year_month_from_name(stem)
                    if parsed:
                        ym = f"{parsed[0]}-{parsed[1]}"
                    else:
                        ym = _infer_occupancy_year_month(stem, df)
                    if not ym:
                        # Try legacy year directory name
                        parent_year = excel_file.parent.name if excel_file.parent.name.isdigit() else ''
                        if parent_year and any(m in stem.lower() for m in _MONTH_MAP.keys()):
                            # Best-effort month from name + parent year
                            for key, mm in _MONTH_MAP.items():
                                if key in stem.lower():
                                    ym = f"{parent_year}-{mm}"
                                    break
                    if not ym:
                        raise ValueError("Unable to infer year-month for Occupancy file")
                    csv_filename = f"{ym}_Occupancy.csv"

                csv_path = output_dir / data_type / csv_filename
                df.to_csv(csv_path, index=False)
                print(f"    Saved: {csv_path}")
                total += 1

            except Exception as e:
                msg = f"{data_type}/{excel_file.name}: {e}"
                print(f"    Error converting {excel_file.name}: {e}")
                failures.append(msg)

    if failures:
        print("\nConversion failures (see above):")
        for f in failures:
            print(f"  - {f}")
        raise SystemExit(1)
    else:
        print(f"\nConverted {total} Excel files to CSV without errors.")

if __name__ == "__main__":
    print("Converting Excel files to CSV...")
    convert_xlsx_to_csv()
    print("\nConversion complete!") 
