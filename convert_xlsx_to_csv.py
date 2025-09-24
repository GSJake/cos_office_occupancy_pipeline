#!/usr/bin/env python3
"""
Simple script to convert Excel files to CSV format.
Converts all .xlsx files from Inputs/ directory structure to CSV.

Guardrails:
- Requires openpyxl; fails fast with clear instructions (Databricks: use %pip install -r requirements.txt).
- Output filenames include year (and for Deskcount, prefer YYYY-MM based on snapshot Date column) to avoid overwrites.
- Reports failures and exits non-zero if any file fails to convert.
"""

import pandas as pd
import os
from pathlib import Path
from typing import List, Optional

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

    # Process each data type directory
    for data_type_dir in inputs_dir.iterdir():
        if data_type_dir.is_dir():
            data_type = data_type_dir.name
            print(f"\nProcessing {data_type} files...")
            
            # Process each year subdirectory
            for year_dir in data_type_dir.iterdir():
                if year_dir.is_dir():
                    year = year_dir.name
                    print(f"  Processing {year}...")
                    
                    # Process each Excel file
                    for excel_file in year_dir.glob("*.xlsx"):
                        try:
                            print(f"    Converting {excel_file.name}...")
                            
                            # Read Excel file
                            df = pd.read_excel(excel_file, engine='openpyxl')
                            
                            # Create output filename
                            # Include uniqueness in output filename to avoid overwrites across years.
                            if data_type.lower() == 'deskcount':
                                ym = _infer_deskcount_year_month(year, excel_file.stem, df)
                                if ym:
                                    csv_filename = f"{ym}_Deskcount.csv"
                                else:
                                    csv_filename = f"{year}_" + excel_file.stem + ".csv"
                            else:
                                csv_filename = f"{year}_" + excel_file.stem + ".csv"
                            csv_path = output_dir / data_type / csv_filename
                            
                            # Save as CSV
                            df.to_csv(csv_path, index=False)
                            print(f"    Saved: {csv_path}")
                            total += 1
                            
                        except Exception as e:
                            msg = f"{data_type}/{year}/{excel_file.name}: {e}"
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
