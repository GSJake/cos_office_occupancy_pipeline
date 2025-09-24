#!/usr/bin/env python3
"""
Simple script to combine CSV files by data type.
Combines all CSV files of the same type into single master files.
"""

import pandas as pd
import os
from pathlib import Path

def _get_base_dir() -> Path:
    try:
        return Path(__file__).resolve().parent
    except NameError:
        # Running in environments like Databricks/IPython where __file__ is undefined
        return Path.cwd()

BASE_DIR = _get_base_dir()


def _resolve_paths() -> tuple[Path, Path]:
    out = os.environ.get('COS_OUTPUT_DIR')
    base = Path(out) if out else BASE_DIR
    return base / 'converted_data', base / 'combined_data'


def combine_csv_files():
    """Combine all CSV files by data type into master files."""
    
    # Create output directory
    converted_dir, output_dir = _resolve_paths()
    output_dir.mkdir(exist_ok=True)
    print(f"  Converted dir: {converted_dir}")
    print(f"  Output dir: {output_dir}")
    
    # Process each data type directory
    for data_type_dir in converted_dir.iterdir():
        if data_type_dir.is_dir():
            data_type = data_type_dir.name
            print(f"\nCombining {data_type} files...")
            
            # List to store all dataframes
            all_dataframes = []
            
            # Prefer new-style filenames first (YYYY-MM_<Type>.csv). Fallback to any CSVs if none.
            if data_type.lower() == 'deskcount':
                files = sorted(data_type_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9]_Deskcount.csv"))
            elif data_type.lower() == 'occupancy':
                files = sorted(data_type_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9]_Occupancy.csv"))
            else:
                files = []
            # Fallback to any CSVs present (legacy names)
            if not files:
                files = sorted(data_type_dir.glob("*.csv"))
            if not files:
                print(f"  No CSV files found for {data_type} in {data_type_dir}")
                raise SystemExit(1)
            print(f"  Found {len(files)} file(s) to combine")
            for csv_file in files:
                try:
                    print(f"  Reading {csv_file.name}...")
                    df = pd.read_csv(csv_file)
                    all_dataframes.append(df)
                    
                except Exception as e:
                    print(f"  Error reading {csv_file.name}: {e}")
            
            # Combine all dataframes
            if all_dataframes:
                combined_df = pd.concat(all_dataframes, ignore_index=True)
                
                # Save combined file
                output_file = output_dir / f"{data_type}.csv"
                combined_df.to_csv(output_file, index=False)
                
                print(f"  Combined {len(all_dataframes)} files into {output_file}")
                print(f"  Total rows: {len(combined_df)}")
            else:
                print(f"  No CSV files found for {data_type}")

if __name__ == "__main__":
    print("Combining CSV files...")
    combine_csv_files()
    print("\nCombining complete!") 
