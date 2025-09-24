#!/usr/bin/env python3
"""
Simple script to combine CSV files by data type.
Combines all CSV files of the same type into single master files.
"""

import pandas as pd
import os
from pathlib import Path

def combine_csv_files():
    """Combine all CSV files by data type into master files."""
    
    # Create output directory
    output_dir = Path("combined_data")
    output_dir.mkdir(exist_ok=True)
    
    converted_dir = Path("converted_data")
    
    # Process each data type directory
    for data_type_dir in converted_dir.iterdir():
        if data_type_dir.is_dir():
            data_type = data_type_dir.name
            print(f"\nCombining {data_type} files...")
            
            # List to store all dataframes
            all_dataframes = []
            
            # Process each CSV file in the data type directory
            files = list(data_type_dir.glob("*.csv"))
            if not files:
                print(f"  No CSV files found for {data_type} in {data_type_dir}")
                raise SystemExit(1)
            for csv_file in sorted(files):
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
