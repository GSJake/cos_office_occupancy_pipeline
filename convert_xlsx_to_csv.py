#!/usr/bin/env python3
"""
Simple script to convert Excel files to CSV format.
Converts all .xlsx files from Inputs/ directory structure to CSV.
"""

import pandas as pd
import os
from pathlib import Path

def convert_xlsx_to_csv():
    """Convert all Excel files in Inputs directory to CSV format."""
    
    # Create output directory
    output_dir = Path("converted_data")
    output_dir.mkdir(exist_ok=True)
    
    # Create subdirectories for each data type
    for data_type in ["Occupancy", "Deskcount"]:
        (output_dir / data_type).mkdir(exist_ok=True)
    
    inputs_dir = Path("Inputs")
    
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
                            df = pd.read_excel(excel_file)
                            
                            # Create output filename
                            csv_filename = excel_file.stem + ".csv"
                            csv_path = output_dir / data_type / csv_filename
                            
                            # Save as CSV
                            df.to_csv(csv_path, index=False)
                            print(f"    Saved: {csv_path}")
                            
                        except Exception as e:
                            print(f"    Error converting {excel_file.name}: {e}")

if __name__ == "__main__":
    print("Converting Excel files to CSV...")
    convert_xlsx_to_csv()
    print("\nConversion complete!") 