#!/usr/bin/env python3
"""
Step 4: Clean Deskcount Data
Clean deskcount data according to specific requirements.
"""

import pandas as pd
import re
from pathlib import Path

def clean_deskcount_data(skip_include_filter=False):
    """Clean deskcount data according to the specified requirements.

    Args:
        skip_include_filter: If True, skip filtering by 'Include in Occupancy Calculation' flag.
                           Use this when the flag excludes locations that have occupancy data.
    """

    print("Loading deskcount data...")
    df = pd.read_csv('combined_data/Deskcount.csv')
    print(f"Original data shape: {df.shape}")
    print(f"Original columns: {df.columns.tolist()}")

    # Optional filter: respect "Include in Occupancy Calculation" flag if present
    include_col = None
    for c in df.columns:
        if c.strip().lower() == 'include in occupancy calculation':
            include_col = c
            break

    if include_col is not None and not skip_include_filter:
        before = len(df)
        df = df[df[include_col].astype(str).str.strip().str.lower() == 'yes']
        excluded = before - len(df)
        print(f"\nStep 4a: Filtering by '{include_col}' flag...")
        print(f"  Before: {before} rows")
        print(f"  After: {len(df)} rows")
        print(f"  Excluded: {excluded} rows marked as 'No'")

        if excluded > 50:
            print(f"\n⚠️  WARNING: {excluded} rows excluded - this may cause deskcount=0 issues")
            print(f"⚠️  If most deskcounts are 0, re-run with skip_include_filter=True")
    elif include_col is not None and skip_include_filter:
        print(f"\nℹ️  Skipping '{include_col}' filter (keeping all locations)")
    else:
        print(f"\nℹ️  No 'Include in Occupancy Calculation' column found")
    
    # Step 4a: Keep only the specified columns: office_location, deskcount, date
    required_columns = [
        'OfficeLocation',     # office_location
        'Deskcount',          # deskcount  
        'Date'                # date
    ]
    
    print(f"\nStep 4a: Keeping only required columns...")
    df_clean = df[required_columns].copy()
    
    # Rename columns to match the target names
    df_clean = df_clean.rename(columns={
        'OfficeLocation': 'office_location',
        'Deskcount': 'deskcount',
        'Date': 'date'
    })
    
    print(f"After column selection: {df_clean.shape}")
    print(f"Final columns: {df_clean.columns.tolist()}")
    
    # Normalize office_location text (trim, collapse whitespace, strip trailing punctuation)
    def _normalize_location(val):
        if pd.isna(val):
            return val
        s = str(val).strip()
        s = re.sub(r"\s+", " ", s)
        s = s.rstrip('.,;:')
        return s

    df_clean['office_location'] = df_clean['office_location'].map(_normalize_location)

    # Convert date to proper format
    print(f"\nConverting date column to datetime format...")
    df_clean['date'] = pd.to_datetime(df_clean['date'])
    
    # Ensure numeric deskcount; keep missing as NA (do NOT coerce to zero)
    df_clean['deskcount'] = pd.to_numeric(df_clean['deskcount'], errors='coerce')
    # Treat non-positive values (<=0) as missing; do not forward-fill
    df_clean.loc[df_clean['deskcount'] <= 0, 'deskcount'] = pd.NA
    # Cast to nullable integer to preserve NA
    df_clean['deskcount'] = df_clean['deskcount'].astype('Int64')
    
    # Display some sample data
    print(f"\nSample of cleaned data:")
    print(df_clean.head(10))
    
    print(f"\nData types:")
    print(df_clean.dtypes)
    
    # Check for any null values
    print(f"\nNull values check:")
    print(df_clean.isnull().sum())
    
    # Save the cleaned data
    output_dir = Path("cleaned_data")
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / "Deskcount_cleaned.csv"
    df_clean.to_csv(output_file, index=False)
    
    print(f"\nCleaned deskcount data saved to: {output_file}")
    print(f"Final data shape: {df_clean.shape}")
    
    return df_clean

if __name__ == "__main__":
    import sys

    # Check for --skip-filter argument
    skip_filter = '--skip-filter' in sys.argv or '--skip-include-filter' in sys.argv

    if skip_filter:
        print("Step 4: Cleaning Deskcount Data (SKIPPING include filter)...")
    else:
        print("Step 4: Cleaning Deskcount Data...")

    clean_data = clean_deskcount_data(skip_include_filter=skip_filter)
    print("\nStep 4 complete!") 
