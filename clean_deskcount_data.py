#!/usr/bin/env python3
"""
Step 4: Clean Deskcount Data
Clean deskcount data according to specific requirements.
"""

import pandas as pd
import re
from pathlib import Path

def clean_deskcount_data():
    """Clean deskcount data according to the specified requirements."""
    
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
    if include_col is not None:
        before = len(df)
        df = df[df[include_col].astype(str).str.strip().str.lower() == 'yes']
        print(f"Filtered by include flag '{include_col}': {before} -> {len(df)} rows")
    
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
    
    # Ensure numeric deskcount
    df_clean['deskcount'] = pd.to_numeric(df_clean['deskcount'], errors='coerce').fillna(0).astype(int)
    
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
    print("Step 4: Cleaning Deskcount Data...")
    clean_data = clean_deskcount_data()
    print("\nStep 4 complete!") 
