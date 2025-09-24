#!/usr/bin/env python3
"""
Step 3: Clean Occupancy Data
Clean occupancy data according to specific requirements.
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

def clean_occupancy_data():
    """Clean occupancy data according to the specified requirements."""
    
    print("Loading occupancy data...")
    df = pd.read_csv('combined_data/Occupancy.csv', low_memory=False)
    print(f"Original data shape: {df.shape}")
    
    # Step 3a: Keep only the specified columns
    required_columns = [
        'Username',           # username
        'OfficeLocation',     # office_location  
        'LineOfBusiness',     # line_of_business
        'OfficeLocationCityState',  # city_state_country_region (closest match)
        'LogonDate',          # logon_date
        'DayofWeek',          # day_of_week
        'JobFamily'           # job_family
    ]
    
    print(f"\nStep 3a: Keeping only required columns...")
    df_clean = df[required_columns].copy()
    
    # Rename columns to match the target names
    df_clean = df_clean.rename(columns={
        'Username': 'username',
        'OfficeLocation': 'office_location',
        'LineOfBusiness': 'line_of_business',
        'OfficeLocationCityState': 'city_state_country_region',
        'LogonDate': 'logon_date',
        'DayofWeek': 'day_of_week',
        'JobFamily': 'job_family'
    })
    
    # Normalize office_location text (trim, collapse whitespace, strip trailing punctuation)
    def _normalize_location(val):
        if pd.isna(val):
            return val
        s = str(val).strip()
        s = re.sub(r"\s+", " ", s)
        s = s.rstrip('.,;:')
        return s
    df_clean['office_location'] = df_clean['office_location'].map(_normalize_location)
    
    print(f"After column selection: {df_clean.shape}")
    
    # Step 3b: Add year, month, week_in_month columns
    print(f"\nStep 3b: Adding year, month, week_in_month columns...")
    df_clean['logon_date'] = pd.to_datetime(df_clean['logon_date'])
    df_clean['year'] = df_clean['logon_date'].dt.year
    df_clean['month'] = df_clean['logon_date'].dt.month
    
    # Calculate week_in_month (1-5, where week 1 starts on the 1st of the month)
    df_clean['week_in_month'] = ((df_clean['logon_date'].dt.day - 1) // 7) + 1
    
    # Step 3c: Remove duplicate rows (same username, logon_date, office_location)
    print(f"\nStep 3c: Removing duplicate username/date/location combinations...")
    print(f"Before deduplication: {len(df_clean)} rows")
    
    df_clean = df_clean.drop_duplicates(
        subset=['username', 'logon_date', 'office_location'],
        keep='first'
    )
    print(f"After deduplication: {len(df_clean)} rows")
    
    # Step 3d: Convert usernames to 1s (remove personal data)
    print(f"\nStep 3d: Converting usernames to 1s for privacy...")
    df_clean['username'] = 1
    
    # Step 3e: Fix line_of_business values
    print(f"\nStep 3e: Standardizing line_of_business values...")
    print("Before standardization:")
    print(df_clean['line_of_business'].value_counts(dropna=False))
    
    # Apply the business rules
    df_clean['line_of_business'] = df_clean['line_of_business'].fillna('Corporate')  # NaN -> Corporate
    df_clean.loc[df_clean['line_of_business'] == 'Pending', 'line_of_business'] = 'Corporate'
    df_clean.loc[df_clean['line_of_business'] == 'Development & Construction', 'line_of_business'] = 'Development and Construction'
    
    print("\nAfter standardization:")
    print(df_clean['line_of_business'].value_counts(dropna=False))
    
    # Save the cleaned data
    output_dir = Path("cleaned_data")
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / "Occupancy_cleaned.csv"
    df_clean.to_csv(output_file, index=False)
    
    print(f"\nCleaned occupancy data saved to: {output_file}")
    print(f"Final data shape: {df_clean.shape}")
    print(f"Final columns: {df_clean.columns.tolist()}")
    
    return df_clean

if __name__ == "__main__":
    print("Step 3: Cleaning Occupancy Data...")
    clean_data = clean_occupancy_data()
    print("\nStep 3 complete!") 
