#!/usr/bin/env python3
"""
Step 5: Create DimDate Table
Create a comprehensive date dimension table covering 2024 to 2027.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

def create_dim_date():
    """Create a comprehensive date dimension table for 2024-2027."""
    
    print("Creating DimDate table for years 2024-2027...")
    
    # Generate date range from 2024-01-01 to 2027-12-31
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2027, 12, 31)
    
    # Create date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    print(f"Generating {len(date_range)} dates from {start_date.date()} to {end_date.date()}")
    
    # Create DataFrame
    dim_date = pd.DataFrame({'date': date_range})
    
    # Add date_key in YYYYMMDD format (e.g., 20240101 for 2024-01-01)
    dim_date['date_key'] = dim_date['date'].dt.strftime('%Y%m%d').astype(int)
    
    # Extract date components
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['quarter'] = dim_date['date'].dt.quarter
    
    # Day of week (1 = Monday, 7 = Sunday)
    dim_date['day_of_week'] = dim_date['date'].dt.dayofweek + 1
    
    # Day and month names
    dim_date['day_name'] = dim_date['date'].dt.day_name()
    dim_date['month_name'] = dim_date['date'].dt.month_name()
    
    # Weekend flag
    dim_date['is_weekend'] = dim_date['day_of_week'].isin([6, 7])  # Saturday and Sunday
    
    # Week in month (1-5)
    dim_date['week_in_month'] = ((dim_date['day'] - 1) // 7) + 1
    
    # Additional useful attributes
    dim_date['day_of_year'] = dim_date['date'].dt.dayofyear
    dim_date['week_of_year'] = dim_date['date'].dt.isocalendar().week
    
    # Format date as string for easier joining
    dim_date['date_string'] = dim_date['date'].dt.strftime('%Y-%m-%d')
    
    # Reorder columns for better readability
    column_order = [
        'date_key',
        'date',
        'date_string', 
        'year',
        'quarter',
        'month',
        'month_name',
        'week_of_year',
        'week_in_month',
        'day',
        'day_of_week',
        'day_name',
        'day_of_year',
        'is_weekend'
    ]
    
    dim_date = dim_date[column_order]
    
    # Display sample data
    print(f"\nSample of DimDate table:")
    print(dim_date.head(10))
    
    print(f"\nData types:")
    print(dim_date.dtypes)
    
    print(f"\nDate range summary:")
    print(f"First date: {dim_date['date'].min()}")
    print(f"Last date: {dim_date['date'].max()}")
    print(f"Total days: {len(dim_date)}")
    print(f"Years covered: {sorted(dim_date['year'].unique())}")
    print(f"Date key range: {dim_date['date_key'].min()} to {dim_date['date_key'].max()}")
    
    # Save the dimension table
    output_dir = Path("dimensions")
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / "DimDate.csv"
    dim_date.to_csv(output_file, index=False)
    
    print(f"\nDimDate table saved to: {output_file}")
    print(f"Final table shape: {dim_date.shape}")
    
    return dim_date

if __name__ == "__main__":
    print("Step 5: Creating DimDate Table...")
    dim_date = create_dim_date()
    print("\nStep 5 complete!") 