#!/usr/bin/env python3
"""
Create FactOccupancy Table
Build a comprehensive fact table showing attendance by office_location, line_of_business, and date.
Includes 0s for days with no attendance, calculates occupancy rates, and flags hybrid days.
"""

import pandas as pd
from pathlib import Path

def calculate_hybrid_day_flags(fact_table):
    """
    Calculate hybrid day flags using vectorized logic aligned with business rules:
    - ISO week grouping per office
    - Eligible only if weekday and the month has >=3 weekdays in that ISO week
    - Mark top-3 attendance dates among eligible per week/location across all LOBs
    """
    print("Calculating hybrid day flags for each week/location combination (vectorized)...")

    # Ensure datetime
    fact_table['date'] = pd.to_datetime(fact_table['date'])

    # Date components
    fact_table['year'] = fact_table['date'].dt.year
    fact_table['month'] = fact_table['date'].dt.month
    iso = fact_table['date'].dt.isocalendar()
    fact_table['week_of_year'] = iso.week
    fact_table['week_year'] = iso.year
    fact_table['is_weekday_tmp'] = fact_table['date'].dt.dayofweek < 5

    # Precompute weekday counts per (ISO week, month) using unique dates only (no office dependency)
    date_df = fact_table[['date']].drop_duplicates().copy()
    iso_dates = date_df['date'].dt.isocalendar()
    date_df['week_year'] = iso_dates.year
    date_df['week_of_year'] = iso_dates.week
    date_df['month'] = date_df['date'].dt.month
    date_df['dow'] = date_df['date'].dt.dayofweek  # 0=Mon..6=Sun

    weekday_counts = (
        date_df[date_df['dow'] < 5]
        .groupby(['week_year', 'week_of_year', 'month'])['date']
        .nunique()
        .reset_index(name='weekday_count_in_month_week')
    )

    # Derive per-date eligibility (weekday and month contributes >=3 weekdays in that ISO week)
    date_elig = date_df.merge(
        weekday_counts,
        on=['week_year', 'week_of_year', 'month'],
        how='left'
    )
    date_elig['weekday_count_in_month_week'] = date_elig['weekday_count_in_month_week'].fillna(0).astype(int)
    date_elig['eligible_date'] = (date_elig['dow'] < 5) & (date_elig['weekday_count_in_month_week'] >= 3)

    # Daily totals per (location, iso-week, date) across all LOBs for ranking
    daily_totals = (
        fact_table.groupby(['office_location', 'week_year', 'week_of_year', 'date'])['attendance_count']
        .sum()
        .reset_index(name='daily_total_attendance')
    )

    # Add per-date eligibility to daily totals
    daily_totals = daily_totals.merge(
        date_elig[['date', 'eligible_date']], on='date', how='left'
    )
    daily_totals['eligible'] = daily_totals['eligible_date'].fillna(False)

    # Select top 3 eligible dates per (location, iso-week)
    daily_totals_eligible = daily_totals[daily_totals['eligible']].copy()
    daily_totals_eligible.sort_values(
        ['office_location', 'week_year', 'week_of_year', 'daily_total_attendance'],
        ascending=[True, True, True, False],
        inplace=True
    )
    top3 = daily_totals_eligible.groupby(['office_location', 'week_year', 'week_of_year']).head(3)
    top_keys = set(zip(top3['office_location'], top3['week_year'], top3['week_of_year'], top3['date']))

    # Initialize flag False
    fact_table['is_hybrid_day'] = False

    # Mark True when date is in top3 set
    sel = [
        (ol, wy, ww, d) in top_keys
        for ol, wy, ww, d in zip(
            fact_table['office_location'],
            fact_table['week_year'],
            fact_table['week_of_year'],
            fact_table['date'],
        )
    ]
    fact_table.loc[sel, 'is_hybrid_day'] = True

    # Drop temporary columns
    fact_table.drop(columns=['week_of_year', 'week_year', 'is_weekday_tmp'], inplace=True)

    # Print summary statistics
    total_days = len(fact_table)
    hybrid_days = fact_table['is_hybrid_day'].sum()
    print(f"\nHybrid Day Statistics:")
    print(f"Total days: {total_days:,}")
    print(f"Hybrid days: {hybrid_days:,}")
    if total_days > 0:
        print(f"Hybrid percentage: {(hybrid_days/total_days)*100:.1f}%")

    return fact_table

def create_fact_occupancy():
    """Create comprehensive fact table with attendance metrics and hybrid day flags."""
    
    print("Creating FactOccupancy table...")
    
    # Load dimension tables
    print("Loading dimension tables...")
    dim_date = pd.read_csv('dimensions/DimDate.csv')
    dim_location = pd.read_csv('dimensions/DimLocation.csv')
    dim_lob = pd.read_csv('dimensions/DimLineOfBusiness.csv')
    
    print(f"Loaded {len(dim_date)} dates, {len(dim_location)} locations, {len(dim_lob)} line of business")
    
    # Load cleaned data
    print("Loading cleaned data...")
    occupancy_data = pd.read_csv('cleaned_data/Occupancy_cleaned.csv', low_memory=False)
    deskcount_data = pd.read_csv('cleaned_data/Deskcount_cleaned.csv')
    
    print(f"Loaded {len(occupancy_data)} occupancy records and {len(deskcount_data)} deskcount records")
    
    # Convert date columns to proper format
    occupancy_data['logon_date'] = pd.to_datetime(occupancy_data['logon_date'])
    deskcount_data['date'] = pd.to_datetime(deskcount_data['date'])
    dim_date['date'] = pd.to_datetime(dim_date['date'])
    
    # Limit scope to dates with valid deskcount snapshots (no forward-fill)
    latest_desk_date = deskcount_data['date'].max()
    occupancy_data = occupancy_data[occupancy_data['logon_date'] <= latest_desk_date]
    dim_date = dim_date[dim_date['date'] <= latest_desk_date]

    # Create date_key in occupancy data for joining
    occupancy_data['date_key'] = occupancy_data['logon_date'].dt.strftime('%Y%m%d').astype(int)
    
    print("\nStep 1: Creating complete date × location × line_of_business matrix...")
    
    # Create complete cartesian product with dimension keys included
    date_loc_lob = dim_date[['date_key', 'date']].merge(
        dim_location[['location_key', 'office_location']], how='cross'
    ).merge(
        dim_lob[['lob_key', 'line_of_business']], how='cross'
    )
    
    print(f"Created complete matrix with {len(date_loc_lob)} combinations")
    
    print("\nStep 2: Counting actual attendance by date/location/line_of_business...")
    
    # Count attendance from occupancy data
    # Assuming 'username' column contains unique identifiers for attendance
    attendance_counts = occupancy_data.groupby([
        'date_key', 'office_location', 'line_of_business'
    ]).size().reset_index(name='attendance_count')
    
    print(f"Calculated attendance for {len(attendance_counts)} date/location/LOB combinations")
    
    print("\nStep 3: Joining with complete matrix to fill gaps with 0s...")
    
    # Left join to get all combinations, filling missing with 0
    fact_table = date_loc_lob.merge(
        attendance_counts,
        on=['date_key', 'office_location', 'line_of_business'],
        how='left'
    )
    
    # Fill missing attendance with 0
    fact_table['attendance_count'] = fact_table['attendance_count'].fillna(0).astype(int)
    
    print(f"Fact table now has {len(fact_table)} rows with complete coverage")
    
    print("\nStep 4: Adding deskcount data using efficient merge...")

    # --- PERFORMANCE OPTIMIZATION START ---
    # The original nested loops were very slow. 
    # pd.merge_asof is the correct, high-performance tool for finding the
    # last known value for each record.

    # Ensure both DataFrames are globally sorted by 'on' first, then 'by' for merge_asof
    # Pandas requires the 'on' key to be globally monotonic, not just within groups
    fact_table = fact_table.sort_values(['date', 'office_location'])
    deskcount_data = deskcount_data.sort_values(['date', 'office_location'])

    # Use merge_asof to efficiently find the last known deskcount for each date and location
    fact_table = pd.merge_asof(
        fact_table,
        deskcount_data[['date', 'office_location', 'deskcount']],
        on='date',
        by='office_location',
        direction='backward'  # Finds the last value in deskcount_data on or before the date
    )

    # Debug: report deskcount coverage after merge
    merged_non_null = fact_table['deskcount'].notna().sum()
    print(f"Deskcount populated on {merged_non_null:,} of {len(fact_table):,} rows after merge")
    # --- PERFORMANCE OPTIMIZATION END ---

    # Keep missing deskcount as NA (no valid capacity for that date/location)
    fact_table['deskcount'] = fact_table['deskcount'].astype('Int64')
    
    print("\nStep 5: Calculating occupancy rate...")
    
    # Calculate occupancy rate only where deskcount > 0; leave NA otherwise
    dc = fact_table['deskcount'].astype('Float64')
    fact_table['occupancy_rate'] = (fact_table['attendance_count'] / dc).where(dc > 0)
    
    print("\nStep 6: Adding hybrid day flags...")
    
    fact_table = calculate_hybrid_day_flags(fact_table)
    # Add weekend flag for downstream filtering
    fact_table['is_weekend'] = fact_table['date'].dt.dayofweek >= 5
    
    print("\nStep 7: Final column organization...")
    
    # Reorder columns for better readability
    final_columns = [
        'date_key',
        'location_key', 
        'lob_key',
        'date',
        'office_location',
        'line_of_business',
        'year',
        'month',
        'is_weekend',
        'attendance_count',
        'deskcount',
        'occupancy_rate',
        'is_hybrid_day'
    ]
    
    fact_table = fact_table[final_columns]
    
    # Sort by date, location, line of business
    fact_table = fact_table.sort_values(['date_key', 'office_location', 'line_of_business']).reset_index(drop=True)
    
    print(f"\nFinal FactOccupancy table:")
    print(f"Shape: {fact_table.shape}")
    print(f"Date range: {fact_table['date_key'].min()} to {fact_table['date_key'].max()}")
    print(f"Locations: {fact_table['office_location'].nunique()}")
    print(f"Line of Business: {fact_table['line_of_business'].nunique()}")
    
    print(f"\nSample data:")
    print(fact_table.head(10))
    
    print(f"\nHybrid day analysis:")
    hybrid_summary = fact_table['is_hybrid_day'].value_counts()
    print(f"Non-hybrid days: {hybrid_summary.get(False, 0):,}")
    print(f"Hybrid days: {hybrid_summary.get(True, 0):,}")
    
    print(f"\nData quality checks:")
    print(f"Null values:\n{fact_table.isnull().sum()}")
    
    # Save the fact table
    output_dir = Path("facts")
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / "FactOccupancy.csv"
    fact_table.to_csv(output_file, index=False)
    
    print(f"\nFactOccupancy table saved to: {output_file}")
    
    return fact_table

if __name__ == "__main__":
    print("Starting FactOccupancy Table Creation...")
    fact_occupancy = create_fact_occupancy()
    print("\nFactOccupancy creation complete!")
