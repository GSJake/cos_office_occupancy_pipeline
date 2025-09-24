#!/usr/bin/env python3
"""
Create FactOccupancyAggregated Table
Build a comprehensive fact table showing attendance by office_location and date.
Aggregates attendance across all lines of business, includes 0s for days with no attendance, 
calculates occupancy rates, and flags hybrid days.
"""

import pandas as pd
from pathlib import Path

def calculate_hybrid_day_flags(fact_table):
    """
    Calculate hybrid day flags for the fact table.
    
    For each week/location combination:
    1. Group data by week
    2. For each week, find the top 3 days by attendance
    3. Mark only those specific days as hybrid days
    
    Args:
        fact_table (pd.DataFrame): Fact table with attendance data
        
    Returns:
        pd.DataFrame: Fact table with is_hybrid_day column added
    """
    print("Calculating hybrid day flags for each week/location combination...")
    
    # Add date components for hybrid calculations
    fact_table['year'] = fact_table['date'].dt.year
    fact_table['month'] = fact_table['date'].dt.month
    # Use ISO week-year and week number to define weeks unambiguously
    iso_calendar = fact_table['date'].dt.isocalendar()
    fact_table['week_of_year'] = iso_calendar.week
    fact_table['week_year'] = iso_calendar.year

    # Initialize hybrid flag to False
    fact_table['is_hybrid_day'] = False

    # Group by location and ISO week (year + week number)
    weekly_groups = fact_table.groupby(['office_location', 'week_year', 'week_of_year'])

    # Process each week
    for _, group in weekly_groups:
        # Use unique dates to avoid duplication and derive month fresh from the date
        unique_dates = group['date'].drop_duplicates()

        # Eligibility considers weekdays only (Mon–Fri)
        weekday_dates = unique_dates[unique_dates.dt.dayofweek < 5]

        # Count number of weekday dates in this ISO week by calendar month
        month_counts = weekday_dates.dt.month.value_counts()
        eligible_months = set(month_counts[month_counts >= 3].index.tolist())

        if not eligible_months:
            continue

        # Candidate dates are weekdays within eligible months
        candidate_dates = unique_dates[(unique_dates.dt.dayofweek < 5) & (unique_dates.dt.month.isin(eligible_months))]

        # Compute total attendance per candidate day
        daily_attendance = (
            group[group['date'].isin(candidate_dates)]
            .groupby('date')['attendance_count']
            .sum()
            .reset_index()
        )

        if daily_attendance.empty:
            continue

        # Select top 3 days by attendance from the candidate pool
        top_3_days = daily_attendance.nlargest(3, 'attendance_count')['date']

        # Update only rows within this group that match the selected top days
        # and are weekdays in eligible months (hard guard)
        indices_to_update = group[(group['date'].isin(top_3_days)) & (group['date'].dt.dayofweek < 5) & (group['date'].dt.month.isin(eligible_months))].index
        fact_table.loc[indices_to_update, 'is_hybrid_day'] = True

    # Drop temporary columns
    fact_table.drop(columns=['week_of_year', 'week_year'], inplace=True)

    # Print summary statistics
    total_days = len(fact_table)
    hybrid_days = fact_table['is_hybrid_day'].sum()
    print(f"\nHybrid Day Statistics:")
    print(f"Total days: {total_days:,}")
    print(f"Hybrid days: {hybrid_days:,}")
    if total_days > 0:
        print(f"Hybrid percentage: {(hybrid_days/total_days)*100:.1f}%")

    return fact_table

def create_fact_occupancy_aggregated():
    """Create comprehensive fact table with attendance metrics aggregated across all lines of business."""
    
    print("Creating FactOccupancyAggregated table...")
    
    # Load dimension tables
    print("Loading dimension tables...")
    dim_date = pd.read_csv('dimensions/DimDate.csv')
    dim_location = pd.read_csv('dimensions/DimLocation.csv')
    
    print(f"Loaded {len(dim_date)} dates, {len(dim_location)} locations")
    
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
    
    print("\nStep 1: Creating complete date × location matrix...")
    
    # Create complete cartesian product with dimension keys included (no LOB)
    date_loc = dim_date[['date_key', 'date']].merge(
        dim_location[['location_key', 'office_location']], how='cross'
    )
    
    print(f"Created complete matrix with {len(date_loc)} combinations")
    
    print("\nStep 2: Counting actual attendance by date/location (aggregated across all LOBs)...")
    
    # Count attendance from occupancy data, aggregating across all lines of business
    attendance_counts = occupancy_data.groupby([
        'date_key', 'office_location'
    ]).size().reset_index(name='attendance_count')
    
    print(f"Calculated attendance for {len(attendance_counts)} date/location combinations")
    
    print("\nStep 3: Joining with complete matrix to fill gaps with 0s...")
    
    # Left join to get all combinations, filling missing with 0
    fact_table = date_loc.merge(
        attendance_counts,
        on=['date_key', 'office_location'],
        how='left'
    )
    
    # Fill missing attendance with 0
    fact_table['attendance_count'] = fact_table['attendance_count'].fillna(0).astype(int)
    
    print(f"Fact table now has {len(fact_table)} rows with complete coverage")
    
    print("\nStep 4: Adding deskcount data using efficient merge...")

    # Ensure both dataframes are sorted by by-keys and 'on' for merge_asof
    fact_table = fact_table.sort_values(['office_location', 'date'])
    deskcount_data = deskcount_data.sort_values(['office_location', 'date'])

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

    # Keep missing deskcount as NA (no valid capacity for that date/location)
    # Use pandas nullable integer to preserve NA
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
        'date',
        'office_location',
        'year',
        'month',
        'is_weekend',
        'attendance_count',
        'deskcount',
        'occupancy_rate',
        'is_hybrid_day'
    ]
    
    fact_table = fact_table[final_columns]
    
    # Sort by date, location
    fact_table = fact_table.sort_values(['date_key', 'office_location']).reset_index(drop=True)
    
    print(f"\nFinal FactOccupancyAggregated table:")
    print(f"Shape: {fact_table.shape}")
    print(f"Date range: {fact_table['date_key'].min()} to {fact_table['date_key'].max()}")
    print(f"Locations: {fact_table['office_location'].nunique()}")
    
    print(f"\nSample data:")
    print(fact_table.head(10))
    
    print(f"\nHybrid day analysis:")
    hybrid_summary = fact_table['is_hybrid_day'].value_counts()
    print(f"Non-hybrid days: {hybrid_summary.get(False, 0):,}")
    print(f"Hybrid days: {hybrid_summary.get(True, 0):,}")
    
    print(f"\nData quality checks:")
    print(f"Null values:\n{fact_table.isnull().sum()}")
    print(f"Attendance count range: {fact_table['attendance_count'].min()} to {fact_table['attendance_count'].max()}")
    print(f"Days with zero attendance: {len(fact_table[fact_table['attendance_count'] == 0])}")
    print(f"Days with attendance: {len(fact_table[fact_table['attendance_count'] > 0])}")
    
    # Save the fact table
    output_dir = Path("facts")
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / "FactOccupancyAggregated.csv"
    fact_table.to_csv(output_file, index=False)
    
    print(f"\nFactOccupancyAggregated table saved to: {output_file}")
    
    return fact_table

if __name__ == "__main__":
    print("Starting FactOccupancyAggregated Table Creation...")
    fact_occupancy_aggregated = create_fact_occupancy_aggregated()
    print("\nFactOccupancyAggregated creation complete!") 
