#!/usr/bin/env python3
"""
Create FactOccupancyAggregated Table
Build a comprehensive fact table showing attendance by office_location and date.
Aggregates attendance across all lines of business, includes 0s for days with no attendance, 
calculates occupancy rates, and flags hybrid days.
"""

import os
import pandas as pd
from pathlib import Path

def calculate_hybrid_day_flags(fact_table):
    """
    Calculate hybrid day flags for the fact table using vectorized logic.

    Rules:
    - ISO week grouping (week_year + week_of_year) per office_location
    - Only weekdays (Mon–Fri) are eligible
    - Only days from months that contribute >=3 weekdays within that ISO week are eligible
    - Mark at most the top-3 attendance days among eligible dates per location/week
    """
    print("Calculating hybrid day flags for each week/location combination (vectorized)...")

    # Ensure datetime
    fact_table['date'] = pd.to_datetime(fact_table['date'])

    # Date components
    fact_table['year'] = fact_table['date'].dt.year
    fact_table['month'] = fact_table['date'].dt.month
    # Use week periods ending on Sunday so start_time is Monday of the week
    fact_table['week_start'] = fact_table['date'].dt.to_period('W-SUN').dt.start_time
    fact_table['is_weekday_tmp'] = fact_table['date'].dt.dayofweek < 5

    # Precompute weekday counts per (ISO week, month) using unique dates only (no office dependency)
    date_df = fact_table[['date']].drop_duplicates().copy()
    date_df['week_start'] = date_df['date'].dt.to_period('W-SUN').dt.start_time
    date_df['month'] = date_df['date'].dt.month
    date_df['dow'] = date_df['date'].dt.dayofweek  # 0=Mon..6=Sun

    weekday_counts = (
        date_df[date_df['dow'] < 5]
        .groupby(['week_start', 'month'])['date']
        .nunique()
        .reset_index(name='weekday_count_in_month_week')
    )

    # Derive per-date eligibility (weekday and month contributes >=3 weekdays in that ISO week)
    date_elig = date_df.merge(
        weekday_counts,
        on=['week_start', 'month'],
        how='left'
    )
    date_elig['weekday_count_in_month_week'] = date_elig['weekday_count_in_month_week'].fillna(0).astype(int)
    date_elig['eligible_date'] = (date_elig['dow'] < 5) & (date_elig['weekday_count_in_month_week'] >= 3)

    # Daily totals per (location, iso-week, date) for ranking
    daily_totals = (
        fact_table.groupby(['office_location', 'week_start', 'date'])['attendance_count']
        .sum()
        .reset_index(name='daily_total_attendance')
    )

    # Add per-date eligibility to daily totals
    daily_totals = daily_totals.merge(
        date_elig[['date', 'eligible_date']], on='date', how='left'
    )
    daily_totals['eligible'] = daily_totals['eligible_date'].fillna(False)

    # Select top 3 eligible dates per (location, iso-week). Allow zero-attendance weekdays
    # so long as they meet the month/week rule.
    daily_totals_eligible = daily_totals[
        daily_totals['eligible']
    ].copy()
    daily_totals_eligible.sort_values(
        ['office_location', 'week_start', 'daily_total_attendance'],
        ascending=[True, True, False],
        inplace=True
    )
    top3 = daily_totals_eligible.groupby(['office_location', 'week_start']).head(3)
    top_keys = set(zip(top3['office_location'], top3['week_start'], top3['date']))

    # Initialize flag False
    fact_table['is_hybrid_day'] = False

    # Mark True when date is in top3 set
    sel = [
        (ol, ws, d) in top_keys
        for ol, ws, d in zip(
            fact_table['office_location'],
            fact_table['week_start'],
            fact_table['date'],
        )
    ]
    fact_table.loc[sel, 'is_hybrid_day'] = True

    # Hard eligibility guard: never flag a day if its date-level eligibility is False
    fact_table = fact_table.merge(date_elig[['date', 'eligible_date']], on='date', how='left')
    fact_table['is_hybrid_day'] = fact_table['is_hybrid_day'] & fact_table['eligible_date'].fillna(False)
    fact_table.drop(columns=['eligible_date'], inplace=True)

    # Drop temporary columns
    fact_table.drop(columns=['week_start', 'is_weekday_tmp'], inplace=True)

    # Optional debug for London W27 2025
    if os.getenv('HYBRID_DEBUG_W27', '').lower() in ('1','true','yes'):
        target = pd.Timestamp('2025-06-30')
        ws = target.to_period('W-SUN').start_time
        dbg = date_elig[date_elig['week_start'] == ws][['date','month','dow','weekday_count_in_month_week','eligible_date']].sort_values('date')
        print("[debug] date eligibility for week starting", ws.date())
        print(dbg.to_string(index=False))
        # Show any violations (should be none)
        viol = fact_table[(fact_table['week_start'] == ws) & fact_table['is_hybrid_day']]
        if not viol.empty:
            print("[debug] flagged hybrid rows for that week (post-guard):")
            print(viol[['date','office_location','attendance_count','is_hybrid_day']].sort_values(['office_location','date']).to_string(index=False))

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
    
    # Limit scope to the occupancy data window (capped to end of latest deskcount month)
    first_occ_date = occupancy_data['logon_date'].min()
    last_occ_date = occupancy_data['logon_date'].max()
    latest_desk_date = deskcount_data['date'].max()
    if pd.notna(latest_desk_date):
        coverage_end = min(last_occ_date, (latest_desk_date + pd.offsets.MonthEnd(0)))
    else:
        coverage_end = last_occ_date

    occupancy_data = occupancy_data[occupancy_data['logon_date'] <= coverage_end]
    dim_date = dim_date[(dim_date['date'] >= first_occ_date) & (dim_date['date'] <= coverage_end)]

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

    # Ensure both dataframes are sorted by by-keys and 'on' column for merge_asof
    fact_table = fact_table.sort_values(['date', 'office_location'], kind='mergesort').reset_index(drop=True)
    deskcount_data = deskcount_data.sort_values(['date', 'office_location'], kind='mergesort').reset_index(drop=True)

    # Use merge_asof to efficiently find the last known deskcount for each date and location
    fact_table = pd.merge_asof(
        fact_table,
        deskcount_data[['date', 'office_location', 'deskcount']],
        on='date',
        by='office_location',
        direction='backward'
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
