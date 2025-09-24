#!/usr/bin/env python3
"""
Office-level YTD Attendance and 5-day LOB Distribution

Outputs a CSV with, per office:
- YTD Avg. Attendance Count (avg daily in-office attendance on weekdays, YTD)
- % of population by Line of Business (5-day), approximated using non-hybrid weekdays

Assumptions:
- "YTD" means from Jan 1 of the latest year present in facts to the latest available date.
- "5-day" population is approximated by using non-hybrid weekdays (is_hybrid_day == False and is_weekend == False).
- Source: facts/FactOccupancy.csv produced by the pipeline.

Writes to: reports/office_lob_ytd_5day.csv
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> int:
    facts_path = Path("facts/FactOccupancy.csv")
    if not facts_path.exists():
        raise FileNotFoundError(f"Missing required file: {facts_path}")

    df = pd.read_csv(facts_path, low_memory=False)

    # Normalize dtypes
    df['date'] = pd.to_datetime(df['date'])

    # Determine YTD year = year of the latest date with non-zero attendance (fallback to max year)
    nonzero = df[df['attendance_count'] > 0]
    if len(nonzero) > 0:
        latest_nonzero_date = pd.to_datetime(nonzero['date']).max()
        latest_year = int(latest_nonzero_date.year)
        latest_cutoff_date = latest_nonzero_date
    else:
        latest_year = int(df['date'].dt.year.max())
        latest_cutoff_date = pd.to_datetime(df['date']).max()

    # Filter YTD (Jan 1 to latest nonzero date of that year) and weekdays
    df_ytd = df[(df['date'].dt.year == latest_year) & (df['date'] <= latest_cutoff_date)].copy()
    if 'is_weekend' in df_ytd.columns:
        df_ytd = df_ytd[~df_ytd['is_weekend']]
    else:
        # Fallback: compute weekday flag if not present
        df_ytd = df_ytd[df_ytd['date'].dt.dayofweek < 5]

    # YTD Avg. Attendance Count per office (sum LOB per day, then mean over days)
    daily_office_totals = (
        df_ytd.groupby(['office_location', 'date'])['attendance_count']
        .sum()
        .reset_index(name='attendance_total')
    )
    ytd_avg_attendance = (
        daily_office_totals.groupby('office_location')['attendance_total']
        .mean()
        .reset_index(name='ytd_avg_attendance_count')
    )

    # 5-day LOB distribution: use non-hybrid weekdays as proxy for 5-day
    if 'is_hybrid_day' in df_ytd.columns:
        df_5day = df_ytd[df_ytd['is_hybrid_day'] == False].copy()
    else:
        # If hybrid flag is missing, fall back to all weekdays
        df_5day = df_ytd.copy()

    lob_sums = (
        df_5day.groupby(['office_location', 'line_of_business'])['attendance_count']
        .sum()
        .reset_index(name='lob_attendance')
    )

    office_totals = (
        lob_sums.groupby('office_location')['lob_attendance']
        .sum()
        .reset_index(name='office_attendance_5day')
    )

    lob_pct = lob_sums.merge(office_totals, on='office_location', how='left')
    lob_pct['pct_5day'] = lob_pct['lob_attendance'] / lob_pct['office_attendance_5day']

    # Pivot to columns by LOB
    pivot = (
        lob_pct.pivot(index='office_location', columns='line_of_business', values='pct_5day')
        .reset_index()
    )

    # Map column names to requested labels
    col_map = {
        'Corporate': 'Corporate (5-day %)',
        'Development and Construction': 'Development & Construction (5-day %)',
        'Investment Management': 'Investment Management (5-day %)',
        'Property Management': 'Property Management (5-day %)',
    }
    pivot = pivot.rename(columns=col_map)

    # Merge with YTD avg attendance
    result = ytd_avg_attendance.merge(pivot, on='office_location', how='left')

    # Order columns
    ordered_cols = ['office_location', 'ytd_avg_attendance_count',
                    'Development & Construction (5-day %)',
                    'Investment Management (5-day %)',
                    'Property Management (5-day %)',
                    'Corporate (5-day %)']
    # Ensure all expected columns exist
    for c in ordered_cols:
        if c not in result.columns:
            result[c] = pd.NA
    result = result[ordered_cols]

    # Formatting: round attendance to 1 decimal, pct to 1 decimal percent string
    result['ytd_avg_attendance_count'] = result['ytd_avg_attendance_count'].round(1)
    pct_cols = [c for c in result.columns if c.endswith('(5-day %)')]
    for c in pct_cols:
        result[c] = (result[c] * 100).round(1)

    # Write to reports
    out_dir = Path('reports')
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / 'office_lob_ytd_5day.csv'
    result.to_csv(out_path, index=False)

    print(f"Wrote: {out_path}")
    print(result.head(10).to_string(index=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
