#!/usr/bin/env python3
"""
Validation Report for COS Office Occupancy Pipeline

Checks data quality and common pitfalls affecting occupancy percentages.
Outputs a concise console summary and optional CSV reports under reports/.

Usage:
  python validation_report.py                  # validate default outputs
  python validation_report.py --out reports    # choose output folder
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def pct(n: int, d: int) -> str:
    return f"{(n/d*100):.1f}%" if d else "n/a"


def load_csv(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path, **kwargs)


def validate(out_dir: Path) -> int:
    out_dir.mkdir(exist_ok=True)

    # Load required outputs
    fact = load_csv(Path("facts/FactOccupancy.csv"), low_memory=False)
    fact_agg = load_csv(Path("facts/FactOccupancyAggregated.csv"))
    dim_date = load_csv(Path("dimensions/DimDate.csv"))
    dim_loc = load_csv(Path("dimensions/DimLocation.csv"))
    occ = load_csv(Path("cleaned_data/Occupancy_cleaned.csv"), low_memory=False)
    desk = load_csv(Path("cleaned_data/Deskcount_cleaned.csv"))

    # Normalize dtypes
    fact['date'] = pd.to_datetime(fact['date'])
    fact_agg['date'] = pd.to_datetime(fact_agg['date'])
    desk['date'] = pd.to_datetime(desk['date'])

    # Basic summaries
    summary_lines = []
    summary_lines.append("== Summary ==")
    summary_lines.append(f"Fact rows: {len(fact):,}; Agg rows: {len(fact_agg):,}")
    summary_lines.append(
        f"Fact date range: {fact['date'].min().date()} to {fact['date'].max().date()}"
    )
    summary_lines.append(
        f"Agg date range: {fact_agg['date'].min().date()} to {fact_agg['date'].max().date()}"
    )
    summary_lines.append(f"Locations: {fact['office_location'].nunique()} (dim: {len(dim_loc)})")
    summary_lines.append(
        f"LOBs: {fact['line_of_business'].nunique()} (in fact)"
    )

    # Weekend vs weekday occupancy
    if 'is_weekend' in fact.columns:
        wk = fact[~fact['is_weekend']]
        we = fact[fact['is_weekend']]
        summary_lines.append(
            f"Mean occupancy (weekday): {wk['occupancy_rate'].mean():.3f}; (weekend): {we['occupancy_rate'].mean():.3f}"
        )

    # Merge success: attendance>0 but deskcount==0
    mask_merge_issue = (fact['attendance_count'] > 0) & (fact['deskcount'] == 0)
    n_merge_issue = int(mask_merge_issue.sum())
    summary_lines.append(
        f"Rows with attendance>0 and deskcount==0: {n_merge_issue:,} ({pct(n_merge_issue, len(fact))})"
    )
    if n_merge_issue:
        df_merge_issues = (
            fact.loc[mask_merge_issue, ['date', 'office_location', 'line_of_business', 'attendance_count', 'deskcount']]
            .sort_values(['office_location', 'date'])
        )
        df_merge_issues.to_csv(out_dir / 'deskcount_merge_issues.csv', index=False)

    # Over-capacity days: occupancy_rate > 1.0
    mask_overcap = fact['occupancy_rate'] > 1.0
    n_overcap = int(mask_overcap.sum())
    summary_lines.append(
        f"Rows with occupancy_rate > 1.0: {n_overcap:,} ({pct(n_overcap, len(fact))})"
    )
    if n_overcap:
        df_overcap = (
            fact.loc[mask_overcap, ['date', 'office_location', 'line_of_business', 'attendance_count', 'deskcount', 'occupancy_rate']]
            .sort_values(['office_location', 'date'])
        )
        df_overcap.to_csv(out_dir / 'over_capacity_days.csv', index=False)

    # Deskcount recency vs occupancy recency
    latest_occ_date = pd.to_datetime(occ['logon_date']).max()
    latest_desk_date = desk['date'].max()
    gap_days = (latest_occ_date - latest_desk_date).days
    summary_lines.append(
        f"Latest occupancy: {latest_occ_date.date()}, latest deskcount: {latest_desk_date.date()}, gap: {gap_days} days"
    )

    # By location: rate mean (weekday), merge issues, overcap counts
    by_loc = []
    if 'is_weekend' in fact.columns:
        ff = fact[~fact['is_weekend']]
    else:
        ff = fact
    grp = ff.groupby('office_location')
    for loc, g in grp:
        issues = int(((g['attendance_count'] > 0) & (g['deskcount'] == 0)).sum())
        overc = int((g['occupancy_rate'] > 1.0).sum())
        by_loc.append(
            {
                'office_location': loc,
                'rows': len(g),
                'mean_occupancy_rate': round(float(g['occupancy_rate'].mean()), 4),
                'merge_issues': issues,
                'over_capacity_days': overc,
            }
        )
    df_by_loc = pd.DataFrame(by_loc).sort_values(['merge_issues', 'over_capacity_days', 'mean_occupancy_rate'], ascending=[False, False, True])
    df_by_loc.to_csv(out_dir / 'by_location_summary.csv', index=False)

    # Write summary text
    summary_path = out_dir / 'validation_summary.txt'
    summary_path.write_text("\n".join(summary_lines))

    # Console echo
    print("\n".join(summary_lines))
    print(f"\nDetailed CSVs written to: {out_dir}")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validation report for pipeline outputs")
    p.add_argument('--out', default='reports', help='Output directory for validation files')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out)
    return validate(out_dir)


if __name__ == '__main__':
    raise SystemExit(main())

