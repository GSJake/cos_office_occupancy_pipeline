#!/usr/bin/env python3
"""
Central entrypoint for COS Office Occupancy project.

Subcommands:
  run       Run the ETL pipeline (stages 1-9)
  validate  Generate validation report from outputs
  all       Run pipeline then validation (default)
  publish   Publish outputs to Delta tables (aggregated by default)

Examples:
  python main.py                 # same as `all`
  python main.py run --from 3 --to 7
  python main.py validate --out reports
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import run_pipeline
import validation_report
import publish_to_delta


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Central runner for COS Office Occupancy")
    sub = parser.add_subparsers(dest='cmd')

    # run subcommand (proxy args to run_pipeline)
    p_run = sub.add_parser('run', help='Run ETL pipeline (stages 1-9) and publish')
    p_run.add_argument('--from', dest='from_stage', type=int, default=1)
    p_run.add_argument('--to', dest='to_stage', type=int, default=9)
    p_run.add_argument('--only', dest='only', type=int, nargs='+')
    p_run.add_argument('--skip', dest='skip', type=int, nargs='+', default=[])
    p_run.add_argument('--dry-run', action='store_true')
    p_run.add_argument('--table', default='dev.jb_off_occ.fact_occupancy_aggregated')
    p_run.add_argument('--mode', default='overwrite', choices=['overwrite','append'])
    p_run.add_argument('--no-publish', action='store_true', help='Do not publish to Delta at the end')

    # validate subcommand
    p_val = sub.add_parser('validate', help='Generate validation report')
    p_val.add_argument('--out', default='reports')

    # all subcommand (default)
    p_all = sub.add_parser('all', help='Run pipeline then validation (and publish)')
    p_all.add_argument('--from', dest='from_stage', type=int, default=1)
    p_all.add_argument('--to', dest='to_stage', type=int, default=9)
    p_all.add_argument('--only', dest='only', type=int, nargs='+')
    p_all.add_argument('--skip', dest='skip', type=int, nargs='+', default=[])
    p_all.add_argument('--dry-run', action='store_true')
    p_all.add_argument('--out', default='reports')
    p_all.add_argument('--table', default='dev.jb_off_occ.fact_occupancy_aggregated')
    p_all.add_argument('--mode', default='overwrite', choices=['overwrite','append'])
    p_all.add_argument('--no-publish', action='store_true', help='Do not publish to Delta at the end')

    # publish subcommand
    p_pub = sub.add_parser('publish', help='Publish to Delta (aggregated)')
    p_pub.add_argument('--table', default='dev.jb_off_occ.fact_occupancy_aggregated')
    p_pub.add_argument('--mode', default='overwrite', choices=['overwrite','append'])

    # In Databricks/IPython, extra args like '-f <json>' are injected.
    # Use parse_known_args to ignore unknowns and default to 'all'.
    args, _unknown = parser.parse_known_args(argv)
    return args


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    # Databricks/IPython may inject args (e.g., -f <json>) when running a file.
    # If no known subcommand is present, default to running the full pipeline (+ validation).
    known_cmds = {'run', 'validate', 'all'}
    if not any(tok in known_cmds for tok in argv):
        rc = run_pipeline.main([])
        if rc != 0:
            return rc
        return validation_report.validate(Path('reports'))
    args = parse_args(argv)

    # Default to 'all' if no subcommand
    cmd = args.cmd or 'all'

    if cmd == 'run':
        rc = run_pipeline.main([
            '--from', str(args.from_stage),
            '--to', str(args.to_stage),
            *([] if not args.only else ['--only', *map(str, args.only)]),
            *([] if not args.skip else ['--skip', *map(str, args.skip)]),
            *(['--dry-run'] if args.dry_run else []),
        ])
        if rc != 0:
            return rc
        # Publish if not a dry-run and user didn't opt out
        if not args.dry_run and not args.no_publish:
            try:
                publish_to_delta.publish_fact_occupancy_aggregated(args.table, args.mode)
            except Exception as e:
                print(f"[publish] Skipped or failed: {e}")
        return 0

    if cmd == 'validate':
        # Call the function directly with provided output dir
        return validation_report.validate(Path(args.out))

    if cmd == 'publish':
        publish_to_delta.publish_fact_occupancy_aggregated(args.table, args.mode)
        return 0

    if cmd == 'all':
        rc = run_pipeline.main([
            '--from', str(args.from_stage),
            '--to', str(args.to_stage),
            *([] if not args.only else ['--only', *map(str, args.only)]),
            *([] if not args.skip else ['--skip', *map(str, args.skip)]),
            *(['--dry-run'] if args.dry_run else []),
        ])
        if rc != 0:
            return rc
        # Only validate if not a dry-run
        if not args.dry_run:
            return validation_report.validate(Path(args.out))
        # After validation, publish unless disabled
        if not getattr(args, 'no_publish', False):
            try:
                publish_to_delta.publish_fact_occupancy_aggregated(args.table, args.mode)
            except Exception as e:
                print(f"[publish] Skipped or failed: {e}")
        return 0

    raise SystemExit(f"Unknown command: {cmd}")


if __name__ == '__main__':
    raise SystemExit(main())
