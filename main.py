#!/usr/bin/env python3
"""
Central entrypoint for COS Office Occupancy project.

Subcommands:
  run       Run the ETL pipeline (stages 1-9)
  validate  Generate validation report from outputs
  all       Run pipeline then validation (default)

Examples:
  python main.py                 # same as `all`
  python main.py run --from 3 --to 7
  python main.py validate --out reports
"""

from __future__ import annotations

import argparse
import sys

import run_pipeline
import validation_report


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Central runner for COS Office Occupancy")
    sub = parser.add_subparsers(dest='cmd')

    # run subcommand (proxy args to run_pipeline)
    p_run = sub.add_parser('run', help='Run ETL pipeline (stages 1-9)')
    p_run.add_argument('--from', dest='from_stage', type=int, default=1)
    p_run.add_argument('--to', dest='to_stage', type=int, default=9)
    p_run.add_argument('--only', dest='only', type=int, nargs='+')
    p_run.add_argument('--skip', dest='skip', type=int, nargs='+', default=[])
    p_run.add_argument('--dry-run', action='store_true')

    # validate subcommand
    p_val = sub.add_parser('validate', help='Generate validation report')
    p_val.add_argument('--out', default='reports')

    # all subcommand (default)
    p_all = sub.add_parser('all', help='Run pipeline then validation')
    p_all.add_argument('--from', dest='from_stage', type=int, default=1)
    p_all.add_argument('--to', dest='to_stage', type=int, default=9)
    p_all.add_argument('--only', dest='only', type=int, nargs='+')
    p_all.add_argument('--skip', dest='skip', type=int, nargs='+', default=[])
    p_all.add_argument('--dry-run', action='store_true')
    p_all.add_argument('--out', default='reports')

    return parser.parse_args(argv)


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    args = parse_args(argv)

    # Default to 'all' if no subcommand
    cmd = args.cmd or 'all'

    if cmd == 'run':
        return run_pipeline.main([
            '--from', str(args.from_stage),
            '--to', str(args.to_stage),
            *([] if not args.only else ['--only', *map(str, args.only)]),
            *([] if not args.skip else ['--skip', *map(str, args.skip)]),
            *(['--dry-run'] if args.dry_run else []),
        ])

    if cmd == 'validate':
        # validation_report.main() reads argparse itself, so call function directly
        return validation_report.validate(Path(args.out))

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
            from pathlib import Path
            return validation_report.validate(Path(args.out))
        return 0

    raise SystemExit(f"Unknown command: {cmd}")


if __name__ == '__main__':
    raise SystemExit(main())

