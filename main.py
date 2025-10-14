#!/usr/bin/env python3
"""
Main Pipeline Orchestrator
Runs the entire office occupancy data pipeline from start to finish.

Pipeline Steps:
1. Convert Excel files to CSV
2. Combine CSV files by data type
3. Clean occupancy data
4. Clean deskcount data
5. Create date dimension
6. Create location dimension
7. Create line of business dimension
8. Create occupancy fact table
9. Create aggregated occupancy fact table
"""

import sys
import time
from datetime import datetime
from pathlib import Path

# Import all pipeline modules
from convert_xlsx_to_csv import convert_xlsx_to_csv
from combine_csv_files import combine_csv_files
from clean_occupancy_data import clean_occupancy_data
from clean_deskcount_data import clean_deskcount_data
from create_dim_date import create_dim_date
from create_dim_location import create_dim_location
from create_dim_line_of_business import create_dim_line_of_business
from create_fact_occupancy import create_fact_occupancy
from create_fact_occupancy_aggregated import create_fact_occupancy_aggregated


def print_header(step_num, step_name):
    """Print a formatted header for each pipeline step."""
    separator = "=" * 80
    print(f"\n{separator}")
    print(f"STEP {step_num}: {step_name}")
    print(f"{separator}\n")


def print_step_complete(step_num, duration):
    """Print completion message for a step."""
    print(f"\n{'─' * 80}")
    print(f"Step {step_num} completed in {duration:.2f} seconds")
    print(f"{'─' * 80}")


def run_pipeline(start_from_step=1, end_at_step=9):
    """
    Run the complete data pipeline.

    Args:
        start_from_step (int): Step number to start from (1-9)
        end_at_step (int): Step number to end at (1-9)
    """
    pipeline_start = time.time()

    print("\n" + "=" * 80)
    print("OFFICE OCCUPANCY DATA PIPELINE")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    steps_completed = []
    steps_failed = []

    try:
        # Step 1: Convert Excel to CSV
        if start_from_step <= 1 <= end_at_step:
            step_start = time.time()
            print_header(1, "Convert Excel Files to CSV")
            convert_xlsx_to_csv()
            duration = time.time() - step_start
            print_step_complete(1, duration)
            steps_completed.append(("Step 1: Convert Excel to CSV", duration))

        # Step 2: Combine CSV files
        if start_from_step <= 2 <= end_at_step:
            step_start = time.time()
            print_header(2, "Combine CSV Files")
            combine_csv_files()
            duration = time.time() - step_start
            print_step_complete(2, duration)
            steps_completed.append(("Step 2: Combine CSV Files", duration))

        # Step 3: Clean occupancy data
        if start_from_step <= 3 <= end_at_step:
            step_start = time.time()
            print_header(3, "Clean Occupancy Data")
            clean_occupancy_data()
            duration = time.time() - step_start
            print_step_complete(3, duration)
            steps_completed.append(("Step 3: Clean Occupancy Data", duration))

        # Step 4: Clean deskcount data
        if start_from_step <= 4 <= end_at_step:
            step_start = time.time()
            print_header(4, "Clean Deskcount Data")
            clean_deskcount_data()
            duration = time.time() - step_start
            print_step_complete(4, duration)
            steps_completed.append(("Step 4: Clean Deskcount Data", duration))

        # Step 5: Create date dimension
        if start_from_step <= 5 <= end_at_step:
            step_start = time.time()
            print_header(5, "Create Date Dimension")
            create_dim_date()
            duration = time.time() - step_start
            print_step_complete(5, duration)
            steps_completed.append(("Step 5: Create Date Dimension", duration))

        # Step 6: Create location dimension
        if start_from_step <= 6 <= end_at_step:
            step_start = time.time()
            print_header(6, "Create Location Dimension")
            create_dim_location()
            duration = time.time() - step_start
            print_step_complete(6, duration)
            steps_completed.append(("Step 6: Create Location Dimension", duration))

        # Step 7: Create line of business dimension
        if start_from_step <= 7 <= end_at_step:
            step_start = time.time()
            print_header(7, "Create Line of Business Dimension")
            create_dim_line_of_business()
            duration = time.time() - step_start
            print_step_complete(7, duration)
            steps_completed.append(("Step 7: Create LOB Dimension", duration))

        # Step 8: Create occupancy fact table
        if start_from_step <= 8 <= end_at_step:
            step_start = time.time()
            print_header(8, "Create Occupancy Fact Table")
            create_fact_occupancy()
            duration = time.time() - step_start
            print_step_complete(8, duration)
            steps_completed.append(("Step 8: Create Occupancy Fact Table", duration))

        # Step 9: Create aggregated occupancy fact table
        if start_from_step <= 9 <= end_at_step:
            step_start = time.time()
            print_header(9, "Create Aggregated Occupancy Fact Table")
            create_fact_occupancy_aggregated()
            duration = time.time() - step_start
            print_step_complete(9, duration)
            steps_completed.append(("Step 9: Create Aggregated Fact Table", duration))

    except Exception as e:
        print(f"\n{'!' * 80}")
        print(f"PIPELINE ERROR: {str(e)}")
        print(f"{'!' * 80}")
        steps_failed.append(str(e))
        raise

    finally:
        # Print final summary
        pipeline_duration = time.time() - pipeline_start

        print("\n" + "=" * 80)
        print("PIPELINE SUMMARY")
        print("=" * 80)

        if steps_completed:
            print("\nCompleted Steps:")
            for step_name, duration in steps_completed:
                print(f"  ✓ {step_name} ({duration:.2f}s)")

        if steps_failed:
            print("\nFailed Steps:")
            for error in steps_failed:
                print(f"  ✗ {error}")

        print(f"\nTotal Pipeline Duration: {pipeline_duration:.2f} seconds ({pipeline_duration/60:.2f} minutes)")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Output directory summary
        print("\nOutput Directories:")
        for directory in ['converted_data', 'combined_data', 'cleaned_data', 'dimensions', 'facts']:
            dir_path = Path(directory)
            if dir_path.exists():
                file_count = len(list(dir_path.glob('*.csv')))
                print(f"  {directory}/: {file_count} files")

        print("=" * 80 + "\n")


def main():
    """Main entry point for the pipeline."""

    # Default: run entire pipeline
    start_step = 1
    end_step = 9

    # Parse command-line arguments if provided
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print("""
Office Occupancy Data Pipeline
==============================

Usage:
    python main.py [start_step] [end_step]

Arguments:
    start_step: Step number to start from (1-9), default: 1
    end_step: Step number to end at (1-9), default: 9

Pipeline Steps:
    1. Convert Excel files to CSV
    2. Combine CSV files by data type
    3. Clean occupancy data
    4. Clean deskcount data
    5. Create date dimension
    6. Create location dimension
    7. Create line of business dimension
    8. Create occupancy fact table
    9. Create aggregated occupancy fact table

Examples:
    python main.py              # Run entire pipeline
    python main.py 1 4          # Run steps 1 through 4
    python main.py 5 9          # Run steps 5 through 9 (assumes steps 1-4 already completed)
    python main.py 8 9          # Run only fact table creation
            """)
            return

        try:
            start_step = int(sys.argv[1])
            if len(sys.argv) > 2:
                end_step = int(sys.argv[2])
        except ValueError:
            print("Error: Arguments must be integers between 1 and 9")
            return

    # Validate step numbers
    if not (1 <= start_step <= 9) or not (1 <= end_step <= 9):
        print("Error: Step numbers must be between 1 and 9")
        return

    if start_step > end_step:
        print("Error: Start step must be less than or equal to end step")
        return

    # Run the pipeline
    run_pipeline(start_from_step=start_step, end_at_step=end_step)


if __name__ == "__main__":
    main()
