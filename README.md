COS Office Occupancy Pipeline

Project to process office occupancy and deskcount data into clean dimensions and fact tables for analysis and reporting.

Inputs
- New convention (recommended):
  - Deskcount: place files under `Inputs/Deskcount/` named like `YYYY_MM_deskcount.xlsx` (e.g., `2025_01_deskcount.xlsx`).
  - Occupancy: place files under `Inputs/Occupancy/` named like `YYYY_MM_occupancy.xlsx`.
- Legacy support: Files under `Inputs/<Type>/<Year>/*.xlsx` still work; the converter infers year-month from filenames or sheet dates.

Pipeline stages
- 1 Convert: Read Excel files from `Inputs/<Type>/` (and legacy `Inputs/<Type>/<Year>/`) and write CSVs under `converted_data/`.
  - Deskcount CSVs are written as `YYYY-MM_Deskcount.csv`; Occupancy as `YYYY-MM_Occupancy.csv`.
  - Combine prefers the new `YYYY-MM_*.csv` files; if both old and new exist, only new are used. Clean `converted_data/` if you want a fresh run.
- 2 Combine: Merge per-type CSVs into `combined_data/Occupancy.csv` and `combined_data/Deskcount.csv`.
- 3 Clean Occupancy: Normalize and de‑duplicate occupancy into `cleaned_data/Occupancy_cleaned.csv`.
- 4 Clean Deskcount: Select and normalize deskcount into `cleaned_data/Deskcount_cleaned.csv`.
- 5 DimDate: Generate 2024–2027 calendar in `dimensions/DimDate.csv`.
- 6 DimLocation: Build locations + RSF from data in `dimensions/DimLocation.csv`.
- 7 DimLineOfBusiness: Build LOB dimension in `dimensions/DimLineOfBusiness.csv`.
- 8 FactOccupancy: Attendance by date/location/LOB in `facts/FactOccupancy.csv`.
- 9 FactOccupancyAggregated: Attendance by date/location (all LOBs) in `facts/FactOccupancyAggregated.csv`.

Quick start
- Python 3.10+ recommended.
- Install deps: `python3 -m pip install -r requirements.txt`
- Place raw Excel files under `Inputs/Occupancy/<Year>/*.xlsx` and `Inputs/Deskcount/<Year>/*.xlsx`.
- Central entrypoint: `python3 main.py` (runs pipeline then validation).

Run options
- Full: `python3 main.py all --out reports`
- Pipeline only: `python3 main.py run --from 3 --to 7`
- Validation only: `python3 main.py validate --out reports`
- Run underlying runner directly: `python3 run_pipeline.py --only 1 2 3`

Repo layout
- Scripts: standalone Python files per stage (importable by the runner).
- Data folders are created as needed:
  - `converted_data/`, `combined_data/`, `cleaned_data/`, `dimensions/`, `facts/`.

Validation report
- Script: `validation_report.py` writes summary to `reports/validation_summary.txt` and CSVs:
  - `deskcount_merge_issues.csv`: attendance>0 with deskcount==0
  - `over_capacity_days.csv`: occupancy_rate>1.0
  - `by_location_summary.csv`: weekday mean rates, merge issues, over-capacity counts

Notes
- Requirements are minimal: `pandas`, `openpyxl`.
- Outputs are overwritten on re‑runs; keep originals in `Inputs/`.
 - In Databricks, install deps with: `%pip install -r requirements.txt`.
