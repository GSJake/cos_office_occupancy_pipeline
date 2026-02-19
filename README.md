COS Office Occupancy Pipeline

SP Link: https://greystar365.sharepoint.com/sites/CorporateOfficeStrategy/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FCorporateOfficeStrategy%2FShared%20Documents%2FGeneral%2FOccupancy%20Data%2FDashboard%2FInputs&viewid=4e59d49e%2Ddb8e%2D4efe%2D905d%2De1d52f2702ca&FolderCTID=0x0120006DA6C298DDD3814BA348924264A470BC

Project to process office occupancy and deskcount data into clean dimensions and fact tables for analysis and reporting.

Inputs
- **Automatic (recommended):** Stage 0 fetches Excel files from SharePoint automatically. See "SharePoint setup" below.
- Manual: place files under `Inputs/Deskcount/` named like `YYYY_MM_deskcount.xlsx` and `Inputs/Occupancy/YYYY_MM_occupancy.xlsx`.
- Legacy support: Files under `Inputs/<Type>/<Year>/*.xlsx` still work; the converter infers year-month from filenames or sheet dates.

Pipeline stages
- 0 Fetch: Download .xlsx files from SharePoint into `Inputs/Deskcount/` and `Inputs/Occupancy/`. Skip with `--skip-fetch`.
- 1 Convert: Read Excel files from `Inputs/<Type>/` (and legacy `Inputs/<Type>/<Year>/`) and write CSVs under `converted_data/`.
  - Deskcount CSVs are written as `YYYY-MM_Deskcount.csv`; Occupancy as `YYYY-MM_Occupancy.csv`.
  - Combine prefers the new `YYYY-MM_*.csv` files; if both old and new exist, only new are used. Clean `converted_data/` if you want a fresh run.
  - Converter now clears previous files in `converted_data/*` and `combined_data/*.csv` at start to avoid stale outputs.
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
- Full: `python3 main.py all --out reports` (runs pipeline, validation, then publishes to Delta)
- Skip SharePoint fetch: `python3 main.py run --skip-fetch` (use existing local files)
- Pipeline only: `python3 main.py run --from 3 --to 7` (publishes by default; disable with `--no-publish`)
- Validation only: `python3 main.py validate --out reports`
- Run underlying runner directly: `python3 run_pipeline.py --only 1 2 3`
- Publish aggregated fact to Delta (Databricks):
  - Auto-publish at the end of `main.py all` to `dev.jb_off_occ.fact_occupancy_aggregated` (disable with `--no-publish`).
  - Manual publish: `python3 main.py publish --table dev.jb_off_occ.fact_occupancy_aggregated --mode overwrite`
  - Requires a Spark session (run inside Databricks). Writes from `facts/FactOccupancyAggregated.csv`.

Repo layout
- Scripts: standalone Python files per stage (importable by the runner).
- Data folders are created as needed:
  - `converted_data/`, `combined_data/`, `cleaned_data/`, `dimensions/`, `facts/`.

Validation report
- Script: `validation_report.py` writes summary to `reports/validation_summary.txt` and CSVs:
  - `deskcount_merge_issues.csv`: attendance>0 with deskcount==0
  - `over_capacity_days.csv`: occupancy_rate>1.0
  - `by_location_summary.csv`: weekday mean rates, merge issues, over-capacity counts

SharePoint setup
Stage 0 uses the "Office Occupancy Power BI Report" app registration to fetch files via Microsoft Graph API.

Databricks (production): Create a secret scope and store credentials:
```
databricks secrets put-secret office-occupancy client-id --string-value "216fbee8-cb8b-4754-8b2c-4ae797c07e0f"
databricks secrets put-secret office-occupancy client-secret --string-value "<your-secret>"
databricks secrets put-secret office-occupancy tenant-id --string-value "15cb6c53-0a50-4876-a66c-9a753d760a7d"
```

Local dev: Set environment variables instead:
```
export SP_CLIENT_ID="216fbee8-cb8b-4754-8b2c-4ae797c07e0f"
export SP_CLIENT_SECRET="<your-secret>"
export SP_TENANT_ID="15cb6c53-0a50-4876-a66c-9a753d760a7d"
```

Document library name: Defaults to `"Documents"`. If files are in a different library (e.g., `"Shared Documents"`), override with `--library`:
```
python fetch_sharepoint_files.py --library "Shared Documents"
```

Notes
- Requirements: `pandas`, `openpyxl`, `msal`, `requests`.
- Outputs are overwritten on re‑runs; keep originals in `Inputs/`.
 - In Databricks, install deps with: `%pip install -r requirements.txt`.
