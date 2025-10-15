# Deployment Guide - COS Office Occupancy Pipeline

## Overview

This pipeline follows a **separation of concerns** architecture:
- **Pipeline scripts** (create_*.py) generate CSV files locally or in Databricks
- **Publisher script** (publish_to_delta.py) handles Delta table writes with proper schema management

## Local Development Workflow

```bash
# Run full pipeline to generate CSVs
python run_pipeline.py

# Or run specific stages
python run_pipeline.py --from 6  # Regenerate dimensions and facts
```

## Databricks Deployment Workflow

### Step 1: Sync Code to Databricks

```bash
# From your local repo
git pull origin master

# In Databricks
%sh
cd /Workspace/Users/<your.email>/cos_office_occupancy_pipeline
git pull origin master
```

### Step 2: Run Pipeline (CSV Generation)

```python
# In Databricks notebook
%run ./run_pipeline.py

# Or run from specific stage
%run ./run_pipeline.py --from 6
```

**Important**: The pipeline scripts (create_fact_*.py) should **only write CSVs**. They should NOT contain Databricks-specific Delta write code.

### Step 3: Publish to Delta Tables

```python
# Publish aggregated fact table
%run ./publish_to_delta.py --table dev.jb_off_occ.fact_occupancy_aggregated --mode overwrite

# Or use custom table name
%run ./publish_to_delta.py --table your_catalog.your_schema.your_table --mode overwrite
```

## Why This Architecture?

### ✅ Benefits

1. **Schema Evolution**: `publish_to_delta.py` handles schema changes with `overwriteSchema=true`
2. **Environment Agnostic**: Same pipeline code works locally and in Databricks
3. **Separation of Concerns**: Data transformation (pipeline) vs. storage (publisher)
4. **Testing**: Can test pipeline locally without Databricks connection
5. **Flexibility**: Easy to publish to different tables or catalogs

### ❌ Anti-Pattern: Inline Delta Writes

**DO NOT** add Delta write code directly to `create_fact_*.py` files:

```python
# ❌ BAD - Don't add this to create_fact_*.py
spark_df = spark.createDataFrame(fact_table)
spark_df.write.saveAsTable("dev.jb_off_occ.fact_occupancy_aggregated")
```

**Why?**
- Breaks local development (requires Spark/Databricks)
- Schema conflicts when types change
- Harder to test and maintain
- Coupling of concerns

## Troubleshooting

### "DELTA_FAILED_TO_MERGE_FIELDS" Error

**Cause**: Schema mismatch between new data and existing Delta table

**Solution**: Use `publish_to_delta.py` which has `overwriteSchema=true`:
```python
%run ./publish_to_delta.py --table dev.jb_off_occ.fact_occupancy_aggregated --mode overwrite
```

Or drop and recreate the table:
```sql
DROP TABLE IF EXISTS dev.jb_off_occ.fact_occupancy_aggregated;
```

### Missing location_key Values

**Cause**: DimLocation doesn't include all locations from source data

**Solution**: Re-run from stage 6 to regenerate dimensions:
```python
%run ./run_pipeline.py --from 6
%run ./publish_to_delta.py --table dev.jb_off_occ.fact_occupancy_aggregated --mode overwrite
```

## File Structure

```
create_dim_*.py          → Generate dimension CSVs
create_fact_*.py         → Generate fact CSVs
publish_to_delta.py      → Publish CSVs to Delta tables
run_pipeline.py          → Orchestrate pipeline stages
```

## Best Practices

1. **Always use publish_to_delta.py** for Delta writes
2. **Keep pipeline scripts environment-agnostic** (CSV output only)
3. **Test locally first** before deploying to Databricks
4. **Version control** all pipeline changes
5. **Document schema changes** in commit messages

---

## Using main.py (Recommended Entrypoint)

### Quick Start

```python
# Run everything: pipeline + validation + publish
python main.py

# Or explicitly
python main.py all
```

### Subcommands

**1. Run Pipeline Only (+ Auto-Publish)**
```python
python main.py run
python main.py run --from 6  # Start from stage 6
python main.py run --no-publish  # Skip Delta publish
```

**2. Validation Only**
```python
python main.py validate
python main.py validate --out my_reports
```

**3. Publish Only**
```python
python main.py publish
python main.py publish --table dev.jb_off_occ.custom_table --mode append
```

**4. Full Workflow (Default)**
```python
python main.py all
# Equivalent to:
# 1. python run_pipeline.py
# 2. python validation_report.py
# 3. python publish_to_delta.py
```

### Key Benefits

1. **Single Command**: No need to manually chain scripts
2. **Auto-Publish**: Automatically publishes to Delta after pipeline succeeds
3. **Validation**: Generates data quality reports automatically
4. **Spark Session**: Handles Spark initialization for Databricks

### Comparison

| Feature | main.py | run_pipeline.py + manual steps |
|---------|---------|-------------------------------|
| Pipeline execution | ✅ | ✅ |
| Validation report | ✅ Auto | ❌ Manual |
| Delta publish | ✅ Auto | ❌ Manual |
| Spark setup | ✅ Auto | ❌ Manual |
| Commands needed | 1 | 3 |

### When to Use Each

**Use `main.py`** (recommended):
- Production workflows
- Databricks scheduled jobs
- Complete end-to-end runs
- When you want validation + publish automatically

**Use `run_pipeline.py`**:
- Local development/testing
- When you only need CSVs
- Custom workflows with manual control
- Debugging specific stages

