#!/usr/bin/env python3
"""
Step 6: Create DimLocation Table
Extract unique office locations and create location dimension table with RSF and location metadata.
"""

import pandas as pd
from pathlib import Path

def create_dim_location():
    """Create location dimension table from occupancy and deskcount data."""
    
    print("Creating DimLocation table...")

    # Load occupancy data
    print("Loading cleaned occupancy data...")
    df_occupancy = pd.read_csv('cleaned_data/Occupancy_cleaned.csv')
    print(f"Loaded occupancy data with {len(df_occupancy)} rows")
    
    # Load cleaned deskcount data (for RSF values)
    # Note: This uses the cleaned file which filters by "Include in Occupancy Calculation" flag
    print("Loading cleaned deskcount data for RSF...")
    df_deskcount = pd.read_csv('cleaned_data/Deskcount_cleaned.csv')
    print(f"Loaded cleaned deskcount data with {len(df_deskcount)} rows")

    # Also load raw deskcount to get RSF values (RSF column not in cleaned data)
    print("Loading raw deskcount for RSF values...")
    df_deskcount_raw = pd.read_csv('combined_data/Deskcount.csv')

    # Filter raw deskcount by Include flag for consistency
    df_deskcount_raw = df_deskcount_raw[df_deskcount_raw['Include in Occupancy Calculation'] == 'Yes']
    print(f"Filtered to {len(df_deskcount_raw)} rows where Include = Yes")
    
    # Get the most recent RSF and Date for each office
    print("\nGetting most recent RSF for each location...")
    rsf_data = (
        df_deskcount_raw.groupby('OfficeLocation')
        .agg({'RSF': 'last', 'Date': 'max'})
        .reset_index()
        .rename(columns={'OfficeLocation': 'office_location'})
    )
    print(f"Found RSF data for {len(rsf_data)} locations (filtered by Include flag)")

    # Extract unique office locations from occupancy data
    print("\nExtracting unique office locations...")
    unique_locations = df_occupancy['office_location'].dropna().unique()
    unique_locations = sorted(unique_locations)  # Sort alphabetically for consistency
    print(f"Found {len(unique_locations)} unique office locations")
    
    # Create base dimension table with unique locations
    dim_location = pd.DataFrame({
        'office_location': unique_locations
    })

    # Merge with RSF data
    dim_location = pd.merge(dim_location, rsf_data[['office_location', 'RSF']], 
                            on='office_location', how='left')

    # Add location_key (sequential integer starting from 1)
    dim_location.insert(0, 'location_key', range(1, len(dim_location) + 1))

    # Fill missing RSF with 0
    dim_location['RSF'] = dim_location['RSF'].fillna(0).astype(int)
    
    # Display summary
    print(f"\nDimLocation table summary:")
    print(f"Total unique locations: {len(dim_location)}")
    print(f"Location key range: {dim_location['location_key'].min()} to {dim_location['location_key'].max()}")
    print(f"RSF range: {dim_location['RSF'].min():,} to {dim_location['RSF'].max():,}")
    print(f"Total RSF across all locations: {dim_location['RSF'].sum():,}")
    
    print(f"\nSample data (first 10 rows):")
    print(dim_location.head(10).to_string(index=False))
    
    # Save to CSV for local backup
    output_dir = Path("dimensions")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "DimLocation.csv"
    dim_location.to_csv(output_file, index=False)
    print(f"\nLocal CSV saved to: {output_file}")

    # Write to Databricks table
    print("\nWriting to Databricks table...")
    spark_df = spark.createDataFrame(dim_location)
    spark_df.write.mode("overwrite").saveAsTable("dev.jb_off_occ.dim_location")
    print("DimLocation table written to dev.jb_off_occ.dim_location")
    
    print(f"\nFinal table shape: {dim_location.shape}")
    
    return dim_location

if __name__ == "__main__":
    print("Step 6: Creating DimLocation Table...")
    dim_location = create_dim_location()
    print("\nStep 6 complete!") 