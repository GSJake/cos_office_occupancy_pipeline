#!/usr/bin/env python3
"""
Step 6: Create DimLocation Table
Extract unique office locations and create location dimension table with RSF data.
"""

import pandas as pd
import re
from pathlib import Path

def create_dim_location():
    """Create location dimension table from cleaned occupancy data and RSF from deskcount data."""
    
    print("Creating DimLocation table...")
    
    # Load cleaned occupancy data for unique locations
    print("Loading cleaned occupancy data...")
    df_occupancy = pd.read_csv('cleaned_data/Occupancy_cleaned.csv', low_memory=False)
    print(f"Loaded occupancy data with {len(df_occupancy)} rows")
    
    # Load original deskcount data for RSF information
    print("Loading original deskcount data for RSF...")
    df_deskcount_original = pd.read_csv('combined_data/Deskcount.csv')
    print(f"Loaded original deskcount data with {len(df_deskcount_original)} rows")
    
    # Step 6a: Find all unique office_location values from occupancy data
    print(f"\nStep 6a: Finding unique office_location values...")
    # Normalize locations in occupancy data
    def _normalize_location(val):
        if pd.isna(val):
            return val
        s = str(val).strip()
        s = re.sub(r"\s+", " ", s)
        s = s.rstrip('.,;:')
        return s

    df_occupancy['office_location'] = df_occupancy['office_location'].map(_normalize_location)
    unique_locations = df_occupancy['office_location'].dropna().unique()
    unique_locations = sorted(unique_locations)  # Sort alphabetically for consistency
    
    print(f"Found {len(unique_locations)} unique office locations:")
    for i, location in enumerate(unique_locations[:10]):  # Show first 10
        print(f"  {i+1}. {location}")
    if len(unique_locations) > 10:
        print(f"  ... and {len(unique_locations) - 10} more")
    
    # Step 6b: Get RSF data for each location (use most recent RSF value)
    print(f"\nStep 6b: Getting RSF data for each location...")
    
    # Get the most recent RSF for each office location
    rsf_data = df_deskcount_original.groupby('OfficeLocation').agg({
        'RSF': 'last',  # Take the last (most recent) RSF value
        'Date': 'max'   # Show which date the RSF is from
    }).reset_index()
    
    rsf_data.rename(columns={'OfficeLocation': 'office_location'}, inplace=True)
    rsf_data['office_location'] = rsf_data['office_location'].map(_normalize_location)
    
    print(f"Found RSF data for {len(rsf_data)} locations")
    
    # Step 6c: Create location_key for each unique office_location
    print(f"\nStep 6c: Creating location_key and combining with RSF data...")
    
    # Create the dimension table with unique locations
    dim_location = pd.DataFrame({
        'office_location': unique_locations
    })
    
    # Add location_key (sequential integer starting from 1)
    dim_location['location_key'] = range(1, len(dim_location) + 1)
    
    # Merge with RSF data
    dim_location = dim_location.merge(
        rsf_data[['office_location', 'RSF']], 
        on='office_location', 
        how='left'
    )
    
    # Fill missing RSF with 0 if any locations don't have RSF data
    dim_location['RSF'] = dim_location['RSF'].fillna(0).astype(int)
    
    # Reorder columns
    dim_location = dim_location[['location_key', 'office_location', 'RSF']]
    
    # Display the complete dimension table
    print(f"\nComplete DimLocation table with RSF:")
    print(dim_location.to_string(index=False))
    
    print(f"\nData types:")
    print(dim_location.dtypes)
    
    print(f"\nSummary:")
    print(f"Total unique locations: {len(dim_location)}")
    print(f"Location key range: {dim_location['location_key'].min()} to {dim_location['location_key'].max()}")
    print(f"RSF range: {dim_location['RSF'].min()} to {dim_location['RSF'].max()}")
    print(f"Total RSF across all locations: {dim_location['RSF'].sum():,}")
    
    # Save the dimension table
    output_dir = Path("dimensions")
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / "DimLocation.csv"
    dim_location.to_csv(output_file, index=False)
    
    print(f"\nDimLocation table saved to: {output_file}")
    print(f"Final table shape: {dim_location.shape}")
    
    return dim_location

if __name__ == "__main__":
    print("Step 6: Creating DimLocation Table...")
    dim_location = create_dim_location()
    print("\nStep 6 complete!") 
