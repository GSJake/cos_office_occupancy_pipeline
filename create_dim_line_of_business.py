#!/usr/bin/env python3
"""
Step 7: Create DimLineOfBusiness Table
Extract unique line of business values and create line of business dimension table.
"""

import pandas as pd
from pathlib import Path

def create_dim_line_of_business():
    """Create line of business dimension table from cleaned occupancy data."""
    
    print("Creating DimLineOfBusiness table...")
    
    # Load cleaned occupancy data
    print("Loading cleaned occupancy data...")
    df_occupancy = pd.read_csv('cleaned_data/Occupancy_cleaned.csv', low_memory=False)
    print(f"Loaded occupancy data with {len(df_occupancy)} rows")
    
    # Step 7a: Find all unique line_of_business values
    print(f"\nStep 7a: Finding unique line_of_business values...")
    unique_lobs = df_occupancy['line_of_business'].dropna().unique()
    unique_lobs = sorted(unique_lobs)  # Sort alphabetically for consistency
    
    print(f"Found {len(unique_lobs)} unique line of business values:")
    for i, lob in enumerate(unique_lobs):
        print(f"  {i+1}. {lob}")
    
    # Check the distribution of line of business values
    print(f"\nDistribution of line_of_business values:")
    lob_counts = df_occupancy['line_of_business'].value_counts()
    print(lob_counts)
    
    # Step 7b: Create lob_key for each unique line_of_business
    print(f"\nStep 7b: Creating lob_key for each line_of_business...")
    
    # Create the dimension table
    dim_lob = pd.DataFrame({
        'line_of_business': unique_lobs
    })
    
    # Add lob_key (sequential integer starting from 1)
    dim_lob['lob_key'] = range(1, len(dim_lob) + 1)
    
    # Reorder columns to put key first
    dim_lob = dim_lob[['lob_key', 'line_of_business']]
    
    # Display the complete dimension table
    print(f"\nComplete DimLineOfBusiness table:")
    print(dim_lob.to_string(index=False))
    
    print(f"\nData types:")
    print(dim_lob.dtypes)
    
    print(f"\nSummary:")
    print(f"Total unique line of business: {len(dim_lob)}")
    print(f"LOB key range: {dim_lob['lob_key'].min()} to {dim_lob['lob_key'].max()}")
    
    # Save the dimension table
    output_dir = Path("dimensions")
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / "DimLineOfBusiness.csv"
    dim_lob.to_csv(output_file, index=False)
    
    print(f"\nDimLineOfBusiness table saved to: {output_file}")
    print(f"Final table shape: {dim_lob.shape}")
    
    return dim_lob

if __name__ == "__main__":
    print("Step 7: Creating DimLineOfBusiness Table...")
    dim_lob = create_dim_line_of_business()
    print("\nStep 7 complete!") 