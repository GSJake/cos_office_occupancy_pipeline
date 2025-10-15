#!/usr/bin/env python3
"""
Fix Databricks versions of create_fact_*.py files
Removes inline Delta write code to prevent schema conflicts.
Run this in Databricks to fix the files automatically.
"""

import re
from pathlib import Path

def fix_file(filepath):
    """Remove Delta write code after CSV save, keeping only CSV output."""
    print(f"\nFixing {filepath}...")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Find the end of CSV save section
    csv_save_pattern = r'(.*print\(f"\\n.*table saved to: \{output_file\}"\))\s*\n\s*return fact_table'
    
    # Check if file already has Delta write code
    if 'Writing to Databricks table' in content or 'spark.createDataFrame' in content:
        print(f"  ⚠️  Found Delta write code - removing it...")
        
        # Remove everything between "CSV saved" and "return fact_table"
        # Keep only the CSV save and return statement
        match = re.search(csv_save_pattern, content, re.DOTALL)
        
        if match:
            # Extract everything up to and including the CSV save message
            before_save = match.group(1)
            
            # Find the if __name__ section
            main_section = re.search(r'(if __name__ == "__main__":.*)', content, re.DOTALL)
            
            if main_section:
                # Reconstruct file: before save + return + main section
                new_content = before_save + "\n    \n    return fact_table\n\n" + main_section.group(1)
                
                # Write back
                with open(filepath, 'w') as f:
                    f.write(new_content)
                
                print(f"  ✅ Fixed! Removed Delta write code.")
                return True
        
        print(f"  ⚠️  Could not auto-fix. Manual removal needed.")
        return False
    else:
        print(f"  ✅ Already clean (no Delta write code found)")
        return True

def main():
    """Fix both fact table files."""
    print("=" * 60)
    print("Databricks File Fixer")
    print("Removes inline Delta write code from fact table scripts")
    print("=" * 60)
    
    files_to_fix = [
        'create_fact_occupancy_aggregated.py',
        'create_fact_occupancy.py'
    ]
    
    results = {}
    for filename in files_to_fix:
        filepath = Path(filename)
        if filepath.exists():
            results[filename] = fix_file(filepath)
        else:
            print(f"\n⚠️  {filename} not found - skipping")
            results[filename] = None
    
    print("\n" + "=" * 60)
    print("Summary:")
    for filename, result in results.items():
        if result is True:
            print(f"  ✅ {filename}")
        elif result is False:
            print(f"  ❌ {filename} - NEEDS MANUAL FIX")
        else:
            print(f"  ⊘  {filename} - not found")
    
    print("\nNext steps:")
    print("  1. Run: %run ./main.py run --from 6")
    print("  2. Check Delta table for results")
    print("=" * 60)

if __name__ == '__main__':
    main()
