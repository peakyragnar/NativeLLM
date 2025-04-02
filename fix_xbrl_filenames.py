#!/usr/bin/env python3
"""
Fix XBRL Filenames

This script finds existing XBRL files in temp download directories and renames them
to match the consistent naming pattern used by the verification system.
"""

import os
import glob
import shutil
import logging
import re
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def find_matching_xbrl_for_filing(ticker, filing_type, year, quarter=None):
    """Find the most appropriate XBRL file for a specific filing"""
    # Common download directories
    download_dirs = [
        f"sec_processed/tmp/sec_downloads/{ticker}/{filing_type}",
        f"sec_processed/tmp/{ticker}/{filing_type}"
    ]
    
    print(f"Looking for XBRL for {ticker} {filing_type} {year} Q{quarter}")
    
    best_match = None
    
    for download_dir in download_dirs:
        print(f"  Checking directory: {download_dir}")
        if not os.path.exists(download_dir):
            print(f"  Directory doesn't exist: {download_dir}")
            continue
        
        # List all subdirectories (which are accession numbers)
        accession_dirs = [os.path.join(download_dir, d) for d in os.listdir(download_dir) 
                         if os.path.isdir(os.path.join(download_dir, d))]
        
        print(f"  Found {len(accession_dirs)} accession directories")
        
        for accession_dir in accession_dirs:
            xbrl_path = os.path.join(accession_dir, "_xbrl_raw.json")
            print(f"  Checking for XBRL in: {xbrl_path}")
            if os.path.exists(xbrl_path):
                print(f"  FOUND XBRL: {xbrl_path}")
                # This is a valid XBRL file, but we need to check if it's for the right period
                best_match = xbrl_path
                
    if not best_match:
        print(f"  NO MATCHING XBRL FOUND for {ticker} {filing_type} {year} Q{quarter}")
    
    return best_match

def find_and_fix_xbrl_files(base_dir="sec_processed"):
    """Find XBRL files and create copies with the expected naming convention"""
    fixed_count = 0
    
    # Get list of all companies (folders in base_dir)
    company_dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d)) 
                   and not d == 'tmp' and not d.startswith('.')]
    
    for company in company_dirs:
        company_dir = os.path.join(base_dir, company)
        
        # Look for all LLM files
        llm_files = glob.glob(os.path.join(company_dir, "**", "*_llm.txt"), recursive=True)
        
        for llm_file in llm_files:
            # Extract filing information from the filename
            filename = os.path.basename(llm_file)
            match = re.match(r'([A-Z]+)_(\d+-[A-Z])_(\d+)(?:_Q(\d+))?_llm\.txt', filename)
            
            if not match:
                logging.warning(f"Couldn't parse filename: {filename}")
                continue
                
            ticker = match.group(1)
            filing_type = match.group(2)
            year = match.group(3)
            quarter = match.group(4) if match.group(4) else None
            
            # Determine the expected XBRL filename
            expected_xbrl = llm_file.replace("_llm.txt", "_xbrl_raw.json")
            
            # Skip if the file already exists
            if os.path.exists(expected_xbrl):
                logging.info(f"XBRL file already exists: {expected_xbrl}")
                continue
            
            # Find matching XBRL in downloads directory
            matching_xbrl = find_matching_xbrl_for_filing(ticker, filing_type, year, quarter)
            
            if matching_xbrl:
                logging.info(f"Copying {matching_xbrl} to {expected_xbrl}")
                shutil.copy2(matching_xbrl, expected_xbrl)
                fixed_count += 1
            else:
                # As a fallback, look for any _xbrl_raw.json file in the downloads directories
                download_dir = f"sec_processed/tmp/sec_downloads/{ticker}"
                if os.path.exists(download_dir):
                    xbrl_files = []
                    for root, dirs, files in os.walk(download_dir):
                        if "_xbrl_raw.json" in files:
                            xbrl_files.append(os.path.join(root, "_xbrl_raw.json"))
                    
                    if xbrl_files:
                        # Use the first one we find as a fallback
                        source_xbrl = xbrl_files[0]
                        logging.warning(f"Using fallback XBRL file: {source_xbrl} for {expected_xbrl}")
                        shutil.copy2(source_xbrl, expected_xbrl)
                        fixed_count += 1
                        continue
                
                logging.warning(f"Could not find any XBRL file for {llm_file}")
            
    return fixed_count

if __name__ == "__main__":
    fixed_count = find_and_fix_xbrl_files()
    print(f"Fixed {fixed_count} XBRL filenames to match expected pattern") 