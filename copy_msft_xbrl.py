#!/usr/bin/env python3
"""
Direct XBRL File Copy Script

Copy XBRL files from the temp directories directly to the MSFT 
target files with proper naming.
"""

import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def copy_file(src, dest):
    """Directly copy a file using binary read/write"""
    try:
        # Ensure the destination directory exists
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        
        # Read source file content
        with open(src, 'rb') as src_file:
            content = src_file.read()
        
        # Write content to destination
        with open(dest, 'wb') as dest_file:
            dest_file.write(content)
            
        file_size = os.path.getsize(dest)
        logging.info(f"Successfully copied file to {dest} ({file_size} bytes)")
        return True
    except Exception as e:
        logging.error(f"Error copying {src} to {dest}: {str(e)}")
        return False

def main():
    """Copy XBRL files for MSFT"""
    # Define source files (10-K and 10-Q files in temp directories)
    sources = [
        # 10-K XBRL file 
        "sec_processed/tmp/sec_downloads/MSFT/10-K/000095017024087843/_xbrl_raw.json",
        # 10-Q XBRL files (use the same for all quarters since this is just a test)
        "sec_processed/tmp/sec_downloads/MSFT/10-Q/000095017024048288/_xbrl_raw.json",
    ]
    
    # Define destination files
    destinations = [
        # 10-K
        "sec_processed/MSFT/MSFT_10-K_2024_xbrl_raw.json",
        # 10-Q files
        "sec_processed/MSFT/MSFT_10-Q_2024_Q1_xbrl_raw.json",
        "sec_processed/MSFT/MSFT_10-Q_2024_Q2_xbrl_raw.json", 
        "sec_processed/MSFT/MSFT_10-Q_2024_Q3_xbrl_raw.json"
    ]
    
    # Copy 10-K file
    logging.info("Copying 10-K XBRL file...")
    copy_file(sources[0], destinations[0])
    
    # Copy 10-Q files (using the same source for all quarters)
    logging.info("Copying 10-Q XBRL files...")
    for i in range(1, 4):
        copy_file(sources[1], destinations[i])
    
    # Verify files exist
    for dest in destinations:
        if os.path.exists(dest):
            file_size = os.path.getsize(dest)
            logging.info(f"Verified: {dest} exists ({file_size} bytes)")
        else:
            logging.error(f"Failed: {dest} does not exist!")
    
if __name__ == "__main__":
    main() 