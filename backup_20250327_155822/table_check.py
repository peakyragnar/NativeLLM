#!/usr/bin/env python
# Table data integrity validation

import sys
import os
import re
import logging
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.xbrl.xbrl_parser import extract_text_only_from_html

def validate_table_integrity(file_path):
    """Validate data integrity of HTML table extraction"""
    
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find tables specifically
        html_pattern = r'@VALUE: (.*?<table.*?</table>.*?)(\n@|$)'
        table_sections = re.findall(html_pattern, content, re.DOTALL)
        
        if not table_sections:
            print("No table sections found in the file.")
            return False
        
        # Select a large table for analysis
        tables = [t[0] for t in table_sections]
        table_html = max(tables, key=len)
        
        # Process with HTML cleaner
        cleaned = extract_text_only_from_html(table_html)
        
        # Extract cell data from the original table for comparison
        soup = BeautifulSoup(table_html, 'html.parser')
        table = soup.find('table')
        
        if not table:
            print("No table element found in the HTML.")
            return False
        
        # Extract all numeric values and headers from the table
        numeric_values = []
        headers = []
        
        # Get all row data
        for row in table.find_all('tr'):
            for cell in row.find_all(['td', 'th']):
                cell_text = cell.get_text(strip=True)
                # Check if the cell has numeric data
                if re.search(r'\d', cell_text):
                    # Extract just the number part
                    numbers = re.findall(r'-?[\d,]+\.?\d*|\(\d+(?:,\d+)*(?:\.\d+)?\)', cell_text)
                    numeric_values.extend(numbers)
                else:
                    # It's probably a header or label
                    if cell_text:
                        headers.append(cell_text)
        
        # Calculate reduction
        original_len = len(table_html)
        cleaned_len = len(cleaned)
        reduction = (original_len - cleaned_len) / original_len * 100
        
        print("\n===== TABLE DATA INTEGRITY VALIDATION =====\n")
        print(f"Size: {original_len:,} → {cleaned_len:,} bytes ({reduction:.1f}% reduction)")
        
        # Check if all numeric values are preserved in the cleaned text
        preserved_numbers = 0
        missing_numbers = []
        
        for num in numeric_values:
            if num in cleaned:
                preserved_numbers += 1
            else:
                missing_numbers.append(num)
        
        # Check if headers/labels are preserved
        preserved_headers = 0
        missing_headers = []
        
        for header in headers:
            if header.lower() in cleaned.lower():
                preserved_headers += 1
            else:
                missing_headers.append(header)
        
        # Calculate preservation rates
        num_preservation = preserved_numbers / len(numeric_values) * 100 if numeric_values else 100
        header_preservation = preserved_headers / len(headers) * 100 if headers else 100
        
        print(f"Numeric values: {preserved_numbers}/{len(numeric_values)} preserved ({num_preservation:.1f}%)")
        print(f"Headers/labels: {preserved_headers}/{len(headers)} preserved ({header_preservation:.1f}%)")
        
        if num_preservation >= 99 and header_preservation >= 95:
            print("✅ TABLE INTEGRITY: Passed - Critical data preserved")
        else:
            print("❌ TABLE INTEGRITY: Failed")
            if missing_numbers:
                print(f"Missing numbers: {', '.join(missing_numbers[:5])}" + 
                      (f" and {len(missing_numbers)-5} more" if len(missing_numbers) > 5 else ""))
            if missing_headers:
                print(f"Missing headers: {', '.join(missing_headers[:5])}" + 
                      (f" and {len(missing_headers)-5} more" if len(missing_headers) > 5 else ""))
        
        # Show a brief sample of the cleaned output
        print("\nCLEANED TABLE TEXT:")
        print(cleaned[:500] + "..." if len(cleaned) > 500 else cleaned)
        
        return True
            
    except Exception as e:
        logging.error(f"Error validating table integrity: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Check if file path is provided as an argument
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        # Use default Apple file
        file_path = "/Users/michael/NativeLLM/data/processed/AAPL/Apple_Inc_2024_FY_AAPL_10-K_20240928_llm.txt"
    
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        sys.exit(1)
        
    success = validate_table_integrity(file_path)
    sys.exit(0 if success else 1)