# test_html_extraction.py
import os
import sys
import json
import argparse

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.edgar.filing_finder import find_company_filings
from src.xbrl.html_text_extractor import process_html_filing

def test_html_extraction(ticker, filing_type="10-K"):
    """
    Test the HTML text extraction functionality for a specific company and filing type
    
    Args:
        ticker: Company ticker symbol
        filing_type: Filing type (10-K or 10-Q)
        
    Returns:
        None (prints results)
    """
    print(f"Testing HTML extraction for {ticker} {filing_type}")
    
    # Step 1: Find the filing
    filing_types = [filing_type]
    filings_result = find_company_filings(ticker, filing_types)
    
    if "error" in filings_result:
        print(f"Error finding filing: {filings_result['error']}")
        return
    
    # Check if the requested filing type was found
    if filing_type not in filings_result.get("filings", {}):
        print(f"No {filing_type} filing found for {ticker}")
        return
    
    # Get the filing metadata
    filing_metadata = filings_result["filings"][filing_type]
    filing_metadata["ticker"] = ticker
    filing_metadata["company_name"] = filings_result.get("company_name", f"{ticker} Inc.")
    
    print(f"Found {filing_type} filing for {ticker}:")
    print(f"  Filing Date: {filing_metadata.get('filing_date')}")
    print(f"  Period End Date: {filing_metadata.get('period_end_date')}")
    print(f"  CIK: {filing_metadata.get('cik')}")
    print(f"  Accession Number: {filing_metadata.get('accession_number')}")
    
    # Step 2: Process the HTML filing
    html_result = process_html_filing(filing_metadata)
    
    if "error" in html_result:
        print(f"Error processing HTML: {html_result['error']}")
        return
    
    # Print the results
    print("\nHTML Processing Results:")
    print(f"  Success: {html_result.get('success', False)}")
    
    file_path = html_result.get("file_path", "")
    file_size = html_result.get("file_size", 0)
    
    if file_path and os.path.exists(file_path):
        print(f"\n  Text File:")
        print(f"    Path: {file_path}")
        print(f"    Size: {file_size:,} bytes")
        
        # Print a sample of the content
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read(1000)  # Read first 1000 characters
            
            print("\n    Sample Content:")
            print(f"    {content[:500]}...")
            
            # Check for section markers
            with open(file_path, 'r', encoding='utf-8') as f:
                full_content = f.read()
                
            # Count section markers
            section_starts = full_content.count("@SECTION_START:")
            section_ends = full_content.count("@SECTION_END:")
            
            print(f"\n    Section Analysis:")
            print(f"    - Total section markers: {section_starts} starts, {section_ends} ends")
            
            # List the sections found
            if section_starts > 0:
                sections = []
                section_guide = False
                
                # Check for section guide
                if "@SECTION_GUIDE" in full_content:
                    section_guide = True
                    print(f"    - Section guide: Yes")
                    
                    # Extract section IDs from guide
                    guide_lines = full_content.split("@SECTION_GUIDE", 1)[1].split("\n\n", 1)[0].split("\n")
                    for line in guide_lines:
                        if line.startswith("@SECTION:"):
                            section_id = line.split("|")[0].replace("@SECTION:", "").strip()
                            if section_id:
                                sections.append(section_id)
                else:
                    print(f"    - Section guide: No")
                    
                    # Extract section IDs from markers
                    import re
                    section_markers = re.findall(r"@SECTION_START: (\w+)", full_content)
                    sections = list(set(section_markers))  # Remove duplicates
                
                print(f"    - Sections identified: {len(sections)}")
                if len(sections) <= 10:  # Only show all sections if there aren't too many
                    print(f"    - Section IDs: {', '.join(sections)}")
                else:
                    print(f"    - Sample section IDs: {', '.join(sections[:5])}...")
                    
        except Exception as e:
            print(f"    Error reading file: {str(e)}")
    else:
        print(f"  No text file found or file doesn't exist: {file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test HTML text extraction")
    parser.add_argument('ticker', help='Company ticker symbol')
    parser.add_argument('--filing_type', default="10-K", choices=["10-K", "10-Q"], 
                        help='Filing type (10-K or 10-Q)')
    
    args = parser.parse_args()
    test_html_extraction(args.ticker, args.filing_type)