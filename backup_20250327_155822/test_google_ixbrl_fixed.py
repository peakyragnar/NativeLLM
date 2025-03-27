"""
Test script to verify the fixes for Google iXBRL download issues.
"""

import os
import sys
from pprint import pprint

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from src.xbrl.ixbrl_downloader import download_sec_primary_document

def test_google_filing_download():
    """Test downloading a Google 10-K filing"""
    
    # Use Google's metadata
    filing_metadata = {
        "ticker": "GOOGL",
        "cik": "1652044",
        "accession_number": "000165204425-000014",
        "filing_type": "10-K",
        "filing_date": "2024-01-30",
        "period_end_date": "2023-12-31"
    }
    
    print("Testing Google 10-K download with fixed URL construction...")
    
    # Try to download the document
    result = download_sec_primary_document(filing_metadata)
    
    print("\nResult:")
    pprint(result)
    
    if "error" in result:
        print(f"\nError: {result['error']}")
        return False
    
    print(f"\nSuccessfully downloaded document to: {result.get('file_path')}")
    return True

if __name__ == "__main__":
    success = test_google_filing_download()
    print(f"\nTest {'successful' if success else 'failed'}")