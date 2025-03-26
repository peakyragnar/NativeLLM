#!/usr/bin/env python3
"""
Script to print information about the Microsoft filing extraction process.
Doesn't actually modify any files, just shows details for debugging.
"""

import os
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set credentials path
CREDENTIALS_PATH = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"
if os.path.exists(CREDENTIALS_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
    logging.info(f"Set GCP credentials from {CREDENTIALS_PATH}")

def analyze_microsoft_filing():
    """Analyze the Microsoft filing process step by step"""
    try:
        # Import necessary modules
        from src2.sec.downloader import SECDownloader
        from src2.sec.fiscal import fiscal_registry, CompanyFiscalCalendar
        
        # 1. Get the Microsoft filing
        print("\n1. GETTING MICROSOFT FILING DATA")
        downloader = SECDownloader(
            user_agent="NativeLLM_Debugger/1.0",
            contact_email="user@example.com"
        )
        
        # Get filings
        filings = downloader.get_company_filings(
            ticker="MSFT",
            filing_type="10-K",
            count=1
        )
        
        if not filings:
            print("No filings found for Microsoft")
            return
        
        filing_info = filings[0]
        
        # 2. Print filing information
        print("\n2. FILING INFORMATION")
        print(f"Filing Type: {filing_info.get('filing_type')}")
        print(f"Filing Date: {filing_info.get('filing_date')}")
        print(f"Period End Date: {filing_info.get('period_end_date')}")
        print(f"Accession Number: {filing_info.get('accession_number')}")
        print(f"Primary Doc URL: {filing_info.get('primary_doc_url')}")
        
        # 3. Test fiscal calendar
        print("\n3. FISCAL CALENDAR TEST")
        ticker = "MSFT"
        period_end_date = filing_info.get('period_end_date')
        filing_type = filing_info.get('filing_type')
        
        print(f"Input values: ticker={ticker}, period_end_date={period_end_date}, filing_type={filing_type}")
        
        # Test with CompanyFiscalCalendar directly
        company_calendar = CompanyFiscalCalendar(ticker)
        company_result = company_calendar.determine_fiscal_period(period_end_date, filing_type)
        
        print("\nUsing CompanyFiscalCalendar directly:")
        print(f"Fiscal Year: {company_result.get('fiscal_year')}")
        print(f"Fiscal Period: {company_result.get('fiscal_period')}")
        
        # Test with fiscal registry
        registry_result = fiscal_registry.determine_fiscal_period(ticker, period_end_date, filing_type)
        
        print("\nUsing fiscal_registry:")
        print(f"Fiscal Year: {registry_result.get('fiscal_year')}")
        print(f"Fiscal Period: {registry_result.get('fiscal_period')}")
        
        # 4. Test document ID creation
        print("\n4. DOCUMENT ID CREATION")
        
        # Mock GCP storage methods
        document_id = f"{ticker}_{filing_type}_{registry_result.get('fiscal_year')}"
        print(f"Document ID would be: {document_id}")
        
        # 5. Test metadata extraction from XBRL
        print("\n5. CHECKING XBRL METADATA")
        try:
            from src2.processor.xbrl_processor import xbrl_processor
            
            # Download the filing
            download_result = downloader.download_filing(filing_info)
            
            if "doc_path" in download_result:
                doc_path = download_result["doc_path"]
                if os.path.exists(doc_path):
                    print(f"Testing XBRL extraction from: {doc_path}")
                    
                    # Parse XBRL
                    xbrl_data = xbrl_processor.parse_xbrl_file(doc_path, ticker)
                    
                    # Extract metadata
                    metadata = xbrl_processor.get_filing_metadata(xbrl_data)
                    
                    print(f"XBRL extracted fiscal_year: {metadata.get('fiscal_year')}")
                    print(f"XBRL extracted fiscal_period: {metadata.get('fiscal_period')}")
                else:
                    print(f"Document not found: {doc_path}")
            else:
                print("No document path in download result")
        except Exception as e:
            print(f"Error checking XBRL: {str(e)}")
        
        return True
    except Exception as e:
        print(f"Error analyzing Microsoft filing: {str(e)}")
        return False

if __name__ == "__main__":
    print("MICROSOFT FILING ANALYSIS")
    analyze_microsoft_filing()