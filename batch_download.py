# batch_download.py
import os
import sys
import argparse
import datetime
import logging
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.edgar.edgar_utils import sec_request, get_company_name_from_cik
from src.edgar.filing_finder import find_filings_by_cik
from src.xbrl.html_text_extractor import process_html_filing
from src.process_company import process_company

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("batch_download.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def get_date_range(months_back):
    """Calculate the date range for filings based on months back from today"""
    end_date = datetime.datetime.now().date()
    start_date = end_date - datetime.timedelta(days=30 * months_back)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def process_single_filing(filing_metadata, include_html=True, include_xbrl=True):
    """Process a single filing for both HTML and XBRL extraction"""
    results = {
        "cik": filing_metadata.get("cik"),
        "ticker": filing_metadata.get("ticker"),
        "filing_type": filing_metadata.get("filing_type"),
        "filing_date": filing_metadata.get("filing_date"),
        "period_end_date": filing_metadata.get("period_end_date"),
        "fiscal_year": filing_metadata.get("fiscal_year"),
        "fiscal_quarter": filing_metadata.get("fiscal_quarter"),
        "html_result": None,
        "xbrl_result": None
    }
    
    # Log with fiscal quarter information if available
    fiscal_info = ""
    if filing_metadata.get("fiscal_year") and filing_metadata.get("fiscal_quarter"):
        fiscal_info = f" [FY{filing_metadata.get('fiscal_year')} {filing_metadata.get('fiscal_quarter')}]"
    
    logging.info(f"Processing {filing_metadata.get('ticker')} {filing_metadata.get('filing_type')}{fiscal_info} for period ending {filing_metadata.get('period_end_date')}")
    
    # Process HTML extraction if requested
    if include_html:
        try:
            import re
            # Ensure we have required metadata with fallbacks
            if filing_metadata.get("period_end_date") is None:
                # Try to extract from the accession number or filename
                instance_url = filing_metadata.get("instance_url", "")
                if instance_url:
                    # Try to extract date from filename (e.g., aapl-20241228_htm.xml)
                    date_match = re.search(r'-(\d{8})_', instance_url)
                    if date_match:
                        date_str = date_match.group(1)
                        filing_metadata["period_end_date"] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            # Add ticker if missing
            if filing_metadata.get("ticker") is None:
                cik = filing_metadata.get("cik")
                if cik == "0000320193":
                    filing_metadata["ticker"] = "AAPL"
                else:
                    filing_metadata["ticker"] = f"CIK{cik[-6:]}" if cik else "unknown"
            
            logging.info(f"Processing HTML with metadata: ticker={filing_metadata.get('ticker')}, type={filing_metadata.get('filing_type')}, date={filing_metadata.get('period_end_date')}")
            
            # Now process the HTML
            html_result = process_html_filing(filing_metadata)
            if "error" in html_result:
                logging.warning(f"HTML extraction error: {html_result['error']}")
                results["html_result"] = {"error": html_result["error"]}
            else:
                results["html_result"] = {
                    "success": True,
                    "file_path": html_result.get("file_path", ""),
                    "file_size": html_result.get("file_size", 0)
                }
                logging.info(f"HTML extraction successful: {html_result.get('file_path')}")
        except Exception as e:
            logging.error(f"Exception in HTML extraction: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            results["html_result"] = {"error": str(e)}
    
    # Process XBRL extraction if requested
    if include_xbrl:
        try:
            import re
            # Ensure we have required metadata with fallbacks
            if filing_metadata.get("period_end_date") is None:
                # Try to extract from the accession number or filename
                instance_url = filing_metadata.get("instance_url", "")
                if instance_url:
                    # Try to extract date from filename (e.g., aapl-20241228_htm.xml)
                    date_match = re.search(r'-(\d{8})_', instance_url)
                    if date_match:
                        date_str = date_match.group(1)
                        filing_metadata["period_end_date"] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            # Add ticker if missing
            if filing_metadata.get("ticker") is None:
                cik = filing_metadata.get("cik")
                if cik == "0000320193":
                    filing_metadata["ticker"] = "AAPL"
                else:
                    filing_metadata["ticker"] = f"CIK{cik[-6:]}" if cik else "unknown"
                    
            # Log what we're about to do
            logging.info(f"Downloading XBRL for {filing_metadata.get('ticker')} {filing_metadata.get('filing_type')} (accession: {filing_metadata.get('accession_number')})")
            
            # The process_company function expects a ticker, but we're working with CIK
            # We need to adapt it for this use case
            from src.xbrl.xbrl_downloader import download_xbrl_instance
            from src.xbrl.xbrl_parser import parse_xbrl_file
            from src.formatter.llm_formatter import generate_llm_format, save_llm_format
            
            # Download the XBRL instance
            download_result = download_xbrl_instance(filing_metadata)
            if "error" in download_result:
                logging.warning(f"XBRL download error: {download_result['error']}")
                results["xbrl_result"] = {"error": download_result["error"]}
            else:
                # Parse the downloaded XBRL file
                file_path = download_result.get("file_path")
                parsed_result = parse_xbrl_file(file_path)
                
                if "error" in parsed_result:
                    logging.warning(f"XBRL parsing error: {parsed_result['error']}")
                    results["xbrl_result"] = {"error": parsed_result["error"]}
                else:
                    # Generate LLM format
                    llm_content = generate_llm_format(parsed_result, filing_metadata)
                    
                    # Save to file
                    save_result = save_llm_format(llm_content, filing_metadata)
                    
                    if "error" in save_result:
                        logging.warning(f"XBRL save error: {save_result['error']}")
                        results["xbrl_result"] = {"error": save_result["error"]}
                    else:
                        results["xbrl_result"] = {
                            "success": True,
                            "file_path": save_result.get("file_path"),
                            "file_size": save_result.get("size", 0)
                        }
                        logging.info(f"XBRL extraction successful: {save_result.get('file_path')}")
        except Exception as e:
            logging.error(f"Exception in XBRL extraction: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            results["xbrl_result"] = {"error": str(e)}
    
    # Add some delay to respect SEC rate limits
    time.sleep(1)
    
    return results

def determine_fiscal_quarter(filing_date, fiscal_year_end_date):
    """
    Determine fiscal quarter based on the relationship to the 10-K filing date
    
    Args:
        filing_date: Date of the filing to determine quarter for (YYYY-MM-DD)
        fiscal_year_end_date: Date of the most recent 10-K (fiscal year end) (YYYY-MM-DD)
        
    Returns:
        Tuple of (quarter_number, fiscal_year) where quarter_number is 1-4
    """
    import datetime
    
    # Parse the dates
    try:
        filing_dt = datetime.datetime.strptime(filing_date, "%Y-%m-%d").date()
        fiscal_end_dt = datetime.datetime.strptime(fiscal_year_end_date, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        logging.warning(f"Could not parse dates for quarter determination: {filing_date}, {fiscal_year_end_date}")
        return None, None
    
    # Get the fiscal year of the 10-K
    fiscal_year = fiscal_end_dt.year
    
    # Calculate days difference
    days_diff = (filing_dt - fiscal_end_dt).days
    
    # Determine the quarter based on timing relative to fiscal year end
    if days_diff > 0:
        # This is after the fiscal year end, so it's Q1 of the next fiscal year
        return 1, fiscal_year + 1
    elif days_diff > -90:
        # Within ~3 months before year end: Q4 of same fiscal year
        return 4, fiscal_year
    elif days_diff > -180:
        # ~3-6 months before year end: Q3
        return 3, fiscal_year
    elif days_diff > -270:
        # ~6-9 months before year end: Q2
        return 2, fiscal_year
    else:
        # ~9-12 months before year end: Q1
        return 1, fiscal_year

def batch_download_filings(cik, months_back=12, include_html=True, include_xbrl=True, max_workers=3):
    """
    Download and process all filings for a CIK within a specified time range
    
    Args:
        cik: The CIK number (10-digit string with leading zeros)
        months_back: Number of months to look back from today
        include_html: Whether to process HTML text extraction
        include_xbrl: Whether to process XBRL data extraction
        max_workers: Maximum number of concurrent downloads
        
    Returns:
        Dictionary with processing results
    """
    # Ensure CIK is properly formatted (10 digits with leading zeros)
    cik = cik.zfill(10) if cik.isdigit() else cik
    
    # Get company name
    company_name = get_company_name_from_cik(cik)
    if not company_name:
        logging.error(f"Could not find company name for CIK {cik}")
        return {"error": f"Invalid CIK: {cik}"}
    
    # Calculate date range
    start_date, end_date = get_date_range(months_back)
    
    logging.info(f"Starting batch download for {company_name} (CIK: {cik})")
    logging.info(f"Date range: {start_date} to {end_date}")
    
    # First, find and process 10-K filings (fiscal year end)
    annual_filings = []
    try:
        annual_filings = find_filings_by_cik(cik, "10-K", start_date, end_date)
        if annual_filings and isinstance(annual_filings, list):
            for filing in annual_filings:
                filing["company_name"] = company_name
        else:
            annual_filings = []
    except Exception as e:
        logging.error(f"Error finding 10-K filings: {str(e)}")
        annual_filings = []
    
    # Sort 10-K filings by date (most recent first)
    if annual_filings:
        annual_filings.sort(
            key=lambda x: x.get("filing_date", ""), 
            reverse=True
        )
        logging.info(f"Found {len(annual_filings)} 10-K filings, most recent: {annual_filings[0].get('filing_date')}")
    else:
        logging.warning("No 10-K filings found in date range to use as fiscal year anchors")
    
    # Add fiscal year and quarter information to 10-K filings
    for filing in annual_filings:
        if "period_end_date" in filing and filing["period_end_date"]:
            try:
                year = filing["period_end_date"].split("-")[0]
                filing["fiscal_year"] = year
                filing["fiscal_quarter"] = "4Q"  # 10-K is always Q4
                logging.info(f"10-K filing from {filing.get('filing_date')} identified as {year}_4Q")
            except:
                logging.warning(f"Could not determine fiscal year for 10-K filed on {filing.get('filing_date')}")
    
    # Now find quarterly filings
    quarterly_filings = []
    try:
        quarterly_filings = find_filings_by_cik(cik, "10-Q", start_date, end_date)
        if quarterly_filings and isinstance(quarterly_filings, list):
            for filing in quarterly_filings:
                filing["company_name"] = company_name
        else:
            quarterly_filings = []
    except Exception as e:
        logging.error(f"Error finding 10-Q filings: {str(e)}")
        quarterly_filings = []
    
    # Use 10-K dates to determine fiscal quarters for 10-Q filings
    if annual_filings and quarterly_filings:
        for q_filing in quarterly_filings:
            if "filing_date" not in q_filing or not q_filing["filing_date"]:
                continue
                
            # Find the closest 10-K filing date to use as fiscal year anchor
            for a_filing in annual_filings:
                if "filing_date" not in a_filing or not a_filing["filing_date"]:
                    continue
                    
                quarter_num, fiscal_year = determine_fiscal_quarter(
                    q_filing.get("filing_date"), 
                    a_filing.get("filing_date")
                )
                
                if quarter_num and fiscal_year:
                    q_filing["fiscal_quarter"] = f"{quarter_num}Q"
                    q_filing["fiscal_year"] = str(fiscal_year)
                    logging.info(f"10-Q filing from {q_filing.get('filing_date')} identified as {fiscal_year}_{quarter_num}Q")
                    break  # Found a match, stop checking other 10-Ks
    
    # Combine all filings
    filings = annual_filings + quarterly_filings
    
    logging.info(f"Found {len(filings)} total filings to process")
    
    if not filings:
        return {
            "cik": cik,
            "company_name": company_name,
            "date_range": f"{start_date} to {end_date}",
            "error": "No filings found in the specified date range"
        }
    
    # Process all filings with limited concurrency
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_filing = {
            executor.submit(process_single_filing, filing, include_html, include_xbrl): filing 
            for filing in filings
        }
        
        for future in as_completed(future_to_filing):
            filing = future_to_filing[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logging.error(f"Processing failed for {filing.get('filing_type')} on {filing.get('filing_date')}: {str(e)}")
                results.append({
                    "cik": filing.get("cik"),
                    "filing_type": filing.get("filing_type"),
                    "filing_date": filing.get("filing_date"),
                    "error": str(e)
                })
    
    # Summarize results
    successful_html = sum(1 for r in results if r.get("html_result", {}).get("success", False))
    successful_xbrl = sum(1 for r in results if r.get("xbrl_result", {}).get("success", False))
    
    summary = {
        "cik": cik,
        "company_name": company_name,
        "date_range": f"{start_date} to {end_date}",
        "total_filings": len(filings),
        "successful_html": successful_html,
        "successful_xbrl": successful_xbrl,
        "results": results
    }
    
    logging.info(f"Batch processing complete: {successful_html}/{len(filings)} HTML extractions and {successful_xbrl}/{len(filings)} XBRL extractions successful")
    
    return summary

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch download SEC filings by CIK")
    parser.add_argument('cik', help='SEC CIK number (10 digits with leading zeros)')
    parser.add_argument('--months', type=int, default=36, help='Number of months to look back')
    parser.add_argument('--html', action='store_true', default=True, help='Include HTML text extraction')
    parser.add_argument('--no-html', action='store_false', dest='html', help='Skip HTML text extraction')
    parser.add_argument('--xbrl', action='store_true', default=True, help='Include XBRL data extraction')
    parser.add_argument('--no-xbrl', action='store_false', dest='xbrl', help='Skip XBRL data extraction')
    parser.add_argument('--workers', type=int, default=3, help='Maximum number of concurrent workers')
    
    args = parser.parse_args()
    
    result = batch_download_filings(
        args.cik, 
        months_back=args.months,
        include_html=args.html,
        include_xbrl=args.xbrl,
        max_workers=args.workers
    )
    
    if "error" in result:
        logging.error(f"Batch processing error: {result['error']}")
        sys.exit(1)
    else:
        logging.info(f"Batch processing complete. Processed {result['total_filings']} filings for {result['company_name']}")
        
        # Print summary
        print(f"\nSummary for {result['company_name']} ({args.cik}):")
        print(f"Date range: {result['date_range']}")
        print(f"Total filings found: {result['total_filings']}")
        print(f"Successful HTML extractions: {result['successful_html']}")
        print(f"Successful XBRL extractions: {result['successful_xbrl']}")