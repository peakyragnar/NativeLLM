import os
import sys
import argparse
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage, firestore
import time
import re

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik
from src.edgar.filing_finder import find_company_filings
from src.edgar.company_fiscal import fiscal_registry
from src.xbrl.xbrl_downloader import download_xbrl_instance
from src.xbrl.html_text_extractor import process_html_filing
from src.xbrl.xbrl_parser import parse_xbrl_file
from src.formatter.llm_formatter import generate_llm_format, save_llm_format
from src.config import INITIAL_COMPANIES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('calendar_download.log'),
        logging.StreamHandler()
    ]
)

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def check_existing_filing(ticker, filing_type, fiscal_year, fiscal_period, db):
    """Check if a filing already exists in Firestore"""
    # Standardize quarter format for consistency
    if fiscal_period in ["FY", "annual"]:
        firestore_period = "annual"
    elif fiscal_period.startswith("Q"):
        firestore_period = fiscal_period
    else:
        quarter_number = fiscal_period[0]
        firestore_period = f"Q{quarter_number}"
    
    filing_id = f"{ticker}-{filing_type}-{fiscal_year}-{firestore_period}"
    filing_ref = db.collection('filings').document(filing_id).get()
    
    if filing_ref.exists:
        logging.info(f"Filing already exists in Firestore: {filing_id}")
        return True
    return False

def upload_to_gcs(local_file_path, ticker, filing_type, fiscal_year, fiscal_period, file_format, bucket_name="native-llm-filings"):
    """Upload a filing to Google Cloud Storage"""
    # Determine quarter folder
    if fiscal_period in ["FY", "annual"]:
        quarter_folder = "annual"  # For 10-K
    else:
        # Convert 1Q, 2Q, 3Q, 4Q to Q1, Q2, Q3, Q4
        if fiscal_period.startswith("Q"):
            quarter_folder = fiscal_period  # Already in Q1 format
        else:
            quarter_number = fiscal_period[0]
            quarter_folder = f"Q{quarter_number}"
    
    # Construct GCS path
    gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{quarter_folder}/{file_format}.txt"
    
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Create blob and upload
        blob = bucket.blob(gcs_path)
        
        # Check if blob already exists
        if blob.exists():
            logging.info(f"File already exists in GCS: gs://{bucket_name}/{gcs_path}")
            # Return the path and size of existing file
            blob.reload()  # Refresh metadata
            return gcs_path, blob.size
        
        # Upload the file
        with open(local_file_path, 'rb') as f:
            blob.upload_from_file(f)
        
        logging.info(f"Successfully uploaded {local_file_path} to gs://{bucket_name}/{gcs_path}")
        return gcs_path, os.path.getsize(local_file_path)
    except Exception as e:
        logging.error(f"Error uploading file to GCS: {str(e)}")
        return None, 0

def add_filing_metadata(company_ticker, company_name, filing_type, fiscal_year, fiscal_period, 
                       period_end_date, filing_date, text_path, llm_path,
                       text_size, llm_size):
    """Add metadata for a filing to Firestore"""
    try:
        db = firestore.Client(database='nativellm')
        
        # Ensure company exists in Firestore
        company_ref = db.collection('companies').document(company_ticker)
        if not company_ref.get().exists:
            company_ref.set({
                'ticker': company_ticker,
                'name': company_name,
                'last_updated': datetime.datetime.now()
            })
            logging.info(f"Added company to Firestore: {company_ticker} - {company_name}")
        
        # Create a unique filing ID
        if fiscal_period in ["FY", "annual"]:
            firestore_period = "annual"
        elif fiscal_period.startswith("Q"):
            firestore_period = fiscal_period
        else:
            quarter_number = fiscal_period[0]
            firestore_period = f"Q{quarter_number}"
            
        filing_id = f"{company_ticker}-{filing_type}-{fiscal_year}-{firestore_period}"
        
        # Check if filing already exists
        filing_ref = db.collection('filings').document(filing_id)
        if filing_ref.get().exists:
            logging.info(f"Filing metadata already exists: {filing_id}")
            return filing_id
        
        # Add to filings collection
        filing_data = {
            'filing_id': filing_id,
            'company_ticker': company_ticker,
            'company_name': company_name,
            'filing_type': filing_type,
            'fiscal_year': fiscal_year,
            'fiscal_period': firestore_period,
            'period_end_date': period_end_date,
            'filing_date': filing_date,
            'text_file_path': text_path,
            'llm_file_path': llm_path,
            'text_file_size': text_size,
            'llm_file_size': llm_size,
            'storage_class': 'STANDARD',
            'last_accessed': datetime.datetime.now(),
            'access_count': 0
        }
        
        filing_ref.set(filing_data)
        logging.info(f"Added filing metadata: {filing_id}")
        
        return filing_id
    except Exception as e:
        logging.error(f"Error adding metadata to Firestore: {str(e)}")
        return None

def is_filing_in_range(filing_date, start_date, end_date):
    """Check if a filing date is within the specified range"""
    try:
        filing_dt = datetime.datetime.strptime(filing_date, '%Y-%m-%d').date()
        start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        
        return start_dt <= filing_dt <= end_dt
    except Exception as e:
        logging.error(f"Error comparing dates: {str(e)}")
        return False

def process_filing(filing_metadata, include_html=True, include_xbrl=True):
    """Process a single filing and upload to cloud storage"""
    try:
        ticker = filing_metadata.get("ticker")
        company_name = filing_metadata.get("company_name")
        filing_type = filing_metadata.get("filing_type")
        filing_date = filing_metadata.get("filing_date")
        period_end_date = filing_metadata.get("period_end_date")
        fiscal_year = filing_metadata.get("fiscal_year")
        fiscal_period = filing_metadata.get("fiscal_period")
        
        # Ensure fiscal information is set using company fiscal calendar
        if (not fiscal_year or not fiscal_period) and period_end_date:
            try:
                # Extract fiscal year and period based on period_end_date and company-specific logic
                period_date = datetime.datetime.strptime(period_end_date, '%Y-%m-%d')
                
                # Special handling for companies with non-calendar fiscal years
                if ticker == "AAPL":
                    # Apple's fiscal year ends in September
                    # Q1: Oct-Dec, Q2: Jan-Mar, Q3: Apr-Jun, Q4: Jul-Sep
                    month = period_date.month
                    
                    if month in [10, 11, 12]:  # Oct-Dec = Q1 of next calendar year
                        if not fiscal_year:
                            fiscal_year = str(period_date.year + 1)
                        if not fiscal_period and filing_type != "10-K":
                            fiscal_period = "Q1"
                    elif month in [1, 2, 3]:  # Jan-Mar = Q2
                        if not fiscal_year:
                            fiscal_year = str(period_date.year)
                        if not fiscal_period and filing_type != "10-K":
                            fiscal_period = "Q2"
                    elif month in [4, 5, 6]:  # Apr-Jun = Q3
                        if not fiscal_year:
                            fiscal_year = str(period_date.year)
                        if not fiscal_period and filing_type != "10-K":
                            fiscal_period = "Q3"
                    else:  # Jul-Sep
                        if not fiscal_year:
                            fiscal_year = str(period_date.year)
                        # For Apple, there should be no 10-Q for Q4, only a 10-K
                        # If we find a 10-Q in this period, it's actually Q3
                        if not fiscal_period:
                            if filing_type == "10-K":
                                fiscal_period = "annual"
                            else:
                                fiscal_period = "Q3"  # Important: Apple doesn't have Q4 10-Q filings
                    
                    logging.info(f"Set Apple fiscal period: FY{fiscal_year} {fiscal_period} for period ending {period_end_date}")
                elif ticker == "MSFT":
                    # Microsoft's fiscal year ends in June
                    # Q1: Jul-Sep, Q2: Oct-Dec, Q3: Jan-Mar, annual (never Q4): Apr-Jun
                    month = period_date.month
                    
                    # For Microsoft, determine fiscal year based on the month
                    if month >= 7:  # Jul-Dec
                        if not fiscal_year:
                            fiscal_year = str(period_date.year + 1)
                    else:  # Jan-Jun
                        if not fiscal_year:
                            fiscal_year = str(period_date.year)
                    
                    # Determine fiscal period
                    if month in [7, 8, 9]:  # Jul-Sep = Q1
                        if not fiscal_period and filing_type != "10-K":
                            fiscal_period = "Q1"
                    elif month in [10, 11, 12]:  # Oct-Dec = Q2
                        if not fiscal_period and filing_type != "10-K":
                            fiscal_period = "Q2"
                    elif month in [1, 2, 3]:  # Jan-Mar = Q3
                        if not fiscal_period and filing_type != "10-K":
                            fiscal_period = "Q3"
                    else:  # Apr-Jun = ALWAYS annual for both 10-K and 10-Q
                        if not fiscal_period:
                            fiscal_period = "annual"
                    
                    # For 10-K at fiscal year end (June 30), use the current year
                    if filing_type == "10-K" and month == 6 and period_date.day == 30:
                        fiscal_year = str(period_date.year)
                    
                    # Always set 10-K filings to annual
                    if filing_type == "10-K" and not fiscal_period:
                        fiscal_period = "annual"
                    
                    logging.info(f"Set Microsoft fiscal period: FY{fiscal_year} {fiscal_period} for period ending {period_end_date}")
                else:
                    # Default behavior for other companies - assume calendar fiscal year
                    if not fiscal_year:
                        fiscal_year = str(period_date.year)
                    
                    if not fiscal_period and filing_type != "10-K":
                        month = period_date.month
                        quarter_map = {1: "Q1", 2: "Q1", 3: "Q1", 4: "Q2", 5: "Q2", 6: "Q2", 
                                      7: "Q3", 8: "Q3", 9: "Q3", 10: "Q4", 11: "Q4", 12: "Q4"}
                        fiscal_period = quarter_map.get(month, "Q")
                
                # Always set 10-K as annual
                if filing_type == "10-K" and not fiscal_period:
                    fiscal_period = "annual"
                
                # Update the metadata
                filing_metadata["fiscal_year"] = fiscal_year
                filing_metadata["fiscal_period"] = fiscal_period
                
                logging.info(f"Set fiscal info from period_end_date: {fiscal_year}-{fiscal_period}")
                
            except Exception as e:
                logging.warning(f"Error processing period date: {str(e)}, using fallback logic")
                
                # Extract year from period end date (fallback)
                if not fiscal_year:
                    try:
                        if '-' in period_end_date:
                            fiscal_year = period_end_date.split('-')[0]
                        elif len(period_end_date) >= 4:
                            fiscal_year = period_end_date[:4]
                        else:
                            fiscal_year = str(datetime.datetime.now().year)
                        
                        # Update the metadata
                        filing_metadata["fiscal_year"] = fiscal_year
                        logging.info(f"Set fiscal_year from period_end_date (fallback): {fiscal_year}")
                    except Exception as e:
                        fiscal_year = str(datetime.datetime.now().year)
                        filing_metadata["fiscal_year"] = fiscal_year
                        logging.warning(f"Using current year ({fiscal_year}) for fiscal_year due to error: {str(e)}")
                
                # If still no fiscal_period, use simple logic (fallback)
                if not fiscal_period:
                    # Default fiscal period based on filing type
                    if filing_type == "10-K":
                        fiscal_period = "annual"
                    else:
                        # Try to extract quarter from period_end_date
                        try:
                            if period_end_date and '-' in period_end_date:
                                month = int(period_end_date.split('-')[1])
                                quarter_map = {1: "Q1", 2: "Q1", 3: "Q1", 4: "Q2", 5: "Q2", 6: "Q2", 
                                              7: "Q3", 8: "Q3", 9: "Q3", 10: "Q4", 11: "Q4", 12: "Q4"}
                                fiscal_period = quarter_map.get(month, "Q")
                            else:
                                fiscal_period = "Q"
                        except:
                            fiscal_period = "Q"  # Default if we can't determine quarter
                    
                    # Update the metadata
                    filing_metadata["fiscal_period"] = fiscal_period
                    logging.info(f"Set fiscal_period using fallback logic: {fiscal_period}")
        
        # Check if this filing already exists in Firestore
        db = firestore.Client(database='nativellm')
        if check_existing_filing(ticker, filing_type, fiscal_year, fiscal_period, db):
            logging.info(f"Skipping existing filing: {ticker} {filing_type} {fiscal_year} {fiscal_period}")
            return {
                "ticker": ticker,
                "filing_type": filing_type,
                "fiscal_year": fiscal_year,
                "fiscal_period": fiscal_period,
                "status": "skipped",
                "reason": "already exists"
            }
        
        results = {
            "ticker": ticker,
            "filing_type": filing_type,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "filing_date": filing_date
        }
        
        # Process HTML if requested
        if include_html:
            # Make sure we have a document URL, either from metadata or from the filing_finder
            if "html_url" not in filing_metadata and "document_url" in filing_metadata:
                filing_metadata["html_url"] = filing_metadata["document_url"]
            
            # Check if we have a valid URL to process
            if "html_url" in filing_metadata or "document_url" in filing_metadata:
                # Process HTML filing
                logging.info(f"Processing HTML for {ticker} {filing_type} {fiscal_year} {fiscal_period}")
                html_processing_result = process_html_filing(filing_metadata)
                
                if "error" in html_processing_result:
                    logging.warning(f"Error processing HTML: {html_processing_result['error']}")
                else:
                    # The text content is already saved to a file by process_html_filing
                    local_text_path = html_processing_result.get("file_path")
                    text_size = html_processing_result.get("file_size", 0)
                    
                    if local_text_path and os.path.exists(local_text_path):
                        logging.info(f"Found local text file: {local_text_path}")
                        # Upload to GCS
                        text_gcs_path, text_size = upload_to_gcs(
                            local_text_path, 
                            ticker, 
                            filing_type, 
                            fiscal_year, 
                            fiscal_period, 
                            "text"
                        )
                        results["text_file"] = {"local_path": local_text_path, "gcs_path": text_gcs_path, "size": text_size}
            else:
                logging.warning(f"Skipping HTML processing: No URL available for {ticker} {filing_type}")
        
        # Process XBRL if requested
        llm_content = None
        if include_xbrl and ("instance_url" in filing_metadata or "xbrl_url" in filing_metadata):
            # Use instance_url or fall back to xbrl_url
            xbrl_url = filing_metadata.get("instance_url") or filing_metadata.get("xbrl_url")
            # Set xbrl_url for backward compatibility
            filing_metadata["xbrl_url"] = xbrl_url
            download_result = download_xbrl_instance(filing_metadata)
            
            if "error" not in download_result:
                xbrl_file_path = download_result.get("file_path")
                
                # Parse XBRL with company information
                parsed_result = parse_xbrl_file(xbrl_file_path, ticker=ticker, filing_metadata=filing_metadata)
                
                if "error" not in parsed_result:
                    # Generate LLM format
                    llm_content = generate_llm_format(parsed_result, filing_metadata)
                    
                    # Save LLM content to local file first (keeping existing format)
                    if llm_content:
                        local_dir = os.path.join("data", "processed", ticker)
                        os.makedirs(local_dir, exist_ok=True)
                        
                        # Use existing naming format
                        file_name = f"{company_name.replace(' ', '_')}_{fiscal_year}_{fiscal_period}_{ticker}_{filing_type}_{period_end_date.replace('-', '')}_llm.txt"
                        local_llm_path = os.path.join(local_dir, file_name)
                        
                        with open(local_llm_path, 'w', encoding='utf-8') as f:
                            f.write(llm_content)
                        
                        # Upload to GCS
                        llm_gcs_path, llm_size = upload_to_gcs(
                            local_llm_path, 
                            ticker, 
                            filing_type, 
                            fiscal_year, 
                            fiscal_period, 
                            "llm"
                        )
                        results["llm_file"] = {"local_path": local_llm_path, "gcs_path": llm_gcs_path, "size": llm_size}
        
        # Add metadata to Firestore if both files were processed
        if "text_file" in results and "llm_file" in results:
            metadata_id = add_filing_metadata(
                company_ticker=ticker,
                company_name=company_name,
                filing_type=filing_type,
                fiscal_year=fiscal_year,
                fiscal_period=fiscal_period,
                period_end_date=period_end_date,
                filing_date=filing_date,
                text_path=results["text_file"]["gcs_path"],
                llm_path=results["llm_file"]["gcs_path"],
                text_size=results["text_file"]["size"],
                llm_size=results["llm_file"]["size"]
            )
            results["metadata_id"] = metadata_id
            results["status"] = "success"
        else:
            results["status"] = "partial"
            if "text_file" not in results:
                results["missing"] = "text_file"
            if "llm_file" not in results:
                results["missing"] = "llm_file"
        
        return results
    
    except Exception as e:
        logging.error(f"Error processing filing: {str(e)}")
        return {
            "ticker": filing_metadata.get("ticker"),
            "filing_type": filing_metadata.get("filing_type"),
            "status": "error",
            "error": str(e)
        }

def process_company_calendar_range(ticker, start_date, end_date, filing_types):
    """Process all filings for a company within a calendar date range"""
    try:
        logging.info(f"Processing {ticker} filings from {start_date} to {end_date}")
        
        # Get CIK from ticker
        cik = get_cik_from_ticker(ticker)
        if not cik:
            return {"error": f"Could not find CIK for ticker {ticker}"}
        
        # Get company name
        company_name = get_company_name_from_cik(cik)
        
        results = {
            "ticker": ticker,
            "cik": cik,
            "company_name": company_name,
            "date_range": f"{start_date} to {end_date}",
            "filings_processed": []
        }
        
        # Process 10-K filings first to establish fiscal calendar pattern
        if "10-K" in filing_types:
            # Find all 10-K filings to analyze fiscal patterns
            logging.info(f"Finding 10-K filings first to establish fiscal pattern for {ticker}")
            k_filings_result = find_company_filings(ticker, ["10-K"])
            
            if "filings" in k_filings_result and "10-K" in k_filings_result["filings"]:
                # Use 10-K filings to update fiscal calendar
                fiscal_registry.update_calendar(ticker, [k_filings_result["filings"]["10-K"]])
                logging.info(f"Updated fiscal calendar for {ticker} based on 10-K filings")
        
        # Find filings for each type
        for filing_type in filing_types:
            filings_result = find_company_filings(ticker, [filing_type])
            
            if "error" in filings_result:
                logging.error(f"Error finding {filing_type} filings for {ticker}: {filings_result['error']}")
                continue
            
            # For calendar year filtering, we need to look at all available filings
            # not just the first one returned
            
            # First, let's check if there are any filings of this type
            if "filings" in filings_result and filing_type in filings_result["filings"]:
                # Let's extract the filing date from the index page
                # The index page contains multiple filings with their dates in a table
                
                # We'll need to parse the index page to get all filings with their dates
                index_page_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={filing_type}&count=100"
                
                logging.info(f"Getting all {filing_type} filings to filter by date: {index_page_url}")
                try:
                    from src.edgar.edgar_utils import sec_request
                    response = sec_request(index_page_url)
                    
                    if response.status_code == 200:
                        from bs4 import BeautifulSoup
                        
                        soup = BeautifulSoup(response.text, 'html.parser')
                        filing_tables = soup.find_all('table', {'class': 'tableFile2'})
                        
                        if filing_tables:
                            table = filing_tables[0]
                            rows = table.find_all('tr')
                            
                            # Process each row (filing) in the table
                            for row in rows:
                                cells = row.find_all('td')
                                if len(cells) >= 4:  # Ensure we have enough cells
                                    # Extract filing info from this row
                                    row_filing_type = cells[0].text.strip()
                                    filing_date = cells[3].text.strip()
                                    
                                    # Only process if it's the right filing type and within date range
                                    if row_filing_type == filing_type and is_filing_in_range(filing_date, start_date, end_date):
                                        logging.info(f"Found {filing_type} filing from {filing_date} within date range")
                                        
                                        # Get the documents link for this filing
                                        doc_link = None
                                        for link in cells[1].find_all('a'):
                                            if 'documentsbutton' in link.get('id', ''):
                                                doc_link = link.get('href')
                                                break
                                        
                                        if doc_link:
                                            # Process this specific filing
                                            doc_url = f"https://www.sec.gov{doc_link}"
                                            logging.info(f"Processing filing from document link: {doc_url}")
                                            
                                            # We need to fetch this specific filing's metadata
                                            # Rather than processing the latest one from filings_result
                                            specific_filing_result = find_company_filings(ticker, [filing_type], specific_url=doc_url)
                                            
                                            if "filings" in specific_filing_result and filing_type in specific_filing_result["filings"]:
                                                filing_metadata = specific_filing_result["filings"][filing_type]
                                                filing_metadata["filing_date"] = filing_date  # Ensure date is set
                                                
                                                # Add company info to metadata
                                                filing_metadata["ticker"] = ticker
                                                filing_metadata["company_name"] = company_name
                                                
                                                # Process the filing
                                                filing_result = process_filing(filing_metadata)
                                                results["filings_processed"].append(filing_result)
                                        else:
                                            logging.warning(f"Could not find documents link for {filing_type} filing from {filing_date}")
                    else:
                        logging.warning(f"Failed to get index page: {response.status_code}")
                
                except Exception as e:
                    logging.error(f"Error processing index page: {str(e)}")
                    
                    # Fallback to using the latest filing if we can't parse the index
                    logging.info("Falling back to latest filing method")
                    filing_metadata = filings_result["filings"][filing_type]
                    
                    # Check if filing is within date range
                    filing_date = filing_metadata.get("filing_date")
                    if filing_date and is_filing_in_range(filing_date, start_date, end_date):
                        # Add company info to metadata
                        filing_metadata["ticker"] = ticker
                        filing_metadata["company_name"] = company_name
                        
                        # Process the filing
                        filing_result = process_filing(filing_metadata)
                        results["filings_processed"].append(filing_result)
        
        return results
    
    except Exception as e:
        logging.error(f"Error processing {ticker}: {str(e)}")
        return {"ticker": ticker, "error": str(e)}

def download_filings_by_calendar_years(start_year, end_year, companies=None, 
                                     include_10k=True, include_10q=True, 
                                     max_workers=3):
    """
    Download all SEC filings within the specified calendar year range
    
    Args:
        start_year: Starting calendar year (e.g., 2022)
        end_year: Ending calendar year (e.g., 2025)
        companies: List of company tickers (default: use configured list)
        include_10k: Whether to include 10-K filings
        include_10q: Whether to include 10-Q filings
        max_workers: Maximum parallel workers
        
    Returns:
        Summary of downloaded filings
    """
    # Generate date range
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    
    # Define filing types to include
    filing_types = []
    if include_10k:
        filing_types.append("10-K")
    if include_10q:
        filing_types.append("10-Q")
    
    # Get company list (from parameter or config)
    company_list = companies or [company["ticker"] for company in INITIAL_COMPANIES]
    
    # Default to Apple if no companies specified
    if not company_list:
        company_list = ["AAPL"]
    
    logging.info(f"Starting download for {len(company_list)} companies from {start_year} to {end_year}")
    logging.info(f"Companies: {', '.join(company_list)}")
    logging.info(f"Filing types: {filing_types}")
    logging.info(f"Max workers: {max_workers}")
    
    # Process companies in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_company = {
            executor.submit(
                process_company_calendar_range, 
                ticker, 
                start_date, 
                end_date, 
                filing_types
            ): ticker for ticker in company_list
        }
        
        for future in as_completed(future_to_company):
            ticker = future_to_company[future]
            try:
                result = future.result()
                results[ticker] = result
                logging.info(f"Completed processing for {ticker}")
            except Exception as e:
                results[ticker] = {"error": str(e)}
                logging.error(f"Failed processing for {ticker}: {str(e)}")
    
    # Summarize results
    successful_companies = sum(1 for r in results.values() if "error" not in r)
    total_filings = sum(
        len(r.get("filings_processed", [])) 
        for r in results.values() 
        if "error" not in r
    )
    successful_filings = sum(
        sum(1 for f in r.get("filings_processed", []) if f.get("status") == "success")
        for r in results.values() 
        if "error" not in r
    )
    skipped_filings = sum(
        sum(1 for f in r.get("filings_processed", []) if f.get("status") == "skipped")
        for r in results.values() 
        if "error" not in r
    )
    
    summary = {
        "calendar_range": f"{start_year}-{end_year}",
        "companies_processed": len(results),
        "successful_companies": successful_companies,
        "failed_companies": len(results) - successful_companies,
        "total_filings_found": total_filings,
        "successful_filings": successful_filings,
        "skipped_filings": skipped_filings,
        "details": results
    }
    
    logging.info(f"Download summary: {summary['successful_filings']}/{summary['total_filings_found']} filings processed successfully")
    logging.info(f"Skipped filings (already exists): {summary['skipped_filings']}")
    
    return summary

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download SEC filings for a calendar year range")
    parser.add_argument("start_year", type=int, help="Starting calendar year (e.g., 2022)")
    parser.add_argument("end_year", type=int, help="Ending calendar year (e.g., 2025)")
    parser.add_argument("--tickers", nargs="+", help="Specific company tickers to process (default: AAPL)")
    parser.add_argument("--skip-10k", action="store_true", help="Skip 10-K filings")
    parser.add_argument("--skip-10q", action="store_true", help="Skip 10-Q filings")
    parser.add_argument("--workers", type=int, default=3, help="Maximum number of parallel workers")
    
    args = parser.parse_args()
    
    # Validate years
    if args.start_year > args.end_year:
        print("Error: start_year must be less than or equal to end_year")
        sys.exit(1)
        
    # Set default company if not specified
    companies = args.tickers if args.tickers else ["AAPL"]
    
    # Download filings
    download_filings_by_calendar_years(
        start_year=args.start_year,
        end_year=args.end_year,
        companies=companies,
        include_10k=not args.skip_10k,
        include_10q=not args.skip_10q,
        max_workers=args.workers
    )