#!/usr/bin/env python3
"""
Batch SEC Filing Pipeline

Process multiple SEC filings across a date range for a given ticker.
Supports processing all 10-K and 10-Q filings for a specified year range.
"""

import os
import sys
import time
import logging
import argparse
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import main pipeline
from .pipeline import SECFilingPipeline

class BatchSECPipeline:
    """
    Batch processor for SEC filings across multiple years.
    
    This class extends the functionality of SECFilingPipeline to process
    multiple filings for a given ticker across a date range.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the batch SEC pipeline.
        
        Args:
            **kwargs: Arguments to pass to the SECFilingPipeline constructor
        """
        self.pipeline = SECFilingPipeline(**kwargs)
        logging.info("Initialized Batch SEC Pipeline")
    
    def process_filings_by_years(self, ticker, start_year, end_year, 
                               include_10k=True, include_10q=True,
                               max_workers=1):
        """
        Process all filings for a ticker within a fiscal year range.
        
        Args:
            ticker: Company ticker symbol
            start_year: Start fiscal year (inclusive)
            end_year: End fiscal year (inclusive)
            include_10k: Whether to include 10-K filings
            include_10q: Whether to include 10-Q filings
            max_workers: Maximum number of concurrent workers
            
        Returns:
            Dictionary with results for all processed filings
        """
        results = {
            "ticker": ticker,
            "start_fiscal_year": start_year,
            "end_fiscal_year": end_year,
            "filings_processed": [],
            "start_time": time.time()
        }
        
        # Get current date
        current_date = datetime.datetime.now()
        
        # Try to determine fiscal year end month for this company
        # Microsoft (MSFT) uses June 30 as fiscal year end
        fiscal_year_end_month = 6  # Default to June for MSFT
        fiscal_year_end_day = 30
        
        # Try to load company-specific fiscal calendar if available
        try:
            from src2.sec.fiscal import fiscal_registry
            
            # Check if the ticker exists in the calendar
            if ticker in fiscal_registry.calendars:
                company_calendar = fiscal_registry.calendars[ticker]
                if company_calendar.fiscal_year_end_month is not None:
                    fiscal_year_end_month = company_calendar.fiscal_year_end_month
                    fiscal_year_end_day = company_calendar.fiscal_year_end_day
                    logging.info(f"Using fiscal year end month {fiscal_year_end_month} (day {fiscal_year_end_day}) for {ticker}")
        except ImportError:
            logging.warning(f"Fiscal registry not available, using default fiscal year end month {fiscal_year_end_month}")
        
        # Calculate current fiscal year
        if current_date.month > fiscal_year_end_month or (current_date.month == fiscal_year_end_month and current_date.day >= fiscal_year_end_day):
            current_fiscal_year = current_date.year
        else:
            current_fiscal_year = current_date.year - 1
            
        # Calculate current fiscal quarter (0-3, where 0 is Q1)
        # Define quarter boundaries based on fiscal year end
        quarter_end_months = []
        for i in range(4):
            month = (fiscal_year_end_month - 9 + i*3) % 12
            if month <= 0:
                month += 12
            quarter_end_months.append(month)
            
        # Determine current fiscal quarter
        current_month = current_date.month
        current_fiscal_quarter = -1
        for i, month in enumerate(quarter_end_months):
            if current_month <= month:
                if current_month < month or current_date.day < fiscal_year_end_day:
                    current_fiscal_quarter = i - 1
                else:
                    current_fiscal_quarter = i
                break
        
        # If we didn't find a match, we're in the last quarter
        if current_fiscal_quarter == -1:
            current_fiscal_quarter = 3
            
        # Convert to 1-based for logging
        logging.info(f"Processing fiscal year filings for {ticker} from {start_year} to {end_year}")
        logging.info(f"Current date: {current_date.strftime('%Y-%m-%d')}")
        logging.info(f"Current fiscal year: {current_fiscal_year}, Fiscal quarter: Q{current_fiscal_quarter+1}")
        logging.info(f"Fiscal year ends: Month {fiscal_year_end_month}, Day {fiscal_year_end_day}")
        
        # Create a list of all filings to process based on fiscal years
        filings_to_process = []

        # Special case for Microsoft 2025 fiscal year Q1 and Q2
        # As of March 2025, Microsoft's fiscal year 2025 covers:
        # Q1: Jul-Sep 2024 (already released)
        # Q2: Oct-Dec 2024 (already released)
        # Let's add specific handling for this case
        ms_quarters_released = {}
        is_msft = ticker.upper() == "MSFT"
        
        # If we're processing Microsoft, add tracking for known released quarters
        if is_msft:
            ms_quarters_released = {
                2025: [1, 2]  # Q1 and Q2 for FY2025 are available (as of March 2025)
            }
            logging.info(f"Using Microsoft-specific quarterly information")
        
        # For each fiscal year in the range
        for fiscal_year in range(start_year, end_year + 1):
            # Skip future fiscal years, with special handling for MSFT 2025
            if fiscal_year > current_fiscal_year and not (is_msft and fiscal_year == 2025):
                logging.info(f"Skipping future fiscal year {fiscal_year}")
                continue
                
            # For 10-K filings (one annual report per fiscal year)
            if include_10k:
                # Process only if this fiscal year is complete or it's the most recent one
                if fiscal_year < current_fiscal_year or (fiscal_year == current_fiscal_year and current_fiscal_quarter == 3):
                    filings_to_process.append({
                        "ticker": ticker,
                        "filing_type": "10-K",
                        "year": fiscal_year,
                        "filing_index": 0  # Always get the most recent 10-K for this fiscal year
                    })
                    logging.info(f"Added 10-K for fiscal year {fiscal_year}")
                
            # For 10-Q filings
            if include_10q:
                # Calculate how many quarters to process for this fiscal year
                quarters_to_process = []
                
                if is_msft and fiscal_year in ms_quarters_released:
                    # Special case for Microsoft - use predefined quarters that are released
                    for q in ms_quarters_released[fiscal_year]:
                        quarters_to_process.append(q)
                    logging.info(f"Using {len(quarters_to_process)} pre-defined quarters for MSFT FY{fiscal_year}")
                elif fiscal_year < current_fiscal_year:
                    # For past fiscal years, we process all quarters
                    quarters_to_process = [1, 2, 3]  # Q1, Q2, Q3 (Q4 is covered by 10-K)
                else:
                    # For current fiscal year, only process completed quarters
                    for q in range(1, min(4, current_fiscal_quarter + 1)):
                        quarters_to_process.append(q)
                
                # Sort quarters for logical processing order (not for filing_index)
                quarters_to_process.sort()
                
                # Add each quarter filing with specific period information
                for q in quarters_to_process:
                    # For Microsoft, we need to map fiscal quarters to calendar years/months
                    # to properly filter for the correct period
                    if is_msft:
                        # Map Microsoft fiscal quarters to calendar months
                        if q == 1:  # Q1: Jul-Sep
                            calendar_months = (7, 8, 9)
                            # For fiscal year 2024, Q1 is in calendar year 2023
                            calendar_year = fiscal_year - 1
                        elif q == 2:  # Q2: Oct-Dec
                            calendar_months = (10, 11, 12)
                            # For fiscal year 2024, Q2 is in calendar year 2023
                            calendar_year = fiscal_year - 1
                        elif q == 3:  # Q3: Jan-Mar
                            calendar_months = (1, 2, 3)
                            # For fiscal year 2024, Q3 is in calendar year 2024
                            calendar_year = fiscal_year
                        else:  # Annual/Q4: Apr-Jun
                            calendar_months = (4, 5, 6)
                            calendar_year = fiscal_year
                    else:
                        # For other companies, default to the fiscal year
                        calendar_year = fiscal_year
                        calendar_months = None
                    
                    filings_to_process.append({
                        "ticker": ticker,
                        "filing_type": "10-Q",
                        "year": fiscal_year,
                        "quarter": q,  # 1-based quarter number (Q1, Q2, Q3)
                        "calendar_year": calendar_year,  # Calendar year for period filtering
                        "calendar_months": calendar_months,  # Calendar months for period filtering
                        "filing_index": 0  # Use 0 as default, will be overridden by period filtering
                    })
                    logging.info(f"Added 10-Q for fiscal year {fiscal_year}, Q{q} (calendar year: {calendar_year}, months: {calendar_months})")
        
        logging.info(f"Created batch of {len(filings_to_process)} filings to process")
        for filing in filings_to_process:
            logging.info(f"  - {filing['ticker']} {filing['filing_type']} ({filing['year']}) index: {filing['filing_index']}")
        
        # Process filings (either sequentially or in parallel)
        if max_workers <= 1 or len(filings_to_process) <= 1:
            # Process sequentially
            logging.info("Processing filings sequentially")
            for filing_info in filings_to_process:
                result = self._process_single_filing(filing_info)
                results["filings_processed"].append(result)
                
                # Add short delay between filings to respect SEC rate limits
                time.sleep(1)
        else:
            # Process in parallel with ThreadPoolExecutor
            logging.info(f"Processing filings in parallel with {max_workers} workers")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_filing = {
                    executor.submit(self._process_single_filing, filing_info): filing_info
                    for filing_info in filings_to_process
                }
                
                for future in as_completed(future_to_filing):
                    filing_info = future_to_filing[future]
                    try:
                        result = future.result()
                        results["filings_processed"].append(result)
                    except Exception as e:
                        logging.error(f"Error processing {filing_info['ticker']} {filing_info['filing_type']} for {filing_info['year']}: {str(e)}")
                        results["filings_processed"].append({
                            "ticker": filing_info["ticker"],
                            "filing_type": filing_info["filing_type"],
                            "year": filing_info["year"],
                            "error": str(e),
                            "status": "error"
                        })
        
        # Calculate summary statistics
        successful_filings = sum(1 for f in results["filings_processed"] if f.get("status") == "success")
        failed_filings = len(results["filings_processed"]) - successful_filings
        
        # Add summary to results
        results["summary"] = {
            "total_filings": len(results["filings_processed"]),
            "successful_filings": successful_filings,
            "failed_filings": failed_filings,
            "total_time_seconds": time.time() - results["start_time"]
        }
        
        logging.info(f"Batch processing complete: {successful_filings}/{len(results['filings_processed'])} filings processed successfully")
        
        return results
    
    def _process_single_filing(self, filing_info):
        """Process a single filing and return the result"""
        ticker = filing_info["ticker"]
        filing_type = filing_info["filing_type"]
        filing_index = filing_info["filing_index"]
        year = filing_info["year"]
        quarter = filing_info.get("quarter")
        calendar_year = filing_info.get("calendar_year")
        calendar_months = filing_info.get("calendar_months")
        
        log_message = f"Processing {ticker} {filing_type} for {year}"
        if quarter:
            log_message += f", Q{quarter}"
        if calendar_year and calendar_months:
            log_message += f" (calendar year: {calendar_year}, months: {calendar_months})"
        else:
            log_message += f" (index: {filing_index})"
        
        logging.info(log_message)
        
        try:
            # Use custom downloader for period-specific filings if we have calendar data
            if calendar_year and calendar_months and quarter:
                # This is the specialized Microsoft quarter processing path
                from src2.sec.downloader import SECDownloader
                
                # Extract the email from the user_agent string format "{agent_name} ({email})"
                contact_email = "info@example.com"  # Default fallback
                user_agent = self.pipeline.downloader.user_agent
                
                # Try to extract email from the user agent string if it's in the expected format
                import re
                email_match = re.search(r'\((.*?)\)', user_agent)
                if email_match:
                    contact_email = email_match.group(1)
                
                # Create a new downloader with the same configuration
                downloader = SECDownloader(
                    user_agent=user_agent.split(" (")[0] if "(" in user_agent else user_agent,
                    contact_email=contact_email,
                    download_dir=self.pipeline.downloader.download_dir
                )
                
                # Get all filings for the specified year and filing type
                all_filings = downloader.get_company_filings(
                    ticker=ticker,
                    filing_type=filing_type,
                    count=10  # Get enough filings to find the right one
                )
                
                logging.info(f"Retrieved {len(all_filings)} {filing_type} filings for {ticker}")
                
                # Filter filings by period end date to find the specific quarter
                target_filing = None
                for filing in all_filings:
                    period_end = filing.get("period_end_date")
                    if period_end:
                        try:
                            # Parse the period end date
                            end_date = datetime.datetime.strptime(period_end, '%Y-%m-%d')
                            # Check if it matches the calendar year and is in the target months
                            if end_date.year == calendar_year and end_date.month in calendar_months:
                                target_filing = filing
                                logging.info(f"Found target filing for {ticker} FY{year} Q{quarter}: {period_end}")
                                break
                        except (ValueError, TypeError):
                            continue
                
                if target_filing:
                    # Process using the specific filing we found
                    result = self.pipeline.process_filing_with_info(target_filing)
                    # Add fiscal metadata
                    result["fiscal_year"] = str(year)
                    result["fiscal_quarter"] = f"Q{quarter}"
                else:
                    # No filing found for this period
                    logging.error(f"No filing found for {ticker} FY{year} Q{quarter} in calendar year {calendar_year}")
                    return {
                        "ticker": ticker,
                        "filing_type": filing_type,
                        "year": year,
                        "quarter": quarter,
                        "error": f"No filing found for this fiscal period in calendar year {calendar_year}",
                        "status": "error"
                    }
            else:
                # Standard filing index processing for non-Microsoft or non-quarterly filings
                result = self.pipeline.process_filing(
                    ticker=ticker,
                    filing_type=filing_type,
                    filing_index=filing_index
                )
            
            # Add year information from filing_info
            if result.get("success"):
                result["year"] = year
                result["status"] = "success"
                
                # Get file sizes from result if available
                if "stages" in result and "extract" in result["stages"]:
                    extract_result = result["stages"]["extract"].get("result", {})
                    
                    if "file_size_mb" in extract_result:
                        file_size_mb = extract_result["file_size_mb"]
                        result["file_size_mb"] = file_size_mb
                        
                        if file_size_mb < 0.1:
                            logging.warning(f"Small file warning: {ticker} {filing_type} file size is {file_size_mb:.2f} MB")
                            result["warning"] = f"Small file size: {file_size_mb:.2f} MB"
                
                # Log success
                logging.info(f"Successfully processed {ticker} {filing_type} for {year}")
                return result
            else:
                # Filing could not be processed
                logging.error(f"Error processing {ticker} {filing_type} for {year}: {result.get('error', 'unknown error')}")
                return {
                    "ticker": ticker,
                    "filing_type": filing_type,
                    "year": year,
                    "error": result.get("error", "unknown error"),
                    "status": "error"
                }
        except Exception as e:
            # Exception during processing
            logging.error(f"Exception processing {ticker} {filing_type} for {year}: {str(e)}")
            return {
                "ticker": ticker,
                "filing_type": filing_type,
                "year": year,
                "error": str(e),
                "status": "error"
            }

# Command-line entry point
def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Process multiple SEC filings across fiscal years")
    
    # Required arguments
    parser.add_argument("ticker", help="Ticker symbol of the company")
    
    # Year range arguments
    parser.add_argument("--start-year", type=int, default=datetime.datetime.now().year - 1,
                        help="Start FISCAL year for filings (default: last year)")
    parser.add_argument("--end-year", type=int, default=datetime.datetime.now().year,
                        help="End FISCAL year for filings (default: current year)")
    
    # Filing type arguments
    parser.add_argument("--10k", dest="process_10k", action="store_true", default=True,
                        help="Process 10-K filings (default: True)")
    parser.add_argument("--no-10k", dest="process_10k", action="store_false",
                        help="Skip 10-K filings")
    parser.add_argument("--10q", dest="process_10q", action="store_true", default=True,
                        help="Process 10-Q filings (default: True)")
    parser.add_argument("--no-10q", dest="process_10q", action="store_false",
                        help="Skip 10-Q filings")
    
    # Other options
    parser.add_argument("--email", help="Contact email for SEC identification",
                        default="info@example.com")
    parser.add_argument("--workers", type=int, default=1,
                        help="Maximum number of concurrent workers (default: 1)")
    parser.add_argument("--output", help="Output directory for processed files")
    parser.add_argument("--gcp-bucket", help="GCS bucket name for upload")
    parser.add_argument("--gcp-project", help="GCP project ID for upload")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.start_year > args.end_year:
        parser.error("start-year must be less than or equal to end-year")
        
    if args.workers < 1:
        parser.error("workers must be at least 1")
        
    if not args.process_10k and not args.process_10q:
        parser.error("Must process at least one filing type (10-K or 10-Q)")
    
    # Create batch pipeline
    batch = BatchSECPipeline(
        user_agent=f"NativeLLM_BatchPipeline/1.0",
        contact_email=args.email,
        output_dir=args.output or "./sec_processed",
        gcp_bucket=args.gcp_bucket,
        gcp_project=args.gcp_project
    )
    
    # Process filings
    try:
        print(f"\n=== Starting batch processing for {args.ticker} from {args.start_year} to {args.end_year} ===")
        
        results = batch.process_filings_by_years(
            ticker=args.ticker,
            start_year=args.start_year,
            end_year=args.end_year,
            include_10k=args.process_10k,
            include_10q=args.process_10q,
            max_workers=args.workers
        )
        
        # Print a summary of results
        print("\n=== Batch Processing Results ===")
        print(f"Company: {args.ticker}")
        print(f"Fiscal Years: {args.start_year} to {args.end_year}")
        print(f"Filings Processed: {results['summary']['total_filings']}")
        print(f"Successful: {results['summary']['successful_filings']}")
        print(f"Failed: {results['summary']['failed_filings']}")
        print(f"Total Time: {results['summary']['total_time_seconds']:.2f} seconds")
        
        # Print details of any failed filings
        if results['summary']['failed_filings'] > 0:
            print("\nFailed Filings:")
            for filing in results["filings_processed"]:
                if filing.get("status") != "success":
                    print(f"  - {filing['filing_type']} ({filing['year']}): {filing.get('error', 'Unknown error')}")
        
        print("\nProcessing complete!")
        
        return 0
    except Exception as e:
        print(f"\nError in batch processing: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())