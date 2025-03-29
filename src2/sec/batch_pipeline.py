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
from concurrent.futures import ThreadPoolExecutor, as_completed

# We'll import re and datetime locally in each method to avoid scope issues

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
                     Including:
                     - force_upload: Override GCS existence checks
                     - amendments_only: Process only amended filings (10-K/A, 10-Q/A)
                     - skip_text_files: Skip generating text.txt files
        """
        # Extract specialized flags
        self.force_upload = kwargs.pop("force_upload", False)
        self.amendments_only = kwargs.pop("amendments_only", False)
        self.skip_text_files = kwargs.pop("skip_text_files", False)
        
        if self.force_upload:
            logging.info("FORCE UPLOAD MODE ENABLED - Will upload all files regardless of existence in GCS")
            
        if self.amendments_only:
            logging.info("AMENDMENTS-ONLY MODE ENABLED - Will only process amended filings (10-K/A, 10-Q/A)")
            
        if self.skip_text_files:
            logging.info("TEXT FILE GENERATION DISABLED - Only generating LLM-formatted files")
            # Update the config to skip text files
            from src2.config import OUTPUT_FORMAT
            OUTPUT_FORMAT["GENERATE_TEXT_FILES"] = False
        
        # Pass remaining kwargs to pipeline
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
        # Import libraries locally to avoid scope issues
        import re
        import datetime
        
        results = {
            "ticker": ticker,
            "start_fiscal_year": start_year,
            "end_fiscal_year": end_year,
            "filings_processed": [],
            "start_time": time.time()
        }
        
        # Get current date
        current_date = datetime.datetime.now()
        
        # Get fiscal year end month for this company from our fiscal registry
        try:
            from src2.sec.fiscal import fiscal_registry
            from src2.sec.fiscal.fiscal_manager import CompanyFiscalModel
            
            ticker = ticker.upper()
            logging.info(f"Looking up fiscal pattern for {ticker}")
            
            # First check if it's in KNOWN_FISCAL_PATTERNS
            fiscal_year_end_month = 12  # Default to calendar year
            fiscal_year_end_day = 31
            
            # Get fiscal pattern from CompanyFiscalModel's KNOWN_FISCAL_PATTERNS
            if ticker in CompanyFiscalModel.KNOWN_FISCAL_PATTERNS:
                pattern = CompanyFiscalModel.KNOWN_FISCAL_PATTERNS[ticker]
                fiscal_year_end_month = pattern["fiscal_year_end_month"]
                fiscal_year_end_day = pattern["fiscal_year_end_day"]
                logging.info(f"Found {ticker} in KNOWN_FISCAL_PATTERNS: year end {fiscal_year_end_month}-{fiscal_year_end_day}")
                
                # CRITICAL: Adjust requested fiscal years based on company's fiscal calendar
                # For companies with early fiscal year end (Jan-Jun), their fiscal year X
                # spans parts of calendar years X-1 and X
                if fiscal_year_end_month <= 6:
                    logging.info(f"Company {ticker} has fiscal year ending in month {fiscal_year_end_month}")
                    logging.info(f"Original requested fiscal years: {start_year}-{end_year}")
                    
                    # For companies with non-standard fiscal years, create a mapping to standardize
                    # This ensures that "fiscal year 2022" consistently means the same period
                    # regardless of company fiscal calendar
                    adjusted_start_year = start_year
                    adjusted_end_year = end_year
                    
                    # We don't adjust the years at input - instead, we'll make the adjustment
                    # when calculating calendar dates to ensure consistent fiscal year meaning
                    logging.info(f"Using consistent fiscal year definition for {ticker}")
                    logging.info(f"For fiscal year X, period: {(fiscal_year_end_month+1)%12 or 12}/X-1 to {fiscal_year_end_month}/X")
            
            # Use the model directly from registry as fallback
            # This handles companies where we've defined a model but not added to KNOWN_FISCAL_PATTERNS
            if not ticker in CompanyFiscalModel.KNOWN_FISCAL_PATTERNS:
                model = fiscal_registry.get_model(ticker)
                fiscal_year_end_month = model.get_fiscal_year_end_month()
                fiscal_year_end_day = model.get_fiscal_year_end_day()
                logging.info(f"Using fiscal model for {ticker}: year end {fiscal_year_end_month}-{fiscal_year_end_day}")
        
        except (ImportError, Exception) as e:
            logging.warning(f"Could not determine fiscal pattern: {str(e)}, defaulting to calendar year")
            fiscal_year_end_month = 12  # Default to calendar year end
            fiscal_year_end_day = 31
        
        # Calculate current fiscal year
        # For companies with fiscal year ending early in the calendar year (like NVDA in January),
        # we need special handling
        if fiscal_year_end_month <= 6:  # Fiscal year ends in first half of calendar year
            # If current date is after fiscal year end, we're in the next fiscal year
            if (current_date.month > fiscal_year_end_month or 
                (current_date.month == fiscal_year_end_month and current_date.day >= fiscal_year_end_day)):
                current_fiscal_year = current_date.year + 1  # We're in next fiscal year already
            else:
                current_fiscal_year = current_date.year  # Still in current fiscal year
        else:  # Fiscal year ends in second half of calendar year (like AAPL in September)
            # For companies with fiscal year ending in the second half:
            # - Oct-Dec of year X: We're in fiscal year X+1
            # - Jan-Sep of year X+1: We're still in fiscal year X+1
            if (current_date.month < fiscal_year_end_month or 
                (current_date.month == fiscal_year_end_month and current_date.day < fiscal_year_end_day)):
                # Before fiscal year end (Jan-Sep), we're in the fiscal year equal to the calendar year
                current_fiscal_year = current_date.year
            else:
                # After fiscal year end (Oct-Dec), we're in the fiscal year of next calendar year
                current_fiscal_year = current_date.year + 1
            
        # Calculate current fiscal quarter (0-3, where 0 is Q1)
        # Define quarter boundaries based on fiscal year end
        quarter_end_months = []
        for i in range(4):
            month = (fiscal_year_end_month - 9 + i*3) % 12
            if month <= 0:
                month += 12
            quarter_end_months.append(month)
            
        # Log quarter end months for debugging
        logging.info(f"Fiscal year end month: {fiscal_year_end_month}, Quarter end months: {quarter_end_months}")
        
        # Determine current fiscal quarter
        current_month = current_date.month
        current_fiscal_quarter = -1
        
        # Handle the case where fiscal year ends in first half (like NVDA)
        if fiscal_year_end_month <= 6:
            # Print extra debugging
            logging.info(f"Using first-half fiscal year logic for quarter calculation")
            
            # For early fiscal year end, the quarters are shifted
            for i, month in enumerate(quarter_end_months):
                logging.info(f"Checking quarter {i+1}: end month={month}, current month={current_month}")
                
                if current_month == month:
                    # If we're at the end of a quarter, check if we've passed the fiscal day
                    if current_date.day >= fiscal_year_end_day:
                        # We've completed this quarter
                        current_fiscal_quarter = i
                    else:
                        # We're still in the previous quarter
                        current_fiscal_quarter = (i - 1) % 4
                    break
                elif current_month < month:
                    # We're in the previous quarter
                    current_fiscal_quarter = (i - 1) % 4
                    break
        else:
            # Original logic for standard fiscal years (ending in second half)
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
        
        # Handle negative quarters (can happen with modulo operations)
        if current_fiscal_quarter < 0:
            current_fiscal_quarter += 4
            
        # Convert to 1-based for logging
        logging.info(f"Processing fiscal year filings for {ticker} from {start_year} to {end_year}")
        logging.info(f"Current date: {current_date.strftime('%Y-%m-%d')}")
        logging.info(f"Current fiscal year: {current_fiscal_year}, Fiscal quarter: Q{current_fiscal_quarter+1}")
        logging.info(f"Fiscal year ends: Month {fiscal_year_end_month}, Day {fiscal_year_end_day}")
        
        # Create a list of all filings to process based on fiscal years
        filings_to_process = []

        # Get quarter mapping from fiscal pattern
        quarter_mapping = {}
        try:
            # Try to get company-specific quarter mapping
            if ticker in CompanyFiscalModel.KNOWN_FISCAL_PATTERNS:
                pattern = CompanyFiscalModel.KNOWN_FISCAL_PATTERNS[ticker]
                if "quarter_mapping" in pattern:
                    quarter_mapping = pattern["quarter_mapping"]
                    logging.info(f"Using quarter mapping from KNOWN_FISCAL_PATTERNS for {ticker}")
        except Exception as e:
            logging.warning(f"Could not get quarter mapping: {str(e)}")
            
        # Create mapping of fiscal quarters to calendar months for any company
        fiscal_to_calendar = {}
        
        # Calculate which calendar months belong to each fiscal quarter
        # This works for ANY company with ANY fiscal year end
        for q in range(1, 5):  # Q1 through Q4
            # Calculate starting month for this quarter (1-based)
            start_month = (fiscal_year_end_month + q*3 - 2) % 12
            if start_month == 0:
                start_month = 12
                
            # Calculate the 3 months in this quarter
            months = []
            for i in range(3):
                month = (start_month + i - 1) % 12 + 1
                months.append(month)
            
            # Store in our mapping
            fiscal_to_calendar[q] = months
            
            # Add special warning for Q4 mapping - we calculate it but won't use it
            if q == 4:
                logging.info(f"INFO: Q4 mapping calculated for reference only - there are never Q4 filings, only 10-K annual reports")
            
        logging.info(f"Fiscal-to-calendar mapping for {ticker}: {fiscal_to_calendar}")
        
        # Track information about known company releases
        quarters_released = {}
        
        # As of March 2025, we know specific companies have released these quarters
        if ticker == "MSFT":
            quarters_released = {
                2025: [1, 2]  # Q1 and Q2 for FY2025 are available (as of March 2025)
            }
            logging.info(f"Using Microsoft-specific quarterly information")
        elif ticker == "NVDA":
            quarters_released = {
                2024: [1, 2, 3],  # All quarters for FY2024 are available
                2025: [1, 2, 3]   # Q1, Q2, and Q3 for FY2025 are available (as of March 2025)
            }
            logging.info(f"Using NVIDIA-specific quarterly information")
        
        # For each fiscal year in the range
        for fiscal_year in range(start_year, end_year + 1):
            # For early fiscal year companies (like NVDA), we need special handling
            if fiscal_year_end_month <= 6:
                # For NVDA, 2024 is definitely not a future fiscal year in March 2025
                if ticker == "NVDA" and fiscal_year == 2024:
                    # Always process 2024 for NVDA
                    pass
                # Skip years that are too far in the future
                elif fiscal_year > current_fiscal_year and not (ticker in ["MSFT", "NVDA"] and fiscal_year in quarters_released):
                    logging.info(f"Skipping future fiscal year {fiscal_year}")
                    continue
            else:
                # For normal fiscal year companies, use standard logic
                if fiscal_year > current_fiscal_year and not (ticker in ["MSFT", "NVDA"] and fiscal_year in quarters_released):
                    logging.info(f"Skipping future fiscal year {fiscal_year}")
                    continue
                
            # For 10-K filings (one annual report per fiscal year)
            if include_10k:
                # Process 10-K if:
                # 1. Fiscal year is complete (fiscal_year < current_fiscal_year), OR
                # 2. Current fiscal year AND we're in the last quarter (fiscal_quarter == 3), OR
                # 3. Special case for NVDA fiscal year 2025 (we know it exists)
                # 4. Special case for other early fiscal year companies (like NVDA)
                should_process = False
                reason = ""
                
                # Check if fiscal year is complete
                if fiscal_year < current_fiscal_year:
                    should_process = True
                    reason = "fiscal year complete"
                # Check if we're in the last quarter of current fiscal year
                elif fiscal_year == current_fiscal_year and current_fiscal_quarter == 3:
                    should_process = True
                    reason = "current fiscal year, last quarter"
                # Special handling for NVDA FY2024 and FY2025 (we know they exist)
                elif ticker == "NVDA" and (fiscal_year == 2024 or fiscal_year == 2025):
                    should_process = True
                    reason = f"special case for NVDA FY{fiscal_year}"
                # Special case for early fiscal year companies in Q1 of next fiscal year
                elif fiscal_year == current_fiscal_year - 1 and fiscal_year_end_month <= 6 and current_date.month <= fiscal_year_end_month + 3:
                    should_process = True
                    reason = f"early fiscal year (ends month {fiscal_year_end_month}), now in Q1 of next fiscal year"
                
                if should_process:
                    filings_to_process.append({
                        "ticker": ticker,
                        "filing_type": "10-K",
                        "year": fiscal_year,
                        "filing_index": 0,  # Always get the most recent 10-K for this fiscal year
                        "amendments_only": self.amendments_only  # Flag for amendments-only mode
                    })
                    logging.info(f"Added 10-K for fiscal year {fiscal_year} (reason: {reason})")
                
            # For 10-Q filings
            if include_10q:
                # Calculate how many quarters to process for this fiscal year
                quarters_to_process = []
                
                # Check if this is a special company with known quarterly releases
                if ticker in ["MSFT", "NVDA"] and fiscal_year in quarters_released:
                    # Use predefined quarters that are known to be released
                    for q in quarters_released[fiscal_year]:
                        quarters_to_process.append(q)
                    logging.info(f"Using {len(quarters_to_process)} pre-defined quarters for {ticker} FY{fiscal_year}")
                elif fiscal_year < current_fiscal_year:
                    # For past fiscal years, we process all quarters EXCEPT Q4
                    # IMPORTANT: There is never a Q4 filing for any company - the 10-K always covers Q4
                    quarters_to_process = [1, 2, 3]  # Q1, Q2, Q3 only (Q4 is always covered by 10-K)
                else:
                    # For current fiscal year, only process completed quarters (up to Q3)
                    # Never include Q4 as it doesn't exist as a separate filing
                    for q in range(1, min(4, current_fiscal_quarter + 1)):
                        # Skip Q4 - it's never a separate filing
                        if q < 4:
                            quarters_to_process.append(q)
                
                # Sort quarters for logical processing order (not for filing_index)
                quarters_to_process.sort()
                
                # Add each quarter filing with specific period information
                for q in quarters_to_process:
                    # CRITICAL CHECK: Never process Q4 as a separate filing
                    if q == 4:
                        logging.warning(f"Skipping Q4 for {ticker} FY{fiscal_year} - Q4 is always covered by 10-K, not 10-Q")
                        continue
                        
                    # For any company, we need to map fiscal quarters to calendar years/months
                    # to properly filter for the correct period
                    if q in fiscal_to_calendar:
                        # Get the calendar months for this fiscal quarter
                        calendar_months = tuple(fiscal_to_calendar[q])
                        
                        # Determine calendar year based on fiscal quarter
                        # This needs special handling based on fiscal year end
                        # If the quarter contains months that come after the fiscal year end,
                        # then those months are in the previous calendar year
                        
                        # MATHEMATICAL MAPPING FORMULA
                        # For fiscal year FY with fiscal year end month FYE_M:
                        # - Quarter Q period ends in month M = (FYE_M + 3*Q) % 12 
                        # - Calendar year = FY if M ≤ FYE_M, otherwise FY-1
                        
                        # Calculate expected period end month for this quarter
                        period_end_month = (fiscal_year_end_month + 3*q) % 12
                        if period_end_month == 0:
                            period_end_month = 12
                            
                        # Calculate calendar year based on mathematical formula
                        # For consistent handling across all companies:
                        # 1. A fiscal year X always means the period ending in fiscal year end month of year X
                        # 2. This applies regardless of fiscal calendar (early or late fiscal year end)
                        if period_end_month <= fiscal_year_end_month:
                            calendar_year = fiscal_year
                        else:
                            calendar_year = fiscal_year - 1
                            
                        # Log detailed mapping for debugging and transparency
                        logging.info(f"Fiscal-to-calendar mapping: {ticker} FY{fiscal_year} Q{q} -> Period end {calendar_year}-{period_end_month}")
                            
                        # Update calendar_months to only include the expected period end month
                        calendar_months = [period_end_month]
                            
                        logging.info(f"Mapped {ticker} FY{fiscal_year} Q{q} to calendar year {calendar_year}, months {calendar_months}")
                    else:
                        # Fallback to using fiscal year directly
                        calendar_year = fiscal_year
                        calendar_months = None
                        logging.warning(f"Could not map fiscal quarter {q} to calendar months, using fiscal year directly")
                    
                    filings_to_process.append({
                        "ticker": ticker,
                        "filing_type": "10-Q",
                        "year": fiscal_year,
                        "quarter": q,  # 1-based quarter number (Q1, Q2, Q3)
                        "calendar_year": calendar_year,  # Calendar year for period filtering
                        "calendar_months": calendar_months,  # Calendar months for period filtering
                        "filing_index": 0,  # Use 0 as default, will be overridden by period filtering
                        "fiscal_year_end_month": fiscal_year_end_month,  # Store fiscal year end month for later use
                        "fiscal_year_end_day": fiscal_year_end_day,  # Store fiscal year end day for later use
                        "amendments_only": self.amendments_only  # Flag for amendments-only mode
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
    
    def _reorganize_files_by_fiscal_year(self, ticker, filing_type, fiscal_year, fiscal_period, result):
        """
        Reorganize processed files to use fiscal year naming instead of accession numbers.
        
        Args:
            ticker: Company ticker symbol
            filing_type: Filing type (10-K, 10-Q)
            fiscal_year: Fiscal year as string (e.g., "2021")
            fiscal_period: Fiscal period (e.g., "Q1", "annual")
            result: The processing result containing file paths
            
        Returns:
            Updated result with new file paths
        """
        import os
        import shutil
        from pathlib import Path
        
        # Get local output directory from pipeline
        output_dir = Path(self.pipeline.output_dir)
        company_dir = output_dir / ticker
        
        # Check if we have text and llm paths in the result
        text_path = result.get("text_path")
        llm_path = result.get("llm_path")
        
        if not text_path or not llm_path:
            logging.warning(f"No text or LLM paths found in result for {ticker} {filing_type} {fiscal_year}")
            return
            
        # Create the fiscal year-based destination directory
        if fiscal_period and filing_type == "10-Q":
            fiscal_dir = company_dir / f"{filing_type}_{fiscal_year}_{fiscal_period}"
        else:
            fiscal_dir = company_dir / f"{filing_type}_{fiscal_year}"
            
        # Make the directory if it doesn't exist
        os.makedirs(fiscal_dir, exist_ok=True)
        
        # Construct new filenames
        if fiscal_period and filing_type == "10-Q":
            new_text_path = fiscal_dir / f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}_text.txt"
            new_llm_path = fiscal_dir / f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}_llm.txt"
        else:
            new_text_path = fiscal_dir / f"{ticker}_{filing_type}_{fiscal_year}_text.txt"
            new_llm_path = fiscal_dir / f"{ticker}_{filing_type}_{fiscal_year}_llm.txt"
            
        # Copy the files to the new locations
        try:
            # Store original paths for cleanup
            original_paths = []
            
            if os.path.exists(text_path):
                logging.info(f"Copying text file from {text_path} to {new_text_path}")
                shutil.copy2(text_path, new_text_path)
                result["reorganized_text_path"] = str(new_text_path)
                original_paths.append(text_path)
                
            if os.path.exists(llm_path):
                logging.info(f"Copying LLM file from {llm_path} to {new_llm_path}")
                shutil.copy2(llm_path, new_llm_path)
                result["reorganized_llm_path"] = str(new_llm_path)
                original_paths.append(llm_path)
                
            # Clean up local accession-based files after copying
            try:
                import re
                fiscal_year_pattern = re.compile(r'^.*_20[0-9]{2}_.*$')  # Match filenames containing fiscal years
                
                for original_path in original_paths:
                    # Only delete files that don't have fiscal years in their names
                    if os.path.exists(original_path) and not fiscal_year_pattern.match(os.path.basename(original_path)):
                        logging.info(f"Removing original accession-based file: {original_path}")
                        os.remove(original_path)
                        result.setdefault("removed_local_files", []).append(str(original_path))
            except Exception as local_e:
                logging.warning(f"Error cleaning up local accession-based files: {str(local_e)}")
                
                
            # Upload to GCP with proper fiscal paths if GCP is configured
            if self.pipeline.gcp_storage and self.pipeline.gcp_storage.is_enabled():
                # Construct GCS paths
                if filing_type == "10-K" or fiscal_period == "annual":
                    gcs_text_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/text.txt"
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/llm.txt"
                elif fiscal_period:
                    gcs_text_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_period}/text.txt"
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_period}/llm.txt"
                else:
                    gcs_text_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/text.txt"
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/llm.txt"
                
                # Find and remove existing incorrect GCP paths
                logging.info(f"Looking for incorrect paths in GCS to remove")
                
                # Get the original accession-based paths from result
                original_gcs_text_path = None
                original_gcs_llm_path = None
                
                # Check if we have "stages" and "upload" in the result
                if "stages" in result and "upload" in result["stages"]:
                    upload_result = result["stages"]["upload"].get("result", {})
                    
                    # Extract original GCS paths from text and LLM uploads
                    if "text_upload" in upload_result:
                        text_upload = upload_result["text_upload"]
                        if "gcs_path" in text_upload:
                            original_gcs_text_path = text_upload["gcs_path"]
                            logging.info(f"Found original text GCS path: {original_gcs_text_path}")
                    
                    if "llm_upload" in upload_result:
                        llm_upload = upload_result["llm_upload"]
                        if "gcs_path" in llm_upload:
                            original_gcs_llm_path = llm_upload["gcs_path"]
                            logging.info(f"Found original LLM GCS path: {original_gcs_llm_path}")
                
                # Delete the incorrect paths if they're different from the fiscal year paths
                if original_gcs_text_path and original_gcs_text_path != gcs_text_path:
                    try:
                        logging.info(f"Deleting incorrect GCS text path: {original_gcs_text_path}")
                        self.pipeline.gcp_storage.delete_file(original_gcs_text_path)
                        result["deleted_original_gcs_text_path"] = original_gcs_text_path
                    except Exception as e:
                        logging.warning(f"Error deleting original GCS text path: {str(e)}")
                
                if original_gcs_llm_path and original_gcs_llm_path != gcs_llm_path:
                    try:
                        logging.info(f"Deleting incorrect GCS LLM path: {original_gcs_llm_path}")
                        self.pipeline.gcp_storage.delete_file(original_gcs_llm_path)
                        result["deleted_original_gcs_llm_path"] = original_gcs_llm_path
                    except Exception as e:
                        logging.warning(f"Error deleting original GCS LLM path: {str(e)}")
                        
                # Comprehensive cleanup for ALL accession-based paths in GCP
                try:
                    if self.pipeline.gcp_storage and self.pipeline.gcp_storage.is_enabled():
                        # Check all possible accession number directories in GCP
                        ticker_prefix = f"companies/{ticker}/{filing_type}/"
                        
                        # Pattern to identify potential accession numbers vs fiscal years
                        import re
                        accession_pattern = re.compile(r'^[0-9]{3,}$')  # Match numeric directories that aren't fiscal years
                        fiscal_year_pattern = re.compile(r'^20[0-9]{2}$')  # Match typical fiscal years (2000-2099)
                        
                        logging.info(f"Performing comprehensive GCP cleanup for ALL accession directories under {ticker_prefix}")
                        
                        try:
                            # Use delimiter to get "directory"-like prefixes
                            prefixes = []
                            blobs_iter = self.pipeline.gcp_storage.bucket.list_blobs(
                                prefix=ticker_prefix, 
                                delimiter='/'
                            )
                            
                            # Process the prefixes (directory-like listings)
                            for prefix in blobs_iter.prefixes:
                                # Strip the trailing slash and get the last component
                                dir_name = prefix.rstrip('/').split('/')[-1]
                                prefixes.append(dir_name)
                                logging.info(f"Found directory: {dir_name}")
                            
                            # If prefixes don't work, try another approach by listing all blobs
                            if not prefixes:
                                logging.info("No directory prefixes found, trying alternative approach")
                                all_blobs = list(self.pipeline.gcp_storage.bucket.list_blobs(prefix=ticker_prefix))
                                
                                # Extract directory names from blob paths
                                for blob in all_blobs:
                                    parts = blob.name.split('/')
                                    if len(parts) >= 4:  # companies/TICKER/FILING_TYPE/DIR/...
                                        dir_name = parts[3]
                                        if dir_name not in prefixes:
                                            prefixes.append(dir_name)
                                logging.info(f"Found directories through blob listing: {prefixes}")
                            
                            # Now check for accession numbers vs fiscal years
                            for dir_name in prefixes:
                                # If it's all numeric but not a typical fiscal year, it's likely an accession number
                                if accession_pattern.match(dir_name) and not fiscal_year_pattern.match(dir_name):
                                    logging.info(f"Found suspected accession directory: {dir_name}")
                                    
                                    # Delete all files in this directory
                                    accession_prefix = f"{ticker_prefix}{dir_name}/"
                                    logging.info(f"Listing blobs with prefix: {accession_prefix}")
                                    
                                    try:
                                        # List and delete all blobs with this prefix
                                        acc_blobs = list(self.pipeline.gcp_storage.bucket.list_blobs(prefix=accession_prefix))
                                        
                                        for blob in acc_blobs:
                                            logging.info(f"Deleting accession-based file: {blob.name}")
                                            try:
                                                self.pipeline.gcp_storage.delete_file(blob.name)
                                                result.setdefault("deleted_accession_files", []).append(blob.name)
                                            except Exception as del_e:
                                                logging.warning(f"Error deleting file {blob.name}: {str(del_e)}")
                                    except Exception as list_e:
                                        logging.warning(f"Error listing files in accession directory: {str(list_e)}")
                        except Exception as iter_e:
                            logging.warning(f"Error iterating GCS directories: {str(iter_e)}")
                            
                        # Also try the original cleanup approach as a fallback
                        if original_gcs_text_path:
                            # Extract path components
                            path_parts = original_gcs_text_path.split('/')
                            # Check if there are enough parts and if the part right after filing_type might be an accession number
                            if len(path_parts) >= 4:
                                # Check if the potential accession part is numeric and not a fiscal year
                                potential_accession = path_parts[3]
                                if accession_pattern.match(potential_accession) and not fiscal_year_pattern.match(potential_accession):
                                    # Construct a potential accession directory path
                                    gcs_prefix = '/'.join(path_parts[:-1])  # Remove the filename
                                    
                                    # Get all blobs with this prefix
                                    logging.info(f"Checking for accession-based files with specific prefix: {gcs_prefix}")
                                    accession_blobs = list(self.pipeline.gcp_storage.bucket.list_blobs(prefix=gcs_prefix))
                                    
                                    # Delete each blob
                                    for blob in accession_blobs:
                                        logging.info(f"Deleting accession-based file: {blob.name}")
                                        self.pipeline.gcp_storage.delete_file(blob.name)
                                        result.setdefault("deleted_accession_files", []).append(blob.name)
                except Exception as acc_e:
                    logging.warning(f"Error cleaning up accession directories: {str(acc_e)}")
                
                # Upload the reorganized files to GCS
                logging.info(f"Re-uploading files to GCS with fiscal year paths")
                
                force_upload = result.get("force_upload", False)
                text_result = self.pipeline.gcp_storage.upload_file(str(new_text_path), gcs_text_path, force=True)
                
                if os.path.exists(new_llm_path):
                    llm_result = self.pipeline.gcp_storage.upload_file(str(new_llm_path), gcs_llm_path, force=True)
                    if llm_result.get("success"):
                        result["reorganized_gcs_llm_path"] = gcs_llm_path
                
                if text_result.get("success"):
                    result["reorganized_gcs_text_path"] = gcs_text_path
                    
                    # Update Firestore metadata with the correct paths
                    try:
                        # Extract period_end_date from original result with more thorough search
                        period_end_date = None
                        
                        # Check in stages->download->result if it exists
                        if "stages" in result and "download" in result["stages"]:
                            download_result = result["stages"]["download"]["result"]
                            
                            # Check if period_end_date is directly in download result
                            if "period_end_date" in download_result:
                                period_end_date = download_result["period_end_date"]
                                logging.info(f"Found period_end_date in download result: {period_end_date}")
                            # Check if filing_info contains period_end_date (most common case)
                            elif "filing_info" in download_result:
                                filing_info = download_result["filing_info"]
                                if "period_end_date" in filing_info:
                                    period_end_date = filing_info["period_end_date"]
                                    logging.info(f"Found period_end_date in download_result.filing_info: {period_end_date}")
                        
                        # If not found in download result, check other locations
                        if not period_end_date:
                            # Check if it's in the filing_info section of the result
                            if "filing_info" in result:
                                period_end_date = result["filing_info"].get("period_end_date")
                                logging.info(f"Found period_end_date in filing_info: {period_end_date}")
                            # Or it might be directly in the result
                            elif "period_end_date" in result:
                                period_end_date = result["period_end_date"]
                                logging.info(f"Found period_end_date directly in result: {period_end_date}")
                        
                        # Prepare metadata
                        metadata = {
                            "ticker": ticker,
                            "company_name": ticker,  # Can be overridden if available
                            "filing_type": filing_type,
                            "fiscal_year": fiscal_year,
                            "fiscal_period": fiscal_period if fiscal_period else "annual" if filing_type == "10-K" else None,
                            "text_path": gcs_text_path,
                            "text_size": os.path.getsize(str(new_text_path)),
                            "local_text_path": str(new_text_path)
                        }
                        
                        # ALWAYS include period_end_date in metadata 
                        # This is CRITICAL for proper fiscal period determination
                        if period_end_date:
                            metadata["period_end_date"] = period_end_date
                            logging.info(f"Using preserved period_end_date: {period_end_date}")
                        else:
                            # Access the original filing that was downloaded
                            # This is a deeper way to find the period_end_date
                            try:
                                for result_key in result.keys():
                                    if isinstance(result[result_key], dict) and "period_end_date" in result[result_key]:
                                        period_end_date = result[result_key]["period_end_date"]
                                        metadata["period_end_date"] = period_end_date
                                        logging.info(f"Found period_end_date in result[{result_key}]: {period_end_date}")
                                        break
                            except Exception as e:
                                logging.error(f"Error searching for period_end_date in result: {e}")
                            
                            if not period_end_date:
                                logging.error(f"CRITICAL: No period_end_date found for {ticker} {filing_type} {fiscal_year}, metadata will be incomplete")
                        
                        # Add LLM path if available
                        if os.path.exists(new_llm_path):
                            metadata["llm_path"] = gcs_llm_path
                            metadata["llm_size"] = os.path.getsize(str(new_llm_path))
                            metadata["local_llm_path"] = str(new_llm_path)
                        
                        # Import datetime here to ensure it's available
                        import datetime
                        
                        # Ensure all required fields are present in metadata
                        if "fiscal_year" not in metadata:
                            metadata["fiscal_year"] = fiscal_year
                            logging.info(f"Added fiscal_year={fiscal_year} from function parameters")
                            
                        if "fiscal_period" not in metadata:
                            metadata["fiscal_period"] = fiscal_period
                            logging.info(f"Added fiscal_period={fiscal_period} from function parameters")
                            
                        # As a safety check, if fiscal_year is still missing, use period_end_date or current year
                        if not metadata.get("fiscal_year"):
                            period_end_date = metadata.get("period_end_date", "")
                            if period_end_date and "-" in period_end_date:
                                metadata["fiscal_year"] = period_end_date.split("-")[0]
                                logging.info(f"Extracted fiscal_year={metadata['fiscal_year']} from period_end_date")
                            else:
                                metadata["fiscal_year"] = str(datetime.datetime.now().year)
                                logging.info(f"Using current year as fiscal_year={metadata['fiscal_year']}")
                                
                        # If fiscal_period is still missing, set a safe default
                        if not metadata.get("fiscal_period"):
                            metadata["fiscal_period"] = "annual" if filing_type == "10-K" else "Q?"
                            logging.info(f"Using default fiscal_period={metadata['fiscal_period']}")
                        
                        # Update Firestore with proper fiscal information
                        firestore_result = self.pipeline.gcp_storage.add_filing_metadata(metadata)
                        
                        if firestore_result.get("success"):
                            logging.info(f"Updated Firestore metadata with fiscal year-based paths")
                            result["reorganized_firestore"] = True
                    except Exception as fs_error:
                        logging.warning(f"Error updating Firestore metadata: {str(fs_error)}")
        except Exception as copy_error:
            logging.warning(f"Error copying files to fiscal year directory: {str(copy_error)}")
            raise
            
        return result
    
    def _process_single_filing(self, filing_info):
        """Process a single filing and return the result"""
        ticker = filing_info["ticker"]
        filing_type = filing_info["filing_type"]
        filing_index = filing_info["filing_index"]
        year = filing_info["year"]
        quarter = filing_info.get("quarter")
        calendar_year = filing_info.get("calendar_year")
        calendar_months = filing_info.get("calendar_months")
        
        # Check if this is an amended filing
        is_amended = filing_info.get("is_amended", False)
        original_filing_type = filing_info.get("original_filing_type", filing_type)
        use_amendment_subdirectory = filing_info.get("use_amendment_subdirectory", False)
        
        # Add force_upload flag to filing_info to pass it to the pipeline
        if is_amended:
            # For amended filings, we now use a subdirectory structure instead of skipping
            filing_info["force_upload"] = self.force_upload
            filing_info["skip_gcp_upload"] = False  # Don't skip the upload, just use different path
            # Mark it for storage in "/a" subdirectory
            filing_info["use_amendment_subdirectory"] = True
            logging.info(f"Amended filing will be stored in '/a' subdirectory structure")
        else:
            filing_info["force_upload"] = self.force_upload
            filing_info["skip_gcp_upload"] = False
        
        # Add explicit fiscal year to filing_info for consistent paths
        filing_info["fiscal_year"] = str(year)
        if quarter:
            filing_info["fiscal_period"] = f"Q{quarter}"
        elif filing_type == "10-K":
            filing_info["fiscal_period"] = "annual"
        
        # Create appropriate log message with amendment status
        log_message = f"Processing {ticker} {original_filing_type} for {year}"
        if is_amended:
            log_message = f"Processing {ticker} {original_filing_type} (AMENDED) for {year}"
        
        if quarter:
            log_message += f", Q{quarter}"
        if calendar_year and calendar_months:
            log_message += f" (calendar year: {calendar_year}, months: {calendar_months})"
        else:
            log_message += f" (index: {filing_index})"
        
        # Special debug for NVDA 2024 10-K which appears to be missing
        if ticker == "NVDA" and filing_type == "10-K" and year == 2024:
            logging.info(f"☢️ SPECIAL DEBUGGING: {log_message}")
            logging.info(f"☢️ Will attempt special handling for NVDA 2024 10-K")
        
        logging.info(log_message)
        
        try:
            # Special handling for NVDA 2024 10-K
            if ticker == "NVDA" and filing_type == "10-K" and year == 2024:
                logging.info("Using special handling for NVDA 2024 10-K")
                
                # Create a custom filing info that targets the specific 10-K filing
                from src2.sec.downloader import SECDownloader
                
                # Create a downloader with the same configuration
                user_agent = self.pipeline.downloader.user_agent
                contact_email = "info@example.com"  # Default fallback
                
                # Extract email from user agent if available
                import re
                email_match = re.search(r'\((.*?)\)', user_agent)
                if email_match:
                    contact_email = email_match.group(1)
                
                downloader = SECDownloader(
                    user_agent=user_agent.split(" (")[0] if "(" in user_agent else user_agent,
                    contact_email=contact_email,
                    download_dir=self.pipeline.downloader.download_dir
                )
                
                # Get all filings for the specified year and filing type
                logging.info(f"Getting all 10-K filings for NVDA to find the 2024 10-K")
                all_filings = downloader.get_company_filings(
                    ticker=ticker,
                    filing_type=filing_type,
                    count=20  # Get enough filings to find the right one
                )
                
                logging.info(f"Retrieved {len(all_filings)} 10-K filings for NVDA")
                
                # Look specifically for a filing with January 2024 period end date
                target_filing = None
                # Make sure we have datetime available in this scope
                import datetime
                
                for filing in all_filings:
                    period_end = filing.get("period_end_date")
                    if period_end:
                        try:
                            end_date = datetime.datetime.strptime(period_end, '%Y-%m-%d')
                            logging.info(f"Found a filing with period end date: {period_end}")
                            
                            # For NVDA's 2024 10-K, we expect a January 2024 period end date
                            if end_date.year == 2024 and end_date.month == 1:
                                target_filing = filing
                                logging.info(f"Found NVDA 2024 10-K with period end date: {period_end}")
                                break
                            # Also check for late 2023 period end date, as it might be labeled that way
                            elif end_date.year == 2023 and end_date.month >= 11:
                                # This is potentially a candidate
                                if not target_filing:
                                    target_filing = filing
                                    logging.info(f"Found potential NVDA 2024 10-K with period end date: {period_end}")
                        except (ValueError, TypeError):
                            continue
                
                if target_filing:
                    # Ensure the fiscal year is properly set in the filing info
                    # so it's used in the local filename and GCP path
                    target_filing["fiscal_year"] = str(year)
                    target_filing["fiscal_period"] = "annual"
                    
                    logging.info(f"Processing specific NVDA 2024 10-K filing with explicit fiscal_year={year}")
                    logging.info(f"Setting explicit fiscal_year={target_filing['fiscal_year']} and fiscal_period=annual")
                    
                    result = self.pipeline.process_filing_with_info(target_filing)
                    # Override the year to ensure it's displayed correctly
                    result["year"] = year
                    result["fiscal_year"] = str(year)
                    if result.get("success"):
                        result["status"] = "success"
                    else:
                        result["status"] = "error"
                        result["error"] = result.get("error", "Unknown error processing NVDA 2024 10-K")
                    return result
                else:
                    logging.error(f"Could not find NVDA 2024 10-K filing with appropriate period end date")
                    return {
                        "ticker": ticker,
                        "filing_type": filing_type,
                        "year": year,
                        "error": "No 2024 10-K filing found for NVDA with January 2024 period end date",
                        "status": "error"
                    }
            
            # Use custom downloader for period-specific filings if we have calendar data
            if calendar_year and calendar_months and quarter:
                # This is the specialized Microsoft quarter processing path
                from src2.sec.downloader import SECDownloader
                
                # Make sure we have required imports
                import re
                import datetime
                
                # Extract the email from the user_agent string format "{agent_name} ({email})"
                contact_email = "info@example.com"  # Default fallback
                user_agent = self.pipeline.downloader.user_agent
                
                # Try to extract email from the user agent string if it's in the expected format
                email_match = re.search(r'\((.*?)\)', user_agent)
                if email_match:
                    contact_email = email_match.group(1)
                
                # Create a new downloader with the same configuration
                downloader = SECDownloader(
                    user_agent=user_agent.split(" (")[0] if "(" in user_agent else user_agent,
                    contact_email=contact_email,
                    download_dir=self.pipeline.downloader.download_dir
                )
                
                # Calculate the appropriate number of filings to fetch based on year range
                import datetime
                current_year = datetime.datetime.now().year
                years_to_search = current_year - min(year, current_year) + 1
                filings_per_year = 4  # 4 quarters per year for 10-Q
                filing_count = (years_to_search * filings_per_year) + 4  # Add buffer of 4 filings
                
                # Cap at a reasonable maximum to prevent excessive API calls
                filing_count = min(filing_count, 50)
                
                logging.info(f"Calculated search depth of {filing_count} filings to cover {years_to_search} years ({min(year, current_year)}-{current_year})")
                
                # Get all filings for the specified year and filing type
                all_filings = downloader.get_company_filings(
                    ticker=ticker,
                    filing_type=filing_type,
                    count=filing_count  # Dynamically calculated based on year range
                )
                
                logging.info(f"Retrieved {len(all_filings)} {filing_type} filings for {ticker}")
                
                # Filter filings by period end date to find the specific quarter
                target_filing = None
                
                # Log expected period end details for debugging
                expected_period_month = calendar_months[0] if calendar_months else None
                logging.info(f"Looking for {ticker} FY{year} Q{quarter} filing with period end in year {calendar_year}, month {expected_period_month}")
                
                for filing in all_filings:
                    period_end = filing.get("period_end_date")
                    if period_end:
                        try:
                            # Parse the period end date
                            end_date = datetime.datetime.strptime(period_end, '%Y-%m-%d')
                            
                            # Check if the period end date matches expected month and year
                            if end_date.year == calendar_year and end_date.month in calendar_months:
                                target_filing = filing
                                logging.info(f"Found target filing for {ticker} FY{year} Q{quarter}: {period_end}")
                                break
                            else:
                                # Additional detailed logging to understand what we found vs. what we expected
                                logging.info(f"Filing with date {period_end} (year={end_date.year}, month={end_date.month}) " +
                                           f"doesn't match expected year {calendar_year}, month {expected_period_month}")
                        except (ValueError, TypeError) as e:
                            logging.warning(f"Error parsing period end date '{period_end}': {e}")
                            continue
                
                if target_filing:
                    # Ensure the fiscal year is properly set in the filing info
                    # so it's used in the local filename and GCP path
                    target_filing["fiscal_year"] = str(year)
                    target_filing["fiscal_period"] = f"Q{quarter}"
                    
                    logging.info(f"Processing filing with period end date {target_filing.get('period_end_date')} for {ticker} {filing_type} FY{year} Q{quarter}")
                    logging.info(f"Setting explicit fiscal_year={target_filing['fiscal_year']} and fiscal_period={target_filing['fiscal_period']}")
                    
                    # Process using the specific filing we found
                    result = self.pipeline.process_filing_with_info(target_filing)
                    # Add fiscal metadata
                    result["fiscal_year"] = str(year)
                    result["fiscal_quarter"] = f"Q{quarter}"
                else:
                    # No filing found with exact month/year match - simply report the error
                    logging.error(f"No exact match found for {ticker} FY{year} Q{quarter} with period end in {calendar_year}-{expected_period_month}")
                    return {
                        "ticker": ticker,
                        "filing_type": filing_type,
                        "year": year,
                        "quarter": quarter,
                        "error": f"No filing found for {ticker} FY{year} Q{quarter} with period end in {calendar_year}-{expected_period_month}",
                        "status": "error"
                    }
            else:
                # Modified to apply fiscal period filtering using our fiscal registry
                # Get all possible filings of this type first 
                logging.info(f"Getting all {filing_type} filings for {ticker} to find ones for fiscal year {year}")
                
                # Use a larger count to ensure we capture filings from past years
                count = 10 if filing_type == "10-K" else 20  # More quarterly filings than annual
                
                all_filings = self.pipeline.downloader.get_company_filings(
                    ticker=ticker,
                    filing_type=filing_type,
                    count=count  # Get enough filings to find the right ones
                )
                
                logging.info(f"Retrieved {len(all_filings)} {filing_type} filings for {ticker}")
                
                # Filter filings based on period end date, amendment status, and our fiscal registry
                target_filings = []
                
                # Import fiscal registry
                from src2.sec.fiscal.company_fiscal import fiscal_registry
                
                # Check if we're in amendments-only mode
                amendments_only = filing_info.get("amendments_only", False)
                
                if amendments_only:
                    logging.info(f"AMENDMENTS-ONLY MODE: Will process only amended filings for {ticker} {filing_type}")
                
                for filing in all_filings:
                    period_end_date = filing.get("period_end_date")
                    is_amended = filing.get("is_amended", False)
                    
                    # In amendments-only mode, skip non-amended filings
                    if amendments_only and not is_amended:
                        continue
                    
                    # In regular mode, skip amended filings (they'll be processed separately)
                    if not amendments_only and is_amended:
                        logging.info(f"Skipping amended filing in regular mode: {filing.get('original_filing_type', filing_type)} "
                                     f"from {filing.get('filing_date')}. Use --amendments-only to process this.")
                        continue
                    
                    if period_end_date:
                        try:
                            # Look up fiscal information for this period end date
                            fiscal_info = fiscal_registry.determine_fiscal_period(
                                ticker=ticker,
                                period_end_date=period_end_date,
                                filing_type=filing_type
                            )
                            
                            logging.info(f"Period end date {period_end_date} maps to: {fiscal_info}")
                            
                            # Check if this filing belongs to our target fiscal year
                            if fiscal_info.get("fiscal_year") == str(year):
                                if is_amended:
                                    logging.info(f"Found matching AMENDED filing for fiscal year {year}: {period_end_date}")
                                else:
                                    logging.info(f"Found matching filing for fiscal year {year}: {period_end_date}")
                                target_filings.append(filing)
                        except Exception as e:
                            logging.warning(f"Error determining fiscal period for {period_end_date}: {str(e)}")
                
                if target_filings:
                    # Process the first matching filing
                    selected_filing = target_filings[0]
                    
                    # Ensure the fiscal year is properly set in the filing info
                    # so it's used in the local filename and GCP path
                    selected_filing["fiscal_year"] = str(year)
                    if "quarter" in filing_info and filing_info["quarter"]:
                        selected_filing["fiscal_period"] = f"Q{filing_info['quarter']}"
                    elif filing_type == "10-K":
                        selected_filing["fiscal_period"] = "annual"
                    
                    logging.info(f"Processing filing with period end date {selected_filing.get('period_end_date')} for {ticker} {filing_type} FY{year}")
                    logging.info(f"Setting explicit fiscal_year={selected_filing['fiscal_year']} and fiscal_period={selected_filing.get('fiscal_period', 'None')}")
                    
                    result = self.pipeline.process_filing_with_info(selected_filing)
                else:
                    # No matching filings found
                    logging.error(f"No {filing_type} filing found for {ticker} fiscal year {year}")
                    return {
                        "ticker": ticker,
                        "filing_type": filing_type,
                        "year": year,
                        "error": f"No filing found matching fiscal year {year}",
                        "status": "error"
                    }
            
            # Add year information from filing_info
            if result.get("success"):
                result["year"] = year
                result["status"] = "success"
                # Ensure GCS upload worked if GCP is configured
                if self.pipeline.gcp_storage and "text_gcs_path" not in result and "error" not in result:
                    result["warning"] = "GCS upload may have failed - no GCS path returned but process marked successful"
                    logging.warning(f"WARNING: {ticker} {filing_type} for {year} was marked successful but has no GCS path")
                
                # Get file sizes from result if available
                if "stages" in result and "extract" in result["stages"]:
                    extract_result = result["stages"]["extract"].get("result", {})
                    
                    if "file_size_mb" in extract_result:
                        file_size_mb = extract_result["file_size_mb"]
                        result["file_size_mb"] = file_size_mb
                        
                        if file_size_mb < 0.1:
                            logging.warning(f"Small file warning: {ticker} {filing_type} file size is {file_size_mb:.2f} MB")
                            result["warning"] = f"Small file size: {file_size_mb:.2f} MB"
                
                # Reorganize files to use fiscal years instead of accession numbers
                try:
                    self._reorganize_files_by_fiscal_year(
                        ticker=ticker, 
                        filing_type=filing_type, 
                        fiscal_year=str(year), 
                        fiscal_period=filing_info.get("fiscal_period", "annual") if filing_type == "10-K" else f"Q{quarter}" if quarter else None,
                        result=result
                    )
                except Exception as reorg_error:
                    logging.warning(f"Error reorganizing files by fiscal year: {str(reorg_error)}")
                    result["warning"] = f"File reorganization error: {str(reorg_error)}"
                
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
    # Import libraries locally to avoid scope issues
    import re
    import datetime
    
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
    parser.add_argument("--amendments-only", action="store_true", default=False,
                        help="Process only amended filings (10-K/A, 10-Q/A)")
    parser.add_argument("--email", help="Contact email for SEC identification",
                        default="info@example.com")
    parser.add_argument("--workers", type=int, default=1,
                        help="Maximum number of concurrent workers (default: 1)")
    parser.add_argument("--output", help="Output directory for processed files")
    parser.add_argument("--gcp-bucket", help="GCS bucket name for upload")
    parser.add_argument("--gcp-project", help="GCP project ID for upload")
    parser.add_argument("--force-upload", action="store_true", default=False,
                        help="Force upload of files even if they already exist in GCS (useful for initial load)")
    parser.add_argument("--skip-text-files", action="store_true", default=False,
                        help="Skip generating text.txt files and only produce LLM-formatted (llm.txt) files")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.start_year > args.end_year:
        parser.error("start-year must be less than or equal to end-year")
        
    if args.workers < 1:
        parser.error("workers must be at least 1")
        
    if not args.process_10k and not args.process_10q:
        parser.error("Must process at least one filing type (10-K or 10-Q)")
    
    # Update config based on command line arguments
    if args.skip_text_files:
        from src2.config import OUTPUT_FORMAT
        OUTPUT_FORMAT["GENERATE_TEXT_FILES"] = False
        print("Skipping text.txt file generation - only LLM format will be produced")

    # Create batch pipeline
    batch = BatchSECPipeline(
        user_agent=f"NativeLLM_BatchPipeline/1.0",
        contact_email=args.email,
        output_dir=args.output or "./sec_processed",
        gcp_bucket=args.gcp_bucket,
        gcp_project=args.gcp_project,
        force_upload=args.force_upload,  # Pass the force_upload flag
        amendments_only=args.amendments_only,  # Pass the amendments_only flag
        skip_text_files=args.skip_text_files  # Pass the skip_text_files flag
    )
    
    # Process filings
    try:
        # Create a descriptive message about what we're processing
        if args.amendments_only:
            print(f"\n=== Starting amendments-only processing for {args.ticker} from {args.start_year} to {args.end_year} ===")
            print(f"Note: This will only process amended filings (10-K/A, 10-Q/A)")
        else:
            print(f"\n=== Starting batch processing for {args.ticker} from {args.start_year} to {args.end_year} ===")
        
        results = batch.process_filings_by_years(
            ticker=args.ticker,
            start_year=args.start_year,
            end_year=args.end_year,
            include_10k=args.process_10k,
            include_10q=args.process_10q,
            max_workers=args.workers
        )
        
        # Count amended filings
        amended_filings = []
        for filing in results["filings_processed"]:
            if filing.get("is_amended", False) and filing.get("status") == "success":
                filing_type = filing.get("original_filing_type", filing.get("filing_type", "Unknown"))
                amended_filings.append({
                    "ticker": filing.get("ticker", args.ticker),
                    "filing_type": filing_type,
                    "year": filing.get("year", "Unknown"),
                    "quarter": filing.get("quarter", None),
                    "period_end_date": filing.get("period_end_date", "Unknown"),
                    "local_path": filing.get("local_path", ""),
                    # Include cloud paths for amended filings
                    "gcs_text_path": filing.get("amended_gcs_text_path", ""),
                    "gcs_llm_path": filing.get("amended_gcs_llm_path", "")
                })
        
        # Print a summary of results
        if args.amendments_only:
            print("\n=== Amendments-Only Processing Results ===")
        else:
            print("\n=== Batch Processing Results ===")
        print(f"Company: {args.ticker}")
        print(f"Fiscal Years: {args.start_year} to {args.end_year}")
        print(f"Filings Processed: {results['summary']['total_filings']}")
        print(f"Successful: {results['summary']['successful_filings']}")
        print(f"Failed: {results['summary']['failed_filings']}")
        print(f"Amended Filings: {len(amended_filings)}")
        print(f"Total Time: {results['summary']['total_time_seconds']:.2f} seconds")
        
        # Print details of any amended filings
        if amended_filings:
            print("\nAmended Filings (stored in '/a' subdirectories):")
            for filing in amended_filings:
                filing_info = f"{filing['filing_type']} ({filing['year']}"
                if filing.get("quarter"):
                    filing_info += f", Q{filing['quarter']}"
                filing_info += ")"
                
                print(f"  - {filing_info}:")
                print(f"    • Local: {filing.get('local_path', 'Unknown path')}")
                if filing.get('gcs_text_path'):
                    print(f"    • GCS Text: {filing.get('gcs_text_path')}")
                if filing.get('gcs_llm_path'):
                    print(f"    • GCS LLM: {filing.get('gcs_llm_path')}")
        
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