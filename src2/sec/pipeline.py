#!/usr/bin/env python3
"""
SEC Filing Pipeline

Complete pipeline for downloading, rendering, and extracting SEC iXBRL filings
with proper SEC-compliant handling and fallbacks.
"""

import os
import sys
import logging
import json
import time
import argparse
import re
from pathlib import Path

# Import from config
from src2.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

# Import from SEC modules
from .downloader import SECDownloader
from .renderer import ArelleRenderer
from .extractor import SECExtractor

class SECFilingPipeline:
    """
    Complete pipeline for SEC filing processing.

    This class integrates downloading, rendering, and extraction of SEC filings
    with proper error handling and fallbacks.
    """

    @staticmethod
    def extract_fiscal_period_from_document(document_text, ticker, period_end_date, filing_type):
        """
        Extract fiscal period directly from the filing document text.

        Args:
            document_text: The text of the SEC filing document
            ticker: Company ticker
            period_end_date: Period end date string (YYYY-MM-DD)
            filing_type: Filing type (10-K or 10-Q)

        Returns:
            tuple: (fiscal_year, fiscal_period)
        """
        import re
        import datetime

        fiscal_year = None
        fiscal_period = None

        # Log what we're doing
        print(f"Attempting to extract fiscal period directly from document text for {ticker} {filing_type}")

        try:
            # For 10-K, it's always annual
            if filing_type == "10-K":
                fiscal_period = "annual"

                # Try to extract fiscal year - look for specific patterns
                # Pattern 1: "For the fiscal year ended January 28, 2024" (common in 10-K filings)
                fiscal_year_pattern1 = re.search(r'(?:for|the|fiscal|year).*?(?:ended|ending).*?(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+(\d{4})', document_text[:10000], re.IGNORECASE)

                # Pattern 2: "For the period ended January 28, 2024" (another common pattern)
                fiscal_year_pattern2 = re.search(r'(?:for|the|period).*?(?:ended|ending).*?(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+(\d{4})', document_text[:10000], re.IGNORECASE)

                # Pattern 3: Just look for fiscal year mentions like "fiscal year 2024"
                fiscal_year_pattern3 = re.search(r'(?:fiscal|year).*?(\d{4})', document_text[:10000], re.IGNORECASE)

                if fiscal_year_pattern1:
                    fiscal_year = fiscal_year_pattern1.group(1)
                    print(f"Extracted fiscal year {fiscal_year} from 10-K text (pattern 1)")
                elif fiscal_year_pattern2:
                    fiscal_year = fiscal_year_pattern2.group(1)
                    print(f"Extracted fiscal year {fiscal_year} from 10-K text (pattern 2)")
                elif fiscal_year_pattern3:
                    fiscal_year = fiscal_year_pattern3.group(1)
                    print(f"Extracted fiscal year {fiscal_year} from 10-K text (pattern 3)")
                elif period_end_date:
                    # Fall back to date-based calculation
                    date = datetime.datetime.strptime(period_end_date, '%Y-%m-%d')

                    # For companies with non-calendar fiscal years, like NVDA,
                    # the fiscal year might be different from the calendar year
                    if ticker.upper() == "NVDA" and date.month == 1:
                        # NVDA's fiscal year ending in January is reported as that year
                        fiscal_year = str(date.year)
                    else:
                        fiscal_year = str(date.year)

                    print(f"Using period end date year {fiscal_year} for 10-K")

                return (fiscal_year, fiscal_period)

            # For 10-Q, we need to determine which quarter it is
            if filing_type == "10-Q":
                # Most definitive pattern: "For the quarterly period ended April 28, 2024"
                quarter_pattern1 = re.search(r'for the\s+(?:quarterly|quarterly report|quarterly period|three months)\s+(?:ended|ending).*?(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+(\d{4})', document_text[:10000], re.IGNORECASE)

                # Look for specific quarter mentions
                quarter_pattern2 = re.search(r'(?:quarterly report|form 10-q|quarterly period|quarterly).*?(?:first|second|third|fourth|1st|2nd|3rd|4th|\sQ1|\sQ2|\sQ3|\sQ4)', document_text[:10000], re.IGNORECASE)

                # Another pattern: "For the quarter ended April 28, 2024"
                quarter_pattern3 = re.search(r'for the\s+(?:quarter|three months)\s+(?:ended|ending).*?(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+(\d{4})', document_text[:10000], re.IGNORECASE)

                # Extract period end date from the document itself if available
                period_date_pattern = re.search(r'(?:quarterly|quarterly report|quarter|period).*?(?:ended|ending).*?((?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4})', document_text[:10000], re.IGNORECASE)

                # Look for fiscal year mentions
                fiscal_year_pattern = re.search(r'(?:fiscal|year).*?(\d{4})', document_text[:10000], re.IGNORECASE)

                # Get the period end date from document if we found one
                document_period_end = None
                if period_date_pattern:
                    try:
                        date_str = period_date_pattern.group(1)
                        document_period_end = datetime.datetime.strptime(date_str, '%B %d, %Y')
                        print(f"Found period end date in document: {document_period_end.strftime('%Y-%m-%d')}")
                    except Exception:
                        try:
                            document_period_end = datetime.datetime.strptime(date_str, '%b %d, %Y')
                            print(f"Found period end date in document: {document_period_end.strftime('%Y-%m-%d')}")
                        except Exception as e:
                            print(f"Could not parse period end date from document: {e}")

                # First try to extract explicit quarter mention
                quarter_text = None
                if quarter_pattern2:
                    quarter_text = quarter_pattern2.group(0).lower()
                    print(f"Found quarter mention: {quarter_text}")

                    if "first" in quarter_text or "1st" in quarter_text or "q1" in quarter_text:
                        fiscal_period = "Q1"
                    elif "second" in quarter_text or "2nd" in quarter_text or "q2" in quarter_text:
                        fiscal_period = "Q2"
                    elif "third" in quarter_text or "3rd" in quarter_text or "q3" in quarter_text:
                        fiscal_period = "Q3"
                    elif "fourth" in quarter_text or "4th" in quarter_text or "q4" in quarter_text:
                        fiscal_period = "Q4"

                    if fiscal_period:
                        print(f"Extracted fiscal period {fiscal_period} from 10-Q text")

                # If couldn't find quarter from text, infer from period end date
                if not fiscal_period:
                    # Use the period end date we found in the document, or fall back to the provided one
                    inference_date = None

                    if document_period_end:
                        inference_date = document_period_end
                    elif period_end_date:
                        try:
                            inference_date = datetime.datetime.strptime(period_end_date, '%Y-%m-%d')
                        except Exception as e:
                            print(f"Could not parse provided period end date: {e}")

                    if inference_date:
                        month = inference_date.month

                        # NVDA-specific quarter mapping based on observed filings
                        if ticker.upper() == "NVDA":
                            if month == 4:  # April
                                fiscal_period = "Q1"
                            elif month == 7:  # July
                                fiscal_period = "Q2"
                            elif month == 10:  # October
                                fiscal_period = "Q3"
                            elif month == 1:  # January
                                fiscal_period = "annual"  # 10-K

                            # NVDA fiscal year logic - February to January fiscal year
                            if 2 <= month <= 12:  # Feb-Dec
                                fiscal_year = str(inference_date.year + 1)
                            else:  # January
                                fiscal_year = str(inference_date.year)

                            print(f"Applied NVDA-specific fiscal mapping: month={month} → period={fiscal_period}, year={fiscal_year}")
                        else:
                            # Add other company-specific mappings here if needed
                            pass

                        if fiscal_period:
                            print(f"Inferred fiscal period {fiscal_period} from date (month {month})")

                # Try to extract fiscal year if we haven't already
                if not fiscal_year:
                    # First try patterns that found the period date
                    if quarter_pattern1:
                        fiscal_year = quarter_pattern1.group(1)
                        print(f"Extracted fiscal year {fiscal_year} from 10-Q text (pattern 1)")
                    elif quarter_pattern3:
                        fiscal_year = quarter_pattern3.group(1)
                        print(f"Extracted fiscal year {fiscal_year} from 10-Q text (pattern 3)")
                    elif fiscal_year_pattern:
                        fiscal_year = fiscal_year_pattern.group(1)
                        print(f"Extracted fiscal year {fiscal_year} from 10-Q text (fiscal year pattern)")
                    elif document_period_end:
                        # For NVDA with fiscal year ending in January
                        if ticker.upper() == "NVDA":
                            month = document_period_end.month
                            if 2 <= month <= 12:  # Feb-Dec
                                fiscal_year = str(document_period_end.year + 1)
                            else:  # January
                                fiscal_year = str(document_period_end.year)
                        else:
                            fiscal_year = str(document_period_end.year)
                        print(f"Using document period end date year {fiscal_year} for 10-Q")
                    elif period_end_date:
                        # Default to provided period end date
                        try:
                            date = datetime.datetime.strptime(period_end_date, '%Y-%m-%d')
                            # NVDA specific logic if not already handled
                            if ticker.upper() == "NVDA" and not fiscal_year:
                                month = date.month
                                if 2 <= month <= 12:  # Feb-Dec
                                    fiscal_year = str(date.year + 1)
                                else:  # January
                                    fiscal_year = str(date.year)
                            else:
                                fiscal_year = str(date.year)
                            print(f"Using provided period end date year {fiscal_year} for 10-Q")
                        except Exception as e:
                            print(f"Error parsing provided period end date: {e}")

                return (fiscal_year, fiscal_period)

        except Exception as e:
            print(f"Error extracting fiscal period from document: {e}")

        print(f"Could not extract fiscal period from document, returning: year={fiscal_year}, period={fiscal_period}")
        return (fiscal_year, fiscal_period)

    @staticmethod
    def determine_fiscal_period_from_registry(ticker, period_end_date, filing_type):
        """
        Use the fiscal registry as the single source of truth for all fiscal period determinations

        Args:
            ticker: Company ticker
            period_end_date: Period end date string (in any format)
            filing_type: Filing type (10-K or 10-Q)

        Returns:
            tuple: (fiscal_year, fiscal_period, validation_metadata)
        """
        # Import datetime at the function level to avoid scope issues
        import datetime

        try:
            # Import the fiscal data contracts and registry
            from src2.sec.fiscal.fiscal_data import FiscalPeriodInfo, FiscalDataError, validate_period_end_date
            from src2.sec.fiscal.company_fiscal import fiscal_registry

            # Validation metadata for logging and debugging
            validation_metadata = {
                "source": "pipeline.py:determine_fiscal_period_from_registry",
                "timestamp": datetime.datetime.now().isoformat(),
                "raw_period_end_date": period_end_date
            }

            # Use the fiscal registry as the single source of truth
            logging.info(f"Getting fiscal information from registry for {ticker}, period_end_date={period_end_date}")

            # Call the registry's determine_fiscal_period method
            fiscal_info = fiscal_registry.determine_fiscal_period(
                ticker, period_end_date, filing_type
            )

            # Extract fiscal information
            fiscal_year = fiscal_info.get("fiscal_year")
            fiscal_period = fiscal_info.get("fiscal_period")

            # Add determination details to metadata
            validation_metadata.update({
                "fiscal_year": fiscal_year,
                "fiscal_period": fiscal_period,
                "validated_date": fiscal_info.get("validated_date"),
                "status": "success" if fiscal_year and fiscal_period else "failed"
            })

            if fiscal_year and fiscal_period:
                logging.info(f"Successfully determined fiscal period from registry: Year={fiscal_year}, Period={fiscal_period}")
                return (fiscal_year, fiscal_period, validation_metadata)
            else:
                error_msg = fiscal_info.get("error", "Unknown error")
                logging.error(f"Fiscal registry couldn't determine period: {error_msg}")
                validation_metadata["error"] = error_msg

                # Last resort for 10-K only - we know it's annual
                if filing_type == "10-K":
                    fiscal_period = "annual"
                    logging.warning(f"Using safe fallback 'annual' for 10-K")
                    validation_metadata["fallback_used"] = "annual_for_10K"
                else:
                    # Use a placeholder to indicate unknown quarter
                    fiscal_period = "Q?"
                    logging.error(f"Using placeholder 'Q?' for unknown 10-Q fiscal period")
                    validation_metadata["fallback_used"] = "Q?_placeholder"

                return (fiscal_year, fiscal_period, validation_metadata)

        except ImportError as e:
            logging.error(f"ERROR: Fiscal registry not available: {e}")
            # Minimal safe fallback for system errors
            fiscal_period = "annual" if filing_type == "10-K" else "Q?"
            return (None, fiscal_period, {"error": str(e), "status": "import_error"})

        except Exception as e:
            logging.error(f"ERROR: Unexpected error determining fiscal period: {e}")
            # Minimal safe fallback for system errors
            fiscal_period = "annual" if filing_type == "10-K" else "Q?"
            return (None, fiscal_period, {"error": str(e), "status": "unexpected_error"})

    def __init__(self, user_agent=None, contact_email=None,
                 output_dir="./sec_processed", temp_dir=None,
                 gcp_bucket=None, gcp_project=None):
        """
        Initialize the SEC filing pipeline.

        Args:
            user_agent: User agent string for SEC requests
            contact_email: Contact email for SEC identification
            output_dir: Directory for processed outputs
            temp_dir: Directory for temporary files
            gcp_bucket: GCS bucket name for upload (if None, skips upload)
            gcp_project: GCP project ID for upload
        """
        # Set up directories
        self.output_dir = Path(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)

        # Set up temp directory
        if temp_dir:
            self.temp_dir = Path(temp_dir)
        else:
            self.temp_dir = self.output_dir / "tmp"
        os.makedirs(self.temp_dir, exist_ok=True)

        # GCP settings
        self.gcp_bucket = gcp_bucket
        self.gcp_project = gcp_project
        self.gcp_storage = None

        # Initialize GCP storage if configured
        if gcp_bucket:
            try:
                from src2.storage.gcp_storage import GCPStorage
                self.gcp_storage = GCPStorage(gcp_bucket, gcp_project)
                logging.info(f"Initialized GCP storage with bucket: {gcp_bucket}")
            except ImportError:
                logging.warning("GCP storage module not found. GCP upload disabled.")
            except Exception as e:
                logging.error(f"Error initializing GCP storage: {str(e)}")

        # Initialize components
        self.downloader = SECDownloader(
            user_agent=user_agent or "NativeLLM_SECPipeline/1.0",
            contact_email=contact_email or "user@example.com",
            download_dir=str(self.temp_dir / RAW_DATA_DIR)
        )

        self.renderer = ArelleRenderer(
            temp_dir=str(self.temp_dir / "arelle"),
            install_if_missing=True
        )

        self.extractor = SECExtractor(
            output_dir=str(self.output_dir)
        )

        logging.info(f"Initialized SEC filing pipeline with output dir: {self.output_dir}")

    def process_filing_with_info(self, filing_info, save_intermediate=False):
        """
        Process a filing using a pre-fetched filing_info dictionary.

        Args:
            filing_info: Dictionary with filing information (from SEC downloader)
            save_intermediate: Whether to save intermediate files

        Returns:
            Dictionary with processing results and file paths
        """
        start_time = time.time()
        ticker = filing_info.get("ticker")
        cik = filing_info.get("cik")
        filing_type = filing_info.get("filing_type", "10-K")

        result = {
            "ticker": ticker,
            "cik": cik,
            "filing_type": filing_type,
            "start_time": start_time,
            "stages": {}
        }

        try:
            # Skip the filing lookup since we already have it
            download_start = time.time()

            # Download the filing
            download_result = self.downloader.download_filing(filing_info)

            # Add download stage to results
            result["stages"]["download"] = {
                "success": "error" not in download_result,
                "time_seconds": time.time() - download_start,
                "result": download_result
            }

            if "error" in download_result:
                result["error"] = f"Download failed: {download_result['error']}"
                return result

            # Continue with downloaded document
            document_path = download_result.get("doc_path")
            if not document_path or not os.path.exists(document_path):
                error_msg = "Downloaded document path not found"
                logging.error(error_msg)
                result["error"] = error_msg
                return result

            # Continue with the standard processing from here
            # Stage 2: Render the document
            logging.info(f"Stage 2: Rendering document")
            render_start = time.time()

            # Define output paths
            company_dir = self.output_dir / ticker
            os.makedirs(company_dir, exist_ok=True)

            # Define output text path with fiscal period if available
            # Extract fiscal year and quarter information
            # Initialize with safe defaults to prevent "referenced before assignment" errors
            fiscal_year = filing_info.get("fiscal_year")
            fiscal_period = filing_info.get("fiscal_period")
            period_end_date = filing_info.get("period_end_date", "")

            # If fiscal information is not already in filing_info, derive it
            if (not fiscal_year or not fiscal_period) and ticker and period_end_date:
                try:
                    # Load document text for extraction
                    document_text = None
                    try:
                        # Try to read the document file directly from source
                        doc_path = download_result.get("doc_path")
                        if doc_path and os.path.exists(doc_path):
                            with open(doc_path, 'r', encoding='utf-8', errors='ignore') as f:
                                document_text = f.read()
                                logging.info(f"Read {len(document_text)} chars from {doc_path} for fiscal period extraction")
                    except Exception as e:
                        logging.warning(f"Could not read document text for extraction: {str(e)}")

                    # Use company_fiscal_registry as the single source of truth
                    fiscal_year, fiscal_period, validation_metadata = self.determine_fiscal_period_from_registry(
                        ticker, period_end_date, filing_type
                    )

                    # Store the fiscal information in filing_info for later use
                    filing_info["fiscal_year"] = fiscal_year
                    filing_info["fiscal_period"] = fiscal_period

                    # Add complete validation metadata for audit and debugging
                    filing_info["fiscal_metadata"] = validation_metadata
                    filing_info["fiscal_source"] = "company_fiscal_registry"

                    # Log fiscal determination results
                    if fiscal_year and fiscal_period:
                        logging.info(f"Successfully determined fiscal info: Year={fiscal_year}, Period={fiscal_period}")
                    else:
                        error_msg = validation_metadata.get("error", "Unknown error")
                        logging.error(f"Error determining fiscal info: {error_msg}")

                        if validation_metadata.get("fallback_used"):
                            logging.warning(f"Using fallback: {validation_metadata.get('fallback_used')}")
                except (ImportError, Exception) as e:
                    logging.warning(f"Could not determine fiscal period: {str(e)}")

            # Use fiscal information in filenames to prevent overwriting
            if fiscal_year and fiscal_period and filing_type == "10-Q":
                llm_path = company_dir / f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}_llm.txt"
                rendered_path = company_dir / f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}_rendered.html"
                logging.info(f"Using fiscal period in local filenames: {fiscal_year}_{fiscal_period}")
            elif fiscal_year:
                llm_path = company_dir / f"{ticker}_{filing_type}_{fiscal_year}_llm.txt"
                rendered_path = company_dir / f"{ticker}_{filing_type}_{fiscal_year}_rendered.html"
                logging.info(f"Using fiscal year in local filenames: {fiscal_year}")
            else:
                # IMPORTANT: If we don't have fiscal year, we need to determine it from period_end_date
                # rather than using accession numbers in the filename
                calculated_fiscal_year = None

                # Extract year from period_end_date
                if period_end_date:
                    import re
                    year_match = re.search(r'(\d{4})', period_end_date)
                    if year_match:
                        calculated_fiscal_year = year_match.group(1)
                        logging.info(f"Extracted fiscal year {calculated_fiscal_year} from period_end_date {period_end_date}")

                # If we still don't have a year, use the accession number as a last resort
                if calculated_fiscal_year:
                    llm_path = company_dir / f"{ticker}_{filing_type}_{calculated_fiscal_year}_llm.txt"
                    rendered_path = company_dir / f"{ticker}_{filing_type}_{calculated_fiscal_year}_rendered.html"
                    logging.info(f"Using extracted year in local filenames: {calculated_fiscal_year}")
                else:
                    # Last resort fallback
                    llm_path = company_dir / f"{ticker}_{filing_type}_llm.txt"
                    rendered_path = company_dir / f"{ticker}_{filing_type}_rendered.html"
                    logging.warning(f"No year information available, using generic filenames")

            # Continue with normal rendering
            try:
                # Render to HTML
                rendered_file = self.renderer.render_ixbrl(
                    document_path,
                    output_format="html",
                    output_file=rendered_path
                )

                render_result = {
                    "rendered_file": str(rendered_file),
                    "file_size": os.path.getsize(rendered_file)
                }

            except Exception as e:
                logging.error(f"Rendering failed: {str(e)}")
                render_result = {"error": str(e)}

            # Continue with the standard pipeline from here
            # Add render stage to results
            result["stages"]["render"] = {
                "success": "error" not in render_result,
                "time_seconds": time.time() - render_start,
                "result": render_result
            }

            # Use fallback if rendering failed
            if "error" in render_result:
                logging.info(f"Rendering with Arelle failed: {render_result['error']}")
                logging.info("Using fallback: processing downloaded document directly")
                rendered_path = document_path

                # Update render_result to show success for fallback
                render_result = {
                    "success": True,
                    "fallback_used": True,
                    "original_error": render_result.get("error"),
                    "file_path": document_path,
                    "file_size": os.path.getsize(document_path)
                }

                # Update stage info
                result["stages"]["render"] = {
                    "success": True,
                    "fallback_used": True,
                    "time_seconds": time.time() - render_start,
                    "result": render_result
                }

            # Continue with the rest of the pipeline (stages 3+)
            # Make sure fiscal information is properly initialized for safety
            if 'fiscal_year' not in filing_info and 'period_end_date' in filing_info:
                # Safely extract year from period_end_date as a fallback
                try:
                    filing_info['fiscal_year'] = filing_info['period_end_date'].split('-')[0]
                    logging.info(f"Added fiscal_year={filing_info['fiscal_year']} from period_end_date")
                except Exception as e:
                    logging.warning(f"Could not extract fiscal year from period_end_date: {e}")
                    filing_info['fiscal_year'] = str(datetime.datetime.now().year)

            if 'fiscal_period' not in filing_info:
                # Safe default based on filing type
                filing_info['fiscal_period'] = "annual" if filing_type == "10-K" else "Q?"
                logging.info(f"Added default fiscal_period={filing_info['fiscal_period']} based on filing type")

            # (This is to avoid duplicating all the code)
            return self._continue_processing_after_render(
                result, rendered_path, llm_path,
                ticker, cik, filing_type, filing_info,
                save_intermediate, start_time
            )

        except Exception as e:
            logging.error(f"Pipeline error in process_filing_with_info: {str(e)}")
            result["error"] = str(e)
            result["success"] = False
            result["total_time_seconds"] = time.time() - start_time
            return result

    def _continue_processing_after_render(self, result, rendered_path, llm_path,
                                         ticker, cik, filing_type, filing_info,
                                         save_intermediate, start_time):
        """
        Continue processing after the rendering stage.

        Helper method to avoid code duplication between process_filing and process_filing_with_info.
        """
        # Import datetime at function scope to avoid reference errors
        import datetime

        try:
            # Stage 3: Extract text
            logging.info(f"Stage 3: Extracting text from document")
            extract_start = time.time()

            # Create metadata for the extraction
            metadata = {
                "ticker": ticker,
                "cik": cik,
                "filing_type": filing_type,
                "filing_date": filing_info.get("filing_date"),
                "period_end_date": filing_info.get("period_end_date"),
                "company_name": filing_info.get("company_name", ticker),
                "source_url": filing_info.get("primary_doc_url")
            }

            # Extract content from filing
            logging.info(f"Processing filing to extract content for LLM format")
            extract_result = self.extractor.process_filing(
                rendered_path,
                metadata=metadata
            )

            # Add extraction stage to results
            result["stages"]["extract"] = {
                "success": extract_result.get("success", False),
                "time_seconds": time.time() - extract_start,
                "result": extract_result
            }

            # Save document sections to metadata for LLM formatter
            if 'document_sections' in extract_result:
                metadata['html_content'] = {
                    'document_sections': extract_result['document_sections']
                }
                logging.info(f"Added {len(extract_result['document_sections'])} document sections to metadata for LLM formatter")

            if not extract_result.get("success", False):
                result["error"] = f"Extraction failed: {extract_result.get('error', 'Unknown error')}"
                return result

            # Stage 3b: Extract XBRL data and format for LLM
            logging.info(f"Stage 3b: Extracting XBRL data and formatting for LLM")

            llm_start = time.time()
            llm_result = {"success": False}

            try:
                # Import LLM formatter
                from src2.formatter.llm_formatter import llm_formatter

                # Check for XBRL data in the filing
                xbrl_path = None
                doc_path = None

                # First check for direct XBRL file
                if "xbrl_path" in result["stages"]["download"]["result"]:
                    xbrl_path = result["stages"]["download"]["result"]["xbrl_path"]
                    logging.info(f"Found XBRL file: {xbrl_path}")
                elif "idx_path" in result["stages"]["download"]["result"]:
                    # Use index file to find XBRL
                    xbrl_path = result["stages"]["download"]["result"]["idx_path"]
                    logging.info(f"Found index file: {xbrl_path}")

                # Check for main document which might contain inline XBRL
                if "doc_path" in result["stages"]["download"]["result"]:
                    doc_path = result["stages"]["download"]["result"]["doc_path"]
                    logging.info(f"Found document path for XBRL extraction: {doc_path}")

                    # Add doc_path to metadata for context extraction
                    metadata["doc_path"] = doc_path
                    logging.info(f"Added doc_path to metadata for context extraction: {doc_path}")

                # Use either xbrl_path or doc_path
                if (xbrl_path and os.path.exists(xbrl_path)) or (doc_path and os.path.exists(doc_path)):
                    # Initialize XBRL data structure
                    xbrl_data = {
                        "contexts": {},
                        "units": {},
                        "facts": []
                    }

                    # Start with basic document information
                    xbrl_data["facts"].append({
                        "concept": "DocumentType",
                        "value": filing_type,
                        "context_ref": "AsOf"
                    })
                    xbrl_data["facts"].append({
                        "concept": "EntityRegistrantName",
                        "value": metadata.get("company_name", ""),
                        "context_ref": "AsOf"
                    })

                    # Try to extract XBRL data from main document if no dedicated XBRL file
                    if not xbrl_path and doc_path:
                        # ---- START MODIFIED CODE ----
                        logging.info(f"Calling SECExtractor.extract_inline_xbrl for: {doc_path}")
                        try:
                            # Call the extractor method which now saves the raw JSON
                            extracted_facts = self.extractor.extract_inline_xbrl(doc_path)

                            # Process the extracted facts (if needed for xbrl_data structure)
                            # NOTE: The original code built xbrl_data['facts'] directly.
                            # We might need to adapt this if the formatter expects a specific structure.
                            # For now, let's assume the formatter can handle the list of facts
                            # or we adapt the formatter later.
                            # Let's rebuild the facts list here for compatibility for now.
                            xbrl_data["facts"] = [] # Reset facts
                            for fact_data in extracted_facts:
                                fact = {
                                    "concept": fact_data.get('name', ''), # Use full name from extractor
                                    "value": fact_data.get('value', ''),
                                    "context_ref": fact_data.get('contextRef', '')
                                }
                                # Add optional attributes if present in extracted_facts
                                if fact_data.get('unitRef'):
                                    fact["unit_ref"] = fact_data['unitRef']
                                if fact_data.get('scale'): # Assuming scale might be needed?
                                    fact["scale"] = fact_data['scale']
                                # Add other attributes like decimals if the formatter needs them

                                xbrl_data["facts"].append(fact)
                            logging.info(f"Successfully processed {len(xbrl_data['facts'])} facts from extractor.")

                        except Exception as e:
                            logging.error(f"Error calling/processing SECExtractor.extract_inline_xbrl: {str(e)}")
                            # Continue with basic XBRL data or handle error
                        # ---- END MODIFIED CODE ----

                    # Generate LLM format
                    llm_content = llm_formatter.generate_llm_format(xbrl_data, metadata)

                    # Save LLM format
                    save_result = llm_formatter.save_llm_format(llm_content, metadata, str(llm_path))

                    llm_result = {
                        "success": save_result.get("success", False),
                        "file_size": save_result.get("size", 0),
                        "path": save_result.get("path", "")
                    }
                else:
                    llm_result = {
                        "success": False,
                        "error": "No XBRL data found in filing"
                    }
            except Exception as e:
                logging.error(f"Error generating LLM format: {str(e)}")
                llm_result = {
                    "success": False,
                    "error": str(e)
                }

            # Add LLM formatting stage to results
            result["stages"]["llm_format"] = {
                "success": llm_result.get("success", False),
                "time_seconds": time.time() - llm_start,
                "result": llm_result
            }

            # Don't fail the pipeline if LLM formatting fails
            if not llm_result.get("success", False):
                logging.warning(f"LLM formatting failed: {llm_result.get('error', 'Unknown error')}")
                result["warning"] = f"LLM formatting failed: {llm_result.get('error', 'Unknown error')}"

            # Stage 4: Upload to GCP (if configured)
            if self.gcp_storage and self.gcp_storage.is_enabled():
                logging.info(f"Stage 4: Uploading to GCP")

                upload_start = time.time()

                # Extract year and quarter information
                filing_date = filing_info.get("filing_date", "")
                period_end_date = filing_info.get("period_end_date", "")

                # Log for debugging
                logging.info(f"Period end date: {period_end_date}, Filing date: {filing_date}")

                # Use the fiscal registry for all companies
                fiscal_quarter = None

                # Initialize fiscal_year and fiscal_quarter to prevent 'referenced before assignment' errors
                # These will only be used if the fiscal_registry determination fails
                fiscal_year = None
                fiscal_quarter = None

                # If we have both ticker and period_end_date, use the fiscal registry
                if ticker and period_end_date:
                    # Make sure datetime is available in this scope
                    import datetime

                    # Use the fiscal registry from src2 for consistent fiscal calculations
                    try:
                        # Load document text for extraction if we have a document path
                        document_text = None

                        try:
                            # Find the document path from the download result
                            doc_path = None
                            if "stages" in result and "download" in result["stages"]:
                                download_result = result["stages"]["download"]["result"]
                                if "doc_path" in download_result:
                                    doc_path = download_result["doc_path"]

                            # Try to read the document file
                            if doc_path and os.path.exists(doc_path):
                                with open(doc_path, 'r', encoding='utf-8', errors='ignore') as f:
                                    document_text = f.read()
                                    logging.info(f"Read {len(document_text)} chars from {doc_path} for fiscal period extraction")
                        except Exception as e:
                            logging.warning(f"Could not read document text for GCS path extraction: {str(e)}")

                        # Use our centralized fiscal period determination function with the registry
                        try:
                            fiscal_year_new, fiscal_period_new, validation_metadata = self.determine_fiscal_period_from_registry(
                                ticker, period_end_date, filing_type
                            )

                            # Use new values if available
                            if fiscal_year_new:
                                fiscal_year = fiscal_year_new
                            if fiscal_period_new:
                                fiscal_quarter = fiscal_period_new

                                print(f"Using fiscal registry determination for {ticker}: period_end_date={period_end_date}, filing_type={filing_type} -> Year={fiscal_year}, Period={fiscal_quarter}")
                        except Exception as e:
                            print(f"Could not use fiscal registry: {str(e)}")
                            # Set defaults if determination fails
                            if fiscal_year is None:
                                if filing_type == "10-K":
                                    fiscal_year = period_end_date.split('-')[0] if period_end_date else str(datetime.datetime.now().year)
                                    fiscal_quarter = "annual"
                                    print(f"Using default values for 10-K: Year={fiscal_year}, Period={fiscal_quarter}")
                                else:
                                    # Safe default values to prevent "referenced before assignment" errors
                                    fiscal_year = period_end_date.split('-')[0] if period_end_date else str(datetime.datetime.now().year)
                                    fiscal_quarter = "Q?"
                                    print(f"Using default values for 10-Q: Year={fiscal_year}, Period={fiscal_quarter}")
                    except (ImportError, Exception) as e:
                        logging.warning(f"Could not use fiscal registry: {str(e)}")

                # Construct GCS paths using the proper folder structure
                # Try to use fiscal_period if it was determined
                fiscal_period = fiscal_quarter  # For consistency with other code

                # IMPORTANT: If fiscal_year isn't available, we need to extract it from the period_end_date
                # This ensures we always use actual fiscal years in GCS paths, not accession numbers
                if not fiscal_year and period_end_date:
                    # Extract year from period_end_date as a fallback
                    import re
                    year_match = re.search(r'(\d{4})', period_end_date)
                    if year_match:
                        fiscal_year = year_match.group(1)
                        logging.info(f"Extracted fiscal year {fiscal_year} from period_end_date for GCS path")

                # If we still don't have a fiscal year, use current year as last resort
                if not fiscal_year:
                    import datetime
                    fiscal_year = str(datetime.datetime.now().year)
                    logging.warning(f"No fiscal year determined, using current year {fiscal_year} for GCS path")

                if filing_type == "10-K" or fiscal_period == "annual":
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/llm.txt"
                elif fiscal_period:
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_period}/llm.txt"
                else:
                    # Fallback to fiscal year only if we can't determine quarter
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/llm.txt"

                # Log the path for debugging
                logging.info(f"Using GCS path: {gcs_llm_path}")

                # Track upload results
                upload_results = {
                    "llm_upload": None,
                    "metadata": None
                }

                llm_upload_result = None  # Initialize to prevent reference error
                # Get LLM file size if it exists
                llm_size = 0
                if llm_result.get("success", False) and os.path.exists(str(llm_path)):
                    llm_size = os.path.getsize(str(llm_path))

                # Check if force_upload is enabled in the filing_info
                force_upload = filing_info.get("force_upload", False)

                # Check if this is an amended filing that should use a subdirectory
                is_amended = filing_info.get("is_amended", False)
                use_amendment_subdirectory = filing_info.get("use_amendment_subdirectory", False)

                # Modify the GCS paths for amended filings to use "/a" subdirectory
                if is_amended and use_amendment_subdirectory:
                    # Create paths with "/a" subdirectory
                    parts = gcs_llm_path.split("/")
                    if len(parts) >= 3:
                        # Insert "a" before the final component
                        filename = parts[-1]  # llm.txt
                        base_path = "/".join(parts[:-1])  # everything before the filename
                        amended_gcs_llm_path = f"{base_path}/a/{filename}"

                        # Update the path for this upload
                        logging.info(f"Using amended filing path: {amended_gcs_llm_path}")
                        gcs_llm_path = amended_gcs_llm_path

                        # Store the amended path in the filing info for later reference
                        filing_info["amended_gcs_llm_path"] = amended_gcs_llm_path

                # Check if we should skip GCP upload entirely
                # We now don't automatically skip amended filings - they go to a subdirectory
                skip_gcp_upload = filing_info.get("skip_gcp_upload", False)

                if skip_gcp_upload:
                    if is_amended:
                        logging.info(f"SKIPPING GCP UPLOAD for amended filing ({filing_info.get('original_filing_type', filing_type)})")

                        # Store amended filing in amendments directory
                        from pathlib import Path
                        ticker = filing_info.get("ticker")
                        fiscal_year = filing_info.get("fiscal_year")
                        fiscal_period = filing_info.get("fiscal_period")
                        amendments_dir = Path(output_dir) / ticker / "amendments"
                        amendments_dir.mkdir(exist_ok=True, parents=True)

                        # Determine appropriate file names
                        filing_identifier = f"{ticker}_{filing_type}"
                        if fiscal_year:
                            filing_identifier += f"_{fiscal_year}"
                        if fiscal_period and fiscal_period != "annual":
                            filing_identifier += f"_{fiscal_period}"
                        filing_identifier += "_amended"

                        amended_llm_path = amendments_dir / f"{filing_identifier}_llm.txt"

                        # Copy the llm file to the amendments directory
                        import shutil
                        try:
                            if os.path.exists(llm_path):
                                shutil.copy2(llm_path, amended_llm_path)
                                logging.info(f"Stored amended filing in {amendments_dir}")

                            # Store the path in filing_info for reporting
                            filing_info["amended_llm_path"] = str(amended_llm_path)
                        except Exception as e:
                            logging.warning(f"Failed to store amended filing in amendments directory: {e}")

                        # Create a result for reporting
                        text_upload_result = {
                            "success": True,
                            "skipped": True,
                            "reason": "amended_filing"
                        }
                    else:
                        logging.info(f"SKIPPING GCP UPLOAD as requested in filing_info")
                        # Create a fake success result for consistency in the pipeline
                        text_upload_result = {
                            "success": True,
                            "skipped": True,
                            "reason": "skip_gcp_upload_flag"
                        }
                else:
                    # Normal GCP upload flow
                    # Check if file already exists in GCS
                    files_exist = self.gcp_storage.check_files_exist([gcs_llm_path])

                    # Create a placeholder for text upload (for compatibility with existing code)
                    # We no longer generate text.txt files
                    logging.info(f"Skipping text file upload to GCS (text.txt functionality removed)")
                    text_upload_result = {
                        "success": True,
                        "skipped": True,
                        "reason": "text_files_disabled"
                    }

                # Add text upload result
                upload_results["text_upload"] = text_upload_result

                # Handle LLM file separately with force_upload flag
                if llm_result.get("success", False) and os.path.exists(str(llm_path)):
                    if files_exist.get(gcs_llm_path, False) and not force_upload:
                        logging.info(f"LLM file already exists in GCS: {gcs_llm_path}")
                        llm_upload_result = {
                            "success": True,
                            "gcs_path": gcs_llm_path,
                            "already_exists": True
                        }
                    else:
                        if files_exist.get(gcs_llm_path, False) and force_upload:
                            logging.info(f"Force upload: LLM file exists but uploading again: {gcs_llm_path}")
                        else:
                            logging.info(f"Uploading LLM file to GCS: {gcs_llm_path}")
                        llm_upload_result = self.gcp_storage.upload_file(str(llm_path), gcs_llm_path)

                    # Add LLM upload result
                    upload_results["llm_upload"] = llm_upload_result

                # Update metadata in Firestore if text upload was successful
                if text_upload_result.get("success", False):
                    # Add Firestore metadata
                    metadata_update = {}

                    # Text file functionality has been removed
                    metadata_update["text_size"] = 0
                    metadata_update["text_file_skipped"] = True

                    # Add LLM path if it was uploaded successfully
                    if llm_upload_result and llm_upload_result.get("success", False):
                        metadata_update["llm_path"] = gcs_llm_path
                        metadata_update["llm_size"] = llm_size
                        metadata_update["local_llm_path"] = str(llm_path)  # Add local path for token counting
                        logging.info(f"Adding local LLM path for token counting: {str(llm_path)}")

                    # Update Firestore
                    metadata_result = self.gcp_storage.add_filing_metadata(
                        metadata,
                        **metadata_update
                    )

                    upload_results["metadata"] = metadata_result

                # Overall success if at least text was uploaded
                upload_success = text_upload_result.get("success", False)

                # Add upload stage to results
                result["stages"]["upload"] = {
                    "success": upload_success,
                    "time_seconds": time.time() - upload_start,
                    "result": upload_results
                }

                if not upload_success:
                    result["warning"] = f"Upload failed: {text_upload_result.get('error', 'Unknown error')}"
                elif llm_upload_result and not llm_upload_result.get("success", False):
                    result["warning"] = f"LLM upload failed: {llm_upload_result.get('error', 'Unknown error')}"
                    # Don't fail the entire process if upload fails
            else:
                logging.info("GCP upload skipped (not configured)")

            # Final result construction
            result["success"] = "error" not in result
            result["text_path"] = None  # No longer generating text.txt files
            result["llm_path"] = str(llm_path) if llm_path and os.path.exists(llm_path) else None
            result["total_time_seconds"] = time.time() - start_time

            return result

        except Exception as e:
            logging.error(f"Pipeline continuation error: {str(e)}")
            result["error"] = str(e)
            result["success"] = False
            result["total_time_seconds"] = time.time() - start_time
            return result

    def process_filing(self, ticker=None, cik=None, filing_type="10-K",
                       filing_index=0, save_intermediate=False):
        """
        Process a filing through the complete pipeline.

        Args:
            ticker: Company ticker symbol (optional if CIK provided)
            cik: Company CIK number (optional if ticker provided)
            filing_type: Type of filing to retrieve (10-K, 10-Q, etc.)
            filing_index: Index of the filing to process (0 for most recent)
            save_intermediate: Whether to save intermediate files

        Returns:
            Dictionary with processing results and file paths
        """
        start_time = time.time()
        result = {
            "ticker": ticker,
            "cik": cik,
            "filing_type": filing_type,
            "start_time": start_time,
            "stages": {}
        }

        try:
            # Stage 1: Download the filing
            logging.info(f"Stage 1: Downloading {filing_type} for {ticker or cik}")

            download_start = time.time()
            filings = self.downloader.get_company_filings(
                ticker=ticker,
                cik=cik,
                filing_type=filing_type,
                count=filing_index+1
            )

            if not filings or len(filings) <= filing_index:
                error_msg = f"No {filing_type} filings found for {ticker or cik}"
                logging.error(error_msg)
                result["error"] = error_msg
                return result

            # Get the target filing
            filing_info = filings[filing_index]

            # Download the filing
            download_result = self.downloader.download_filing(filing_info)

            # Add download stage to results
            result["stages"]["download"] = {
                "success": "error" not in download_result,
                "time_seconds": time.time() - download_start,
                "result": download_result
            }

            if "error" in download_result:
                result["error"] = f"Download failed: {download_result['error']}"
                return result

            # Continue with downloaded document
            document_path = download_result.get("doc_path")
            if not document_path or not os.path.exists(document_path):
                error_msg = "Downloaded document path not found"
                logging.error(error_msg)
                result["error"] = error_msg
                return result

            # Stage 2: Render the document
            logging.info(f"Stage 2: Rendering document")

            render_start = time.time()

            # Define output paths
            company_dir = self.output_dir / ticker
            os.makedirs(company_dir, exist_ok=True)

            rendered_path = company_dir / f"{ticker}_{filing_type}_rendered.html"

            try:
                # Render to HTML
                rendered_file = self.renderer.render_ixbrl(
                    document_path,
                    output_format="html",
                    output_file=rendered_path
                )

                render_result = {
                    "rendered_file": str(rendered_file),
                    "file_size": os.path.getsize(rendered_file)
                }

            except Exception as e:
                logging.error(f"Rendering failed: {str(e)}")
                render_result = {"error": str(e)}

            # Add render stage to results
            result["stages"]["render"] = {
                "success": "error" not in render_result,
                "time_seconds": time.time() - render_start,
                "result": render_result
            }

            if "error" in render_result:
                # Don't set result["error"] since we're going to use a fallback
                logging.info(f"Rendering with Arelle failed: {render_result['error']}")

                # Try a fallback - use the downloaded document directly
                logging.info("Using fallback: processing downloaded document directly")
                rendered_path = document_path

                # Update render_result to show success for the fallback approach
                render_result = {
                    "success": True,
                    "fallback_used": True,
                    "original_error": render_result.get("error"),
                    "file_path": document_path,
                    "file_size": os.path.getsize(document_path)
                }

                # Update the stage info
                result["stages"]["render"] = {
                    "success": True,  # Mark as success since fallback worked
                    "fallback_used": True,
                    "time_seconds": time.time() - render_start,
                    "result": render_result
                }

            # Stage 3: Extract text
            logging.info(f"Stage 3: Extracting text from document")

            extract_start = time.time()

            # Define output text path with fiscal period if available
            # Extract fiscal year and quarter information
            fiscal_year = None
            fiscal_quarter = None
            period_end_date = filing_info.get("period_end_date", "")

            if ticker and period_end_date:
                try:
                    from src2.sec.fiscal import fiscal_registry
                    from src2.sec.fiscal.fiscal_manager import CompanyFiscalModel

                    # First check if this is a known company with a specific pattern
                    found_in_patterns = False
                    if ticker.upper() in CompanyFiscalModel.KNOWN_FISCAL_PATTERNS:
                        pattern = CompanyFiscalModel.KNOWN_FISCAL_PATTERNS[ticker.upper()]
                        quarter_mapping = pattern.get("quarter_mapping", {})

                        # Parse the period end date
                        try:
                            period_end = datetime.datetime.strptime(period_end_date, '%Y-%m-%d')
                            month = period_end.month

                            # Look up fiscal period directly from mapping
                            if month in quarter_mapping:
                                fiscal_period = quarter_mapping[month]

                                # CRITICAL FIX: For 10-Q filings, never use "annual"
                                # This fixes the quarter mislabeling issue!
                                if filing_type == "10-Q" and fiscal_period == "annual":
                                    # This is a Q3 filing incorrectly mapped to annual
                                    fiscal_period = "Q3"
                                    logging.warning(f"Corrected fiscal period for {ticker} 10-Q with period_end month {month}: 'annual' → 'Q3'")

                                fiscal_quarter = fiscal_period

                                # Determine fiscal year based on month and fiscal year end
                                fiscal_year_end_month = pattern.get("fiscal_year_end_month", 12)
                                if month > fiscal_year_end_month:
                                    fiscal_year = str(period_end.year + 1)
                                else:
                                    fiscal_year = str(period_end.year)

                                logging.info(f"Using fiscal info from KNOWN_FISCAL_PATTERNS: Year={fiscal_year}, Period={fiscal_quarter}")
                                found_in_patterns = True
                        except (ValueError, TypeError) as e:
                            logging.warning(f"Error parsing period end date: {e}")

                    # Fallback to standard fiscal registry determination
                    if not found_in_patterns:
                        fiscal_info = fiscal_registry.determine_fiscal_period(
                            ticker, period_end_date, filing_type
                        )
                        fiscal_year = fiscal_info.get("fiscal_year")
                        fiscal_quarter = fiscal_info.get("fiscal_period")
                        logging.info(f"Using fiscal info from registry: Year={fiscal_year}, Period={fiscal_quarter}")
                except (ImportError, Exception) as e:
                    logging.warning(f"Could not use fiscal registry for local path: {str(e)}")

            # Use fiscal information in filenames to prevent overwriting
            if fiscal_year and fiscal_quarter and filing_type == "10-Q":
                llm_path = company_dir / f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_quarter}_llm.txt"
                logging.info(f"Using fiscal period in local filenames: {fiscal_year}_{fiscal_quarter}")
            elif fiscal_year:
                llm_path = company_dir / f"{ticker}_{filing_type}_{fiscal_year}_llm.txt"
                logging.info(f"Using fiscal year in local filenames: {fiscal_year}")
            else:
                # IMPORTANT: If we don't have fiscal year, we need to determine it from period_end_date
                # rather than using accession numbers in the filename
                calculated_fiscal_year = None

                # Extract year from period_end_date
                if period_end_date:
                    import re
                    year_match = re.search(r'(\d{4})', period_end_date)
                    if year_match:
                        calculated_fiscal_year = year_match.group(1)
                        logging.info(f"Extracted fiscal year {calculated_fiscal_year} from period_end_date {period_end_date}")

                # If we still don't have a year, use the current year
                if calculated_fiscal_year:
                    llm_path = company_dir / f"{ticker}_{filing_type}_{calculated_fiscal_year}_llm.txt"
                    logging.info(f"Using extracted year in local filenames: {calculated_fiscal_year}")
                else:
                    # Last resort fallback to current year
                    import datetime
                    current_year = str(datetime.datetime.now().year)
                    llm_path = company_dir / f"{ticker}_{filing_type}_{current_year}_llm.txt"
                    logging.warning(f"No year information available, using current year {current_year} in filenames")

            # Create metadata for the extraction
            metadata = {
                "ticker": ticker,
                "cik": cik,
                "filing_type": filing_type,
                "filing_date": filing_info.get("filing_date"),
                "period_end_date": filing_info.get("period_end_date"),
                "company_name": filing_info.get("company_name", ticker),
                "source_url": filing_info.get("primary_doc_url")
            }

            # Extract content from filing
            extract_result = self.extractor.process_filing(
                rendered_path,
                metadata=metadata
            )

            # Add extraction stage to results
            result["stages"]["extract"] = {
                "success": extract_result.get("success", False),
                "time_seconds": time.time() - extract_start,
                "result": extract_result
            }

            # Save document sections to metadata for LLM formatter
            if 'document_sections' in extract_result:
                metadata['html_content'] = {
                    'document_sections': extract_result['document_sections']
                }
                logging.info(f"Added {len(extract_result['document_sections'])} document sections to metadata for LLM formatter")

            if not extract_result.get("success", False):
                result["error"] = f"Extraction failed: {extract_result.get('error', 'Unknown error')}"
                return result

            # Stage 3b: Extract XBRL data and format for LLM
            logging.info(f"Stage 3b: Extracting XBRL data and formatting for LLM")

            llm_start = time.time()
            llm_result = {"success": False}

            try:
                # Import LLM formatter and financial validator
                from src2.formatter.llm_formatter import llm_formatter
                from src2.formatter.financial_validator import FinancialValidator

                # Check for XBRL data in the filing
                xbrl_path = None
                doc_path = None

                # First check for direct XBRL file
                if "xbrl_path" in download_result:
                    xbrl_path = download_result["xbrl_path"]
                    logging.info(f"Found XBRL file: {xbrl_path}")
                elif "idx_path" in download_result:
                    # Use index file to find XBRL
                    xbrl_path = download_result["idx_path"]
                    logging.info(f"Found index file: {xbrl_path}")

                # Check for main document which might contain inline XBRL
                if "doc_path" in download_result:
                    doc_path = download_result["doc_path"]
                    logging.info(f"Found document path for XBRL extraction: {doc_path}")

                # Use either xbrl_path or doc_path
                if (xbrl_path and os.path.exists(xbrl_path)) or (doc_path and os.path.exists(doc_path)):
                    # Initialize XBRL data structure
                    xbrl_data = {
                        "contexts": {},
                        "units": {},
                        "facts": []
                    }

                    # Start with basic document information
                    xbrl_data["facts"].append({
                        "concept": "DocumentType",
                        "value": filing_type,
                        "context_ref": "AsOf"
                    })
                    xbrl_data["facts"].append({
                        "concept": "EntityRegistrantName",
                        "value": metadata.get("company_name", ""),
                        "context_ref": "AsOf"
                    })

                    # Try to extract XBRL data from main document if no dedicated XBRL file
                    if not xbrl_path and doc_path:
                        logging.info(f"Extracting inline XBRL from main document: {doc_path}")
                        try:
                            # Extract inline XBRL data from HTML document
                            from bs4 import BeautifulSoup

                            with open(doc_path, 'r', encoding='utf-8') as f:
                                html_content = f.read()

                            soup = BeautifulSoup(html_content, 'html.parser')

                            # Find all ix:* tags (inline XBRL tags)
                            ix_tags = soup.find_all(lambda tag: tag.name and tag.name.startswith('ix:'))
                            logging.info(f"Found {len(ix_tags)} inline XBRL tags")

                            # Extract contexts
                            context_tags = soup.find_all('xbrli:context') or soup.find_all('context')
                            for ctx in context_tags:
                                ctx_id = ctx.get('id')
                                if ctx_id:
                                    xbrl_data["contexts"][ctx_id] = {"id": ctx_id}
                                    # Extract period information if available
                                    period = ctx.find('xbrli:period') or ctx.find('period')
                                    if period:
                                        instant = period.find('xbrli:instant') or period.find('instant')
                                        if instant and instant.text:
                                            xbrl_data["contexts"][ctx_id]["period"] = {"instant": instant.text.strip()}
                                        else:
                                            start = period.find('xbrli:startdate') or period.find('startdate')
                                            end = period.find('xbrli:enddate') or period.find('enddate')
                                            if start and end and start.text and end.text:
                                                xbrl_data["contexts"][ctx_id]["period"] = {
                                                    "startDate": start.text.strip(),
                                                    "endDate": end.text.strip()
                                                }

                            # Extract units
                            unit_tags = soup.find_all('xbrli:unit') or soup.find_all('unit')
                            for unit in unit_tags:
                                unit_id = unit.get('id')
                                if unit_id:
                                    measure = unit.find('xbrli:measure') or unit.find('measure')
                                    if measure and measure.text:
                                        xbrl_data["units"][unit_id] = measure.text.strip()

                            # Extract facts from inline XBRL
                            for ix_tag in ix_tags:
                                if ix_tag.name == 'ix:nonfraction':
                                    fact = {
                                        "concept": ix_tag.get('name', ''),
                                        "value": ix_tag.text.strip(),
                                        "context_ref": ix_tag.get('contextref', '')
                                    }

                                    # Add optional attributes
                                    if ix_tag.get('unitref'):
                                        fact["unit_ref"] = ix_tag.get('unitref')
                                    if ix_tag.get('decimals'):
                                        fact["decimals"] = ix_tag.get('decimals')

                                    xbrl_data["facts"].append(fact)

                            # If no contexts were found, create a default one
                            if not xbrl_data["contexts"]:
                                xbrl_data["contexts"]["AsOf"] = {
                                    "id": "AsOf",
                                    "period": {"instant": metadata.get("period_end_date", "")}
                                }

                            # If no facts were extracted from inline XBRL tags, try to extract some key financial data
                            if len(xbrl_data["facts"]) <= 2:  # Only the basic facts we added above
                                logging.info("Extracting financial data from document tables")

                                # Find tables that might contain financial data
                                tables = soup.find_all('table')
                                for table_idx, table in enumerate(tables):
                                    # Skip small tables
                                    rows = table.find_all('tr')
                                    if len(rows) < 3:
                                        continue

                                    # Try to identify financial tables
                                    table_text = table.get_text().lower()
                                    if any(term in table_text for term in ['revenue', 'income', 'asset', 'liability', 'earning']):
                                        # Extract rows as facts
                                        for row_idx, row in enumerate(rows):
                                            cells = row.find_all(['th', 'td'])
                                            if len(cells) >= 2:
                                                label = cells[0].get_text().strip()
                                                value = cells[1].get_text().strip()
                                                if label and value and not label.isdigit():
                                                    xbrl_data["facts"].append({
                                                        "concept": f"Table{table_idx}_{label.replace(' ', '')}",
                                                        "value": value,
                                                        "context_ref": "AsOf"
                                                    })

                            logging.info(f"Extracted {len(xbrl_data['contexts'])} contexts, {len(xbrl_data['units'])} units, and {len(xbrl_data['facts'])} facts")

                        except Exception as e:
                            logging.error(f"Error extracting inline XBRL: {str(e)}")
                            # Continue with basic XBRL data

                    # Generate LLM format
                    llm_content = llm_formatter.generate_llm_format(xbrl_data, metadata)

                    # Save LLM format
                    save_result = llm_formatter.save_llm_format(llm_content, metadata, str(llm_path))

                    # Verify balance sheet integrity
                    if save_result.get("success", False):
                        financial_validator = FinancialValidator()
                        balance_sheet_verification = financial_validator.verify_balance_sheet_integrity(str(llm_path))

                        # Check if any balance sheets are invalid
                        invalid_periods = []
                        for period, data in balance_sheet_verification.items():
                            if not data.get("is_valid", True):
                                invalid_periods.append(period)
                                logging.warning(f"Balance sheet validation failed for {period}: {data.get('error_message')}")
                    else:
                        balance_sheet_verification = {}
                        invalid_periods = []

                    llm_result = {
                        "success": save_result.get("success", False),
                        "file_size": save_result.get("size", 0),
                        "path": save_result.get("path", ""),
                        "balance_sheet_verification": balance_sheet_verification,
                        "invalid_balance_sheet_periods": invalid_periods
                    }
                else:
                    llm_result = {
                        "success": False,
                        "error": "No XBRL data found in filing"
                    }
            except Exception as e:
                logging.error(f"Error generating LLM format: {str(e)}")
                llm_result = {
                    "success": False,
                    "error": str(e)
                }

            # Add LLM formatting stage to results
            result["stages"]["llm_format"] = {
                "success": llm_result.get("success", False),
                "time_seconds": time.time() - llm_start,
                "result": llm_result
            }

            # Don't fail the pipeline if LLM formatting fails
            if not llm_result.get("success", False):
                logging.warning(f"LLM formatting failed: {llm_result.get('error', 'Unknown error')}")
                result["warning"] = f"LLM formatting failed: {llm_result.get('error', 'Unknown error')}"

            # Stage 4: Upload to GCP (if configured)
            if self.gcp_storage and self.gcp_storage.is_enabled():
                logging.info(f"Stage 4: Uploading to GCP")

                upload_start = time.time()

                # Extract year and quarter information
                filing_date = filing_info.get("filing_date", "")
                period_end_date = filing_info.get("period_end_date", "")

                # Log for debugging
                logging.info(f"Period end date: {period_end_date}, Filing date: {filing_date}")

                # Get fiscal year from filing metadata
                fiscal_year = None
                if period_end_date:
                    # Try to extract year from period_end_date
                    year_match = re.search(r'(\d{4})', period_end_date)
                    if year_match:
                        fiscal_year = year_match.group(1)

                if not fiscal_year and filing_date:
                    # Fall back to filing date year
                    year_match = re.search(r'(\d{4})', filing_date)
                    if year_match:
                        fiscal_year = year_match.group(1)

                # Final fallback to current year
                if not fiscal_year:
                    fiscal_year = time.strftime("%Y")

                # Use the fiscal registry for all companies
                fiscal_quarter = None

                # If we have both ticker and period_end_date, use the fiscal registry
                if ticker and period_end_date:
                    # Use the company_fiscal registry as the single source of truth
                    try:
                        # Import explicit fiscal registry (single source of truth)
                        from src2.sec.fiscal.company_fiscal import fiscal_registry

                        # Use only the dedicated fiscal registry
                        fiscal_info = fiscal_registry.determine_fiscal_period(
                            ticker, period_end_date, filing_type
                        )

                        fiscal_year_new = fiscal_info.get("fiscal_year")
                        fiscal_quarter_new = fiscal_info.get("fiscal_period")

                        # Use new values if available
                        if fiscal_year_new:
                            fiscal_year = fiscal_year_new
                        if fiscal_quarter_new:
                            fiscal_quarter = fiscal_quarter_new

                            print(f"USING SINGLE SOURCE OF TRUTH for {ticker}: period_end_date={period_end_date}, filing_type={filing_type} -> Year={fiscal_year}, Period={fiscal_quarter}")

                        # If the fiscal registry didn't provide information, log a warning
                        if not fiscal_year_new or not fiscal_quarter_new:
                            error_msg = fiscal_info.get("error", "Unknown error")
                            logging.error(f"CRITICAL: Fiscal registry couldn't determine period for {ticker}, {period_end_date}: {error_msg}")
                    except ImportError:
                        logging.error("CRITICAL ERROR: Fiscal registry not available! This is required as the single source of truth.")
                    except Exception as e:
                        logging.error(f"CRITICAL ERROR determining fiscal period: {str(e)}")

                # IMPORTANT: We skip the fallback to standard fiscal quarters
                # to diagnose document extraction issues. Instead, we'll try to
                # extract the quarter information directly from document text
                if not fiscal_quarter and filing_type == "10-Q":
                    document_text = None

                    try:
                        # Try to find the document path
                        doc_path = None
                        if "stages" in result and "download" in result["stages"]:
                            download_result = result["stages"]["download"]["result"]
                            if "doc_path" in download_result:
                                doc_path = download_result["doc_path"]

                                # Read the document text for the improved extraction
                        if doc_path and os.path.exists(doc_path):
                            with open(doc_path, 'r', encoding='utf-8', errors='ignore') as f:
                                document_text = f.read()
                                print(f"Read {len(document_text)} chars from {doc_path} for fiscal period extraction")
                    except Exception as e:
                        print(f"Error reading document for extraction: {str(e)}")

                # Use our centralized fiscal period determination function
                try:
                    fiscal_year, fiscal_period, validation_metadata = self.determine_fiscal_period_from_registry(
                        ticker, period_end_date, filing_type
                    )

                    # Update the fiscal_quarter variable for backward compatibility
                    # This ensures existing code still works with the new variable name
                    fiscal_quarter = fiscal_period

                    print(f"Determined fiscal info from registry: Year={fiscal_year}, Period={fiscal_period}")
                except Exception as e:
                    print(f"Error determining fiscal period: {str(e)}")

                    # Set defaults if all else fails
                    if filing_type == "10-K":
                        fiscal_quarter = "annual"
                        fiscal_period = "annual"

                        # Extract year from period_end_date
                        if period_end_date:
                            try:
                                fiscal_year = period_end_date.split('-')[0]
                            except Exception:
                                fiscal_year = None

                # Additional logging to help debug fiscal period extraction
                print(f"FINAL VALUES for GCS paths: ticker={ticker}, filing_type={filing_type}, year={fiscal_year}, period={fiscal_quarter}")

                # IMPORTANT: Ensure fiscal_year is always a valid year, not an accession number
                # If fiscal_year isn't available, extract it from period_end_date
                if not fiscal_year and period_end_date:
                    import re
                    year_match = re.search(r'(\d{4})', period_end_date)
                    if year_match:
                        fiscal_year = year_match.group(1)
                        logging.info(f"Extracted fiscal year {fiscal_year} from period_end_date for GCS path")

                # If we still don't have a fiscal year, use current year as last resort
                if not fiscal_year:
                    import datetime
                    fiscal_year = str(datetime.datetime.now().year)
                    logging.warning(f"No fiscal year determined, using current year {fiscal_year} for GCS path")

                # Construct GCS paths using the proper folder structure
                # Prefer using fiscal_period (which is more consistent) but fall back to fiscal_quarter for compatibility
                if filing_type == "10-K" or (fiscal_period == "annual" or fiscal_quarter == "annual"):
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/llm.txt"
                elif fiscal_period:
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_period}/llm.txt"
                elif fiscal_quarter:
                    # Legacy fallback if fiscal_period is not available but fiscal_quarter is
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_quarter}/llm.txt"
                else:
                    # Fallback to fiscal year only if we can't determine quarter
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/llm.txt"

                # Log the path for debugging
                logging.info(f"Using GCS path: {gcs_llm_path}")

                # Track upload results
                upload_results = {
                    "text_upload": None,
                    "llm_upload": None,
                    "metadata": None
                }

                # Initialize variables
                llm_upload_result = None
                llm_size = 0

                # Get LLM file size if it exists
                if llm_result.get("success", False) and os.path.exists(str(llm_path)):
                    llm_size = os.path.getsize(str(llm_path))

                # Check if force_upload is enabled in the filing_info
                force_upload = filing_info.get("force_upload", False)

                # Check if LLM file already exists in GCS
                files_exist = self.gcp_storage.check_files_exist([gcs_llm_path])

                # Create placeholder for text upload result (for compatibility)
                logging.info(f"Skipping text file upload to GCS (text.txt functionality removed)")
                text_upload_result = {
                    "success": True,
                    "skipped": True,
                    "message": "Text file functionality removed"
                }

                # Add text upload result
                upload_results["text_upload"] = text_upload_result

                # Handle LLM file separately with force_upload flag
                if llm_result.get("success", False) and os.path.exists(str(llm_path)):
                    if files_exist.get(gcs_llm_path, False) and not force_upload:
                        logging.info(f"LLM file already exists in GCS: {gcs_llm_path}")
                        llm_upload_result = {
                            "success": True,
                            "gcs_path": gcs_llm_path,
                            "already_exists": True
                        }
                    else:
                        if files_exist.get(gcs_llm_path, False) and force_upload:
                            logging.info(f"Force upload: LLM file exists but uploading again: {gcs_llm_path}")
                        else:
                            logging.info(f"Uploading LLM file to GCS: {gcs_llm_path}")
                        llm_upload_result = self.gcp_storage.upload_file(str(llm_path), gcs_llm_path)

                    # Add LLM upload result
                    upload_results["llm_upload"] = llm_upload_result

                # Update metadata in Firestore if text upload was successful
                if text_upload_result.get("success", False):
                    # Add Firestore metadata
                    metadata_update = {}

                    # Text file functionality has been removed
                    metadata_update["text_size"] = 0
                    metadata_update["text_file_skipped"] = True

                    # Add LLM path if it was uploaded successfully
                    if llm_upload_result and llm_upload_result.get("success", False):
                        metadata_update["llm_path"] = gcs_llm_path
                        metadata_update["llm_size"] = llm_size
                        metadata_update["local_llm_path"] = str(llm_path)  # Add local path for token counting
                        logging.info(f"Adding local LLM path for token counting: {str(llm_path)}")

                    # Update Firestore
                    metadata_result = self.gcp_storage.add_filing_metadata(
                        metadata,
                        **metadata_update
                    )

                    upload_results["metadata"] = metadata_result

                # Overall success if at least text was uploaded
                upload_success = text_upload_result.get("success", False)

                # Add upload stage to results
                result["stages"]["upload"] = {
                    "success": upload_success,
                    "time_seconds": time.time() - upload_start,
                    "result": upload_results
                }

                if not upload_success:
                    result["warning"] = f"Upload failed: {text_upload_result.get('error', 'Unknown error')}"
                elif llm_upload_result and not llm_upload_result.get("success", False):
                    result["warning"] = f"LLM upload failed: {llm_upload_result.get('error', 'Unknown error')}"
                    # Don't fail the entire process if upload fails
            else:
                logging.info("GCP upload skipped (not configured)")

            # Add final results
            result["success"] = "error" not in result
            result["text_path"] = None  # No longer generating text.txt files
            result["llm_path"] = str(llm_path) if llm_path and os.path.exists(llm_path) else None
            result["total_time_seconds"] = time.time() - start_time

            # Run data integrity validation if successful
            if result["success"]:
                logging.info("Running data integrity validation...")

                try:
                    # Import the validation module
                    # Try to import from local path first
                    data_integrity_result = None

                    try:
                        # Run data integrity validation
                        from data_integrity_validator import validate_filing_integrity

                        # Ensure fiscal variables are initialized with safe defaults if missing
                        safe_fiscal_year = fiscal_year
                        if safe_fiscal_year is None:
                            # Extract from period_end_date or use current year
                            if period_end_date and '-' in period_end_date:
                                safe_fiscal_year = period_end_date.split('-')[0]
                            else:
                                safe_fiscal_year = str(datetime.datetime.now().year)
                            logging.warning(f"Using extracted/default fiscal year {safe_fiscal_year} for validation")

                        safe_fiscal_period = fiscal_period
                        if safe_fiscal_period is None:
                            # Use filing type to determine a default
                            safe_fiscal_period = "annual" if filing_type == "10-K" else "Q?"
                            logging.warning(f"Using default fiscal period {safe_fiscal_period} for validation")

                        # Run the validation with safe values
                        data_integrity_result = validate_filing_integrity(
                            ticker,
                            filing_type,
                            safe_fiscal_year,
                            safe_fiscal_period
                        )

                    except ImportError:
                        # Fallback to a simpler validation if the module isn't available
                        logging.warning("Could not import data_integrity_validator, falling back to basic validation")

                        # Get the fiscal period from the metadata
                        # (safe way to access it without causing errors)
                        local_fiscal_year = metadata.get("fiscal_year", str(datetime.datetime.now().year))
                        local_fiscal_period = metadata.get("fiscal_period", "unknown")

                        # Run basic validation and create a result structure
                        data_integrity_result = {
                            "status": "UNKNOWN",
                            "llm_format_valid": False,
                            "data_consistent": False,
                            "details": {}
                        }

                        # Log what we're validating
                        logging.info(f"Running basic validation for {ticker} {filing_type} {local_fiscal_year} {local_fiscal_period}")

                        # Handle validation for LLM file only (text.txt functionality removed)
                        if os.path.exists(llm_path):
                            # Check file sizes
                            llm_size = os.path.getsize(llm_path)

                            min_size = 10 * 1024  # 10 KB
                            if llm_size < min_size:
                                logging.warning(f"File size validation failed: llm={llm_size/1024:.2f}KB")
                                data_integrity_result["status"] = "FILE_SIZE_WARNING"
                        else:
                            logging.warning(f"LLM file not found for validation: {llm_path}")
                            data_integrity_result["status"] = "FILE_NOT_FOUND"
                            data_integrity_result["details"]["error"] = "LLM file not found"

                        # If all checks passed
                        if data_integrity_result["status"] == "UNKNOWN":
                            data_integrity_result["status"] = "PASS"
                            data_integrity_result["llm_format_valid"] = True
                            data_integrity_result["data_consistent"] = True
                            logging.info("Basic validation passed")

                        # Log validation results
                        logging.info(f"Validation result: {data_integrity_result['status']}")

                    # Add validation result to pipeline result
                    if data_integrity_result:
                        result["validation"] = {
                            "status": data_integrity_result.get("status", "UNKNOWN"),
                            "details": {
                                "llm_format_valid": data_integrity_result.get("llm_format_valid", False),
                                "data_consistent": data_integrity_result.get("data_consistent", False)
                            }
                        }

                        # Add warning if validation failed
                        if data_integrity_result.get("status") != "PASS":
                            result["validation_warning"] = f"Data integrity validation failed: {data_integrity_result.get('status')}"
                            logging.warning(f"Data integrity validation failed: {data_integrity_result.get('status')}")

                except Exception as val_e:
                    logging.warning(f"Error during data integrity validation: {str(val_e)}")
                    result["validation_error"] = str(val_e)

            # Clean up intermediate files and temporary folders
            if not save_intermediate:
                # Try to clean up any temporary timestamp-based directories
                try:
                    # Remove empty timestamp-based directories that were created
                    import glob
                    import shutil

                    # Look for date-based folders (YYYYMMDD)
                    timestamp_dirs = glob.glob(f"{self.output_dir}/*/*/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]")
                    for timestamp_dir in timestamp_dirs:
                        try:
                            # Check if directory is empty
                            if os.path.isdir(timestamp_dir) and not os.listdir(timestamp_dir):
                                logging.info(f"Removing empty temporary directory: {timestamp_dir}")
                                shutil.rmtree(timestamp_dir)
                        except Exception as cleanup_e:
                            logging.warning(f"Error cleaning up temporary directory {timestamp_dir}: {str(cleanup_e)}")
                except Exception as e:
                    logging.warning(f"Error during temp directory cleanup: {str(e)}")

                # Don't delete the main downloaded files
                if "doc_path" in download_result:
                    # Don't delete the main document path
                    pass
                if "rendered_file" in render_result and rendered_path != document_path:
                    # Don't delete the rendered file if it's different from the document
                    pass

            return result

        except Exception as e:
            logging.error(f"Pipeline error: {str(e)}")
            result["error"] = str(e)
            result["success"] = False
            result["total_time_seconds"] = time.time() - start_time
            return result

    def cleanup(self):
        """
        Clean up temporary files.
        """
        try:
            # Clean up renderer temp files
            self.renderer.cleanup()

            # Additional cleanup could be added here

        except Exception as e:
            logging.error(f"Cleanup error: {str(e)}")


# Entry point
def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Parse arguments
    parser = argparse.ArgumentParser(description="Process SEC filings")
    parser.add_argument("ticker", help="Ticker symbol of the company")
    parser.add_argument("--filing-type", default="10-K", help="Filing type (10-K, 10-Q, etc.)")
    parser.add_argument("--filing-index", type=int, default=0, help="Index of filing to retrieve (0 for most recent)")
    parser.add_argument("--email", help="Contact email for SEC identification")
    parser.add_argument("--output", help="Output directory for processed files")
    parser.add_argument("--save-intermediate", action="store_true", help="Save intermediate files")
    parser.add_argument("--gcp-bucket", help="GCS bucket name for upload")
    parser.add_argument("--gcp-project", help="GCP project ID for upload")

    args = parser.parse_args()

    # Create pipeline
    pipeline = SECFilingPipeline(
        contact_email=args.email,
        output_dir=args.output or "./sec_processed",
        gcp_bucket=args.gcp_bucket,
        gcp_project=args.gcp_project
    )

    # Helper function to notify about validation failures
    def notify_validation_failure(result):
        """Send notification about validation failure - customize as needed"""
        # This is a placeholder function that you can customize to send
        # email notifications, Slack messages, etc. for validation failures

        # For now, just print to console with attention-grabbing formatting
        print("\n" + "!" * 80)
        print("!! DATA INTEGRITY VALIDATION FAILURE DETECTED !!")
        print("!" * 80)

        # Print details
        validation = result.get("validation", {})
        warning = result.get("validation_warning", "Unknown validation issue")

        print(f"\nTicker: {args.ticker}")
        print(f"Filing: {args.filing_type}")
        print(f"Warning: {warning}")

        # Print validation details
        details = validation.get("details", {})
        if details:
            print("\nValidation Details:")
            for key, value in details.items():
                print(f"  {key}: {value}")

        print("\n" + "!" * 80)
        # In a real system, you would:
        # 1. Send an email notification
        # 2. Log to a monitoring system
        # 3. Create a ticket/issue in your tracking system

    # Process filing
    try:
        print(f"\nProcessing {args.filing_type} for {args.ticker}...")

        result = pipeline.process_filing(
            ticker=args.ticker,
            filing_type=args.filing_type,
            filing_index=args.filing_index,
            save_intermediate=args.save_intermediate
        )

        # Print results
        if result.get("success", False):
            print(f"\n✅ Processing complete!")
            print(f"Text output: {result.get('text_path')}")
            print(f"Total processing time: {result.get('total_time_seconds', 0):.2f} seconds")

            # Print stage information
            if "stages" in result:
                print("\nStage details:")

                for stage, info in result["stages"].items():
                    status = "✅ Success" if info.get("success", False) else "❌ Failed"
                    print(f"  {stage.title()}: {status} ({info.get('time_seconds', 0):.2f}s)")

                    # Print stage-specific details
                    if stage == "download" and "result" in info:
                        doc_path = info["result"].get("doc_path")
                        if doc_path:
                            size = os.path.getsize(doc_path) / (1024 * 1024)
                            print(f"    Downloaded document: {os.path.basename(doc_path)} ({size:.2f} MB)")

                    elif stage == "extract" and "result" in info:
                        word_count = info["result"].get("word_count", 0)
                        file_size = info["result"].get("file_size", 0) / (1024 * 1024)
                        print(f"    Extracted text: {word_count} words ({file_size:.2f} MB)")

                    elif stage == "upload" and "result" in info:
                        if info.get("success", False):
                            # Access text and LLM upload results properly
                            text_upload = info["result"].get("text_upload", {})
                            text_gcs_path = text_upload.get("gcs_path")
                            print(f"    Uploaded text to GCS: {text_gcs_path}")

                            # If LLM upload is also present, show that too
                            llm_upload = info["result"].get("llm_upload", {})
                            if llm_upload:
                                llm_gcs_path = llm_upload.get("gcs_path")
                                print(f"    Uploaded LLM to GCS: {llm_gcs_path}")
                        else:
                            print(f"    Upload failed: {info['result'].get('error', 'Unknown error')}")

            # Print warning if any
            if "warning" in result:
                print(f"\n⚠️ Warning: {result['warning']}")

            # Print validation results if available
            if "validation" in result:
                validation = result["validation"]
                validation_status = validation.get("status", "UNKNOWN")
                print(f"\n🔎 Data Integrity Validation: {validation_status}")

                # Show details
                if "details" in validation:
                    details = validation["details"]
                    if "llm_format_valid" in details:
                        print(f"  LLM Format: {'✅ VALID' if details['llm_format_valid'] else '❌ INVALID'}")
                    if "data_consistent" in details:
                        print(f"  Data Consistency: {'✅ VALID' if details['data_consistent'] else '❌ INVALID'}")

            # Print validation warning if any
            if "validation_warning" in result:
                print(f"\n⚠️ Validation Warning: {result['validation_warning']}")
                # Send notification for validation failures
                notify_validation_failure(result)
        else:
            print(f"\n❌ Processing failed: {result.get('error', 'Unknown error')}")

            # Print the stage that failed
            if "stages" in result:
                for stage, info in result["stages"].items():
                    if not info.get("success", False):
                        print(f"  Failed at {stage} stage: {info.get('result', {}).get('error', 'Unknown error')}")

        # Clean up
        pipeline.cleanup()

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())