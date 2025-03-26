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
            download_dir=str(self.temp_dir / "downloads")
        )
        
        self.renderer = ArelleRenderer(
            temp_dir=str(self.temp_dir / "arelle"),
            install_if_missing=True
        )
        
        self.extractor = SECExtractor(
            output_dir=str(self.output_dir)
        )
        
        logging.info(f"Initialized SEC filing pipeline with output dir: {self.output_dir}")
    
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
            
            # Define output text path
            text_path = company_dir / f"{ticker}_{filing_type}_text.txt"
            llm_path = company_dir / f"{ticker}_{filing_type}_llm.txt"
            
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
            
            # Extract text
            extract_result = self.extractor.process_filing(
                rendered_path,
                output_path=text_path,
                metadata=metadata
            )
            
            # Add extraction stage to results
            result["stages"]["extract"] = {
                "success": extract_result.get("success", False),
                "time_seconds": time.time() - extract_start,
                "result": extract_result
            }
            
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
                    
                # For Microsoft's specific fiscal calendar (fiscal year ends in June)
                fiscal_quarter = None
                if ticker == "MSFT":
                    # Microsoft's fiscal year ends June 30
                    # Q1: Jul-Sep, Q2: Oct-Dec, Q3: Jan-Mar, Annual: Apr-Jun
                    if period_end_date:
                        year_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', period_end_date)
                        if year_match:
                            cal_year = int(year_match.group(1))
                            month = int(year_match.group(2))
                            day = int(year_match.group(3))
                                
                            # Use the fiscal registry from src2 for consistent fiscal calculations
                            from src2.sec.fiscal import fiscal_registry
                            
                            # Get fiscal info from the registry
                            fiscal_info = fiscal_registry.determine_fiscal_period(
                                ticker, period_end_date, filing_type
                            )
                            fiscal_year = fiscal_info.get("fiscal_year")
                            fiscal_quarter = fiscal_info.get("fiscal_period")
                            
                            print(f"Using src2 fiscal registry in pipeline for {ticker}: period_end_date={period_end_date}, filing_type={filing_type} -> Year={fiscal_year}, Period={fiscal_quarter}")
                                    
                            # Fiscal quarter already determined by fiscal registry
                
                # For other companies, use standard quarterly mapping
                if not fiscal_quarter and filing_type == "10-Q" and period_end_date:
                    month_match = re.search(r'\d{4}-(\d{2})-\d{2}', period_end_date)
                    if month_match:
                        month = int(month_match.group(1))
                        if 1 <= month <= 3:
                            fiscal_quarter = "Q1"
                        elif 4 <= month <= 6:
                            fiscal_quarter = "Q2"
                        elif 7 <= month <= 9:
                            fiscal_quarter = "Q3"
                        elif 10 <= month <= 12:
                            fiscal_quarter = "Q4"
                
                # For 10-K, use annual
                if filing_type == "10-K":
                    fiscal_quarter = "annual"
                    
                # Construct GCS paths using the proper folder structure
                if filing_type == "10-K" or fiscal_quarter == "annual":
                    gcs_text_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/text.txt"
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/llm.txt"
                elif fiscal_quarter:
                    gcs_text_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_quarter}/text.txt"
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_quarter}/llm.txt"
                else:
                    # Fallback to fiscal year only if we can't determine quarter
                    gcs_text_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/text.txt"
                    gcs_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/llm.txt"
                
                # Log the paths for debugging
                logging.info(f"Using GCS paths: {gcs_text_path} and {gcs_llm_path}")
                
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
                
                # Check if files already exist in GCS
                files_exist = self.gcp_storage.check_files_exist([gcs_text_path, gcs_llm_path])
                if files_exist.get(gcs_text_path, False) and files_exist.get(gcs_llm_path, False):
                    logging.info(f"Files already exist in GCS, skipping upload")
                    
                    # Create successful upload results
                    text_upload_result = {
                        "success": True,
                        "gcs_path": gcs_text_path,
                        "already_exists": True
                    }
                    
                    llm_upload_result = {
                        "success": True,
                        "gcs_path": gcs_llm_path,
                        "already_exists": True
                    }
                    
                    upload_results["text_upload"] = text_upload_result
                    upload_results["llm_upload"] = llm_upload_result
                else:
                    # Upload text file
                    text_upload_result = self.gcp_storage.upload_file(str(text_path), gcs_text_path)
                    upload_results["text_upload"] = text_upload_result
                    
                    # Upload LLM file if it exists
                    if llm_result.get("success", False) and os.path.exists(str(llm_path)):
                        llm_upload_result = self.gcp_storage.upload_file(str(llm_path), gcs_llm_path)
                        upload_results["llm_upload"] = llm_upload_result
                
                # Update metadata in Firestore if text upload was successful
                if text_upload_result.get("success", False):
                    # Add Firestore metadata
                    metadata_update = {
                        "text_path": gcs_text_path,
                        "text_size": os.path.getsize(str(text_path))
                    }
                    
                    # Add LLM path if it was uploaded successfully
                    if llm_upload_result and llm_upload_result.get("success", False):
                        metadata_update["llm_path"] = gcs_llm_path
                        metadata_update["llm_size"] = llm_size
                    
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
            result["text_path"] = str(text_path) if os.path.exists(text_path) else None
            result["llm_path"] = str(llm_path) if os.path.exists(llm_path) else None
            result["total_time_seconds"] = time.time() - start_time
            
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
                            gcs_path = info["result"].get("gcs_path")
                            print(f"    Uploaded to GCS: {gcs_path}")
                        else:
                            print(f"    Upload failed: {info['result'].get('error', 'Unknown error')}")
            
            # Print warning if any
            if "warning" in result:
                print(f"\n⚠️ Warning: {result['warning']}")
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