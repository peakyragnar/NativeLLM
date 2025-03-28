"""
GCP Storage Module

Responsible for uploading files to Google Cloud Storage.
"""

import os
import logging
import datetime

def estimate_tokens(text_content):
    """
    Estimate the number of tokens in text content.
    Uses a rough approximation of 4 characters per token.
    
    Args:
        text_content: The text content to estimate
        
    Returns:
        Estimated token count
    """
    if not text_content:
        return 0
    return len(text_content) // 4

class GCPStorage:
    """
    Upload files to Google Cloud Storage
    """
    
    def __init__(self, bucket_name, project_id=None):
        """
        Initialize GCP storage
        
        Args:
            bucket_name: GCS bucket name
            project_id: GCP project ID (optional)
        """
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.storage_client = None
        self.firestore_client = None
        
        # Load GCP libraries
        try:
            from google.cloud import storage, firestore
            self.storage_client = storage.Client(project=project_id)
            self.bucket = self.storage_client.bucket(bucket_name)
            
            # Initialize Firestore with the specific database name 'nativellm'
            try:
                self.firestore_client = firestore.Client(database='nativellm')
                logging.info("Firestore client initialized with database 'nativellm'")
            except Exception as e:
                logging.warning(f"Firestore initialization failed: {str(e)}")
                self.firestore_client = None
                
            logging.info(f"Initialized GCP storage with bucket: {bucket_name}")
        except ImportError:
            logging.warning("Google Cloud libraries not found. Cloud storage disabled.")
        except Exception as e:
            logging.error(f"Error initializing GCP storage: {str(e)}")
    
    def is_enabled(self):
        """
        Check if GCP storage is enabled
        
        Returns:
            True if enabled, False otherwise
        """
        return self.storage_client is not None
    
    def is_firestore_enabled(self):
        """
        Check if Firestore is enabled
        
        Returns:
            True if enabled, False otherwise
        """
        return self.firestore_client is not None
        
    def check_files_exist(self, paths):
        """
        Check if files exist in GCS
        
        Args:
            paths: List of GCS paths to check
            
        Returns:
            Dict mapping paths to existence status (True/False)
        """
        if not self.is_enabled():
            logging.warning("GCP storage is not enabled")
            return {path: False for path in paths}
        
        try:
            result = {}
            for path in paths:
                blob = self.bucket.blob(path)
                result[path] = blob.exists()
            return result
        except Exception as e:
            logging.error(f"Error checking file existence: {str(e)}")
            return {path: False for path in paths}
    
    def upload_file(self, local_file_path, gcs_path, force=False):
        """
        Upload a file to GCS
        
        Args:
            local_file_path: Path to local file
            gcs_path: Path in GCS bucket
            force: Whether to upload even if the file already exists in GCS
            
        Returns:
            Dict with upload result
        """
        if not self.is_enabled():
            logging.warning("GCP storage is not enabled")
            return {
                "success": False,
                "error": "GCP storage not enabled",
                "local_path": local_file_path
            }
        
        try:
            # Check if file already exists in GCS
            if not force:
                exists = self.check_files_exist([gcs_path]).get(gcs_path, False)
                if exists:
                    logging.info(f"File already exists in GCS: {gcs_path}, skipping upload")
                    return {
                        "success": True,
                        "local_path": local_file_path,
                        "gcs_path": gcs_path,
                        "already_exists": True,
                        "size": os.path.getsize(local_file_path)
                    }
            
            # Create blob
            blob = self.bucket.blob(gcs_path)
            
            # Upload file
            with open(local_file_path, 'rb') as f:
                blob.upload_from_file(f)
            
            if force:
                logging.info(f"Force uploaded {local_file_path} to gs://{self.bucket_name}/{gcs_path}")
            else:
                logging.info(f"Uploaded {local_file_path} to gs://{self.bucket_name}/{gcs_path}")
            
            return {
                "success": True,
                "local_path": local_file_path,
                "gcs_path": gcs_path,
                "force_upload": force,
                "size": os.path.getsize(local_file_path)
            }
        except Exception as e:
            logging.error(f"Error uploading to GCS: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "local_path": local_file_path
            }
    
    def add_filing_metadata(self, filing_metadata, **kwargs):
        """
        Add filing metadata to Firestore
        
        Args:
            filing_metadata: Filing metadata
            **kwargs: Additional metadata fields, which may include:
                - text_path: Path to text file in GCS
                - llm_path: Path to LLM file in GCS
                - text_size: Size of text file
                - llm_size: Size of LLM file
            
        Returns:
            Dict with result
        """
        if not self.is_firestore_enabled():
            logging.warning("Firestore is not enabled")
            return {
                "success": False,
                "error": "Firestore not enabled"
            }
        
        try:
            # Extract metadata
            ticker = filing_metadata.get("ticker")
            company_name = filing_metadata.get("company_name", ticker)
            filing_type = filing_metadata.get("filing_type")
            filing_date = filing_metadata.get("filing_date")
            period_end_date = filing_metadata.get("period_end_date")
            
            # SINGLE SOURCE OF TRUTH:
            # Always use company_fiscal.py for all fiscal period determinations
            try:
                # Import the fiscal data contracts and registry
                from src2.sec.fiscal.fiscal_data import FiscalPeriodInfo, FiscalDataError, validate_period_end_date
                from src2.sec.fiscal.company_fiscal import fiscal_registry
                
                # Create data integrity metadata section for complete audit trail
                filing_metadata["data_integrity"] = {
                    "validation_source": "gcp_storage.py:fiscal_determination",
                    "validation_timestamp": datetime.datetime.now().isoformat(),
                    "raw_period_end_date": period_end_date
                }

                # Required checks - we must have both ticker and period_end_date
                if not ticker:
                    error_msg = "Missing ticker - cannot determine fiscal period"
                    logging.error(f"DATA INTEGRITY ERROR: {error_msg}")
                    filing_metadata["data_integrity"]["error"] = error_msg
                    filing_metadata["data_integrity"]["status"] = "failed"
                    filing_metadata["fiscal_error"] = error_msg
                    raise FiscalDataError(error_msg)
                    
                if not period_end_date:
                    error_msg = "Missing period_end_date - cannot determine fiscal period"
                    logging.error(f"DATA INTEGRITY ERROR: {error_msg}")
                    filing_metadata["data_integrity"]["error"] = error_msg
                    filing_metadata["data_integrity"]["status"] = "failed"
                    filing_metadata["fiscal_error"] = error_msg
                    
                    # INSTEAD OF FAILING, USE FALLBACKS
                    # 1. Check if fiscal_year and fiscal_period are already in filing_metadata
                    if "fiscal_year" in filing_metadata and "fiscal_period" in filing_metadata:
                        fiscal_year = filing_metadata["fiscal_year"]
                        fiscal_period = filing_metadata["fiscal_period"]
                        logging.warning(f"Using provided fiscal_year={fiscal_year} and fiscal_period={fiscal_period} from metadata despite missing period_end_date")
                        filing_metadata["data_integrity"]["fallback_used"] = "metadata_values"
                        filing_metadata["period_end_date"] = f"{fiscal_year}-01-01"  # Generic placeholder
                    # 2. Use filing_type specific fallbacks
                    else:
                        # For 10-K, we know it's annual
                        if filing_type == "10-K":
                            fiscal_period = "annual"
                            # Try to get year from date in filing_date
                            if filing_metadata.get("filing_date") and "-" in filing_metadata["filing_date"]:
                                fiscal_year = filing_metadata["filing_date"].split("-")[0]
                            else:
                                fiscal_year = str(datetime.datetime.now().year)
                            
                            logging.warning(f"Using fallback fiscal_year={fiscal_year} and fiscal_period={fiscal_period} for 10-K")
                            filing_metadata["data_integrity"]["fallback_used"] = "10K_defaults"
                        # For 10-Q, use Q? placeholder but try to get year
                        else:
                            fiscal_period = "Q?"
                            # Try to get year from filing_date
                            if filing_metadata.get("filing_date") and "-" in filing_metadata["filing_date"]:
                                fiscal_year = filing_metadata["filing_date"].split("-")[0]
                            else:
                                fiscal_year = str(datetime.datetime.now().year)
                                
                            logging.warning(f"Using fallback fiscal_year={fiscal_year} and fiscal_period={fiscal_period} for 10-Q")
                            filing_metadata["data_integrity"]["fallback_used"] = "10Q_defaults"
                        
                    # Add the fallback values to filing_metadata for use downstream
                    filing_metadata["fiscal_year"] = fiscal_year
                    filing_metadata["fiscal_period"] = fiscal_period
                    filing_metadata["using_fallback_values"] = True
                
                # Get fiscal information directly from THE SINGLE SOURCE OF TRUTH
                # But skip this step if we're already using fallback values (no period_end_date)
                if not period_end_date or getattr(filing_metadata, "using_fallback_values", False):
                    # We're already using fallback values, create a mock fiscal_info
                    fiscal_info = {
                        "fiscal_year": filing_metadata.get("fiscal_year"),
                        "fiscal_period": filing_metadata.get("fiscal_period"),
                        "validated_date": filing_metadata.get("period_end_date", f"{filing_metadata.get('fiscal_year', '2025')}-01-01"),
                        "status": "fallback",
                        "using_fallback": True,
                        "reason": "Missing period_end_date"
                    }
                    logging.warning(f"Using fallback fiscal info due to missing period_end_date: Year={fiscal_info['fiscal_year']}, Period={fiscal_info['fiscal_period']}")
                else:
                    # This includes period_end_date validation internally
                    fiscal_info = fiscal_registry.determine_fiscal_period(
                        ticker, period_end_date, filing_type
                    )
                
                # Add all fiscal registry result fields to data integrity metadata
                filing_metadata["data_integrity"].update(fiscal_info)
                
                # Extract key fiscal information for Firestore document
                fiscal_year = fiscal_info.get("fiscal_year")
                fiscal_period = fiscal_info.get("fiscal_period")
                
                # Check if fiscal period determination was successful
                if not fiscal_year or not fiscal_period:
                    error_msg = fiscal_info.get("error", "Unknown error in fiscal period determination")
                    logging.error(f"DATA INTEGRITY ERROR: {error_msg}")
                    
                    # Add error to metadata
                    filing_metadata["fiscal_error"] = error_msg
                    filing_metadata["data_integrity"]["status"] = "failed"
                    
                    # STRICT POLICY: Fail rather than use incorrect data that could lead to data integrity issues
                    if filing_type == "10-K":
                        # Only safe fallback: For 10-K we always know it's "annual"
                        fiscal_period = "annual"
                        logging.warning(f"Using safe fallback 'annual' for 10-K filing")
                        filing_metadata["data_integrity"]["fallback_used"] = "annual_for_10K"
                    else:
                        # Use Q? to clearly indicate an unknown quarter
                        fiscal_period = "Q?"
                        logging.error(f"Using placeholder 'Q?' for unknown fiscal period")
                        filing_metadata["data_integrity"]["fallback_used"] = "Q?_placeholder"
                else:
                    # Success - we have valid fiscal information
                    logging.info(f"DATA INTEGRITY SUCCESS: {ticker}, period_end_date={period_end_date} -> " +
                                 f"Year={fiscal_year}, Period={fiscal_period}")
                    filing_metadata["data_integrity"]["status"] = "success"
                
                # Extract period_end_date that was actually used (normalized)
                if fiscal_info.get("validated_date"):
                    filing_metadata["period_end_date_normalized"] = fiscal_info.get("validated_date")
                
                # Add data integrity and source metadata to filing metadata
                filing_metadata["fiscal_source"] = "company_fiscal_registry"
                filing_metadata["period_end_date_used"] = period_end_date
                
            except ImportError as e:
                # Critical error - the registry module is missing
                error_msg = f"Fiscal registry not available: {str(e)}"
                logging.error(f"SYSTEM ERROR: {error_msg}")
                
                # Record error in metadata
                filing_metadata["fiscal_error"] = error_msg
                filing_metadata["data_integrity"] = {
                    "status": "system_error",
                    "error": error_msg,
                    "timestamp": datetime.datetime.now().isoformat()
                }
                
                # Minimal fallbacks for system errors only
                if filing_type == "10-K":
                    fiscal_period = "annual"
                else:
                    fiscal_period = "Q?"
                    
                if period_end_date:
                    import re
                    year_match = re.search(r'(\d{4})', period_end_date)
                    if year_match:
                        fiscal_year = year_match.group(1)
                
            except Exception as e:
                # Unexpected error
                error_msg = f"Unexpected error in fiscal period determination: {str(e)}"
                logging.error(f"SYSTEM ERROR: {error_msg}")
                
                # Record error in metadata
                filing_metadata["fiscal_error"] = error_msg
                filing_metadata["data_integrity"] = {
                    "status": "system_error",
                    "error": error_msg,
                    "timestamp": datetime.datetime.now().isoformat()
                }
                
                # Minimal fallbacks for system errors only
                if filing_type == "10-K":
                    fiscal_period = "annual"
                else:
                    fiscal_period = "Q?"
                    
                if period_end_date:
                    import re
                    year_match = re.search(r'(\d{4})', period_end_date)
                    if year_match:
                        fiscal_year = year_match.group(1)
            
            # Create document ID using fiscal year and fiscal period for quarterly filings
            # The document ID should match the GCS path format for consistency
            if fiscal_year:
                # For 10-Q filings, include the fiscal quarter in the document ID
                if filing_type == "10-Q" and fiscal_period:
                    document_id = f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}"
                    logging.info(f"Using fiscal year {fiscal_year} and period {fiscal_period} for document ID: {document_id}")
                else:
                    # For 10-K and other filings, just use fiscal year
                    document_id = f"{ticker}_{filing_type}_{fiscal_year}"
                    logging.info(f"Using fiscal year {fiscal_year} for document ID: {document_id}")
            else:
                # Fallback to calendar year from period_end_date or current year
                year = None
                if period_end_date:
                    import re
                    year_match = re.search(r'(\d{4})', period_end_date)
                    if year_match:
                        year = year_match.group(1)
                
                if not year:
                    year = datetime.datetime.now().strftime("%Y")
                
                # For 10-Q filings, try to include quarter in document ID even in fallback mode
                if filing_type == "10-Q" and fiscal_period:
                    document_id = f"{ticker}_{filing_type}_{year}_{fiscal_period}"
                    logging.info(f"Using fallback year {year} and period {fiscal_period} for document ID: {document_id}")
                else:
                    document_id = f"{ticker}_{filing_type}_{year}"
                    logging.info(f"Using fallback year {year} for document ID: {document_id}")
            
            # Check if document already exists
            filing_ref = self.firestore_client.collection("filings").document(document_id)
            doc = filing_ref.get()
            
            # Create document data
            doc_data = {
                'company_ticker': ticker,
                'company_name': company_name,
                'filing_type': filing_type,
                'period_end_date': period_end_date,
                'filing_date': filing_date,
                'fiscal_year': fiscal_year,
                'fiscal_period': fiscal_period,
                'filing_id': document_id,
                'upload_date': datetime.datetime.now(),
                'text_file_path': kwargs.get('text_path'),
                'text_file_size': kwargs.get('text_size', 0),
                'storage_class': 'STANDARD',
                'last_accessed': datetime.datetime.now(),
                'access_count': 0,
                'display_period': f"FY{fiscal_year} {fiscal_period}" if fiscal_period else f"FY{fiscal_year}",
                # Add the following fields for better data integrity and transparency
                'period_end_date_raw': period_end_date,
                'fiscal_source': 'company_fiscal_registry',
                'fiscal_integrity_verified': True
            }
            
            # Add LLM file metadata if provided
            if 'llm_path' in kwargs:
                llm_path = kwargs.get('llm_path')
                doc_data['llm_file_path'] = llm_path
                doc_data['llm_file_size'] = kwargs.get('llm_size', 0)
                doc_data['has_llm_format'] = True
                
                # Estimate token count for LLM file
                try:
                    # Check if path is a GCS path or local path
                    if llm_path.startswith('gs://') or not os.path.exists(llm_path):
                        # If it's a GCS path, look for a local copy in kwargs
                        local_llm_path = kwargs.get('local_llm_path')
                        if local_llm_path and os.path.exists(local_llm_path):
                            llm_path = local_llm_path
                            logging.info(f"Using local LLM file for token counting: {local_llm_path}")
                        else:
                            # Try to find a local copy based on naming convention
                            potential_local_path = os.path.join('sec_processed', ticker, f"{ticker}_{filing_type}_llm.txt")
                            if os.path.exists(potential_local_path):
                                llm_path = potential_local_path
                                logging.info(f"Found local LLM file for token counting: {potential_local_path}")
                    
                    logging.info(f"Attempting to read LLM file for token counting: {llm_path}")
                    if os.path.exists(llm_path):
                        with open(llm_path, 'r', encoding='utf-8') as f:
                            llm_content = f.read()
                        llm_token_count = estimate_tokens(llm_content)
                        doc_data['llm_token_count'] = llm_token_count
                        logging.info(f"SUCCESS: Estimated {llm_token_count:,} tokens for LLM file ({len(llm_content):,} chars)")
                    else:
                        logging.warning(f"LLM file not found for token counting: {llm_path}")
                except Exception as e:
                    logging.warning(f"Could not estimate tokens for LLM file: {str(e)}")
            else:
                doc_data['has_llm_format'] = False
                
            # Estimate token count for text file if available
            text_path = kwargs.get('text_path')
            if text_path:
                try:
                    # Check if path is a GCS path or local path
                    if text_path.startswith('gs://') or not os.path.exists(text_path):
                        # If it's a GCS path, look for a local copy in kwargs
                        local_text_path = kwargs.get('local_text_path')
                        if local_text_path and os.path.exists(local_text_path):
                            text_path = local_text_path
                            logging.info(f"Using local text file for token counting: {local_text_path}")
                        else:
                            # Try to find a local copy based on naming convention
                            potential_local_path = os.path.join('sec_processed', ticker, f"{ticker}_{filing_type}_text.txt")
                            if os.path.exists(potential_local_path):
                                text_path = potential_local_path
                                logging.info(f"Found local text file for token counting: {potential_local_path}")
                    
                    logging.info(f"Attempting to read text file for token counting: {text_path}")
                    if os.path.exists(text_path):
                        with open(text_path, 'r', encoding='utf-8') as f:
                            text_content = f.read()
                        text_token_count = estimate_tokens(text_content)
                        doc_data['text_token_count'] = text_token_count
                        logging.info(f"SUCCESS: Estimated {text_token_count:,} tokens for text file ({len(text_content):,} chars)")
                    else:
                        logging.warning(f"Text file not found for token counting: {text_path}")
                except Exception as e:
                    logging.warning(f"Could not estimate tokens for text file: {str(e)}")
            
            # Add document to Firestore (overwrite if exists)
            filing_ref.set(doc_data)
            
            logging.info(f"Added metadata to Firestore for {document_id}")
            
            return {
                "success": True,
                "document_id": document_id
            }
        except Exception as e:
            logging.error(f"Error adding metadata to Firestore: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def delete_file(self, gcs_path):
        """
        Delete a file from GCS
        
        Args:
            gcs_path: Path in GCS bucket
            
        Returns:
            Dict with deletion result
        """
        if not self.is_enabled():
            logging.warning("GCP storage is not enabled")
            return {
                "success": False,
                "error": "GCP storage not enabled",
                "gcs_path": gcs_path
            }
        
        try:
            # Create blob
            blob = self.bucket.blob(gcs_path)
            
            # Check if it exists
            if not blob.exists():
                logging.warning(f"File does not exist in GCS: {gcs_path}")
                return {
                    "success": False,
                    "error": "File does not exist",
                    "gcs_path": gcs_path
                }
            
            # Delete the blob
            blob.delete()
            
            logging.info(f"Deleted file from GCS: {gcs_path}")
            
            return {
                "success": True,
                "gcs_path": gcs_path
            }
        except Exception as e:
            logging.error(f"Error deleting from GCS: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "gcs_path": gcs_path
            }

# Factory function to create GCP storage
def create_gcp_storage(bucket_name, project_id=None):
    """
    Create a GCP storage instance
    
    Args:
        bucket_name: GCS bucket name
        project_id: GCP project ID (optional)
        
    Returns:
        GCPStorage instance
    """
    storage = GCPStorage(bucket_name, project_id)
    if storage.is_enabled():
        return storage
    else:
        return None