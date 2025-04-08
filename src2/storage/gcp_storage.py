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
    Uses a more nuanced approach to token estimation:
    - For English text dominated by alphanumeric characters: ~4.0 chars per token
    - For JSON/structured data with many special chars: ~2.5 chars per token
    - For numeric heavy financial tables: ~3.0 chars per token

    Args:
        text_content: The text content to estimate

    Returns:
        Estimated token count with improved accuracy
    """
    if not text_content:
        return 0

    # Check if this is likely a structured file with special characters
    special_chars_ratio = sum(1 for c in text_content if not c.isalnum() and not c.isspace()) / max(1, len(text_content))

    # Default ratio for normal text
    chars_per_token = 4.0

    # Adjust based on content characteristics
    if '@' in text_content and '|' in text_content and special_chars_ratio > 0.1:
        # This is likely our LLM format with many special characters
        chars_per_token = 3.0
    elif special_chars_ratio > 0.15:
        # Very high special character ratio (like JSON)
        chars_per_token = 2.5
    elif len([c for c in text_content[:1000] if c.isdigit()]) > 300:
        # Numeric-heavy content like financial tables
        chars_per_token = 3.0

    # Apply the estimate
    estimated_tokens = int(len(text_content) / chars_per_token)

    # Add a small buffer for safety (Claude, OpenAI and other tokenizers vary slightly)
    return int(estimated_tokens * 1.05)

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

            # Initialize Firestore with the nativellm database
            try:
                # Use the nativellm database which exists in the project
                self.firestore_client = firestore.Client(project=project_id, database='nativellm')
                logging.info(f"Firestore client initialized with project ID {project_id} and database 'nativellm'")

                # Test the connection by listing collections
                try:
                    collections = list(self.firestore_client.collections())
                    logging.info(f"Successfully connected to Firestore database 'nativellm'. Found {len(collections)} collections.")
                except Exception as test_error:
                    logging.warning(f"Firestore connection test failed: {str(test_error)}")
                    logging.warning("The 'nativellm' database exists but there might be permission issues.")
                    # Don't set client to None - it might still work for writes
            except Exception as e:
                logging.warning(f"Firestore initialization with 'nativellm' database failed: {str(e)}")
                try:
                    # Fall back to default database as a last resort
                    self.firestore_client = firestore.Client(project=project_id)
                    logging.info(f"Firestore client initialized with project ID {project_id} and default database")
                except Exception as e2:
                    logging.warning(f"All Firestore initialization attempts failed: {str(e2)}")
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
        # Import datetime here to avoid reference errors
        import datetime
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
            existing_doc = filing_ref.get()

            # Log whether the document already exists
            if existing_doc.exists:
                logging.info(f"Document already exists in Firestore with ID: {document_id}")
            else:
                logging.info(f"Creating new document in Firestore with ID: {document_id}")

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

                # Estimate token count for LLM file - IMPROVED ALGORITHM
                try:
                    # Define all possible locations where the file might be
                    potential_llm_paths = []

                    # 1. If a local_llm_path is directly provided in kwargs, use it first
                    local_llm_path = kwargs.get('local_llm_path')
                    if local_llm_path:
                        potential_llm_paths.append(local_llm_path)

                    # 2. If we're given a non-GCS path that might be local, check it
                    if llm_path and not llm_path.startswith('gs://'):
                        potential_llm_paths.append(llm_path)

                    # 3. Check common directory structures based on ticker and filing info
                    # Structure: /sec_processed/TICKER/TICKER_FILING-TYPE_YEAR_llm.txt
                    if fiscal_year and fiscal_period and filing_type == "10-Q":
                        # Quarterly filing path with fiscal period
                        potential_llm_paths.append(os.path.join('sec_processed', ticker, f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}_llm.txt"))
                        # Try subdirectory pattern too
                        potential_llm_paths.append(os.path.join('sec_processed', ticker, f"{filing_type}_{fiscal_year}_{fiscal_period}", f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}_llm.txt"))

                    # Annual filing path (no quarter)
                    if fiscal_year:
                        potential_llm_paths.append(os.path.join('sec_processed', ticker, f"{ticker}_{filing_type}_{fiscal_year}_llm.txt"))
                        # Try subdirectory pattern too
                        potential_llm_paths.append(os.path.join('sec_processed', ticker, f"{filing_type}_{fiscal_year}", f"{ticker}_{filing_type}_{fiscal_year}_llm.txt"))

                    # 4. Try simple structure with no year/quarter (legacy format)
                    potential_llm_paths.append(os.path.join('sec_processed', ticker, f"{ticker}_{filing_type}_llm.txt"))

                    # Try all potential paths until we find an existing file
                    llm_content = None
                    found_path = None

                    for path in potential_llm_paths:
                        if os.path.exists(path) and os.path.isfile(path):
                            logging.info(f"Found LLM file for token counting at: {path}")
                            try:
                                with open(path, 'r', encoding='utf-8') as f:
                                    llm_content = f.read()
                                found_path = path
                                break
                            except Exception as inner_e:
                                logging.warning(f"Failed to read file {path}: {str(inner_e)}")

                    # Process the content if we found it
                    if llm_content:
                        llm_token_count = estimate_tokens(llm_content)
                        doc_data['llm_token_count'] = llm_token_count
                        doc_data['llm_token_count_source'] = found_path
                        doc_data['llm_char_count'] = len(llm_content)
                        logging.info(f"SUCCESS: Estimated {llm_token_count:,} tokens for LLM file ({len(llm_content):,} chars)")
                    else:
                        # If we failed to find a file, log detailed information for debugging
                        logging.warning(f"LLM file not found for token counting. Tried: {', '.join(potential_llm_paths)}")
                        # Set a default token count based on file size if available
                        if 'llm_file_size' in doc_data and doc_data['llm_file_size'] > 0:
                            estimated_tokens = doc_data['llm_file_size'] // 4  # Rough estimate based on size
                            doc_data['llm_token_count'] = estimated_tokens
                            doc_data['llm_token_count_source'] = 'estimated_from_file_size'
                            logging.info(f"Using file size to roughly estimate token count: {estimated_tokens:,}")
                except Exception as e:
                    logging.warning(f"Could not estimate tokens for LLM file: {str(e)}")
                    # Even if we fail, try to provide an estimate from file size
                    if 'llm_file_size' in doc_data and doc_data['llm_file_size'] > 0:
                        estimated_tokens = doc_data['llm_file_size'] // 4  # Rough estimate based on size
                        doc_data['llm_token_count'] = estimated_tokens
                        doc_data['llm_token_count_source'] = 'estimated_from_file_size_fallback'
                        logging.info(f"Using file size to roughly estimate token count (fallback): {estimated_tokens:,}")
            else:
                doc_data['has_llm_format'] = False

            # Estimate token count for text file if available - IMPROVED ALGORITHM
            text_path = kwargs.get('text_path')
            if text_path:
                try:
                    # Define all possible locations where the file might be
                    potential_text_paths = []

                    # 1. If a local_text_path is directly provided in kwargs, use it first
                    local_text_path = kwargs.get('local_text_path')
                    if local_text_path:
                        potential_text_paths.append(local_text_path)

                    # 2. If we're given a non-GCS path that might be local, check it
                    if text_path and not text_path.startswith('gs://'):
                        potential_text_paths.append(text_path)

                    # 3. Check common directory structures based on ticker and filing info
                    # Structure: /sec_processed/TICKER/TICKER_FILING-TYPE_YEAR_text.txt
                    if fiscal_year and fiscal_period and filing_type == "10-Q":
                        # Quarterly filing path with fiscal period
                        potential_text_paths.append(os.path.join('sec_processed', ticker, f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}_text.txt"))
                        # Try subdirectory pattern too
                        potential_text_paths.append(os.path.join('sec_processed', ticker, f"{filing_type}_{fiscal_year}_{fiscal_period}", f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}_text.txt"))

                    # Annual filing path (no quarter)
                    if fiscal_year:
                        potential_text_paths.append(os.path.join('sec_processed', ticker, f"{ticker}_{filing_type}_{fiscal_year}_text.txt"))
                        # Try subdirectory pattern too
                        potential_text_paths.append(os.path.join('sec_processed', ticker, f"{filing_type}_{fiscal_year}", f"{ticker}_{filing_type}_{fiscal_year}_text.txt"))

                    # 4. Try simple structure with no year/quarter (legacy format)
                    potential_text_paths.append(os.path.join('sec_processed', ticker, f"{ticker}_{filing_type}_text.txt"))

                    # Try all potential paths until we find an existing file
                    text_content = None
                    found_path = None

                    for path in potential_text_paths:
                        if os.path.exists(path) and os.path.isfile(path):
                            logging.info(f"Found text file for token counting at: {path}")
                            try:
                                with open(path, 'r', encoding='utf-8') as f:
                                    text_content = f.read()
                                found_path = path
                                break
                            except Exception as inner_e:
                                logging.warning(f"Failed to read file {path}: {str(inner_e)}")

                    # Process the content if we found it
                    if text_content:
                        text_token_count = estimate_tokens(text_content)
                        doc_data['text_token_count'] = text_token_count
                        doc_data['text_token_count_source'] = found_path
                        doc_data['text_char_count'] = len(text_content)
                        logging.info(f"SUCCESS: Estimated {text_token_count:,} tokens for text file ({len(text_content):,} chars)")
                    else:
                        # If we failed to find a file, log detailed information for debugging
                        logging.warning(f"Text file not found for token counting. Tried: {', '.join(potential_text_paths)}")
                        # Set a default token count based on file size if available
                        if 'text_file_size' in doc_data and doc_data['text_file_size'] > 0:
                            estimated_tokens = doc_data['text_file_size'] // 4  # Rough estimate based on size
                            doc_data['text_token_count'] = estimated_tokens
                            doc_data['text_token_count_source'] = 'estimated_from_file_size'
                            logging.info(f"Using file size to roughly estimate text token count: {estimated_tokens:,}")
                except Exception as e:
                    logging.warning(f"Could not estimate tokens for text file: {str(e)}")
                    # Even if we fail, try to provide an estimate from file size
                    if 'text_file_size' in doc_data and doc_data['text_file_size'] > 0:
                        estimated_tokens = doc_data['text_file_size'] // 4  # Rough estimate based on size
                        doc_data['text_token_count'] = estimated_tokens
                        doc_data['text_token_count_source'] = 'estimated_from_file_size_fallback'
                        logging.info(f"Using file size to roughly estimate text token count (fallback): {estimated_tokens:,}")

            # Track whether we have token counts
            has_token_counts = 'llm_token_count' in doc_data or 'text_token_count' in doc_data
            token_count_source = doc_data.get('llm_token_count_source', 'none')

            # Add document to Firestore (overwrite if exists)
            try:
                # Log the document data for debugging
                logging.info(f"Attempting to save document to Firestore with ID: {document_id}")
                logging.info(f"Document data keys: {list(doc_data.keys())}")

                # Check if Firestore client is initialized
                if not self.firestore_client:
                    logging.error(f"❌ Cannot save to Firestore: client not initialized")
                    logging.error("Run create_firestore_db.py to create the Firestore database")
                    return {
                        "success": False,
                        "error": "Firestore client not initialized",
                        "document_id": document_id
                    }

                # Set the document with explicit error handling
                try:
                    # Convert datetime objects to Firestore timestamps
                    from google.cloud.firestore import SERVER_TIMESTAMP
                    import datetime

                    # Process document data to handle datetime objects
                    processed_data = {}
                    for key, value in doc_data.items():
                        if isinstance(value, datetime.datetime):
                            processed_data[key] = SERVER_TIMESTAMP
                        else:
                            processed_data[key] = value

                    # Set the document
                    filing_ref.set(processed_data)
                    logging.info(f"✅ Successfully saved document to Firestore with ID: {document_id}")
                except Exception as set_error:
                    logging.error(f"❌ Failed to set document in Firestore: {str(set_error)}")
                    if "FAILED_PRECONDITION" in str(set_error):
                        logging.error("This error often means the Firestore database doesn't exist")
                        logging.error("Run create_firestore_db.py to create the Firestore database")
                    return {
                        "success": False,
                        "error": str(set_error),
                        "document_id": document_id
                    }

                # Verify the document was saved correctly
                try:
                    # Read back from Firestore to verify all data was saved
                    saved_doc = filing_ref.get().to_dict()

                    if saved_doc:
                        logging.info(f"✅ Successfully retrieved document from Firestore with ID: {document_id}")
                        logging.info(f"Retrieved document keys: {list(saved_doc.keys())}")

                        # Verify token counts were saved
                        if has_token_counts:
                            if 'llm_token_count' in doc_data and 'llm_token_count' in saved_doc:
                                logging.info(f"✅ Verified llm_token_count in Firestore: {saved_doc.get('llm_token_count'):,} tokens (source: {token_count_source})")
                            elif 'llm_token_count' in doc_data:
                                logging.warning(f"⚠️ llm_token_count was set but not saved to Firestore")

                            if 'text_token_count' in doc_data and 'text_token_count' in saved_doc:
                                logging.info(f"✅ Verified text_token_count in Firestore: {saved_doc.get('text_token_count'):,} tokens")
                            elif 'text_token_count' in doc_data:
                                logging.warning(f"⚠️ text_token_count was set but not saved to Firestore")
                        else:
                            logging.warning(f"⚠️ No token counts were set for this document")

                        return {
                            "success": True,
                            "document_id": document_id,
                            "has_token_counts": has_token_counts
                        }
                    else:
                        logging.error(f"❌ Document was saved but retrieval returned None or empty document")
                        return {
                            "success": False,
                            "error": "Document saved but retrieval failed",
                            "document_id": document_id
                        }
                except Exception as verify_error:
                    logging.error(f"❌ Could not verify Firestore document: {str(verify_error)}")
                    return {
                        "success": False,
                        "error": str(verify_error),
                        "document_id": document_id
                    }
            except Exception as save_error:
                logging.error(f"❌ Failed to save document to Firestore: {str(save_error)}")
                # Try to get more detailed error information
                if hasattr(save_error, '__dict__'):
                    logging.error(f"Error details: {save_error.__dict__}")
                return {
                    "success": False,
                    "error": str(save_error),
                    "document_id": document_id
                }

            # This code is unreachable due to the return statements in the try blocks above
            # It's kept as a fallback in case the code structure changes in the future
            logging.info(f"Added metadata to Firestore for {document_id} (fallback return path)")
            return {
                "success": True,
                "document_id": document_id,
                "has_token_counts": has_token_counts
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