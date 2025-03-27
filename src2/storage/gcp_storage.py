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
    
    def upload_file(self, local_file_path, gcs_path):
        """
        Upload a file to GCS
        
        Args:
            local_file_path: Path to local file
            gcs_path: Path in GCS bucket
            
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
            # Create blob
            blob = self.bucket.blob(gcs_path)
            
            # Upload file
            with open(local_file_path, 'rb') as f:
                blob.upload_from_file(f)
            
            logging.info(f"Uploaded {local_file_path} to gs://{self.bucket_name}/{gcs_path}")
            
            return {
                "success": True,
                "local_path": local_file_path,
                "gcs_path": gcs_path,
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
            
            # Determine fiscal year and period using fiscal_manager
            fiscal_year = None
            fiscal_period = None
            
            try:
                # Import fiscal registry from src2
                from src2.sec.fiscal import fiscal_registry
                
                if period_end_date and ticker:
                    # Use fiscal registry consistently for all companies
                    fiscal_info = fiscal_registry.determine_fiscal_period(
                        ticker, period_end_date, filing_type
                    )
                    fiscal_year = fiscal_info.get("fiscal_year")
                    fiscal_period = fiscal_info.get("fiscal_period")
                    
                    logging.info(f"USING src2 FISCAL REGISTRY: {ticker}, period_end_date={period_end_date}, filing_type={filing_type} -> Year={fiscal_year}, Period={fiscal_period}")
            except ImportError:
                logging.warning("Fiscal manager not available, falling back to basic date parsing")
            except Exception as e:
                logging.warning(f"Error determining fiscal period: {str(e)}")
            
            # If fiscal manager failed or wasn't available, fall back to basic parsing
            if not fiscal_year and period_end_date:
                # Extract year from period_end_date
                import re
                year_match = re.search(r'(\d{4})', period_end_date)
                if year_match:
                    fiscal_year = year_match.group(1)
            
            # Determine quarter from filing_type and period_end_date if needed
            if not fiscal_period:
                if filing_type == "10-K":
                    fiscal_period = "annual"
                elif filing_type == "10-Q" and period_end_date:
                    # Use simple quarter determination based on month
                    import re
                    month_match = re.search(r'\d{4}-(\d{2})-\d{2}', period_end_date)
                    if month_match:
                        month = int(month_match.group(1))
                        # Special case for Microsoft
                        if ticker == "MSFT":
                            if 7 <= month <= 9:
                                fiscal_period = "Q1"
                            elif 10 <= month <= 12:
                                fiscal_period = "Q2"
                            elif 1 <= month <= 3:
                                fiscal_period = "Q3"
                            elif 4 <= month <= 6:
                                fiscal_period = "annual"  # Microsoft uses annual for Q4
                        else:
                            # Standard calendar quarters for other companies
                            if 1 <= month <= 3:
                                fiscal_period = "Q1"
                            elif 4 <= month <= 6:
                                fiscal_period = "Q2"
                            elif 7 <= month <= 9:
                                fiscal_period = "Q3"
                            elif 10 <= month <= 12:
                                fiscal_period = "Q4"
            
            # Create document ID using fiscal year and fiscal period for quarterly filings
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
                'access_count': 0
            }
            
            # Add LLM file metadata if provided
            if 'llm_path' in kwargs:
                llm_path = kwargs.get('llm_path')
                doc_data['llm_file_path'] = llm_path
                doc_data['llm_file_size'] = kwargs.get('llm_size', 0)
                doc_data['has_llm_format'] = True
                
                # Estimate token count for LLM file
                try:
                    with open(llm_path, 'r', encoding='utf-8') as f:
                        llm_content = f.read()
                    llm_token_count = estimate_tokens(llm_content)
                    doc_data['llm_token_count'] = llm_token_count
                    logging.info(f"Estimated {llm_token_count:,} tokens for LLM file")
                except Exception as e:
                    logging.warning(f"Could not estimate tokens for LLM file: {str(e)}")
            else:
                doc_data['has_llm_format'] = False
                
            # Estimate token count for text file if available
            text_path = kwargs.get('text_path')
            if text_path:
                try:
                    with open(text_path, 'r', encoding='utf-8') as f:
                        text_content = f.read()
                    text_token_count = estimate_tokens(text_content)
                    doc_data['text_token_count'] = text_token_count
                    logging.info(f"Estimated {text_token_count:,} tokens for text file")
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