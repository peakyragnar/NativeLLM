#!/usr/bin/env python3
"""
GCP Upload Module for NativeLLM

This module provides functions to upload processed XBRL files to Google Cloud Storage
and update metadata in Firestore. It connects the local processing pipeline with the
cloud storage infrastructure.
"""

import os
import sys
import glob
import logging
import datetime
from google.cloud import storage, firestore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Set credentials path - this would typically be in a config file
CREDENTIALS_PATH = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"
GCS_BUCKET_NAME = "native-llm-filings"
FIRESTORE_DB = "nativellm"

def configure_gcp():
    """Set up GCP credentials if not already configured"""
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        if os.path.exists(CREDENTIALS_PATH):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
            logging.info(f"Set GCP credentials from {CREDENTIALS_PATH}")
            return True
        else:
            logging.error(f"Credentials file not found at {CREDENTIALS_PATH}")
            return False
    return True

def extract_metadata_from_filename(filename):
    """Extract metadata from the filename using the established format"""
    # Expected format: Apple_Inc_2024_FY_AAPL_10-K_20240928_llm.txt
    try:
        parts = filename.split('_')
        if len(parts) < 7:
            return None
            
        file_format = parts[-1].split('.')[0]  # llm or text
        period_end_date = parts[-2]
        filing_type = parts[-3]
        ticker = parts[-4]
        
        # Determine fiscal period
        period_info = parts[-5]  # FY, 1Q, 2Q, etc.
        if period_info == "FY":
            fiscal_period = "annual"
        else:
            fiscal_period = period_info
        
        fiscal_year = parts[-6]
        
        # Format period end date if it's in YYYYMMDD format
        if len(period_end_date) == 8 and period_end_date.isdigit():
            period_end_date = f"{period_end_date[:4]}-{period_end_date[4:6]}-{period_end_date[6:8]}"
            
        # Company name is everything before the fiscal year
        company_parts = parts[:-6]
        company_name = " ".join(company_parts).replace("_", " ")
        
        return {
            "company_name": company_name,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "ticker": ticker,
            "filing_type": filing_type,
            "period_end_date": period_end_date,
            "file_format": file_format
        }
    except Exception as e:
        logging.error(f"Error parsing filename {filename}: {str(e)}")
        return None

def upload_file_to_gcs(file_path, bucket_name=GCS_BUCKET_NAME):
    """Upload a single file to Google Cloud Storage with the proper path structure"""
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return None
        
    filename = os.path.basename(file_path)
    metadata = extract_metadata_from_filename(filename)
    
    if not metadata:
        logging.error(f"Could not extract metadata from filename: {filename}")
        return None
    
    ticker = metadata["ticker"]
    filing_type = metadata["filing_type"]
    fiscal_year = metadata["fiscal_year"]
    fiscal_period = metadata["fiscal_period"]
    file_format = metadata["file_format"]
    
    # Determine quarter folder (use 'annual' for 10-K, quarter designation for others)
    if fiscal_period == "annual":
        quarter_folder = "annual"
    else:
        # Convert 1Q, 2Q, 3Q, 4Q to Q1, Q2, Q3, Q4 if needed
        if fiscal_period.startswith("Q"):
            quarter_folder = fiscal_period
        else:
            quarter_number = fiscal_period[0]
            quarter_folder = f"Q{quarter_number}"
    
    # Construct GCS path following the established pattern
    gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{quarter_folder}/{file_format}.txt"
    
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Create blob and upload
        blob = bucket.blob(gcs_path)
        
        # Get file size for metadata
        file_size = os.path.getsize(file_path)
        
        # Upload the file
        with open(file_path, 'rb') as f:
            blob.upload_from_file(f)
        
        logging.info(f"Successfully uploaded {file_path} ({file_size} bytes) to gs://{bucket_name}/{gcs_path}")
        
        # Return metadata with additional info
        metadata.update({
            "gcs_path": gcs_path,
            "file_size": file_size,
            "upload_time": datetime.datetime.now().isoformat()
        })
        
        return metadata
    except Exception as e:
        logging.error(f"Error uploading file to GCS: {str(e)}")
        return None

def update_firestore_metadata(metadata):
    """Update or create metadata in Firestore database"""
    if not metadata or "gcs_path" not in metadata:
        logging.error("Invalid metadata for Firestore update")
        return False
    
    try:
        db = firestore.Client(database=FIRESTORE_DB)
        
        # Add company if it doesn't exist
        ticker = metadata["ticker"]
        company_name = metadata["company_name"]
        
        # Check if company exists
        company_ref = db.collection('companies').document(ticker)
        company_doc = company_ref.get()
        
        if not company_doc.exists:
            # Create new company document
            company_data = {
                'ticker': ticker,
                'name': company_name,
                'last_updated': datetime.datetime.now()
            }
            company_ref.set(company_data)
            logging.info(f"Created company record for {ticker} ({company_name})")
        
        # Create a unique filing ID
        filing_type = metadata["filing_type"]
        fiscal_year = metadata["fiscal_year"]
        fiscal_period = metadata["fiscal_period"]
        filing_id = f"{ticker}-{filing_type}-{fiscal_year}-{fiscal_period}"
        
        # Get file format and path
        file_format = metadata["file_format"]
        gcs_path = metadata["gcs_path"]
        file_size = metadata["file_size"]
        
        # Check if filing exists
        filing_ref = db.collection('filings').document(filing_id)
        filing_doc = filing_ref.get()
        
        if filing_doc.exists:
            # Update existing document with this format's info
            update_data = {
                'last_updated': datetime.datetime.now()
            }
            
            if file_format == "llm":
                update_data['llm_file_path'] = gcs_path
                update_data['llm_file_size'] = file_size
            elif file_format == "text":
                update_data['text_file_path'] = gcs_path
                update_data['text_file_size'] = file_size
            
            filing_ref.update(update_data)
            logging.info(f"Updated filing {filing_id} with {file_format} format")
        else:
            # Create new filing document
            filing_data = {
                'filing_id': filing_id,
                'company_ticker': ticker,
                'company_name': company_name,
                'filing_type': filing_type,
                'fiscal_year': fiscal_year,
                'fiscal_period': fiscal_period,
                'period_end_date': metadata.get("period_end_date"),
                'filing_date': metadata.get("filing_date", datetime.datetime.now().strftime('%Y-%m-%d')),
                'storage_class': 'STANDARD',
                'last_updated': datetime.datetime.now(),
                'access_count': 0
            }
            
            # Add format-specific fields
            if file_format == "llm":
                filing_data['llm_file_path'] = gcs_path
                filing_data['llm_file_size'] = file_size
            elif file_format == "text":
                filing_data['text_file_path'] = gcs_path
                filing_data['text_file_size'] = file_size
            
            filing_ref.set(filing_data)
            logging.info(f"Created filing record for {filing_id}")
        
        return True
    except Exception as e:
        logging.error(f"Error updating Firestore: {str(e)}")
        return False

def upload_processed_file(file_path):
    """Upload a processed file to GCS and update Firestore metadata"""
    if not configure_gcp():
        return False
    
    # Upload file to GCS
    metadata = upload_file_to_gcs(file_path)
    if not metadata:
        return False
    
    # Update Firestore metadata
    result = update_firestore_metadata(metadata)
    return result

def upload_company_files(ticker, bucket_name=GCS_BUCKET_NAME):
    """Find and upload all processed files for a specific company"""
    if not configure_gcp():
        return 0
    
    # Import from config to get the data directory
    from src.config import PROCESSED_DATA_DIR
    
    # Find all files for this ticker
    company_dir = os.path.join(PROCESSED_DATA_DIR, ticker)
    if not os.path.exists(company_dir):
        logging.error(f"No processed files directory found for {ticker}")
        return 0
    
    # Pattern matches both _text.txt and _llm.txt files
    pattern = os.path.join(company_dir, f"*_{ticker}_*_*.txt")
    files = glob.glob(pattern)
    
    if not files:
        logging.warning(f"No processed files found for {ticker}")
        return 0
    
    logging.info(f"Found {len(files)} files to upload for {ticker}")
    
    success_count = 0
    for file_path in files:
        if upload_processed_file(file_path):
            success_count += 1
    
    logging.info(f"Successfully uploaded {success_count} of {len(files)} files for {ticker}")
    return success_count

def main():
    """Command line interface for GCP upload utility"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Upload processed XBRL files to Google Cloud")
    parser.add_argument("--file", help="Upload a specific file to GCS")
    parser.add_argument("--company", help="Upload all files for a specific company")
    parser.add_argument("--all", action="store_true", help="Upload all processed files for all companies")
    parser.add_argument("--verify", action="store_true", help="Verify GCS and Firestore consistency after upload")
    
    args = parser.parse_args()
    
    if not configure_gcp():
        sys.exit(1)
    
    if args.file:
        if upload_processed_file(args.file):
            logging.info(f"Successfully uploaded {args.file}")
        else:
            logging.error(f"Failed to upload {args.file}")
    
    elif args.company:
        count = upload_company_files(args.company)
        logging.info(f"Uploaded {count} files for {args.company}")
    
    elif args.all:
        # Import config to get processed data directory
        from src.config import PROCESSED_DATA_DIR
        
        # Get all company directories
        companies = [d for d in os.listdir(PROCESSED_DATA_DIR) 
                    if os.path.isdir(os.path.join(PROCESSED_DATA_DIR, d))]
        
        total_uploaded = 0
        for company in companies:
            count = upload_company_files(company)
            total_uploaded += count
            logging.info(f"Uploaded {count} files for {company}")
        
        logging.info(f"Total files uploaded: {total_uploaded}")
    
    if args.verify:
        try:
            # Run the consistency checker
            from tests.test_gcs_firestore_consistency import test_gcs_firestore_consistency
            
            # If company specified, check just that company
            if args.company:
                results = test_gcs_firestore_consistency(args.company)
                valid_count = sum(1 for r in results if r['status'] == 'valid')
                print(f"Consistency check: {valid_count} of {len(results)} filings are valid")
            else:
                print("To verify consistency, use --company with --verify")
        except ImportError:
            logging.error("Could not import test_gcs_firestore_consistency")
    
    if not (args.file or args.company or args.all):
        parser.print_help()

if __name__ == "__main__":
    main()