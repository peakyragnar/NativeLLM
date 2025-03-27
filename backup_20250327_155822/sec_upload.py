#!/usr/bin/env python3
"""
SEC Filing Uploader

Utility to upload processed SEC filings to Google Cloud Storage.
"""

import os
import sys
import argparse
import logging
import time
from pathlib import Path
import json

def upload_sec_file(file_path, gcp_bucket, gcp_project=None):
    """
    Upload a SEC filing file to Google Cloud Storage.
    
    Args:
        file_path: Path to the file to upload
        gcp_bucket: GCS bucket name
        gcp_project: GCP project ID (optional)
        
    Returns:
        Dictionary with upload results
    """
    # Set up GCP credentials if needed
    credentials_path = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"
    if os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        logging.info(f"Using GCP credentials from: {credentials_path}")
    
    # Initialize GCP storage
    try:
        from google.cloud import storage, firestore
        
        # Create storage client
        storage_client = storage.Client(project=gcp_project)
        bucket = storage_client.bucket(gcp_bucket)
        
        # Initialize Firestore with the specific database name 'nativellm'
        try:
            firestore_client = firestore.Client(database='nativellm')
            logging.info("Firestore client initialized with database 'nativellm'")
        except Exception as e:
            logging.warning(f"Firestore initialization failed: {str(e)}")
            firestore_client = None
            
    except ImportError:
        logging.error("Google Cloud libraries not found. Install with: pip install google-cloud-storage google-cloud-firestore")
        return {
            "success": False,
            "error": "Google Cloud libraries not found"
        }
    except Exception as e:
        logging.error(f"Error initializing GCP: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing GCP: {str(e)}"
        }
    
    # Extract metadata from file path
    file_path = Path(file_path)
    file_name = file_path.name
    ticker = file_path.parent.name
    
    # Determine file type from name
    file_type = None
    if "text" in file_name.lower():
        file_type = "text"
    elif "llm" in file_name.lower():
        file_type = "llm"
    else:
        file_type = file_path.suffix.replace(".", "")
    
    # Extract filing type (10-K, 10-Q, etc.) from file name
    filing_type = "10-K"  # Default
    if "10-k" in file_name.lower():
        filing_type = "10-K"
    elif "10-q" in file_name.lower():
        filing_type = "10-Q"
    elif "8-k" in file_name.lower():
        filing_type = "8-K"
    
    # Construct GCS path
    date_str = time.strftime("%Y%m%d")
    gcs_path = f"companies/{ticker}/{filing_type}/{date_str}/{file_type}.txt"
    
    try:
        # Create blob
        blob = bucket.blob(gcs_path)
        
        # Upload file
        with open(file_path, 'rb') as f:
            blob.upload_from_file(f)
        
        logging.info(f"Uploaded {file_path} to gs://{gcp_bucket}/{gcs_path}")
        
        # Update Firestore if available
        if firestore_client:
            # Create timestamp for the document ID to ensure uniqueness
            timestamp = time.strftime("%Y%m%d%H%M%S")
            
            # Create document ID
            document_id = f"{ticker}_{filing_type}_{timestamp}"
            
            # Create firestore document in the "nativellm" collection
            filing_ref = firestore_client.collection("nativellm").document(document_id)
            
            # Create document data
            doc_data = {
                'ticker': ticker,
                'company_name': ticker,  # Use ticker as company name if not available
                'filing_type': filing_type,
                'upload_date': time.time(),
                'storage_class': 'STANDARD',
                'last_accessed': time.time(),
                'access_count': 0
            }
            
            # Add appropriate path based on file type
            if file_type == "text":
                doc_data['text_file_path'] = gcs_path
                doc_data['text_file_size'] = os.path.getsize(file_path)
            elif file_type == "llm":
                doc_data['llm_file_path'] = gcs_path
                doc_data['llm_file_size'] = os.path.getsize(file_path)
                doc_data['has_llm_format'] = True
            
            # Check if there's an existing document for this ticker and filing
            existing_docs = firestore_client.collection("nativellm").where("ticker", "==", ticker).where("filing_type", "==", filing_type).get()
            
            existing_doc = None
            for doc in existing_docs:
                existing_doc = doc
                break
            
            if existing_doc:
                # Update existing document
                existing_data = existing_doc.to_dict()
                
                # Preserve existing paths
                if 'text_file_path' in existing_data and file_type != "text":
                    doc_data['text_file_path'] = existing_data['text_file_path']
                    doc_data['text_file_size'] = existing_data.get('text_file_size', 0)
                
                if 'llm_file_path' in existing_data and file_type != "llm":
                    doc_data['llm_file_path'] = existing_data['llm_file_path']
                    doc_data['llm_file_size'] = existing_data.get('llm_file_size', 0)
                
                # Update existing document
                existing_doc.reference.update(doc_data)
                document_id = existing_doc.id
                logging.info(f"Updated existing Firestore document {document_id}")
            else:
                # Add new document
                filing_ref.set(doc_data)
                logging.info(f"Added metadata to Firestore for {document_id}")
        
        return {
            "success": True,
            "gcs_path": gcs_path,
            "file_type": file_type,
            "size": os.path.getsize(file_path)
        }
    except Exception as e:
        logging.error(f"Error uploading to GCS: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "file_path": str(file_path)
        }

def upload_sec_directory(directory, gcp_bucket, gcp_project=None):
    """
    Upload all SEC filing files in a directory to Google Cloud Storage.
    
    Args:
        directory: Directory containing SEC files
        gcp_bucket: GCS bucket name
        gcp_project: GCP project ID (optional)
        
    Returns:
        Number of files successfully uploaded
    """
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Check directory
    directory = Path(directory)
    if not directory.exists() or not directory.is_dir():
        logging.error(f"Directory does not exist: {directory}")
        return 0
    
    # Find all text and LLM files
    text_files = list(directory.glob("*_text.txt"))
    llm_files = list(directory.glob("*_llm.txt"))
    
    logging.info(f"Found {len(text_files)} text files and {len(llm_files)} LLM files")
    
    # Upload files
    success_count = 0
    
    for file_path in text_files + llm_files:
        logging.info(f"Uploading {file_path}...")
        result = upload_sec_file(file_path, gcp_bucket, gcp_project)
        
        if result.get("success", False):
            logging.info(f"Successfully uploaded to {result.get('gcs_path')}")
            success_count += 1
        else:
            logging.error(f"Failed to upload {file_path}: {result.get('error', 'Unknown error')}")
    
    logging.info(f"Uploaded {success_count} out of {len(text_files) + len(llm_files)} files")
    return success_count

def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Upload SEC filings to GCP")
    parser.add_argument("--file", help="Path to file to upload")
    parser.add_argument("--dir", help="Directory containing files to upload")
    parser.add_argument("--ticker", help="Ticker symbol (used with --dir)")
    parser.add_argument("--gcp-bucket", required=True, help="GCS bucket name")
    parser.add_argument("--gcp-project", help="GCP project ID")
    
    args = parser.parse_args()
    
    if not args.file and not args.dir:
        parser.error("Either --file or --dir must be specified")
    
    if args.file:
        # Upload single file
        if not os.path.exists(args.file):
            logging.error(f"File not found: {args.file}")
            return 1
        
        result = upload_sec_file(args.file, args.gcp_bucket, args.gcp_project)
        
        if result.get("success", False):
            print(f"Successfully uploaded to {result.get('gcs_path')}")
            return 0
        else:
            print(f"Failed to upload: {result.get('error', 'Unknown error')}")
            return 1
    
    if args.dir:
        # Upload directory
        directory = args.dir
        if args.ticker:
            directory = os.path.join(directory, args.ticker)
        
        success_count = upload_sec_directory(directory, args.gcp_bucket, args.gcp_project)
        
        if success_count > 0:
            print(f"Successfully uploaded {success_count} files")
            return 0
        else:
            print("Failed to upload any files")
            return 1

if __name__ == "__main__":
    sys.exit(main())