#!/usr/bin/env python3
"""
Fix script for pipeline uploads.

Addresses the issue where Firestore is updated but no files are being uploaded to GCS.
"""

import os
import logging
import argparse
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def setup_gcp_credentials():
    """Set up GCP credentials"""
    credentials_path = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"
    if os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        logging.info(f"Using GCP credentials from: {credentials_path}")
        return True
    else:
        logging.error(f"Credentials file not found at {credentials_path}")
        return False

def check_firestore_entry(ticker, filing_type, fiscal_year, fiscal_period=None):
    """Check if Firestore entry exists and get its details"""
    try:
        from google.cloud import firestore
        
        # Initialize Firestore client
        db = firestore.Client(database='nativellm')
        
        # Determine document ID
        if fiscal_period and filing_type == "10-Q":
            document_id = f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}"
        else:
            document_id = f"{ticker}_{filing_type}_{fiscal_year}"
        
        # Get the document
        doc_ref = db.collection('filings').document(document_id)
        doc = doc_ref.get()
        
        if doc.exists:
            data = doc.to_dict()
            return {
                "exists": True,
                "document_id": document_id,
                "data": data
            }
        else:
            return {
                "exists": False,
                "document_id": document_id
            }
    
    except Exception as e:
        logging.error(f"Error checking Firestore: {str(e)}")
        return {
            "exists": False,
            "error": str(e)
        }

def check_gcs_file(bucket_name, gcs_path):
    """Check if file exists in GCS bucket"""
    try:
        from google.cloud import storage
        
        # Initialize storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Check if blob exists
        blob = bucket.blob(gcs_path)
        
        return {
            "exists": blob.exists(),
            "gcs_path": gcs_path,
            "bucket": bucket_name
        }
    
    except Exception as e:
        logging.error(f"Error checking GCS file: {str(e)}")
        return {
            "exists": False,
            "error": str(e)
        }

def create_local_llm_file(ticker, filing_type, fiscal_year, fiscal_period=None):
    """Create/regenerate local LLM file using the pipeline"""
    try:
        from src2.sec.pipeline import SECFilingPipeline
        
        # Initialize the pipeline
        pipeline = SECFilingPipeline(
            email="info@exascale.capital",
            output_dir=Path("sec_processed"),
            skip_gcp_upload=True  # Skip GCP upload since we'll do it separately
        )
        
        # Determine what pipeline method to use based on filing type
        if filing_type == "10-K":
            # Process annual filing
            result = pipeline.process_fiscal_year(
                ticker=ticker,
                fiscal_year=fiscal_year,
                filing_type=filing_type,
                force_download=True
            )
        elif filing_type == "10-Q" and fiscal_period:
            # Process quarterly filing
            result = pipeline.process_fiscal_period(
                ticker=ticker,
                fiscal_year=fiscal_year,
                fiscal_period=fiscal_period,
                filing_type=filing_type,
                force_download=True
            )
        else:
            return {
                "success": False,
                "error": "Invalid filing type or missing fiscal period for 10-Q"
            }
        
        # Return the path to the generated LLM file
        if "llm_path" in result and result["llm_path"]:
            return {
                "success": True,
                "llm_path": result["llm_path"],
                "pipeline_result": result
            }
        else:
            return {
                "success": False,
                "error": "No LLM file path returned from pipeline",
                "pipeline_result": result
            }
    
    except Exception as e:
        logging.error(f"Error creating local LLM file: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

def upload_to_gcs(local_file_path, bucket_name, gcs_path):
    """Upload file to GCS manually"""
    try:
        from google.cloud import storage
        
        # Initialize storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Create blob
        blob = bucket.blob(gcs_path)
        
        # Upload file
        with open(local_file_path, 'rb') as f:
            blob.upload_from_file(f)
        
        logging.info(f"Uploaded {local_file_path} to gs://{bucket_name}/{gcs_path}")
        
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

def update_firestore_with_llm_path(document_id, llm_path, llm_size):
    """Update Firestore document with LLM file path and size"""
    try:
        from google.cloud import firestore
        
        # Initialize Firestore client
        db = firestore.Client(database='nativellm')
        
        # Get the document reference
        doc_ref = db.collection('filings').document(document_id)
        
        # Update the document
        doc_ref.update({
            "llm_file_path": llm_path,
            "llm_file_size": llm_size,
            "has_llm_format": True,
            "last_modified": firestore.SERVER_TIMESTAMP
        })
        
        logging.info(f"Updated Firestore document {document_id} with LLM path: {llm_path}")
        
        return {
            "success": True,
            "document_id": document_id,
            "llm_path": llm_path
        }
    
    except Exception as e:
        logging.error(f"Error updating Firestore: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "document_id": document_id
        }

def fix_filing_upload(ticker, filing_type, fiscal_year, fiscal_period=None):
    """Fix upload issues for a specific filing"""
    # Step 1: Check if Firestore entry exists
    logging.info("=" * 80)
    logging.info(f"Fixing upload for {ticker} {filing_type} {fiscal_year}{' '+fiscal_period if fiscal_period else ''}")
    logging.info("=" * 80)
    
    # Determine document ID
    if fiscal_period and filing_type == "10-Q":
        document_id = f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}"
    else:
        document_id = f"{ticker}_{filing_type}_{fiscal_year}"
    
    # Check Firestore
    firestore_result = check_firestore_entry(ticker, filing_type, fiscal_year, fiscal_period)
    if firestore_result.get("exists", False):
        logging.info(f"Firestore entry exists for {document_id}")
        
        # Check if it already has an LLM path
        firestore_data = firestore_result.get("data", {})
        if "llm_file_path" in firestore_data and firestore_data["llm_file_path"]:
            logging.info(f"Firestore entry already has LLM path: {firestore_data['llm_file_path']}")
            
            # Step 2: Check if the file exists in GCS
            bucket_name = "native-llm-filings"
            gcs_path = firestore_data["llm_file_path"]
            
            gcs_result = check_gcs_file(bucket_name, gcs_path)
            if gcs_result.get("exists", False):
                logging.info(f"GCS file exists at gs://{bucket_name}/{gcs_path}")
                
                # Step 3: Everything looks good, no action needed
                logging.info("No action needed - both Firestore and GCS are in sync")
                return {
                    "success": True,
                    "action": "none",
                    "message": "Both Firestore and GCS are in sync"
                }
            else:
                logging.warning(f"Firestore has LLM path but file doesn't exist in GCS")
                
                # Step 4: We need to create the LLM file and upload it
                logging.info("Creating local LLM file...")
                llm_result = create_local_llm_file(ticker, filing_type, fiscal_year, fiscal_period)
                
                if llm_result.get("success", False) and "llm_path" in llm_result:
                    local_llm_path = llm_result["llm_path"]
                    logging.info(f"Created local LLM file: {local_llm_path}")
                    
                    # Step 5: Upload the file to GCS
                    logging.info(f"Uploading to GCS at {gcs_path}...")
                    upload_result = upload_to_gcs(local_llm_path, bucket_name, gcs_path)
                    
                    if upload_result.get("success", False):
                        logging.info("Upload successful")
                        
                        # Step 6: Update Firestore with file size
                        llm_size = os.path.getsize(local_llm_path)
                        update_result = update_firestore_with_llm_path(document_id, gcs_path, llm_size)
                        
                        if update_result.get("success", False):
                            logging.info("Firestore update successful")
                            return {
                                "success": True,
                                "action": "upload_and_update",
                                "message": "Created local LLM file, uploaded to GCS, and updated Firestore"
                            }
                        else:
                            logging.error("Failed to update Firestore")
                            return {
                                "success": False,
                                "action": "upload_only",
                                "error": update_result.get("error", "Unknown error updating Firestore")
                            }
                    else:
                        logging.error("Failed to upload to GCS")
                        return {
                            "success": False,
                            "action": "create_only",
                            "error": upload_result.get("error", "Unknown error uploading to GCS")
                        }
                else:
                    logging.error("Failed to create local LLM file")
                    return {
                        "success": False,
                        "action": "none",
                        "error": llm_result.get("error", "Unknown error creating LLM file")
                    }
        else:
            # No LLM path in Firestore
            logging.warning("Firestore entry exists but has no LLM path")
            
            # Step 4: We need to create the LLM file and add it to Firestore
            logging.info("Creating local LLM file...")
            llm_result = create_local_llm_file(ticker, filing_type, fiscal_year, fiscal_period)
            
            if llm_result.get("success", False) and "llm_path" in llm_result:
                local_llm_path = llm_result["llm_path"]
                logging.info(f"Created local LLM file: {local_llm_path}")
                
                # Step 5: Determine GCS path
                bucket_name = "native-llm-filings"
                if filing_type == "10-K":
                    gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/llm.txt"
                elif fiscal_period:
                    gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_period}/llm.txt"
                else:
                    gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/llm.txt"
                
                # Step 6: Upload the file to GCS
                logging.info(f"Uploading to GCS at {gcs_path}...")
                upload_result = upload_to_gcs(local_llm_path, bucket_name, gcs_path)
                
                if upload_result.get("success", False):
                    logging.info("Upload successful")
                    
                    # Step 7: Update Firestore with LLM path and size
                    llm_size = os.path.getsize(local_llm_path)
                    update_result = update_firestore_with_llm_path(document_id, gcs_path, llm_size)
                    
                    if update_result.get("success", False):
                        logging.info("Firestore update successful")
                        return {
                            "success": True,
                            "action": "create_upload_update",
                            "message": "Created local LLM file, uploaded to GCS, and updated Firestore with LLM path"
                        }
                    else:
                        logging.error("Failed to update Firestore")
                        return {
                            "success": False,
                            "action": "create_upload",
                            "error": update_result.get("error", "Unknown error updating Firestore")
                        }
                else:
                    logging.error("Failed to upload to GCS")
                    return {
                        "success": False,
                        "action": "create_only",
                        "error": upload_result.get("error", "Unknown error uploading to GCS")
                    }
            else:
                logging.error("Failed to create local LLM file")
                return {
                    "success": False,
                    "action": "none",
                    "error": llm_result.get("error", "Unknown error creating LLM file")
                }
    else:
        # No Firestore entry
        logging.warning(f"No Firestore entry found for {document_id}")
        
        # Step 4: We need to create the LLM file and create Firestore entry from scratch
        logging.info("Running pipeline to create both local file and Firestore entry...")
        
        # Use the full pipeline with GCP upload enabled
        from src2.sec.pipeline import SECFilingPipeline
        
        # Initialize the pipeline with GCP
        pipeline = SECFilingPipeline(
            email="info@exascale.capital",
            output_dir=Path("sec_processed"),
            gcp_bucket="native-llm-filings"
        )
        
        # Run the appropriate pipeline method
        if filing_type == "10-K":
            result = pipeline.process_fiscal_year(
                ticker=ticker,
                fiscal_year=fiscal_year,
                filing_type=filing_type,
                force_download=True
            )
        elif filing_type == "10-Q" and fiscal_period:
            result = pipeline.process_fiscal_period(
                ticker=ticker,
                fiscal_year=fiscal_year,
                fiscal_period=fiscal_period,
                filing_type=filing_type,
                force_download=True
            )
        else:
            return {
                "success": False,
                "action": "none",
                "error": "Invalid filing type or missing fiscal period for 10-Q"
            }
        
        # Check if pipeline succeeded
        if result.get("success", False):
            logging.info("Pipeline completed successfully")
            
            # Check upload stage
            if "stages" in result and "upload" in result["stages"]:
                upload_stage = result["stages"]["upload"]
                if upload_stage.get("success", False):
                    logging.info("Upload stage completed successfully")
                    
                    # Double-check Firestore and GCS
                    firestore_result = check_firestore_entry(ticker, filing_type, fiscal_year, fiscal_period)
                    if firestore_result.get("exists", False):
                        logging.info("Firestore entry created successfully")
                        
                        # Check LLM path
                        firestore_data = firestore_result.get("data", {})
                        if "llm_file_path" in firestore_data and firestore_data["llm_file_path"]:
                            gcs_path = firestore_data["llm_file_path"]
                            
                            # Check GCS file
                            gcs_result = check_gcs_file("native-llm-filings", gcs_path)
                            if gcs_result.get("exists", False):
                                logging.info(f"GCS file created successfully at {gcs_path}")
                                return {
                                    "success": True,
                                    "action": "full_pipeline",
                                    "message": "Created local LLM file, uploaded to GCS, and created Firestore entry"
                                }
                            else:
                                logging.warning("Firestore entry created but GCS file is missing")
                                return {
                                    "success": False,
                                    "action": "partial_pipeline",
                                    "error": "GCS file upload failed"
                                }
                        else:
                            logging.warning("Firestore entry created but has no LLM path")
                            return {
                                "success": False,
                                "action": "partial_pipeline",
                                "error": "Firestore entry missing LLM path"
                            }
                    else:
                        logging.error("Failed to create Firestore entry")
                        return {
                            "success": False,
                            "action": "partial_pipeline",
                            "error": "Firestore entry creation failed"
                        }
                else:
                    logging.error("Upload stage failed")
                    return {
                        "success": False,
                        "action": "partial_pipeline",
                        "error": "Upload stage failed",
                        "pipeline_result": result
                    }
            else:
                logging.error("Upload stage not found in pipeline result")
                return {
                    "success": False,
                    "action": "partial_pipeline",
                    "error": "Upload stage not found in pipeline result",
                    "pipeline_result": result
                }
        else:
            logging.error("Pipeline failed")
            return {
                "success": False,
                "action": "none",
                "error": "Pipeline failed",
                "pipeline_result": result
            }

def main():
    """Main function"""
    if not setup_gcp_credentials():
        return 1
    
    parser = argparse.ArgumentParser(description='Fix pipeline uploads')
    parser.add_argument('--ticker', type=str, required=True, help='Ticker symbol (e.g., TSLA)')
    parser.add_argument('--filing-type', type=str, required=True, choices=['10-K', '10-Q'], help='Filing type')
    parser.add_argument('--fiscal-year', type=str, required=True, help='Fiscal year')
    parser.add_argument('--fiscal-period', type=str, help='Fiscal period (required for 10-Q)')
    
    args = parser.parse_args()
    
    if args.filing_type == '10-Q' and not args.fiscal_period:
        logging.error("Fiscal period is required for 10-Q filings")
        return 1
    
    # Fix upload issues
    result = fix_filing_upload(
        args.ticker,
        args.filing_type,
        args.fiscal_year,
        args.fiscal_period
    )
    
    if result.get("success", False):
        logging.info(f"SUCCESS: {result.get('message', 'Upload fixed successfully')}")
        return 0
    else:
        logging.error(f"FAILURE: {result.get('error', 'Unknown error')}")
        return 1

if __name__ == "__main__":
    main()