import os
import sys
import logging
import datetime
from google.cloud import storage, firestore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fix_msft_fiscal_periods.log'),
        logging.StreamHandler()
    ]
)

# Set the path to service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def find_msft_q4_entries():
    """
    Find all Microsoft entries in Firestore with Q4 fiscal period
    """
    db = firestore.Client(database='nativellm')
    
    # Query for all Microsoft filings with Q4 fiscal period
    query = db.collection('filings').where('company_ticker', '==', 'MSFT').where('fiscal_period', '==', 'Q4')
    
    docs = query.stream()
    q4_entries = []
    
    for doc in docs:
        data = doc.to_dict()
        q4_entries.append({
            'filing_id': doc.id,
            'fiscal_year': data.get('fiscal_year'),
            'filing_type': data.get('filing_type'),
            'period_end_date': data.get('period_end_date'),
            'text_file_path': data.get('text_file_path'),
            'llm_file_path': data.get('llm_file_path')
        })
    
    logging.info(f"Found {len(q4_entries)} Microsoft Q4 entries in Firestore")
    return q4_entries

def delete_gcs_file(file_path, bucket_name="native-llm-filings"):
    """
    Delete a file from Google Cloud Storage
    """
    if not file_path:
        return False
    
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Create blob and delete
        blob = bucket.blob(file_path)
        
        # Check if blob exists
        if blob.exists():
            blob.delete()
            logging.info(f"Deleted file from GCS: gs://{bucket_name}/{file_path}")
            return True
        else:
            logging.warning(f"File does not exist in GCS: gs://{bucket_name}/{file_path}")
            return False
    except Exception as e:
        logging.error(f"Error deleting file from GCS: {str(e)}")
        return False

def delete_firestore_entry(filing_id):
    """
    Delete an entry from Firestore
    """
    try:
        db = firestore.Client(database='nativellm')
        db.collection('filings').document(filing_id).delete()
        logging.info(f"Deleted Firestore entry: {filing_id}")
        return True
    except Exception as e:
        logging.error(f"Error deleting Firestore entry: {str(e)}")
        return False

def fix_msft_fiscal_periods():
    """
    Main function to fix Microsoft fiscal periods:
    1. Find all Microsoft entries in Firestore with Q4 fiscal period
    2. Delete them permanently from both Firestore and GCS
    """
    logging.info("Starting Microsoft fiscal period fix")
    
    # Find all Microsoft Q4 entries
    q4_entries = find_msft_q4_entries()
    
    if not q4_entries:
        logging.info("No Microsoft Q4 entries found. Nothing to fix.")
        return
    
    # Track successful and failed operations
    successful_deletes = 0
    failed_deletes = 0
    
    # Delete each Q4 entry
    for entry in q4_entries:
        filing_id = entry.get('filing_id')
        logging.info(f"Processing entry: {filing_id}")
        
        # Delete both text and LLM files from GCS
        text_deleted = delete_gcs_file(entry.get('text_file_path'))
        llm_deleted = delete_gcs_file(entry.get('llm_file_path'))
        
        # Delete entry from Firestore
        firestore_deleted = delete_firestore_entry(filing_id)
        
        if text_deleted and llm_deleted and firestore_deleted:
            successful_deletes += 1
        else:
            failed_deletes += 1
            logging.warning(f"Incomplete deletion for filing: {filing_id}")
    
    logging.info(f"Fix complete:")
    logging.info(f"  - Found {len(q4_entries)} Microsoft Q4 entries")
    logging.info(f"  - Successfully deleted {successful_deletes} entries")
    logging.info(f"  - Failed to completely delete {failed_deletes} entries")
    
    # Verify fix was successful
    remaining_q4 = find_msft_q4_entries()
    if remaining_q4:
        logging.warning(f"Fix incomplete! {len(remaining_q4)} Q4 entries still remain.")
    else:
        logging.info("Fix successful! No Q4 entries remain for Microsoft.")

if __name__ == "__main__":
    fix_msft_fiscal_periods()