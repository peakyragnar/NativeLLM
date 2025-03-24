from google.cloud import storage
import os

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def upload_filing_to_gcs(local_file_path, bucket_name="native-llm-filings"):
    """
    Upload a filing from local storage to Google Cloud Storage with proper folder structure
    
    Args:
        local_file_path: Path to local file
        bucket_name: GCS bucket name
    """
    # Extract file components from file name
    # Format: Apple_Inc_2024_FY_AAPL_10-K_20240928_llm.txt
    file_name = os.path.basename(local_file_path)
    file_parts = file_name.split('_')
    
    # Extract relevant info
    ticker = file_parts[4]  # AAPL
    filing_type = file_parts[5]  # 10-K
    fiscal_year = file_parts[2]  # 2024
    period_info = file_parts[3]  # FY or Q1, Q2, etc.
    file_format = file_parts[-1].split('.')[0]  # llm
    
    # Determine quarter folder
    if period_info == "FY":
        quarter_folder = "annual"  # For 10-K
    else:
        # Convert 1Q, 2Q, 3Q, 4Q to Q1, Q2, Q3, Q4
        quarter_number = period_info[0]
        quarter_folder = f"Q{quarter_number}"
    
    # Construct GCS path
    gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{quarter_folder}/{file_format}.txt"
    
    print(f"Attempting to upload to gs://{bucket_name}/{gcs_path}")
    
    try:
        # Initialize GCS client with explicit credentials
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Create blob and upload
        blob = bucket.blob(gcs_path)
        
        # Upload the file
        with open(local_file_path, 'rb') as f:
            blob.upload_from_file(f)
        
        print(f"Successfully uploaded {local_file_path} to gs://{bucket_name}/{gcs_path}")
        return gcs_path
    except Exception as e:
        print(f"Error uploading file: {str(e)}")
        return None

if __name__ == "__main__":
    # Test with an Apple 10-K filing
    test_file = "/Users/michael/NativeLLM/data/processed/AAPL/Apple_Inc_2024_FY_AAPL_10-K_20240928_llm.txt"
    
    # Verify credentials file exists
    creds_file = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    if not os.path.exists(creds_file):
        print(f"Credentials file not found: {creds_file}")
        exit(1)
    else:
        print(f"Using credentials from: {creds_file}")
    
    # Verify test file exists
    if os.path.exists(test_file):
        print(f"Test file found: {test_file}")
        gcs_path = upload_filing_to_gcs(test_file)
        if gcs_path:
            print(f"File uploaded to: gs://native-llm-filings/{gcs_path}")
    else:
        print(f"Test file {test_file} not found. Please update the path.")