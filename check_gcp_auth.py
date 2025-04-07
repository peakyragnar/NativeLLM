#!/usr/bin/env python3
"""
Check GCP Authentication

This script checks if GCP authentication is working correctly.
"""

import os
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_gcp_auth():
    """
    Check if GCP authentication is working correctly.
    
    Returns:
        True if authentication is working, False otherwise
    """
    try:
        # Try to import the Google Cloud libraries
        from google.cloud import storage, firestore
        
        # Try to create a client to test authentication
        storage_client = storage.Client()
        
        # Try to list buckets to verify authentication
        buckets = list(storage_client.list_buckets(max_results=1))
        
        logging.info(f"Successfully authenticated with Google Cloud")
        logging.info(f"Found {len(buckets)} buckets")
        
        return True
    except ImportError as e:
        logging.error(f"Google Cloud libraries not installed: {str(e)}")
        print("\nTo install Google Cloud libraries, run:")
        print("pip install google-cloud-storage google-cloud-firestore")
        return False
    except Exception as e:
        logging.error(f"Failed to authenticate with Google Cloud: {str(e)}")
        print("\nTo authenticate with Google Cloud, you can:")
        print("1. Run 'gcloud auth application-default login' to authenticate via web browser")
        print("2. Or set GOOGLE_APPLICATION_CREDENTIALS environment variable to a service account key file")
        print("\nFor more information, visit: https://cloud.google.com/docs/authentication/getting-started")
        return False

def check_specific_bucket(bucket_name):
    """
    Check if a specific bucket exists and is accessible.
    
    Args:
        bucket_name: Name of the bucket to check
        
    Returns:
        True if bucket exists and is accessible, False otherwise
    """
    try:
        # Try to import the Google Cloud libraries
        from google.cloud import storage
        
        # Try to create a client to test authentication
        storage_client = storage.Client()
        
        # Try to access the bucket
        bucket = storage_client.bucket(bucket_name)
        exists = bucket.exists()
        
        if exists:
            logging.info(f"Bucket {bucket_name} exists and is accessible")
            return True
        else:
            logging.error(f"Bucket {bucket_name} does not exist or is not accessible")
            return False
    except Exception as e:
        logging.error(f"Failed to check bucket {bucket_name}: {str(e)}")
        return False

def check_firestore():
    """
    Check if Firestore is working correctly.
    
    Returns:
        True if Firestore is working, False otherwise
    """
    try:
        # Try to import the Google Cloud libraries
        from google.cloud import firestore
        
        # Try to create a client to test authentication
        firestore_client = firestore.Client()
        
        # Try to list collections to verify authentication
        collections = firestore_client.collections()
        collection_list = list(collections)
        
        logging.info(f"Successfully authenticated with Firestore")
        logging.info(f"Found {len(collection_list)} collections")
        
        return True
    except ImportError as e:
        logging.error(f"Google Cloud Firestore library not installed: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Failed to authenticate with Firestore: {str(e)}")
        return False

def check_firestore_database(database_name):
    """
    Check if a specific Firestore database exists and is accessible.
    
    Args:
        database_name: Name of the database to check
        
    Returns:
        True if database exists and is accessible, False otherwise
    """
    try:
        # Try to import the Google Cloud libraries
        from google.cloud import firestore
        
        # Try to create a client to test authentication
        firestore_client = firestore.Client(database=database_name)
        
        # Try to list collections to verify authentication
        collections = firestore_client.collections()
        collection_list = list(collections)
        
        logging.info(f"Successfully authenticated with Firestore database {database_name}")
        logging.info(f"Found {len(collection_list)} collections")
        
        return True
    except Exception as e:
        logging.error(f"Failed to check Firestore database {database_name}: {str(e)}")
        return False

if __name__ == "__main__":
    # Parse arguments
    import argparse
    parser = argparse.ArgumentParser(description="Check GCP authentication")
    parser.add_argument("--bucket", help="Check if a specific bucket exists and is accessible")
    parser.add_argument("--firestore", help="Check if a specific Firestore database exists and is accessible")
    
    args = parser.parse_args()
    
    # Check GCP authentication
    auth_ok = check_gcp_auth()
    
    # Check specific bucket if requested
    if args.bucket and auth_ok:
        bucket_ok = check_specific_bucket(args.bucket)
    
    # Check specific Firestore database if requested
    if args.firestore and auth_ok:
        firestore_ok = check_firestore_database(args.firestore)
    
    # Check Firestore if authentication is working
    if auth_ok:
        firestore_ok = check_firestore()
    
    # Exit with appropriate status code
    if auth_ok:
        sys.exit(0)
    else:
        sys.exit(1)
