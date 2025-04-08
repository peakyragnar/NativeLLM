#!/usr/bin/env python3
"""
Test Firestore Connection

This script tests the connection to Firestore and creates a test document.
"""

import os
import sys
import logging
import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_firestore_connection(project_id=None, database_id="nativellm"):
    """
    Test the connection to Firestore and create a test document.
    
    Args:
        project_id: GCP project ID (if None, uses default from environment)
        database_id: Firestore database ID (default: "nativellm")
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Import the Google Cloud libraries
        from google.cloud import firestore
        
        # Get the project ID if not provided
        if not project_id:
            # Try to get from environment
            project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
            if not project_id:
                # Try to get from gcloud config
                import subprocess
                try:
                    project_id = subprocess.check_output(
                        ["gcloud", "config", "get-value", "project"],
                        universal_newlines=True
                    ).strip()
                except Exception as e:
                    logging.error(f"Failed to get project ID from gcloud: {str(e)}")
        
        if not project_id:
            logging.error("No project ID provided and couldn't determine from environment")
            return False
        
        logging.info(f"Using project ID: {project_id}")
        logging.info(f"Using database ID: {database_id}")
        
        # Create a client
        db = firestore.Client(project=project_id, database=database_id)
        
        # Create a test document
        doc_ref = db.collection('test').document(f'test_doc_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}')
        
        # Create document data
        doc_data = {
            'test_field': 'test_value',
            'timestamp': firestore.SERVER_TIMESTAMP,
            'created_at': datetime.datetime.now()
        }
        
        # Set the document
        doc_ref.set(doc_data)
        
        logging.info(f"Test document created successfully: {doc_ref.id}")
        
        # Verify it was created
        doc = doc_ref.get()
        if doc.exists:
            logging.info(f"Document exists: {doc.to_dict()}")
            return True
        else:
            logging.error("Document does not exist after creation")
            return False
    
    except ImportError as e:
        logging.error(f"Google Cloud libraries not installed: {str(e)}")
        print("\nTo install Google Cloud libraries, run:")
        print("pip install google-cloud-firestore")
        return False
    except Exception as e:
        logging.error(f"Failed to test Firestore connection: {str(e)}")
        return False

if __name__ == "__main__":
    import argparse
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Test Firestore connection")
    parser.add_argument("--project-id", help="GCP project ID")
    parser.add_argument("--database-id", default="nativellm", help="Firestore database ID")
    
    args = parser.parse_args()
    
    # Test the connection
    success = test_firestore_connection(
        project_id=args.project_id,
        database_id=args.database_id
    )
    
    # Exit with appropriate status code
    if success:
        sys.exit(0)
    else:
        sys.exit(1)
