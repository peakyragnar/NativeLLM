#!/usr/bin/env python3
"""
Create Firestore Database

This script creates a Firestore database if it doesn't exist.
"""

import os
import sys
import logging
import argparse
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_firestore_database(project_id=None, database_id="default", location="us-central1"):
    """
    Create a Firestore database if it doesn't exist.
    
    Args:
        project_id: GCP project ID (if None, uses default from environment)
        database_id: Firestore database ID (default: "default")
        location: GCP location (default: "us-central1")
        
    Returns:
        True if database exists or was created, False otherwise
    """
    try:
        # Import the Google Cloud libraries
        from google.cloud import firestore
        from google.cloud import firestore_admin_v1
        
        # Create a Firestore Admin client
        client = firestore_admin_v1.FirestoreAdminClient()
        
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
        
        # Check if the database already exists
        try:
            # Try to connect to the database
            db = firestore.Client(project=project_id, database=database_id)
            # Try a simple operation to verify connection
            collections = list(db.collections())
            logging.info(f"Database '{database_id}' already exists in project '{project_id}'")
            logging.info(f"Found {len(collections)} collections")
            return True
        except Exception as e:
            if "database does not exist" in str(e).lower():
                logging.info(f"Database '{database_id}' does not exist in project '{project_id}'. Creating...")
            else:
                logging.warning(f"Error checking database existence: {str(e)}")
        
        # Format the parent resource
        parent = f"projects/{project_id}/databases"
        
        # Create the database
        operation = client.create_database(
            request={
                "parent": parent,
                "database_id": database_id,
                "type_": firestore_admin_v1.Database.DatabaseType.FIRESTORE_NATIVE,
                "location_id": location
            }
        )
        
        logging.info(f"Creating database '{database_id}' in project '{project_id}'...")
        
        # Wait for the operation to complete
        database = operation.result(timeout=60)
        
        logging.info(f"Database created: {database.name}")
        
        # Verify the database was created
        try:
            # Try to connect to the database
            db = firestore.Client(project=project_id, database=database_id)
            # Try a simple operation to verify connection
            collections = list(db.collections())
            logging.info(f"Successfully connected to database '{database_id}'")
            logging.info(f"Found {len(collections)} collections")
            return True
        except Exception as e:
            logging.error(f"Failed to connect to created database: {str(e)}")
            return False
    
    except ImportError as e:
        logging.error(f"Google Cloud libraries not installed: {str(e)}")
        print("\nTo install Google Cloud libraries, run:")
        print("pip install google-cloud-firestore google-cloud-firestore-admin")
        return False
    except Exception as e:
        logging.error(f"Failed to create Firestore database: {str(e)}")
        return False

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="Create Firestore database")
    parser.add_argument("--project-id", help="GCP project ID")
    parser.add_argument("--database-id", default="default", help="Firestore database ID")
    parser.add_argument("--location", default="us-central1", help="GCP location")
    
    args = parser.parse_args()
    
    # Create the database
    success = create_firestore_database(
        project_id=args.project_id,
        database_id=args.database_id,
        location=args.location
    )
    
    # Exit with appropriate status code
    if success:
        sys.exit(0)
    else:
        sys.exit(1)
