#!/usr/bin/env python3

from src2.storage.gcp_storage import GCPStorage

def main():
    storage = GCPStorage("native-llm-filings")
    
    print("Listing GCP directories for NVDA 10-K:")
    prefix = "companies/NVDA/10-K/"
    blobs = list(storage.bucket.list_blobs(prefix=prefix))
    
    print(f"Found {len(blobs)} blobs:")
    for blob in blobs:
        print(f"  {blob.name}")

if __name__ == "__main__":
    main()