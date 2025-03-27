#!/usr/bin/env python3
"""
SEC Filing Processor

Command-line utility for downloading, rendering, and extracting SEC filings
with proper handling of iXBRL documents.
"""

import os
import sys
import logging
import argparse
import time
from pathlib import Path

# Import SEC processing modules
from src2.sec.pipeline import SECFilingPipeline

def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("sec_processing.log"),
            logging.StreamHandler()
        ]
    )
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Process SEC filings")
    
    # Company identification
    company_group = parser.add_argument_group('Company Identification')
    company_group.add_argument("ticker", help="Ticker symbol of the company")
    company_group.add_argument("--cik", help="CIK number (optional)")
    
    # Filing options
    filing_group = parser.add_argument_group('Filing Options')
    filing_group.add_argument("--filing-type", default="10-K", 
                         choices=["10-K", "10-Q", "8-K", "S-1", "20-F"],
                         help="Filing type to process")
    filing_group.add_argument("--filing-index", type=int, default=0, 
                         help="Index of filing to retrieve (0 for most recent)")
    
    # Output options
    output_group = parser.add_argument_group('Output Options')
    output_group.add_argument("--output-dir", default="./sec_processed",
                         help="Output directory for processed files")
    output_group.add_argument("--save-intermediate", action="store_true", 
                         help="Save intermediate files")
    
    # SEC access options
    sec_group = parser.add_argument_group('SEC Access')
    sec_group.add_argument("--email", default="user@example.com",
                      help="Contact email for SEC identification (required)")
    sec_group.add_argument("--user-agent", default="NativeLLM_SECProcessor/1.0",
                      help="User agent string for SEC requests")
    sec_group.add_argument("--rate-limit", type=int, default=5,
                      help="Maximum requests per second to SEC (max 10)")
    
    # GCP options
    gcp_group = parser.add_argument_group('Google Cloud')
    gcp_group.add_argument("--gcp-bucket", 
                      help="GCS bucket name for upload (if not specified, skips upload)")
    gcp_group.add_argument("--gcp-project",
                      help="GCP project ID for upload")
    gcp_group.add_argument("--upload-only", action="store_true",
                      help="Only upload already processed files, don't download/process")
    
    # Parse args
    args = parser.parse_args()
    
    # Upload-only mode
    if args.upload_only:
        if not args.gcp_bucket:
            print("Error: --gcp-bucket is required with --upload-only")
            return 1
        
        print(f"\n⏳ Uploading processed files for {args.ticker} to GCP...")
        
        # Import and run the uploader
        try:
            from sec_upload import upload_sec_directory
            
            # Build the directory path
            directory = os.path.join(args.output_dir, args.ticker)
            if not os.path.exists(directory):
                print(f"Error: No processed files directory found for {args.ticker}")
                return 1
            
            # Upload files
            result = upload_sec_directory(directory, args.gcp_bucket, args.gcp_project)
            print(f"Uploaded {result} files for {args.ticker}")
            return 0 if result > 0 else 1
            
        except ImportError:
            print("Error: Could not import sec_upload module. Make sure it exists in the current directory.")
            return 1
        except Exception as e:
            print(f"Error uploading files: {str(e)}")
            return 1
    
    # Set up GCP credentials if needed
    credentials_path = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"
    if os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        print(f"Using GCP credentials from: {credentials_path}")
        
    # Ensure GCP libraries are installed if we're using GCP
    if args.gcp_bucket:
        try:
            import google.cloud.storage
            import google.cloud.firestore
            print("GCP libraries verified")
        except ImportError:
            print("Warning: Google Cloud libraries not installed. GCP upload may fail.")
            print("Install required packages with: pip install google-cloud-storage google-cloud-firestore")

    # Create pipeline for full processing
    pipeline = SECFilingPipeline(
        user_agent=args.user_agent,
        contact_email=args.email,
        output_dir=args.output_dir,
        gcp_bucket=args.gcp_bucket,
        gcp_project=args.gcp_project
    )
    
    # Process filing
    try:
        print(f"\n⏳ Processing {args.filing_type} for {args.ticker}...")
        
        result = pipeline.process_filing(
            ticker=args.ticker,
            cik=args.cik,
            filing_type=args.filing_type,
            filing_index=args.filing_index,
            save_intermediate=args.save_intermediate
        )
        
        # Print results
        if result.get("success", False):
            print(f"\n✅ Processing complete!")
            print(f"Text output: {result.get('text_path')}")
            if result.get('llm_path'):
                print(f"LLM output: {result.get('llm_path')}")
            print(f"Total processing time: {result.get('total_time_seconds', 0):.2f} seconds")
            
            # Print stage information
            if "stages" in result:
                print("\nStage details:")
                
                for stage, info in result["stages"].items():
                    status = "✅ Success" if info.get("success", False) else "❌ Failed"
                    print(f"  {stage.title()}: {status} ({info.get('time_seconds', 0):.2f}s)")
                    
                    # Print stage-specific details
                    if stage == "download" and "result" in info:
                        doc_path = info["result"].get("doc_path")
                        if doc_path:
                            size = os.path.getsize(doc_path) / (1024 * 1024)
                            print(f"    Downloaded document: {os.path.basename(doc_path)} ({size:.2f} MB)")
                    
                    elif stage == "extract" and "result" in info:
                        word_count = info["result"].get("word_count", 0)
                        file_size = info["result"].get("file_size", 0) / (1024 * 1024)
                        print(f"    Extracted text: {word_count} words ({file_size:.2f} MB)")
                        
                    elif stage == "llm_format" and "result" in info:
                        if info.get("success", False):
                            file_size = info["result"].get("file_size", 0) / (1024 * 1024)
                            print(f"    Generated LLM format: {file_size:.2f} MB")
                        else:
                            print(f"    LLM format generation: {status} - {info['result'].get('error', '')}")
                        
                    elif stage == "upload" and "result" in info:
                        if info.get("success", False):
                            # Check for text upload
                            text_result = info["result"].get("text_upload", {})
                            if text_result and text_result.get("success", False):
                                print(f"    Uploaded text to GCS: {text_result.get('gcs_path')}")
                            
                            # Check for LLM upload
                            llm_result = info["result"].get("llm_upload", {})
                            if llm_result and llm_result.get("success", False):
                                print(f"    Uploaded LLM to GCS: {llm_result.get('gcs_path')}")
                            
                            # Check metadata
                            metadata_result = info["result"].get("metadata", {})
                            if metadata_result and metadata_result.get("success", False):
                                print(f"    Added metadata to Firestore: {metadata_result.get('document_id')}")
                        else:
                            print(f"    Upload skipped or failed: {info['result'].get('text_upload', {}).get('error', 'GCP not configured')}")
            
            # Print warning if any
            if "warning" in result:
                print(f"\n⚠️ Warning: {result['warning']}")
        else:
            print(f"\n❌ Processing failed: {result.get('error', 'Unknown error')}")
            
            # Print the stage that failed
            if "stages" in result:
                for stage, info in result["stages"].items():
                    if not info.get("success", False):
                        print(f"  Failed at {stage} stage: {info.get('result', {}).get('error', 'Unknown error')}")
        
        # Clean up
        pipeline.cleanup()
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())