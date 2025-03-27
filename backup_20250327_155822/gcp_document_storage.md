# GCP Document Storage Integration

This document outlines how the NativeLLM system integrates with Google Cloud Platform (GCP) for document storage and metadata management.

## Architecture Overview

The NativeLLM system uses two primary GCP services:

1. **Google Cloud Storage (GCS)** - For storing the actual document files
2. **Google Firestore** - For storing and querying document metadata

## Storage Structure

### Google Cloud Storage

Documents are stored in GCS using the following path structure:

```
companies/{ticker}/{filing_type}/{fiscal_year}/{quarter_folder}/{format}.txt
```

Where:
- `ticker` - Company stock ticker (e.g., "AAPL")
- `filing_type` - SEC filing type (e.g., "10-K", "10-Q")
- `fiscal_year` - The fiscal year of the filing (e.g., "2024")
- `quarter_folder` - Either "annual" for 10-K or "Q1", "Q2", "Q3", "Q4" for quarterly filings
- `format` - Either "text" (plain text) or "llm" (LLM-optimized format)

Examples:
- `companies/AAPL/10-K/2024/annual/llm.txt`
- `companies/GOOGL/10-Q/2024/Q2/text.txt`

### Firestore Database

The Firestore database ("nativellm") contains two main collections:

1. **companies** - Basic information about each company
   - Document ID: Ticker symbol (e.g., "AAPL")
   - Fields:
     - `ticker`: String
     - `name`: String
     - `cik`: String (optional)
     - `sector`: String (optional)
     - `industry`: String (optional)
     - `last_updated`: Timestamp

2. **filings** - Metadata about each filing
   - Document ID: `{ticker}-{filing_type}-{fiscal_year}-{fiscal_period}` (e.g., "AAPL-10-K-2024-annual")
   - Fields:
     - `filing_id`: String
     - `company_ticker`: String
     - `company_name`: String
     - `filing_type`: String
     - `fiscal_year`: String
     - `fiscal_period`: String
     - `period_end_date`: String
     - `filing_date`: String
     - `text_file_path`: String
     - `llm_file_path`: String
     - `text_file_size`: Number
     - `llm_file_size`: Number
     - `storage_class`: String
     - `last_accessed`: Timestamp
     - `access_count`: Number

## Integration Points

### gcp_upload.py

This utility provides functions to:
1. Upload processed files to Google Cloud Storage
2. Update metadata in Firestore
3. Verify consistency between GCS and Firestore

Usage:
```
python gcp_upload.py --file PATH_TO_FILE     # Upload a single file
python gcp_upload.py --company TICKER        # Upload all files for a company
python gcp_upload.py --all                   # Upload all files for all companies
python gcp_upload.py --company TICKER --verify  # Upload and verify consistency
```

### Integration with Processing Pipeline

The main `run_pipeline.py` script has been enhanced with GCP upload capabilities:

```
python run_pipeline.py --company AAPL --process-and-upload  # Process and upload
python run_pipeline.py --upload-company AAPL               # Upload existing files
python run_pipeline.py --upload-gcp                        # Upload all files
```

## Consistency Validation

The `test_gcs_firestore_consistency.py` script verifies that:
1. GCS paths follow the correct format
2. File paths in Firestore metadata match actual GCS paths
3. Files exist at the expected locations

If issues are found, it can optionally fix them by:
- Copying files to the correct locations in GCS
- Updating paths in Firestore metadata

## Authentication

Authentication to GCP services uses a service account key file:
- Path: `/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json`
- Environment variable: `GOOGLE_APPLICATION_CREDENTIALS`

## Best Practices

1. **Processing Pipeline**: Process XBRL files locally first, then upload to GCP
2. **Error Handling**: The GCP upload module includes robust error handling and logging
3. **Metadata Extraction**: File naming conventions enable automatic metadata extraction
4. **Consistency Checking**: Run consistency verification regularly to ensure data integrity
5. **Security**: Keep service account credentials secure and use least-privilege permissions

## Troubleshooting

1. **Permission Errors**: Verify service account credentials are correctly set
2. **Path Issues**: Check that file paths follow the required structure
3. **Consistency Issues**: Run verification with `--verify` flag to check and fix inconsistencies