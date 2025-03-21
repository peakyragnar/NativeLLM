# Financial Document Storage Implementation Guide

This guide outlines the complete implementation for storing and managing SEC filing documents in Google Cloud Platform, with a dual-storage architecture using Firestore for metadata and Google Cloud Storage (GCS) for document files.

## System Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌────────────────┐
│                 │     │                 │     │                │
│  Client/User    │────▶│  Cloud Run API  │────▶│    Firestore   │
│                 │     │                 │     │    Metadata    │
└─────────────────┘     └─────────────────┘     └────────────────┘
                                │                        │
                                ▼                        │
                         ┌─────────────────┐            │
                         │ Cloud Functions │◀───────────┘
                         └─────────────────┘
                                │
                                ▼
                         ┌─────────────────┐
                         │  Google Cloud   │
                         │     Storage     │
                         └─────────────────┘
```

## 1. Google Cloud Storage (GCS) Setup

### 1.1 Bucket Creation and Structure

```bash
# Create main storage bucket
gsutil mb -c standard -l us-central1 gs://financial-filings-data/

# Create folder structure (example for a few companies)
gsutil mkdir gs://financial-filings-data/companies/
gsutil mkdir gs://financial-filings-data/companies/AAPL/
gsutil mkdir gs://financial-filings-data/companies/AAPL/10-Q/
gsutil mkdir gs://financial-filings-data/companies/AAPL/10-Q/2024/
gsutil mkdir gs://financial-filings-data/companies/AAPL/10-Q/2024/Q1/
```

### 1.2 Lifecycle Policy Configuration

Create a `lifecycle.json` file:

```json
{
  "lifecycle": {
    "rule": [
      {
        "action": {
          "type": "SetStorageClass",
          "storageClass": "NEARLINE"
        },
        "condition": {
          "age": 365,
          "matchesPrefix": ["companies/"]
        }
      },
      {
        "action": {
          "type": "SetStorageClass",
          "storageClass": "COLDLINE"
        },
        "condition": {
          "age": 1095,
          "matchesPrefix": ["companies/"]
        }
      }
    ]
  }
}
```

Apply the lifecycle policy:

```bash
gsutil lifecycle set lifecycle.json gs://financial-filings-data/
```

### 1.3 Storage Functions (Python Example)

```python
from google.cloud import storage

def upload_document(company_ticker, filing_type, year, quarter, file_type, content):
    """
    Upload a document to Google Cloud Storage
    
    Args:
        company_ticker: Company ticker symbol (e.g., 'AAPL')
        filing_type: Type of filing (e.g., '10-Q', '10-K')
        year: Fiscal year of filing
        quarter: Fiscal quarter (or None for annual filings)
        file_type: 'text' or 'llm'
        content: Document content as string
    
    Returns:
        GCS path to the uploaded file
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket('financial-filings-data')
    
    # Build the path
    quarter_path = f"Q{quarter}" if quarter else "annual"
    blob_path = f"companies/{company_ticker}/{filing_type}/{year}/{quarter_path}/{file_type}.txt"
    
    # Create the blob and upload
    blob = bucket.blob(blob_path)
    blob.upload_from_string(content)
    
    return blob_path

def download_document(file_path):
    """
    Download a document from Google Cloud Storage
    
    Args:
        file_path: GCS path to the file
    
    Returns:
        Document content as string
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket('financial-filings-data')
    blob = bucket.blob(file_path)
    
    # Check if file exists
    if not blob.exists():
        raise FileNotFoundError(f"File {file_path} not found")
    
    # Pre-warming for older files
    storage_class = blob.storage_class
    if storage_class in ['NEARLINE', 'COLDLINE']:
        print(f"Retrieving file from {storage_class} storage - this may take a moment")
    
    return blob.download_as_text()
```

## 2. Firestore Metadata Database Setup

### 2.1 Database and Collection Structure

Set up the Firestore database:

```python
from google.cloud import firestore

def setup_firestore_collections():
    """Set up initial Firestore collections"""
    db = firestore.Client()
    
    # Create companies collection if it doesn't exist
    companies_ref = db.collection('companies')
    # Create filings collection if it doesn't exist
    filings_ref = db.collection('filings')
    
    return True
```

### 2.2 Document Schema

**Companies Collection:**

```json
{
  "ticker": "AAPL",
  "name": "Apple Inc.",
  "cik": "0000320193",
  "sector": "Technology",
  "industry": "Consumer Electronics",
  "last_updated": "2025-03-21T15:30:00Z"
}
```

**Filings Collection:**

```json
{
  "filing_id": "AAPL-10Q-2024-Q1",
  "company_ticker": "AAPL",
  "filing_type": "10-Q",
  "fiscal_year": 2024,
  "fiscal_period": "Q1",
  "period_end_date": "2024-12-28",
  "filing_date": "2025-01-31",
  "text_file_path": "companies/AAPL/10-Q/2024/Q1/text.txt",
  "llm_file_path": "companies/AAPL/10-Q/2024/Q1/llm.txt",
  "text_file_size": 1250000,
  "llm_file_size": 1620000,
  "storage_class": "STANDARD",
  "last_accessed": "2025-03-21T14:22:10Z",
  "access_count": 42,
  "key_metrics": {
    "revenue": 124300000000,
    "net_income": 36330000000,
    "eps": 2.40,
    "assets": 344085000000,
    "liabilities": 277327000000,
    "equity": 66758000000
  }
}
```

### 2.3 Metadata Functions (Python Example)

```python
from google.cloud import firestore
from datetime import datetime
import uuid

def add_company(ticker, name, cik, sector, industry):
    """Add a new company to the database"""
    db = firestore.Client()
    company_ref = db.collection('companies').document(ticker)
    
    company_ref.set({
        'ticker': ticker,
        'name': name,
        'cik': cik,
        'sector': sector,
        'industry': industry,
        'last_updated': datetime.now()
    })
    
    return ticker

def add_filing_metadata(company_ticker, filing_type, fiscal_year, fiscal_period, 
                        period_end_date, filing_date, text_path, llm_path,
                        text_size, llm_size, key_metrics=None):
    """Add metadata for a new filing"""
    db = firestore.Client()
    
    # Create a unique filing ID
    filing_id = f"{company_ticker}-{filing_type}-{fiscal_year}-{fiscal_period}"
    
    # Add to filings collection
    filing_ref = db.collection('filings').document(filing_id)
    
    filing_data = {
        'filing_id': filing_id,
        'company_ticker': company_ticker,
        'filing_type': filing_type,
        'fiscal_year': fiscal_year,
        'fiscal_period': fiscal_period,
        'period_end_date': period_end_date,
        'filing_date': filing_date,
        'text_file_path': text_path,
        'llm_file_path': llm_path,
        'text_file_size': text_size,
        'llm_file_size': llm_size,
        'storage_class': 'STANDARD',
        'last_accessed': datetime.now(),
        'access_count': 0
    }
    
    # Add key metrics if provided
    if key_metrics:
        filing_data['key_metrics'] = key_metrics
    
    filing_ref.set(filing_data)
    
    return filing_id

def get_filing_metadata(filing_id):
    """Get metadata for a specific filing"""
    db = firestore.Client()
    filing_ref = db.collection('filings').document(filing_id)
    doc = filing_ref.get()
    
    if not doc.exists:
        return None
    
    # Update access metrics
    filing_ref.update({
        'last_accessed': datetime.now(),
        'access_count': firestore.Increment(1)
    })
    
    return doc.to_dict()

def search_filings(company_ticker=None, filing_type=None, year=None, period=None):
    """Search for filings based on criteria"""
    db = firestore.Client()
    query = db.collection('filings')
    
    if company_ticker:
        query = query.where('company_ticker', '==', company_ticker)
    
    if filing_type:
        query = query.where('filing_type', '==', filing_type)
    
    if year:
        query = query.where('fiscal_year', '==', year)
    
    if period:
        query = query.where('fiscal_period', '==', period)
    
    results = query.stream()
    return [doc.to_dict() for doc in results]

def update_storage_class(filing_id, new_storage_class):
    """Update the storage class information in metadata"""
    db = firestore.Client()
    filing_ref = db.collection('filings').document(filing_id)
    
    filing_ref.update({
        'storage_class': new_storage_class
    })
    
    return True
```

## 3. Cloud Functions for Automation

### 3.1 New Filing Ingestion Function

```python
import functions_framework
from google.cloud import storage, firestore
import requests
import datetime
import json
import re
import os

@functions_framework.http
def ingest_new_filing(request):
    """
    Triggered by HTTP request to ingest a new SEC filing
    
    Request should contain:
    - company_ticker: Company ticker symbol
    - filing_type: Type of filing (10-Q, 10-K)
    - filing_date: Date of filing
    - source_url: URL to the SEC filing
    """
    request_json = request.get_json(silent=True)
    
    # Extract parameters
    company_ticker = request_json.get('company_ticker')
    filing_type = request_json.get('filing_type')
    filing_date = request_json.get('filing_date')
    source_url = request_json.get('source_url')
    
    # Download the filing from source_url
    response = requests.get(source_url)
    if response.status_code != 200:
        return f"Failed to download filing: {response.status_code}", 400
    
    filing_content = response.text
    
    # Process the filing content to create text and LLM versions
    text_content = process_for_text(filing_content)
    llm_content = process_for_llm(filing_content)
    
    # Extract fiscal information
    fiscal_info = extract_fiscal_info(filing_content)
    fiscal_year = fiscal_info['year']
    fiscal_period = fiscal_info['period']
    period_end_date = fiscal_info['end_date']
    
    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket('financial-filings-data')
    
    quarter_path = fiscal_period if fiscal_period.startswith('Q') else 'annual'
    base_path = f"companies/{company_ticker}/{filing_type}/{fiscal_year}/{quarter_path}"
    
    text_path = f"{base_path}/text.txt"
    llm_path = f"{base_path}/llm.txt"
    
    # Upload text version
    text_blob = bucket.blob(text_path)
    text_blob.upload_from_string(text_content)
    text_size = len(text_content)
    
    # Upload LLM version
    llm_blob = bucket.blob(llm_path)
    llm_blob.upload_from_string(llm_content)
    llm_size = len(llm_content)
    
    # Extract key financial metrics
    key_metrics = extract_key_metrics(filing_content)
    
    # Add metadata to Firestore
    db = firestore.Client()
    filing_id = f"{company_ticker}-{filing_type}-{fiscal_year}-{fiscal_period}"
    
    filing_ref = db.collection('filings').document(filing_id)
    filing_ref.set({
        'filing_id': filing_id,
        'company_ticker': company_ticker,
        'filing_type': filing_type,
        'fiscal_year': fiscal_year,
        'fiscal_period': fiscal_period,
        'period_end_date': period_end_date,
        'filing_date': filing_date,
        'text_file_path': text_path,
        'llm_file_path': llm_path,
        'text_file_size': text_size,
        'llm_file_size': llm_size,
        'storage_class': 'STANDARD',
        'last_accessed': datetime.datetime.now(),
        'access_count': 0,
        'key_metrics': key_metrics
    })
    
    return f"Successfully ingested {filing_id}", 200
```

### 3.2 Storage Class Management Function

```python
import functions_framework
from google.cloud import storage, firestore
import datetime

@functions_framework.cloud_event
def manage_storage_classes(cloud_event):
    """
    Cloud Function triggered by Cloud Scheduler to manage storage classes
    Updates metadata when GCS lifecycle policies move files to different storage tiers
    """
    db = firestore.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket('financial-filings-data')
    
    # Get all filings
    filings_ref = db.collection('filings')
    filings = filings_ref.stream()
    
    for filing in filings:
        filing_data = filing.to_dict()
        
        # Check text file
        text_path = filing_data.get('text_file_path')
        if text_path:
            text_blob = bucket.blob(text_path)
            if text_blob.exists():
                current_storage_class = text_blob.storage_class
                
                # Update metadata if storage class has changed
                if current_storage_class != filing_data.get('storage_class'):
                    filing.reference.update({
                        'storage_class': current_storage_class
                    })
    
    return "Storage class check completed", 200
```

### 3.3 Pre-warming Function

```python
import functions_framework
from google.cloud import storage, firestore
import tempfile
import os

@functions_framework.http
def prewarm_file(request):
    """
    Cloud Function to pre-warm files from colder storage tiers
    Triggered when user navigates to a company page
    
    Request should contain:
    - company_ticker: Company ticker to pre-warm
    """
    request_json = request.get_json(silent=True)
    company_ticker = request_json.get('company_ticker')
    
    if not company_ticker:
        return "Missing company_ticker parameter", 400
    
    # Get recent filings for this company
    db = firestore.Client()
    query = db.collection('filings')\
              .where('company_ticker', '==', company_ticker)\
              .order_by('filing_date', direction=firestore.Query.DESCENDING)\
              .limit(5)
    
    filings = query.stream()
    
    storage_client = storage.Client()
    bucket = storage_client.bucket('financial-filings-data')
    
    prewarmed_files = []
    
    for filing in filings:
        filing_data = filing.to_dict()
        
        # Check if file is in colder storage
        if filing_data.get('storage_class') in ['NEARLINE', 'COLDLINE']:
            # Get file paths
            text_path = filing_data.get('text_file_path')
            llm_path = filing_data.get('llm_file_path')
            
            # Pre-warm text file
            if text_path:
                text_blob = bucket.blob(text_path)
                if text_blob.exists():
                    # Read a small portion to trigger retrieval
                    _ = text_blob.download_as_bytes(start=0, end=1024)
                    prewarmed_files.append(text_path)
            
            # Pre-warm LLM file
            if llm_path:
                llm_blob = bucket.blob(llm_path)
                if llm_blob.exists():
                    # Read a small portion to trigger retrieval
                    _ = llm_blob.download_as_bytes(start=0, end=1024)
                    prewarmed_files.append(llm_path)
    
    return f"Pre-warmed {len(prewarmed_files)} files for {company_ticker}", 200
```

## 4. Cloud Run API Service

### 4.1 API Endpoints (FastAPI Example)

```python
from fastapi import FastAPI, HTTPException, Depends, Query
from google.cloud import firestore, storage
from typing import List, Optional
import datetime
import uvicorn

app = FastAPI(
    title="Financial Filings API",
    description="API for accessing SEC financial filings",
    version="1.0.0"
)

# Initialize clients
db = firestore.Client()
storage_client = storage.Client()
bucket = storage_client.bucket('financial-filings-data')

@app.get("/api/companies")
async def list_companies():
    """List all available companies"""
    companies_ref = db.collection('companies')
    companies = companies_ref.stream()
    
    return [company.to_dict() for company in companies]

@app.get("/api/companies/{ticker}")
async def get_company(ticker: str):
    """Get details for a specific company"""
    company_ref = db.collection('companies').document(ticker)
    company = company_ref.get()
    
    if not company.exists:
        raise HTTPException(status_code=404, detail=f"Company {ticker} not found")
    
    # Trigger pre-warming function in background (non-blocking)
    import requests
    import threading
    
    def trigger_prewarm():
        prewarm_url = "https://us-central1-your-project.cloudfunctions.net/prewarm_file"
        requests.post(prewarm_url, json={"company_ticker": ticker})
    
    threading.Thread(target=trigger_prewarm).start()
    
    return company.to_dict()

@app.get("/api/companies/{ticker}/filings")
async def list_company_filings(
    ticker: str,
    filing_type: Optional[str] = None,
    year: Optional[int] = None,
    period: Optional[str] = None
):
    """List all filings for a company with optional filters"""
    query = db.collection('filings').where('company_ticker', '==', ticker)
    
    if filing_type:
        query = query.where('filing_type', '==', filing_type)
    
    if year:
        query = query.where('fiscal_year', '==', year)
    
    if period:
        query = query.where('fiscal_period', '==', period)
    
    filings = query.stream()
    return [filing.to_dict() for filing in filings]

@app.get("/api/filings/{filing_id}")
async def get_filing_metadata(filing_id: str):
    """Get metadata for a specific filing"""
    filing_ref = db.collection('filings').document(filing_id)
    filing = filing_ref.get()
    
    if not filing.exists:
        raise HTTPException(status_code=404, detail=f"Filing {filing_id} not found")
    
    # Update access metrics
    filing_ref.update({
        'last_accessed': datetime.datetime.now(),
        'access_count': firestore.Increment(1)
    })
    
    return filing.to_dict()

@app.get("/api/filings/{filing_id}/download")
async def download_filing(filing_id: str, format: str = "text"):
    """
    Download a specific filing
    
    - format: 'text' or 'llm'
    """
    # Get metadata
    filing_ref = db.collection('filings').document(filing_id)
    filing = filing_ref.get()
    
    if not filing.exists:
        raise HTTPException(status_code=404, detail=f"Filing {filing_id} not found")
    
    filing_data = filing.to_dict()
    
    # Get file path based on format
    if format == "text":
        file_path = filing_data.get('text_file_path')
    elif format == "llm":
        file_path = filing_data.get('llm_file_path')
    else:
        raise HTTPException(status_code=400, detail=f"Invalid format: {format}")
    
    # Check if file path exists
    if not file_path:
        raise HTTPException(status_code=404, detail=f"No {format} file available for {filing_id}")
    
    # Get file from GCS
    blob = bucket.blob(file_path)
    
    if not blob.exists():
        raise HTTPException(status_code=404, detail=f"File {file_path} not found in storage")
    
    # Update access metrics
    filing_ref.update({
        'last_accessed': datetime.datetime.now(),
        'access_count': firestore.Increment(1)
    })
    
    # Return file content
    content = blob.download_as_text()
    return {"filing_id": filing_id, "format": format, "content": content}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
```

### 4.2 Deployment to Cloud Run

```bash
# Build the container
gcloud builds submit --tag gcr.io/your-project-id/financial-filings-api

# Deploy to Cloud Run
gcloud run deploy financial-filings-api \
  --image gcr.io/your-project-id/financial-filings-api \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

## 5. Data Ingestion Pipeline

### 5.1 Automated SEC Filing Scraper

Create a Cloud Scheduler job to trigger a Cloud Function that checks for new SEC filings:

```python
import functions_framework
import requests
from bs4 import BeautifulSoup
import re
from google.cloud import firestore, tasks_v2
import datetime
import json

@functions_framework.cloud_event
def check_for_new_filings(cloud_event):
    """
    Function triggered by Cloud Scheduler to check for new SEC filings
    """
    # Companies to monitor (expand as needed)
    companies = [
        {"ticker": "AAPL", "cik": "0000320193"},
        {"ticker": "MSFT", "cik": "0000789019"},
        {"ticker": "GOOGL", "cik": "0001652044"},
        # Add more companies...
    ]
    
    # Get last check time
    db = firestore.Client()
    config_ref = db.collection('config').document('sec_scraper')
    config = config_ref.get()
    
    if config.exists:
        last_check = config.to_dict().get('last_check')
    else:
        # Default to 24 hours ago if no previous check
        last_check = (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat()
        config_ref.set({'last_check': last_check})
    
    # Update last check time
    config_ref.update({'last_check': datetime.datetime.now().isoformat()})
    
    # Create Cloud Tasks client
    tasks_client = tasks_v2.CloudTasksClient()
    parent = tasks_client.queue_path('your-project-id', 'us-central1', 'sec-filing-queue')
    
    new_filings_found = 0
    
    for company in companies:
        # Check SEC EDGAR for new filings
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={company['cik']}&type=10-&dateb=&owner=exclude&count=10"
        response = requests.get(url, headers={'User-Agent': 'Financial Filings App contact@example.com'})
        
        if response.status_code != 200:
            print(f"Failed to fetch SEC data for {company['ticker']}: {response.status_code}")
            continue
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        filing_tables = soup.find_all('table', class_='tableFile2')
        
        if not filing_tables:
            continue
            
        filings = []
        
        # Extract filing information
        for table in filing_tables:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header row
                cells = row.find_all('td')
                if len(cells) >= 4:
                    filing_type = cells[0].text.strip()
                    filing_date = cells[3].text.strip()
                    
                    # Only process 10-Q and 10-K filings
                    if filing_type in ['10-Q', '10-K']:
                        # Check if filing date is after last check
                        filing_datetime = datetime.datetime.strptime(filing_date, '%Y-%m-%d')
                        last_check_datetime = datetime.datetime.fromisoformat(last_check)
                        
                        if filing_datetime > last_check_datetime:
                            # Get link to filing
                            filing_link = cells[1].find('a')
                            if filing_link and 'href' in filing_link.attrs:
                                accession_number = re.search(r'accession_number=([^&]+)', filing_link['href'])
                                if accession_number:
                                    accession = accession_number.group(1)
                                    source_url = f"https://www.sec.gov/Archives/edgar/data/{company['cik']}/{accession.replace('-', '')}/{accession}.txt"
                                    
                                    # Create task to process the filing
                                    task = {
                                        'app_engine_http_request': {
                                            'http_method': tasks_v2.HttpMethod.POST,
                                            'relative_uri': '/ingest-filing',
                                            'body': json.dumps({
                                                'company_ticker': company['ticker'],
                                                'filing_type': filing_type,
                                                'filing_date': filing_date,
                                                'source_url': source_url
                                            }).encode()
                                        }
                                    }
                                    
                                    response = tasks_client.create_task(parent=parent, task=task)
                                    new_filings_found += 1
    
    return f"Check complete. Found {new_filings_found} new filings to process."
```

## 6. Security Configuration

### 6.1 IAM Permissions Setup

```bash
# Create service account for the API
gcloud iam service-accounts create financial-filings-api

# Grant necessary permissions
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:financial-filings-api@your-project-id.iam.gserviceaccount.com" \
  --role="roles/datastore.user"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:financial-filings-api@your-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.objectUser"

# Create service account for Cloud Functions
gcloud iam service-accounts create financial-filings-functions

# Grant necessary permissions
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:financial-filings-functions@your-project-id.iam.gserviceaccount.com" \
  --role="roles/datastore.user"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:financial-filings-functions@your-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.admin"
```

### 6.2 Firestore Security Rules

```
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    // Companies collection - read for all, write for admins
    match /companies/{ticker} {
      allow read: if true;
      allow write: if request.auth != null && request.auth.token.admin == true;
    }
    
    // Filings collection - read for all, write for admins
    match /filings/{filingId} {
      allow read: if true;
      allow write: if request.auth != null && request.auth.token.admin == true;
    }
    
    // Config collection - admin only
    match /config/{configId} {
      allow read, write: if request.auth != null && request.auth.token.admin == true;
    }
  }
}
```

### 6.3 GCS Security

```bash
# Set bucket-level access control
gsutil iam ch allUsers:objectViewer gs://financial-filings-data

# Or for more security, restrict to authenticated users
gsutil iam ch allAuthenticatedUsers:objectViewer gs://financial-filings-data
```

## 7. Monitoring and Maintenance

### 7.1 Cloud Monitoring Setup

```bash
# Set up custom metrics for tracking
gcloud beta monitoring metrics-scopes create \
  --project=your-project-id \
  --location=global \
  --name=financial-filings-metrics

# Create alerting policy for errors
gcloud alpha monitoring policies create \
  --display-name="Filing Processing Errors" \
  --condition="select_ratio(count_true(monitoring.googleapis.com/cloudfunctions/function/execution_count{status=\"error\"}), count(monitoring.googleapis.com/cloudfunctions/function/execution_count)) > 0.1" \
  --duration=300s \
  --notification-channels="projects/your-project-id/notificationChannels/YOUR_CHANNEL_ID"
```

### 7.2 Maintenance Tasks

Set up a regular maintenance function to clean up and optimize the system:

```python
import functions_framework
from google.cloud import firestore, storage
import datetime

@functions_framework.cloud_event
def perform_maintenance(cloud_event):
    """
    Function triggered weekly to perform system maintenance
    """
    db = firestore.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket('financial-filings-data')
    
    # 1. Check for orphaned files in GCS
    filings_ref = db.collection('filings')
    filings = filings_ref.stream()
    
    # Build list of valid file paths
    valid_paths = set()
    for filing in filings:
        filing_data = filing.to_dict()
        if filing_data.get('text_file_path'):
            valid_paths.add(filing_data['text_file_path'])
        if filing_data.get('llm_file_path'):
            valid_paths.add(filing_data['llm_file_path'])
    
    # Check for orphaned files in storage
    blobs = bucket.list_blobs(prefix='companies/')
    orphaned_files = 0
    
    for blob in blobs:
        if blob.name not in valid_paths:
            print(f"Orphaned file: {blob.name}")
            # Uncomment to delete orphaned files:
            # blob.delete()
            orphaned_files += 1
    
    # 2. Validate metadata references
    invalid_records = 0
    for filing in filings:
        filing_data = filing.to_dict()
        
        # Check if referenced files exist
        text_path = filing_data.get('text_file_path')
        llm_path = filing_data.get('llm_file_path')
        
        if text_path and not bucket.blob(text_path).exists():
            print(f"Invalid text file reference in {filing.id}: {text_path}")
            invalid_records += 1
        
        if llm_path and not bucket.blob(llm_path).exists():
            print(f"Invalid LLM file reference in {filing.id}: {llm_path}")
            invalid_records += 1
    
    return f"Maintenance complete. Found {orphaned_files} orphaned files and {invalid_records} invalid records."
```

## 8. Implementation Checklist

- [ ] Create GCS bucket with proper structure
- [ ] Set up Firestore database with collections
- [ ] Configure lifecycle policies for storage tiering
- [ ] Deploy Cloud Functions for automation
- [ ] Create and deploy Cloud Run API
- [ ] Set up SEC filing ingestion pipeline
- [ ] Configure IAM permissions and security rules
- [ ] Set up monitoring and alerts
- [ ] Implement maintenance procedures
- [ ] Test the complete workflow with sample data
- [ ] Scale up by adding more companies