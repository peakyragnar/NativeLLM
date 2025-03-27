# LLM-Native Financial Data Project

This document outlines the NativeLLM system, which extracts financial data from SEC XBRL filings and converts it to an LLM-optimized format without imposing traditional financial frameworks or biases.

## Modular Architecture Overview

The system uses a highly modular architecture organized into specialized components:

1. **Downloader Modules** - Retrieve SEC filings with reliable URL construction
2. **Processor Modules** - Parse XBRL/iXBRL and optimize HTML content
3. **Formatter Modules** - Convert data to LLM-friendly format
4. **Storage Modules** - Store documents and metadata in the cloud

![System Architecture](https://mermaid.ink/img/pako:eNqNlF1P2zAUhv_KyZVAmuglTtMPLqZOm9A0Ntp2EkIIWYkTvDo4w5d2CMp_z3GctUkbsQsu4rzv4_Nx7JNjtZQFV6laZRXDEov0HYLp2MK8U66Db_lRSEbxFcUoFbo5YFv6NdoxmpcpFfFcljbcrLLaslJPCbtZbBnMtWWQc6XsRisOt_Uc85mPyqL9CKu5o5RO0VpsGw-PpClPu8r1e0JmjkuYcHzRWbXkK1G2j_gVLoGeu-sBYt9lS2ld2czfyFyadNqBBknOzKCJBYoNPu37OGdu7bvZaW_Xzl0PZnlVVUy-D02hbTo9mUwsrzgODFe-4SmIBKwNFJ5f6i13vmNHdHDwdjTiUIDJeTzVyryXPTaUTYxrGZr0XIQaS0G7B8FtjX70xpGW9_RE3dHdA_iPj80O3R9-5_neKrxs2RaOSy47MalNdm6n3YZwwx3WM-MXWPb4zzIseb_ITNhsfc4_oMrockYU2GyXQjjSZG8LN9BZbWm5dxg9TPkcPmox_4Y0z03kJlLn2x4Iu21ATzqHksPeaCMOKE033a9_0q2Kzra8U6mjLTeLSxeedane1-DV3I8dzuYNXDXsJ8D_4VsCF71PpChUSvmK-0ZxWeCKoGpTN6FUqcAnv2B9yX0HSPINFkZlo_MkiOlKi32Ub598XunYpLx5_eRSDQT-chV7p3HQG2f9XpxO_TALTtJelmb9gZ8kcZoNh8E-vJDxvxHjLOhncdANgpO9eDgZ-6PwqtVovgHlLgDw?type=png)

## Project Goals

The goal is to create a streamlined pipeline that:
1. Retrieves raw XBRL/iXBRL data directly from SEC EDGAR
2. Handles both traditional XBRL and newer inline XBRL formats
3. Preserves all data exactly as reported by companies
4. Optimizes HTML content while maintaining 100% data integrity
5. Converts data to a format optimized for LLM consumption
6. Stores processed data in Google Cloud Platform
7. Avoids imposing traditional financial frameworks or interpretations

## Modular Components

### 1. Downloader Modules
- `src2/downloader/secedgar_downloader.py`: Uses secedgar library for reliable URL construction
- `src2/downloader/direct_edgar_downloader.py`: Alternative direct HTTP approach

### 2. Processor Modules
- `src2/processor/html_processor.py`: Advanced HTML processing with section recognition
  - Identifies and marks standard SEC document sections (Item 1, Item 2, etc.)
  - Extracts tables of contents and document metadata
  - Cleans and normalizes text with special handling for SEC document formatting
- `src2/processor/xbrl_processor.py`: Multi-format XBRL processing
  - Format detection between traditional XBRL and inline XBRL (iXBRL)
  - Dedicated parsers for each format with fallback mechanisms
  - Comprehensive metadata extraction
- `src2/processor/html_optimizer.py`: HTML optimization with data preservation
  - Reduces file size while maintaining 100% data integrity
  - Special handling for tables and financial data

### 3. Formatter Module
- `src2/formatter/llm_formatter.py`: Converts to standardized LLM-native format
  - Creates consistent, LLM-friendly document structure
  - Preserves all original values and relationships

### 4. Storage Module
- `src2/storage/gcp_storage.py`: Manages cloud storage integration
  - Uploads to Google Cloud Storage
  - Updates Firestore metadata
  - Handles path construction and organization

## Streamlined Data Pipeline

### Data Acquisition
1. **Company and Filing Selection**
   - Choose company tickers to process
   - Select filing types (10-K, 10-Q)
   - Define date ranges

2. **SEC Filing Discovery**
   - Uses secedgar for reliable URL construction
   - Finds the correct filing documents
   - Works consistently across different years (2022-2025)

### Data Processing
1. **Format Detection**
   - Automatically detects traditional XBRL vs. inline XBRL
   - Routes to appropriate processor
   - Implements fallbacks if detection is ambiguous

2. **XBRL/iXBRL Processing**
   - Extracts facts, contexts, and units
   - Preserves relationships between data points
   - Handles both XML and HTML-based documents

3. **HTML Processing**
   - Identifies document structure and sections
   - Marks standard SEC sections (Item 1, Item 2, etc.)
   - Optimizes content while preserving data integrity

### Data Formatting
1. **LLM-Native Format Generation**
   - Creates standardized document with clear section markers
   - Preserves all facts with their contexts and relationships
   - Optimizes for LLM comprehension

2. **Cloud Storage**
   - Organizes files in logical hierarchy
   - Stores metadata in Firestore for efficient queries
   - Enables fast retrieval for LLM applications

## Main Script and Configuration

The main pipeline script integrates these components with flexible options:

```python
# Example pipeline usage
from src2.downloader.secedgar_downloader import secedgar_downloader
from src2.processor.html_processor import html_processor
from src2.processor.xbrl_processor import xbrl_processor
from src2.formatter.llm_formatter import llm_formatter
from src2.storage.gcp_storage import gcp_storage

# Process a company filing
def process_filing(ticker, filing_type, fiscal_year=None, fiscal_period=None):
    # 1. Download the filing
    filing_data = secedgar_downloader.download_filing(ticker, filing_type)
    
    # 2. Process the downloaded filing
    if filing_data.get("format") == "html":
        # Process HTML document
        processed_html = html_processor.extract_text_from_filing(
            filing_data.get("file_path"), ticker, filing_type
        )
        text_file_path = html_processor.save_text_file(
            processed_html, f"output/{ticker}_{filing_type}_text.txt", filing_data
        )
    
    # 3. Process XBRL content
    xbrl_data = xbrl_processor.parse_xbrl_file(filing_data.get("xbrl_path"))
    
    # 4. Generate LLM format
    llm_content = llm_formatter.generate_llm_format(xbrl_data, filing_data)
    llm_file_path = llm_formatter.save_llm_format(
        llm_content, f"output/{ticker}_{filing_type}_llm.txt"
    )
    
    # 5. Upload to cloud storage
    if gcp_storage.is_configured():
        text_gcs_path = gcp_storage.upload_file(
            text_file_path, ticker, filing_type, fiscal_year, fiscal_period, "text"
        )
        llm_gcs_path = gcp_storage.upload_file(
            llm_file_path, ticker, filing_type, fiscal_year, fiscal_period, "llm"
        )
    
    return {
        "ticker": ticker,
        "filing_type": filing_type,
        "text_file": text_file_path,
        "llm_file": llm_file_path,
        "status": "success"
    }
```

## Command Line Interface

```bash
# Process a single company with the new modular architecture
python sec_pipeline.py --ticker MSFT --filing-type 10-K

# Process multiple companies with parallel workers
python sec_pipeline.py --tickers AAPL MSFT GOOGL --filing-type 10-Q --workers 3

# Process by calendar year range
python sec_pipeline.py --calendar-range --start-year 2022 --end-year 2024 --tickers AAPL MSFT

# Skip GCP upload for local processing only
python sec_pipeline.py --ticker MSFT --filing-type 10-K --skip-gcp
```

## Best Practices

1. **Format Detection and Fallbacks**
   - Always implement fallbacks when detection is uncertain
   - Validate output to ensure successful processing
   - Log detailed format information for debugging

2. **Error Handling**
   - Implement comprehensive error handling at each stage
   - Never let one filing failure stop the entire pipeline
   - Capture and record error details for analysis

3. **SEC Rate Limiting**
   - Respect SEC EDGAR's rate limits (10 requests per second)
   - Implement backoff strategies for 429 responses
   - Use a proper user-agent with contact information

4. **Testing Across Years**
   - Test with filings from different years (2022-2025)
   - Validate with both traditional XBRL and inline XBRL
   - Verify processors work with different SEC format variations

5. **Cloud Integration**
   - Use consistent path structure in cloud storage
   - Store rich metadata for efficient searching
   - Implement retry logic for cloud operations

## Performance Considerations

- Use parallel processing for multiple companies
- Limit worker count to respect SEC rate limits
- Consider pre-downloading filings for batch processing
- Optimize HTML content to reduce storage requirements
- Implement appropriate timeouts for all network operations