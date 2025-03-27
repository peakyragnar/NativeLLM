# LLM-Native Financial Data Project

This document outlines the NativeLLM system, which extracts financial data from SEC XBRL filings and converts it to an LLM-optimized format without imposing traditional financial frameworks or biases.

## Modular Architecture Overview

The system uses a highly modular architecture organized into specialized components in the `src2` directory:

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

## Core Components

### 1. Downloader Modules (`src2/downloader`, `src2/sec`)
- **SEC Downloader** (`src2/sec/downloader.py`): SEC-compliant file downloading with proper headers, rate limiting, and URL construction
- **Support for Different Filing Types**: Handles 10-K, 10-Q and other filing types
- **Proper SEC Rate Limiting**: Respects SEC EDGAR's rate limits and user-agent requirements
- **Automatic Retries**: Implements backoff strategies for failed requests

### 2. Processor Modules (`src2/processor`)
- **HTML Processor** (`src2/processor/html_processor.py`): Extracts text from HTML with proper section recognition
- **XBRL Processor** (`src2/processor/xbrl_processor.py`): Parses XBRL data with support for different formats
- **iXBRL Extractor** (`src2/processor/ixbrl_extractor.py`): Specialized extraction for inline XBRL in HTML documents
- **HTML Optimizer** (`src2/processor/html_optimizer.py`): Reduces file size while preserving data integrity

### 3. Formatter Module (`src2/formatter`)
- **LLM Formatter** (`src2/formatter/llm_formatter.py`): Converts financial data to LLM-friendly format
- **Value Normalization** (`src2/formatter/normalize_value.py`): Standardizes numerical values for consistency

### 4. Storage Module (`src2/storage`)
- **GCP Storage** (`src2/storage/gcp_storage.py`): Handles Google Cloud Storage integration
- **Metadata Management**: Maintains filing metadata in Firestore

## HTML Optimization

The HTML optimization module safely reduces the size of XBRL financial data files while preserving all content, structure, and especially numeric values. This is critical for ensuring data integrity in financial reports.

### Key Features

- **Numeric Value Preservation**: All financial values are preserved exactly as reported
- **Document Structure Maintenance**: Table structures and relationships are retained
- **Conservative Optimization**: Only definitively non-essential HTML attributes are removed
- **Multi-stage Verification**: Strict checks ensure no data loss occurs

### Implementation

The optimization is implemented in:
- `src2/processor/html_optimizer.py`: Main optimization logic
- `src2/processor/html_processor.py`: Integration with HTML processing pipeline

The process includes:
1. Identifying financial values using comprehensive pattern matching
2. Filtering non-essential attributes while keeping structural elements
3. Specialized handling for complex financial tables
4. Verification of data integrity post-optimization

### Configuration

The HTML optimization behavior can be configured in `src2/config.py`:

```python
HTML_OPTIMIZATION = {
    # Level of HTML optimization to apply
    "level": "maximum_integrity",  # Options: maximum_integrity, balanced, maximum_reduction
    
    # Attributes that are safe to remove
    "safe_removable_attributes": ["bgcolor", "color", "font", "face", "class"],
    
    # Structural attributes that should be preserved
    "preserved_attributes": ["align", "padding", "margin", "width", "height", "border"],
    
    # Minimum reduction threshold to apply changes
    "min_reduction_threshold": 1.0,  # Percentage
    
    # Enable logging of HTML optimization metrics
    "enable_logging": True
}
```

### Results

In our testing on real financial filings, the HTML optimization achieves:
- **25-35% size reduction** while maintaining 100% data integrity
- **100% preservation** of all numeric values
- **Successful handling** of complex financial tables

## Streamlined Data Pipeline

### Data Acquisition
1. **Company and Filing Selection**
   - Choose company tickers to process
   - Select filing types (10-K, 10-Q)
   - Define date ranges

2. **SEC Filing Discovery**
   - Uses SEC downloader for reliable URL construction
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

## Command Line Interface

```bash
# Process a single company
python -m src2.sec.batch_pipeline MSFT --start-year 2024 --end-year 2025 --gcp-bucket native-llm-filings --email info@exascale.capital

# Process multiple companies in parallel
python -m src2.sec.batch_pipeline AAPL --start-year 2023 --end-year 2024 --workers 3 --gcp-bucket native-llm-filings

# Skip GCP upload for local processing only
python -m src2.sec.batch_pipeline GOOGL --start-year 2024 --end-year 2024 --no-10q
```

## Data Validation

The system includes data integrity validation to ensure quality:

```bash
# Validate data integrity for a company
python verify_file_integrity.py --ticker MSFT --filing-type 10-K --fiscal-year 2024

# Check GCP consistency for a ticker
python clear_gcp_data.py --ticker MSFT
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
   - Use consistent path structure in cloud storage (sec_downloads)
   - Store rich metadata for efficient searching
   - Implement retry logic for cloud operations

## Performance Considerations

- Use parallel processing for multiple companies
- Limit worker count to respect SEC rate limits
- Consider pre-downloading filings for batch processing
- Optimize HTML content to reduce storage requirements
- Implement appropriate timeouts for all network operations