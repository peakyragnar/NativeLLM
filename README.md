# LLM-Native Financial Data Project

This project extracts financial data from SEC XBRL filings and converts it to an LLM-optimized format, preserving the original data exactly as reported by companies without imposing traditional financial frameworks or biases.

## Overview

The system creates a pipeline that:

1. Retrieves raw XBRL/iXBRL data directly from SEC EDGAR
2. Preserves all data exactly as reported by companies
3. Converts it to a format optimized for LLM consumption
4. Avoids imposing traditional financial frameworks or interpretations

## Key Features

- **Multi-format Support**: Handles both traditional XBRL and modern iXBRL formats
- **Company-specific Format Detection**: Adapts automatically to different company filing formats
- **Robust Error Handling**: Implements fallback strategies for increased resilience
- **Cross-validation**: Verifies extracted data against expected patterns
- **Flexible Document Handling**: Adapts to different filing structures across companies

## iXBRL Support

The system now fully supports inline XBRL (iXBRL) documents, which is the modern SEC-mandated format for financial filings. This enables processing of companies like Google (GOOGL/GOOG) that use this format exclusively. Key features of the iXBRL support include:

- **Comprehensive Document Discovery**: Locates HTML documents containing iXBRL data
- **Context and Unit Extraction**: Extracts hidden XBRL sections containing contexts and units
- **Fact Extraction**: Identifies and extracts tagged financial facts embedded within HTML
- **Related Document Handling**: Manages relationships between HTML and linkbase files
- **Section Recognition**: Identifies standard financial sections like balance sheets and income statements

## Architecture

The system is built with a hybrid approach:

- **lxml + BeautifulSoup**: For efficient HTML and XML parsing
- **Multi-stage Processing Pipeline**: Separates download, parsing, and extraction steps
- **Graceful Degradation**: Handles edge cases and format variations with fallback mechanisms
- **Circuit Breakers**: Prevents cascading failures when processing problematic files

## Setup and Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Process a Company

```bash
# Process a specific company
python run_pipeline.py --company GOOGL

# Process initial companies from config
python run_pipeline.py --initial

# Process top N companies in parallel
python run_pipeline.py --top 10 --workers 3
```

### Test iXBRL Extraction

```bash
# Test Google extraction
python test_ixbrl_extraction.py --test-google

# Process and format a specific filing
python test_ixbrl_extraction.py --process-format --ticker GOOGL --filing-type 10-K

# Test multiple companies
python test_ixbrl_extraction.py --test-multiple
```

### Query a Processed Filing

```bash
python query_llm.py --ticker GOOGL --filing_type 10-K --question "What was the revenue for the most recent quarter and how does it compare to the previous quarter?"
```

## Directory Structure

- `/src/edgar/`: SEC EDGAR access modules
- `/src/xbrl/`: XBRL and iXBRL processing modules
- `/src/formatter/`: LLM format generators
- `/data/raw/`: Raw downloaded XBRL/iXBRL files
- `/data/processed/`: Processed LLM-formatted files

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request