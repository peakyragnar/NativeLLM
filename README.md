# LLM-Native Financial Data Project

This project extracts financial data from SEC XBRL filings and converts it to an LLM-optimized format without imposing traditional financial frameworks or biases.

## Project Overview

The pipeline:
1. Retrieves raw XBRL data directly from SEC EDGAR
2. Extracts narrative text from HTML filing documents
3. Preserves all data exactly as reported by companies
4. Converts it to a format optimized for LLM consumption
5. Avoids imposing traditional financial frameworks or interpretations

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Update the `USER_AGENT` in `src/config.py` with your information.

## Usage

1. Set up project directories:
```bash
python run_pipeline.py --setup
```

2. Process a single company to test:
```bash
python run_pipeline.py --company AAPL
```

3. Process initial companies from config:
```bash
python run_pipeline.py --initial
```

4. Process top N companies in parallel:
```bash
python run_pipeline.py --top 10 --workers 3
```

## LLM Queries

You can query processed filings with an LLM using both structured data and narrative text:

```bash
# Combined query (using both structured data and narrative text)
python query_llm.py --ticker AAPL --filing_type 10-Q --question "What was the revenue for the most recent quarter and how does it compare to the previous quarter?" --data_type combined

# Structured data only query
python query_llm.py --ticker AAPL --filing_type 10-Q --question "What was the gross margin for the most recent quarter?" --data_type structured

# Narrative text only query
python query_llm.py --ticker AAPL --filing_type 10-K --question "What are the key risk factors mentioned?" --data_type narrative --narrative_section risk_factors

# MD&A section query
python query_llm.py --ticker AAPL --filing_type 10-Q --question "How does management explain changes in operating expenses?" --data_type narrative --narrative_section mda
```

## Testing

Run all tests using the centralized test runner:
```bash
./run_tests.sh
```

Run specific test categories:
```bash
# Run unit tests only
./run_tests.sh --unit

# Run integration tests only
./run_tests.sh --integration

# Run validation and data integrity tests only
./run_tests.sh --validation

# Run with verbose output
./run_tests.sh --verbose
```

Run data integrity checks (useful for CI pipelines):
```bash
./run_data_integrity_checks.sh
```

Schedule regular integrity validation (for cron jobs):
```bash
./scheduled_integrity_check.sh
```

For more details on the test framework, see the [tests/README.md](tests/README.md).

## Project Structure

- `src/`: Source code for the project
  - `edgar/`: SEC EDGAR access modules
  - `xbrl/`: XBRL processing modules
    - `xbrl_downloader.py`: Downloads XBRL instance documents
    - `xbrl_parser.py`: Parses XBRL into structured data
    - `html_text_extractor.py`: Extracts narrative text from HTML filings
  - `formatter/`: LLM format generation
    - `llm_formatter.py`: Formats structured XBRL data
    - `normalize_value.py`: Normalizes numeric values
- `data/`: Data storage
  - `raw/`: Downloaded XBRL files
  - `processed/`: Generated LLM format files and extracted text
- `tests/`: Test suite
  - `validation/`: Data validation and integrity checks
  - `run_all_tests.py`: Centralized test runner
- `run_pipeline.py`: Main entry point
- `run_tests.sh`: Test runner script
- `run_data_integrity_checks.sh`: Script for CI pipeline validation
- `scheduled_integrity_check.sh`: Script for scheduled validation
- `query_llm.py`: Query tool for LLMs
- `logs/`: Log directory for scheduled validation runs