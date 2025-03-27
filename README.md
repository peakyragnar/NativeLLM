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

## Architecture

The system uses a modular architecture organized into specialized components in the `src2` directory:

1. **Downloader Modules** - Retrieve SEC filings with reliable URL construction
2. **Processor Modules** - Parse XBRL/iXBRL and optimize HTML content
3. **Formatter Modules** - Convert data to LLM-friendly format
4. **Storage Modules** - Store documents and metadata in the cloud

## Usage

```bash
# Process a single company
python -m src2.sec.batch_pipeline MSFT --start-year 2024 --end-year 2025 --gcp-bucket native-llm-filings --email info@exascale.capital

# Process multiple companies in parallel
python -m src2.sec.batch_pipeline AAPL --start-year 2023 --end-year 2024 --workers 3 --gcp-bucket native-llm-filings

# Skip GCP upload for local processing only
python -m src2.sec.batch_pipeline GOOGL --start-year 2024 --end-year 2024 --no-10q
```

## Data Validation

```bash
# Validate data integrity for a company
python verify_file_integrity.py --ticker MSFT --filing-type 10-K --fiscal-year 2024

# Check GCP consistency for a ticker
python clear_gcp_data.py --ticker MSFT
```

## Documentation

For more detailed information about the system, refer to the [CLAUDE.md](./claude.md) document, which contains:

- Complete architectural overview
- Detailed module descriptions
- HTML optimization strategy
- Best practices for usage
- Performance considerations

## License

This project is proprietary and confidential.