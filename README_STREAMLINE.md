# Streamlined SEC XBRL Pipeline

This branch contains a streamlined approach to the SEC XBRL pipeline, combining the best of both worlds:

## Key Features

1. **Reliable URL Handling**: Uses the `secedgar` library for reliable SEC EDGAR URL construction and document discovery
2. **Full Pipeline Functionality**: Maintains all the rich features of the enhanced pipeline
3. **Calendar-Based Filtering**: Process filings by specific year ranges (2022-2025)
4. **Complete HTML Processing**: Full text extraction with section identification
5. **Cloud Integration**: Comprehensive GCP integration for storage and metadata

## How It Works

The streamlined approach solves the issues with 2022-2023 filings by:

1. Using `secedgar` to reliably locate and download filings with proper URL structure
2. Adapting the downloaded files to work with our existing HTML and XBRL processors
3. Maintaining all the cloud integration features for a complete solution

## Using The Pipeline

### Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

### Process Filings by Year Range

```bash
python streamlined_pipeline.py --calendar-range --start-year 2022 --end-year 2023 --tickers MSFT AAPL
```

### Process a Single Filing

```bash
python streamlined_pipeline.py --single-filing --ticker MSFT --filing-type 10-K
```

### Additional Options

- `--skip-10k` - Skip 10-K filings
- `--skip-10q` - Skip 10-Q filings
- `--workers N` - Set number of parallel workers
- `--skip-gcp` - Skip uploading to Google Cloud
- `--test` - Run in test mode (processes MSFT 2022-2023)

## Why This Approach Works Better

The streamlined approach combines:

1. **secedgar's reliable document handling** which works correctly for all filing years
2. **enhanced_pipeline's comprehensive processing** for complete HTML extraction and cloud integration

This eliminates the inconsistent URL handling that caused text files to be missing for 2022-2023 filings.

## Testing

To run a test with Microsoft for 2022-2023 filings:

```bash
python streamlined_pipeline.py --test
```

This will process MSFT filings for 2022-2023 using the new streamlined approach.