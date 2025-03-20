# src/config.py

# SEC EDGAR settings
SEC_BASE_URL = "https://www.sec.gov"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"
USER_AGENT = "Your Company Name user@example.com"  # Replace with your information

# Output settings
RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed"

# Initial companies to process
INITIAL_COMPANIES = [
    {"ticker": "AAPL", "name": "Apple Inc."},
    {"ticker": "MSFT", "name": "Microsoft Corporation"},
    {"ticker": "GOOGL", "name": "Alphabet Inc."},
    {"ticker": "AMZN", "name": "Amazon.com, Inc."},
    {"ticker": "META", "name": "Meta Platforms, Inc."}
]

# Filing types to process
FILING_TYPES = ["10-K", "10-Q"]