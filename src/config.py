# src/config.py

# Company information
COMPANY_NAME = "Exascale Capital"
COMPANY_EMAIL = "info@exascale.capital"

# SEC EDGAR settings
SEC_BASE_URL = "https://www.sec.gov"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"
USER_AGENT = f"{COMPANY_NAME} {COMPANY_EMAIL}"  # Formatted user agent for SEC requests

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