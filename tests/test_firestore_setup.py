from google.cloud import firestore
import os
import datetime

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def setup_firestore_collections():
    """Set up initial Firestore collections"""
    db = firestore.Client(database='nativellm')
    
    # Check if collections exist by attempting to get a document
    companies_ref = db.collection('companies').limit(1).get()
    filings_ref = db.collection('filings').limit(1).get()
    
    print(f"Firestore initialized with collections: companies, filings")
    return True

def add_company(ticker, name, cik=None, sector=None, industry=None):
    """Add a new company to the database"""
    db = firestore.Client(database='nativellm')
    company_ref = db.collection('companies').document(ticker)
    
    company_data = {
        'ticker': ticker,
        'name': name,
        'last_updated': datetime.datetime.now()
    }
    
    # Add optional fields if provided
    if cik:
        company_data['cik'] = cik
    if sector:
        company_data['sector'] = sector
    if industry:
        company_data['industry'] = industry
    
    company_ref.set(company_data)
    print(f"Added company: {ticker} - {name}")
    
    return ticker

def add_filing_metadata(company_ticker, filing_type, fiscal_year, fiscal_period, 
                       period_end_date, filing_date, text_path, llm_path,
                       text_size, llm_size, company_name=None):
    """Add metadata for a new filing"""
    db = firestore.Client(database='nativellm')
    
    # Create a unique filing ID
    filing_id = f"{company_ticker}-{filing_type}-{fiscal_year}-{fiscal_period}"
    
    # Add to filings collection
    filing_ref = db.collection('filings').document(filing_id)
    
    filing_data = {
        'filing_id': filing_id,
        'company_ticker': company_ticker,
        'company_name': company_name,
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
        'access_count': 0
    }
    
    filing_ref.set(filing_data)
    print(f"Added filing metadata: {filing_id}")
    
    return filing_id

def test_metadata_for_uploaded_file():
    """Add metadata for the test file we uploaded to GCS"""
    # 1. First set up collections
    setup_firestore_collections()
    
    # 2. Add company data for Apple
    add_company(
        ticker="AAPL", 
        name="Apple Inc.", 
        cik="0000320193", 
        sector="Technology", 
        industry="Consumer Electronics"
    )
    
    # 3. Add filing metadata for the 10-K we uploaded
    # For a file named: Apple_Inc_2024_FY_AAPL_10-K_20240928_llm.txt
    
    # Get file size (this would normally come from your processing function)
    # We're just estimating here
    file_size = 500000
    
    add_filing_metadata(
        company_ticker="AAPL",
        company_name="Apple Inc.",
        filing_type="10-K",
        fiscal_year=2024,
        fiscal_period="annual",
        period_end_date="2024-09-28",  # Extract from filename or content
        filing_date="2024-10-30",      # Estimated filing date
        text_path="companies/AAPL/10-K/2024/annual/text.txt",
        llm_path="companies/AAPL/10-K/2024/annual/llm.txt",
        text_size=file_size,
        llm_size=file_size
    )
    
    print("Firestore setup complete!")
    
    # 4. Verify we can retrieve the data
    db = firestore.Client(database='nativellm')
    apple_doc = db.collection('companies').document('AAPL').get()
    if apple_doc.exists:
        print(f"Successfully retrieved company data: {apple_doc.to_dict()}")
    
    filing_doc = db.collection('filings').document('AAPL-10-K-2024-annual').get()
    if filing_doc.exists:
        print(f"Successfully retrieved filing metadata!")
    
    return True

if __name__ == "__main__":
    test_metadata_for_uploaded_file()