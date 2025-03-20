# test_pipeline.py
from src.process_company import process_company

def test_single_company():
    """Test processing a single company"""
    ticker = "AAPL"  # Use a well-known company for testing
    result = process_company(ticker)
    
    print("Test result:")
    print(f"Ticker: {result.get('ticker')}")
    print(f"CIK: {result.get('cik')}")
    print(f"Company Name: {result.get('company_name')}")
    print(f"Filings Processed: {len(result.get('filings_processed', []))}")
    
    for filing in result.get('filings_processed', []):
        print(f"  - {filing.get('filing_type')} ({filing.get('filing_date')})")
        
        # Display a sample of the LLM format
        llm_file_path = filing.get('llm_file_path')
        if llm_file_path:
            with open(llm_file_path, 'r', encoding='utf-8') as f:
                content = f.read(1000)  # Read first 1000 characters for preview
            
            print("\nLLM Format Sample:")
            print(content)
            print("...")

if __name__ == "__main__":
    test_single_company()