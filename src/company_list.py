# src/company_list.py
import csv
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def load_sp500_companies():
    """Load S&P 500 companies from a CSV file"""
    # This is a simplified version - in practice, you might download
    # this list from a financial data provider or Wikipedia
    
    companies = [
        {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Information Technology"},
        {"ticker": "MSFT", "name": "Microsoft Corporation", "sector": "Information Technology"},
        {"ticker": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Discretionary"},
        {"ticker": "GOOGL", "name": "Alphabet Inc.", "sector": "Communication Services"},
        {"ticker": "META", "name": "Meta Platforms Inc.", "sector": "Communication Services"},
        {"ticker": "TSLA", "name": "Tesla Inc.", "sector": "Consumer Discretionary"},
        {"ticker": "NVDA", "name": "NVIDIA Corporation", "sector": "Information Technology"},
        {"ticker": "BRK.B", "name": "Berkshire Hathaway Inc.", "sector": "Financials"},
        {"ticker": "UNH", "name": "UnitedHealth Group Inc.", "sector": "Health Care"},
        {"ticker": "JNJ", "name": "Johnson & Johnson", "sector": "Health Care"},
        # Add more companies as needed
    ]
    
    return companies

def get_companies_by_sector(sector=None):
    """Get companies by sector"""
    companies = load_sp500_companies()
    
    if sector:
        return [c for c in companies if c["sector"] == sector]
    
    return companies

def get_top_companies(count=10):
    """Get top N companies"""
    companies = load_sp500_companies()
    return companies[:count]