#!/usr/bin/env python3
"""
Load SEC data into the database.

This script is used to load SEC filings data into the PostgreSQL database.
It assumes that the database schema already exists.
"""

import os
import sys
import argparse
import logging
from typing import List, Dict, Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_HOST = "35.205.197.130"
DB_NAME = "sec_filings"
DB_USER = "postgres"
DB_PASSWORD = "secfilings2024"
DB_PORT = "5432"

def get_connection():
    """Get a connection to the database."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def create_tables():
    """Create the necessary tables if they don't exist."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        
        # Create the companies table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            ticker VARCHAR(10) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            cik VARCHAR(20),
            sic_code VARCHAR(10),
            sic_description TEXT,
            fiscal_year_end DATE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create the financial_data table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_data (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10) NOT NULL,
            company_name TEXT,
            filing_type VARCHAR(10),
            fiscal_year INTEGER,
            fiscal_period VARCHAR(10),
            filing_date DATE,
            accession_number VARCHAR(50),
            statement_type VARCHAR(50),
            concept TEXT,
            value_text TEXT,
            value_numeric DECIMAL(30,2),
            unit VARCHAR(20),
            context_id VARCHAR(100),
            context_label TEXT,
            date_type VARCHAR(20),
            as_of_date DATE,
            period_start_date DATE,
            period_end_date DATE,
            taxonomy_prefix VARCHAR(50),
            is_normalized BOOLEAN,
            FOREIGN KEY (ticker) REFERENCES companies(ticker)
        )
        """)
        
        # Create the text_blocks table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS text_blocks (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10) NOT NULL,
            filing_type VARCHAR(10),
            fiscal_year INTEGER,
            fiscal_period VARCHAR(10),
            filing_date DATE,
            block_id VARCHAR(50),
            title TEXT,
            content TEXT,
            FOREIGN KEY (ticker) REFERENCES companies(ticker)
        )
        """)
        
        # Create indexes
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS financial_data_ticker_idx ON financial_data (ticker);
        CREATE INDEX IF NOT EXISTS financial_data_concept_idx ON financial_data (concept);
        CREATE INDEX IF NOT EXISTS financial_data_period_end_date_idx ON financial_data (period_end_date);
        CREATE INDEX IF NOT EXISTS text_blocks_ticker_idx ON text_blocks (ticker);
        """)
        
        conn.commit()
        logger.info("Tables created or already exist")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
    finally:
        conn.close()

def insert_company(ticker, name, cik=None, sic_code=None, sic_description=None, fiscal_year_end=None):
    """Insert a company into the database."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        
        # Check if the company already exists
        cursor.execute("SELECT ticker FROM companies WHERE ticker = %s", (ticker,))
        if cursor.fetchone() is not None:
            logger.info(f"Company {ticker} already exists")
            return
        
        # Insert the company
        cursor.execute("""
        INSERT INTO companies (ticker, name, cik, sic_code, sic_description, fiscal_year_end)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (ticker, name, cik, sic_code, sic_description, fiscal_year_end))
        
        conn.commit()
        logger.info(f"Inserted company {ticker}")
    except Exception as e:
        logger.error(f"Error inserting company {ticker}: {e}")
    finally:
        conn.close()

def insert_sample_companies():
    """Insert sample companies into the database."""
    companies = [
        {
            "ticker": "NVDA",
            "name": "NVIDIA Corporation",
            "cik": "0001045810",
            "sic_code": "3674",
            "sic_description": "Semiconductors & Related Devices",
            "fiscal_year_end": "2024-01-28"
        },
        {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "cik": "0000320193",
            "sic_code": "3571",
            "sic_description": "Electronic Computers",
            "fiscal_year_end": "2023-09-30"
        },
        {
            "ticker": "TSLA",
            "name": "Tesla, Inc.",
            "cik": "0001318605",
            "sic_code": "3711",
            "sic_description": "Motor Vehicles & Passenger Car Bodies",
            "fiscal_year_end": "2023-12-31"
        }
    ]
    
    for company in companies:
        insert_company(**company)

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Load SEC data into the database.')
    parser.add_argument('--create-tables', action='store_true', help='Create the necessary tables')
    parser.add_argument('--insert-sample-companies', action='store_true', help='Insert sample companies')
    
    args = parser.parse_args()
    
    if args.create_tables:
        create_tables()
    
    if args.insert_sample_companies:
        insert_sample_companies()
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
