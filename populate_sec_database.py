#!/usr/bin/env python3

import sys
print("Script starting...")
print(f"Python version: {sys.version}")

"""
SEC Filings Database Populator

This script populates a PostgreSQL database with SEC filing data stored in Google Cloud Storage.
It can process all filings for a company or a specific filing.

Usage:
    python populate_sec_database.py --ticker AAPL
    python populate_sec_database.py --ticker AAPL --filing-type 10-K --fiscal-year 2023
"""

import os
import re
import argparse
import logging
import tempfile
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

import psycopg2
from psycopg2.extras import execute_values
from google.cloud import storage

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

# Google Cloud Storage parameters
GCS_BUCKET_NAME = "native-llm-filings"
GCS_SEC_PROCESSED_PREFIX = "companies/"  # Actual structure in the bucket

class SECDatabasePopulator:
    """
    Class to populate the SEC filings database from processed text files in Google Cloud Storage.
    """

    def __init__(self):
        """Initialize the database populator."""
        self.conn = None
        self.cursor = None
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(GCS_BUCKET_NAME)

    def connect_to_database(self):
        """Connect to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            self.cursor = self.conn.cursor()
            logger.info("Connected to the database successfully")
        except Exception as e:
            logger.error(f"Error connecting to the database: {str(e)}")
            raise

    def close_connection(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()
        logger.info("Database connection closed")

    def list_company_filings(self, ticker: str) -> List[str]:
        """
        List all available filings for a company in Google Cloud Storage.

        Args:
            ticker: Company ticker symbol

        Returns:
            List of filing paths
        """
        prefix = f"{GCS_SEC_PROCESSED_PREFIX}{ticker}/"
        blobs = list(self.bucket.list_blobs(prefix=prefix))
        filing_paths = [blob.name for blob in blobs if blob.name.endswith('llm.txt')]
        logger.info(f"Found {len(filing_paths)} filings for {ticker}")
        return filing_paths

    def download_filing(self, gcs_path: str) -> str:
        """
        Download a filing from Google Cloud Storage to a temporary file.

        Args:
            gcs_path: Path to the filing in GCS

        Returns:
            Path to the local temporary file
        """
        blob = self.bucket.blob(gcs_path)

        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
        temp_file_path = temp_file.name
        temp_file.close()

        # Download the blob to the temporary file
        blob.download_to_filename(temp_file_path)
        logger.info(f"Downloaded {gcs_path} to {temp_file_path}")

        return temp_file_path

    def parse_filing_metadata(self, gcs_path: str) -> Dict[str, Any]:
        """
        Parse filing metadata from the GCS path.

        Args:
            gcs_path: Path to the filing in GCS

        Returns:
            Dictionary of filing metadata
        """
        # Expected format: companies/TICKER/FILING-TYPE/FISCAL-YEAR/[FISCAL-PERIOD/]llm.txt
        # Example: companies/AAPL/10-K/2023/llm.txt or companies/AAPL/10-Q/2023/Q1/llm.txt

        # Split the path into components
        path_parts = gcs_path.split('/')

        # Extract ticker, filing type, and fiscal year
        ticker = path_parts[1]
        filing_type = path_parts[2]
        fiscal_year = int(path_parts[3])

        # Determine fiscal period
        if filing_type == '10-K':
            fiscal_period = 'annual'
        elif filing_type == '10-Q':
            fiscal_period = path_parts[4]  # Q1, Q2, or Q3
        else:
            fiscal_period = 'unknown'

        metadata = {
            'ticker': ticker,
            'filing_type': filing_type,
            'fiscal_year': fiscal_year,
            'fiscal_period': fiscal_period
        }

        return metadata

    def parse_filing(self, file_path: str, metadata: Dict[str, Any]) -> Tuple[List[Dict], List[Dict]]:
        """
        Parse a filing text file and extract financial data and text blocks.

        Args:
            file_path: Path to the filing text file
            metadata: Filing metadata

        Returns:
            Tuple of (financial_data_list, text_blocks_list)
        """
        financial_data_list = []
        text_blocks_list = []

        # Read the file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Log file size and first 1000 characters
        logger.info(f"File size: {len(content)} bytes")
        logger.info(f"File starts with: {content[:1000]}")

        # Check for key sections
        has_text_blocks = '@TEXT_BLOCKS' in content
        has_normalized_data = '@NORMALIZED_FINANCIAL_DATA' in content
        has_format = '@FORMAT:' in content
        has_statement = '@STATEMENT:' in content

        logger.info(f"File sections: TEXT_BLOCKS={has_text_blocks}, NORMALIZED_DATA={has_normalized_data}, FORMAT={has_format}, STATEMENT={has_statement}")

        # Extract text blocks
        text_blocks = self.extract_text_blocks(content, metadata)
        text_blocks_list.extend(text_blocks)

        # Extract normalized financial data
        financial_data = self.extract_normalized_financial_data(content, metadata)
        financial_data_list.extend(financial_data)

        logger.info(f"Extracted {len(financial_data)} financial data points and {len(text_blocks)} text blocks")

        return financial_data_list, text_blocks_list

    def extract_text_blocks(self, content: str, metadata: Dict[str, Any]) -> List[Dict]:
        """
        Extract text blocks from the filing content.

        Args:
            content: Filing content
            metadata: Filing metadata

        Returns:
            List of text block dictionaries
        """
        text_blocks = []

        # Find the text blocks directly
        block_pattern = r'(tb-\d+)\|@TITLE:\s*(.*?)(?:\s*@TEXT:\s*(.*?))?(?=\s*tb-\d+\||\s*@|\s*$)'
        for match in re.finditer(block_pattern, content, re.DOTALL):
            block_id = match.group(1)
            title = match.group(2).strip() if match.group(2) else ""
            block_content = match.group(3).strip() if match.group(3) else ""

            logger.info(f"Found text block: {block_id} with title: {title[:50]}...")

            text_blocks.append({
                'ticker': metadata['ticker'],
                'filing_type': metadata['filing_type'],
                'fiscal_year': metadata['fiscal_year'],
                'fiscal_period': metadata['fiscal_period'],
                'filing_date': None,  # We'll need to extract this from the filing content if available
                'block_id': block_id,
                'title': title,
                'content': block_content
            })

        return text_blocks

    def extract_normalized_financial_data(self, content: str, metadata: Dict[str, Any]) -> List[Dict]:
        """
        Extract normalized financial data from the filing content.

        Args:
            content: Filing content
            metadata: Filing metadata

        Returns:
            List of financial data dictionaries
        """
        financial_data = []

        # Find the format line directly
        format_line_match = re.search(r'@FORMAT:\s+(.*?)$', content, re.MULTILINE)
        if not format_line_match:
            logger.warning("No format line found in the content")
            return financial_data

        # Find all statement sections
        statement_sections = re.finditer(r'@STATEMENT:\s+(.*?)\n([^@]+)', content, re.DOTALL)

        # Process each statement section
        for statement_match in statement_sections:
            statement_type = statement_match.group(1).strip()
            statement_content = statement_match.group(2).strip()

            logger.info(f"Processing statement: {statement_type} with {len(statement_content)} bytes")

            # Process each data line
            # Format: Statement|Concept|Value|Context|Context_Label
            for line in statement_content.split('\n'):
                if not line.strip() or '|' not in line:
                    continue

                parts = line.split('|')
                if len(parts) < 5:
                    logger.warning(f"Invalid data line: {line}")
                    continue

                # Extract the data
                concept = parts[1].strip()
                value_text = parts[2].strip()
                context_id = parts[3].strip()
                context_label = parts[4].strip()

                # Parse the value to numeric if possible
                value_numeric = None
                if value_text and value_text != '—' and value_text != '$—':
                    try:
                        # Remove $ and , from the value
                        clean_value = value_text.replace('$', '').replace(',', '')
                        value_numeric = float(clean_value)
                    except ValueError:
                        # Keep as None if not a valid number
                        pass

                # Parse dates from context label
                as_of_date = None
                period_start_date = None
                period_end_date = None
                date_type = None

                # Check for "As of YYYY-MM-DD" format
                as_of_match = re.search(r'As of (\d{4}-\d{2}-\d{2})', context_label)
                if as_of_match:
                    date_str = as_of_match.group(1)
                    try:
                        as_of_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        period_end_date = as_of_date  # For INSTANT contexts, period_end_date is the same as as_of_date
                        date_type = 'INSTANT'
                    except ValueError:
                        logger.warning(f"Invalid date format in context label: {context_label}")

                # Check for "Period from YYYY-MM-DD to YYYY-MM-DD" format
                period_match = re.search(r'Period from (\d{4}-\d{2}-\d{2}) to (\d{4}-\d{2}-\d{2})', context_label)
                if period_match:
                    start_str = period_match.group(1)
                    end_str = period_match.group(2)
                    try:
                        period_start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
                        period_end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
                        date_type = 'DURATION'
                    except ValueError:
                        logger.warning(f"Invalid date format in context label: {context_label}")

                # Create the financial data entry
                financial_data.append({
                    'ticker': metadata['ticker'],
                    'company_name': None,  # We'll need to get this from company info
                    'filing_type': metadata['filing_type'],
                    'fiscal_year': metadata['fiscal_year'],
                    'fiscal_period': metadata['fiscal_period'],
                    'filing_date': None,  # We'll need to extract this from the filing content if available
                    'accession_number': None,  # We'll need to extract this if available
                    'statement_type': statement_type,
                    'concept': concept,
                    'value_text': value_text,
                    'value_numeric': value_numeric,
                    'unit': 'usd' if '$' in value_text else None,  # Assume USD if $ is present
                    'context_id': context_id,
                    'context_label': context_label,
                    'date_type': date_type,
                    'as_of_date': as_of_date,
                    'period_start_date': period_start_date,
                    'period_end_date': period_end_date,
                    'taxonomy_prefix': None,  # We'll need to extract this if available
                    'is_normalized': True
                })

        return financial_data

    def insert_company(self, ticker: str, name: str = None) -> None:
        """
        Insert a company into the database if it doesn't exist.

        Args:
            ticker: Company ticker symbol
            name: Company name (optional)
        """
        # Check if the company already exists
        self.cursor.execute("SELECT ticker FROM companies WHERE ticker = %s", (ticker,))
        if self.cursor.fetchone():
            logger.info(f"Company {ticker} already exists in the database")
            return

        # If name is not provided, use ticker as name
        if not name:
            name = ticker

        # Insert the company
        self.cursor.execute(
            "INSERT INTO companies (ticker, name) VALUES (%s, %s)",
            (ticker, name)
        )
        self.conn.commit()
        logger.info(f"Inserted company {ticker} into the database")

    def insert_financial_data(self, financial_data: List[Dict]) -> None:
        """
        Insert financial data into the database.

        Args:
            financial_data: List of financial data dictionaries
        """
        if not financial_data:
            logger.warning("No financial data to insert")
            return

        # Prepare the data for bulk insert
        columns = [
            'ticker', 'company_name', 'filing_type', 'fiscal_year', 'fiscal_period',
            'filing_date', 'accession_number', 'statement_type', 'concept', 'value_text',
            'value_numeric', 'unit', 'context_id', 'context_label', 'date_type',
            'as_of_date', 'period_start_date', 'period_end_date', 'taxonomy_prefix', 'is_normalized'
        ]

        values = [
            (
                item['ticker'], item['company_name'], item['filing_type'], item['fiscal_year'],
                item['fiscal_period'], item['filing_date'], item['accession_number'],
                item['statement_type'], item['concept'], item['value_text'], item['value_numeric'],
                item['unit'], item['context_id'], item['context_label'], item['date_type'],
                item['as_of_date'], item['period_start_date'], item['period_end_date'],
                item['taxonomy_prefix'], item['is_normalized']
            )
            for item in financial_data
        ]

        # Create the SQL query
        query = f"""
        INSERT INTO financial_data (
            {', '.join(columns)}
        ) VALUES %s
        ON CONFLICT (ticker, filing_type, fiscal_year, fiscal_period, statement_type, concept, context_id)
        DO UPDATE SET
            value_text = EXCLUDED.value_text,
            value_numeric = EXCLUDED.value_numeric,
            context_label = EXCLUDED.context_label,
            date_type = EXCLUDED.date_type,
            as_of_date = EXCLUDED.as_of_date,
            period_start_date = EXCLUDED.period_start_date,
            period_end_date = EXCLUDED.period_end_date,
            is_normalized = EXCLUDED.is_normalized
        """

        # Execute the query
        execute_values(self.cursor, query, values)
        self.conn.commit()
        logger.info(f"Inserted {len(values)} financial data points into the database")

    def insert_text_blocks(self, text_blocks: List[Dict]) -> None:
        """
        Insert text blocks into the database.

        Args:
            text_blocks: List of text block dictionaries
        """
        if not text_blocks:
            logger.warning("No text blocks to insert")
            return

        # Prepare the data for bulk insert
        columns = [
            'ticker', 'filing_type', 'fiscal_year', 'fiscal_period',
            'filing_date', 'block_id', 'title', 'content'
        ]

        values = [
            (
                item['ticker'], item['filing_type'], item['fiscal_year'], item['fiscal_period'],
                item['filing_date'], item['block_id'], item['title'], item['content']
            )
            for item in text_blocks
        ]

        # Create the SQL query
        query = f"""
        INSERT INTO text_blocks (
            {', '.join(columns)}
        ) VALUES %s
        ON CONFLICT (ticker, filing_type, fiscal_year, fiscal_period, block_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            content = EXCLUDED.content
        """

        # Execute the query
        execute_values(self.cursor, query, values)
        self.conn.commit()
        logger.info(f"Inserted {len(values)} text blocks into the database")

    def process_filing(self, gcs_path: str) -> None:
        """
        Process a single filing and insert its data into the database.

        Args:
            gcs_path: Path to the filing in GCS
        """
        try:
            # Parse filing metadata
            metadata = self.parse_filing_metadata(gcs_path)
            logger.info(f"Processing filing: {gcs_path}")

            # Download the filing
            local_path = self.download_filing(gcs_path)

            # Make sure the company exists in the database
            self.insert_company(metadata['ticker'])

            # Parse the filing
            financial_data, text_blocks = self.parse_filing(local_path, metadata)

            # Insert the data into the database
            self.insert_financial_data(financial_data)
            self.insert_text_blocks(text_blocks)

            # Clean up the temporary file
            os.unlink(local_path)
            logger.info(f"Completed processing filing: {gcs_path}")
        except Exception as e:
            logger.error(f"Error processing filing {gcs_path}: {str(e)}")
            raise

    def process_company(self, ticker: str) -> None:
        """
        Process all filings for a company.

        Args:
            ticker: Company ticker symbol
        """
        try:
            # List all filings for the company
            filing_paths = self.list_company_filings(ticker)

            if not filing_paths:
                logger.warning(f"No filings found for {ticker}")
                return

            # Make sure the company exists in the database
            self.insert_company(ticker)

            # Process each filing
            for path in filing_paths:
                self.process_filing(path)

            logger.info(f"Completed processing all filings for {ticker}")
        except Exception as e:
            logger.error(f"Error processing company {ticker}: {str(e)}")
            raise

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Populate SEC filings database from Google Cloud Storage.')
    parser.add_argument('--ticker', required=True, help='Company ticker symbol')
    parser.add_argument('--filing-type', help='Filing type (e.g., 10-K, 10-Q)')
    parser.add_argument('--fiscal-year', type=int, help='Fiscal year')
    parser.add_argument('--fiscal-period', help='Fiscal period (e.g., annual, Q1, Q2, Q3)')

    args = parser.parse_args()

    populator = SECDatabasePopulator()

    try:
        # Connect to the database
        populator.connect_to_database()

        if args.filing_type and args.fiscal_year:
            # Process a specific filing
            if args.fiscal_period and args.filing_type == '10-Q':
                # For quarterly filings
                gcs_path = f"{GCS_SEC_PROCESSED_PREFIX}{args.ticker}/{args.filing_type}/{args.fiscal_year}/{args.fiscal_period}/llm.txt"
            else:
                # For annual filings (10-K)
                gcs_path = f"{GCS_SEC_PROCESSED_PREFIX}{args.ticker}/{args.filing_type}/{args.fiscal_year}/llm.txt"

            logger.info(f"Processing specific filing: {gcs_path}")
            populator.process_filing(gcs_path)
        else:
            # Process all filings for the company
            logger.info(f"Processing all filings for {args.ticker}")
            populator.process_company(args.ticker)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1
    finally:
        # Close the database connection
        populator.close_connection()

    return 0

if __name__ == '__main__':
    exit(main())
