# SEC Filings Database Populator

This script populates a PostgreSQL database with SEC filing data stored in Google Cloud Storage.

## Prerequisites

1. A PostgreSQL database with the schema defined in `sec_filings_schema.sql`
2. Google Cloud Storage bucket containing processed SEC filings
3. Python 3.7+ with required packages installed

## Installation

Install the required packages:

```bash
pip install -r requirements_db.txt
```

Make sure you have authenticated with Google Cloud:

```bash
gcloud auth application-default login
```

## Usage

### Process All Filings for a Company

```bash
python populate_sec_database.py --ticker AAPL
```

### Process a Specific Annual Filing

```bash
python populate_sec_database.py --ticker AAPL --filing-type 10-K --fiscal-year 2023
```

### Process a Specific Quarterly Filing

```bash
python populate_sec_database.py --ticker AAPL --filing-type 10-Q --fiscal-year 2023 --fiscal-period Q1
```

## Database Schema

The script populates three tables:

1. `companies` - Basic information about each company
2. `financial_data` - Financial data points from the filings
3. `text_blocks` - Textual content from the filings

For more details on the schema, see `database.md`.

## Example Queries

### Get a Company's Revenue Over Time

```sql
SELECT fiscal_year, fiscal_period, value_text, period_end_date 
FROM financial_data 
WHERE ticker = 'AAPL' 
  AND concept = 'Revenue' 
  AND statement_type = 'Income Statement' 
ORDER BY period_end_date;
```

### Compare Total Assets Across Companies

```sql
SELECT ticker, value_text, as_of_date 
FROM financial_data 
WHERE concept = 'Assets' 
  AND statement_type = 'Balance Sheet' 
  AND fiscal_period = 'Q3' 
  AND fiscal_year = 2023;
```

### Get Management Discussion Text Block

```sql
SELECT content 
FROM text_blocks 
WHERE ticker = 'TSLA' 
  AND fiscal_year = 2023 
  AND fiscal_period = 'annual' 
  AND title LIKE '%Management Discussion%';
```
