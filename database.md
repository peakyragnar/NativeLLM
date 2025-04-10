# SEC Filings Database Schema for LLM/AI Analysis

This document outlines the PostgreSQL database schema for storing SEC filing data processed by the NativeLLM pipeline. The schema is optimized for LLM and AI API interaction, focusing on simplicity, semantic clarity, and direct access patterns.

## Overview

The database uses a denormalized approach with two main tables:

1. **Companies** - Basic information about each company
2. **Financial Data** - A denormalized table containing all financial data points with clear, descriptive columns
3. **Text Blocks** - Textual content from the filings

This structure is designed to be easily queryable by LLMs and AI APIs without requiring complex joins or knowledge of relational database concepts.

## Schema Definition

```sql
-- Companies table
CREATE TABLE companies (
    ticker VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    cik VARCHAR(20),
    sic_code VARCHAR(10),
    sic_description TEXT,
    fiscal_year_end DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Financial data table - denormalized for easy LLM/AI access
CREATE TABLE financial_data (
    id SERIAL PRIMARY KEY,

    -- Company and filing information
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    filing_type VARCHAR(10) NOT NULL, -- 10-K, 10-Q, etc.
    fiscal_year INTEGER NOT NULL,
    fiscal_period VARCHAR(10) NOT NULL, -- annual, Q1, Q2, Q3
    filing_date DATE,
    accession_number VARCHAR(50),

    -- Financial data point information
    statement_type VARCHAR(50) NOT NULL, -- Balance Sheet, Income Statement, Cash Flow, etc.
    concept VARCHAR(255) NOT NULL, -- The financial concept or line item
    value_text VARCHAR(255) NOT NULL, -- Keeping as VARCHAR to preserve formatting like "$1,234"
    value_numeric DECIMAL(30,2), -- Numeric representation for calculations
    unit VARCHAR(20), -- usd, shares, etc.

    -- Context information
    context_id VARCHAR(100), -- e.g., c-21, c-22
    context_label VARCHAR(255), -- e.g., "As of 2024-09-28"
    date_type VARCHAR(20), -- INSTANT or DURATION

    -- Parsed dates for easier querying
    as_of_date DATE, -- For point-in-time contexts (INSTANT)
    period_start_date DATE, -- For duration contexts
    period_end_date DATE, -- For all contexts (either the INSTANT date or the end of DURATION)

    -- Additional metadata
    taxonomy_prefix VARCHAR(50), -- e.g., us-gaap, dei
    is_normalized BOOLEAN DEFAULT TRUE,

    -- Constraints
    FOREIGN KEY (ticker) REFERENCES companies(ticker),
    UNIQUE (ticker, filing_type, fiscal_year, fiscal_period, statement_type, concept, context_id)
);

-- Text blocks table
CREATE TABLE text_blocks (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    filing_type VARCHAR(10) NOT NULL, -- 10-K, 10-Q, etc.
    fiscal_year INTEGER NOT NULL,
    fiscal_period VARCHAR(10) NOT NULL, -- annual, Q1, Q2, Q3
    filing_date DATE,
    block_id VARCHAR(50) NOT NULL, -- e.g., tb-1
    title TEXT,
    content TEXT,
    FOREIGN KEY (ticker) REFERENCES companies(ticker),
    UNIQUE (ticker, filing_type, fiscal_year, fiscal_period, block_id)
);

-- Create indexes for performance
CREATE INDEX idx_financial_data_ticker ON financial_data(ticker);
CREATE INDEX idx_financial_data_filing_type ON financial_data(filing_type);
CREATE INDEX idx_financial_data_fiscal_year ON financial_data(fiscal_year);
CREATE INDEX idx_financial_data_fiscal_period ON financial_data(fiscal_period);
CREATE INDEX idx_financial_data_statement_type ON financial_data(statement_type);
CREATE INDEX idx_financial_data_concept ON financial_data(concept);
CREATE INDEX idx_financial_data_period_end_date ON financial_data(period_end_date);
CREATE INDEX idx_financial_data_as_of_date ON financial_data(as_of_date);
CREATE INDEX idx_text_blocks_ticker ON text_blocks(ticker);

-- Composite indexes for common query patterns
CREATE INDEX idx_financial_data_ticker_concept ON financial_data(ticker, concept);
CREATE INDEX idx_financial_data_ticker_statement ON financial_data(ticker, statement_type);
CREATE INDEX idx_financial_data_ticker_dates ON financial_data(ticker, period_end_date);
```

## Benefits of This Schema for LLM/AI Interaction

1. **Simplified Structure** - The denormalized design reduces the need for complex joins, making it easier for LLMs to generate correct queries.

2. **Semantic Clarity** - Column names are descriptive and intuitive, helping LLMs understand the data model without extensive context.

3. **Direct Access Pattern** - Common queries like "Show me Apple's revenue for Q3 2023" can be executed with simple WHERE clauses.

4. **Parsed Dates** - Date information is extracted from context labels and stored in dedicated columns, making temporal queries straightforward.

5. **Preserved Formatting** - Original value formatting is maintained in `value_text` while also providing `value_numeric` for calculations.

6. **Complete Data** - All essential information from the original files is preserved, including text blocks.

## Example Queries for LLM/AI Interaction

```sql
-- Get a company's revenue over time
SELECT fiscal_year, fiscal_period, value_text, period_end_date
FROM financial_data
WHERE ticker = 'AAPL'
  AND concept = 'Revenue'
  AND statement_type = 'Income Statement'
ORDER BY period_end_date;

-- Compare total assets across companies for the most recent quarter
SELECT ticker, value_text, as_of_date
FROM financial_data
WHERE concept = 'Assets'
  AND statement_type = 'Balance Sheet'
  AND fiscal_period = 'Q3'
  AND fiscal_year = 2023;

-- Get management discussion text block
SELECT content
FROM text_blocks
WHERE ticker = 'TSLA'
  AND fiscal_year = 2023
  AND fiscal_period = 'annual'
  AND title LIKE '%Management Discussion%';
```

## Implementation Notes

To populate this database from the existing SEC filing text files, a parser script will need to:

1. Extract company information and create records in the `companies` table
2. For each filing:
   - Extract metadata (ticker, filing_type, fiscal_year, fiscal_period, etc.)
   - Parse the normalized financial data section
   - For each data point, create a record in the `financial_data` table
   - Parse context labels to extract dates for `as_of_date`, `period_start_date`, and `period_end_date`
   - Extract text blocks and create records in the `text_blocks` table

The parser should handle value formatting, converting values to numeric when possible while preserving the original text format.

This schema is optimized for LLM/AI interaction while maintaining all the essential information from your SEC filing data.
