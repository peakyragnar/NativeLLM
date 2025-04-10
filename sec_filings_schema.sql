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
