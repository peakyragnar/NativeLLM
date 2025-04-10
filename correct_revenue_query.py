#!/usr/bin/env python3
"""
Correct revenue query for 2023 and 2024.
"""

import psycopg2
import pandas as pd

# Database connection parameters
DB_HOST = "35.205.197.130"
DB_NAME = "sec_filings"
DB_USER = "postgres"
DB_PASSWORD = "secfilings2024"
DB_PORT = "5432"

# Connect to the database
conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)

# Correct query for total revenue by company for 2023 and 2024
print("=== Total Revenue by Company for 2023-2024 ===")
query = """
WITH quarterly_revenue AS (
    SELECT 
        ticker,
        fiscal_year,
        fiscal_period,
        SUM(value_numeric) as quarterly_total
    FROM financial_data
    WHERE 
        concept = 'Revenues'
        AND fiscal_year IN (2023, 2024)
    GROUP BY ticker, fiscal_year, fiscal_period
)
SELECT 
    ticker,
    fiscal_year,
    SUM(quarterly_total) as annual_revenue
FROM quarterly_revenue
GROUP BY ticker, fiscal_year
ORDER BY ticker, fiscal_year
"""
df = pd.read_sql_query(query, conn)
print(df)
print()

# Close the connection
conn.close()
