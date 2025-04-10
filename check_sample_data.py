#!/usr/bin/env python3
"""
Check sample data in the database.
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

# Check sample financial data
print("=== Sample Financial Data ===")
query = """
SELECT ticker, fiscal_year, fiscal_period, concept, value_numeric, value_text
FROM financial_data
LIMIT 20
"""
df = pd.read_sql_query(query, conn)
print(df)
print()

# Check available concepts
print("=== Available Concepts (Top 20) ===")
query = """
SELECT concept, COUNT(*) as count
FROM financial_data
GROUP BY concept
ORDER BY count DESC
LIMIT 20
"""
df = pd.read_sql_query(query, conn)
print(df)
print()

# Check NVDA data specifically
print("=== NVDA Financial Data Sample ===")
query = """
SELECT ticker, fiscal_year, fiscal_period, concept, value_numeric, value_text
FROM financial_data
WHERE ticker = 'NVDA'
LIMIT 20
"""
df = pd.read_sql_query(query, conn)
print(df)
print()

# Close the connection
conn.close()
