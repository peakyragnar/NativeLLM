#!/usr/bin/env python3
"""
Check revenue data in the database.
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

# Check revenue data for all years
print("=== Revenue Data by Year ===")
query = """
SELECT ticker, fiscal_year, fiscal_period, concept, value_numeric, value_text
FROM financial_data
WHERE concept = 'Revenues'
ORDER BY ticker, fiscal_year, fiscal_period
"""
df = pd.read_sql_query(query, conn)
print(df)
print()

# Check NVDA revenue data
print("=== NVDA Revenue Data ===")
query = """
SELECT ticker, fiscal_year, fiscal_period, concept, value_numeric, value_text
FROM financial_data
WHERE ticker = 'NVDA' AND concept = 'Revenues'
ORDER BY fiscal_year, fiscal_period
"""
df = pd.read_sql_query(query, conn)
print(df)
print()

# Check total revenue by year for all companies
print("=== Total Revenue by Year ===")
query = """
SELECT fiscal_year, SUM(value_numeric) as total_revenue
FROM financial_data
WHERE concept = 'Revenues'
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
df = pd.read_sql_query(query, conn)
print(df)
print()

# Close the connection
conn.close()
