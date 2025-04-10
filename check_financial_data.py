#!/usr/bin/env python3
"""
Check financial data in the database.
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

# Check financial_data table structure
print("=== Financial Data Table Structure ===")
cursor = conn.cursor()
cursor.execute("""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'financial_data'
ORDER BY ordinal_position
""")
columns = cursor.fetchall()
for col in columns:
    print(f"{col[0]}: {col[1]}")
print()

# Check sample data for 2023 and 2024
print("=== Sample Financial Data for 2023-2024 ===")
query = """
SELECT ticker, fiscal_year, fiscal_period, concept, value_numeric
FROM financial_data
WHERE fiscal_year IN (2023, 2024) AND concept LIKE '%revenue%'
ORDER BY ticker, fiscal_year, fiscal_period
LIMIT 20
"""
df = pd.read_sql_query(query, conn)
print(df)
print()

# Check total records by year
print("=== Record Counts by Year ===")
query = """
SELECT fiscal_year, COUNT(*) as record_count
FROM financial_data
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
df = pd.read_sql_query(query, conn)
print(df)
print()

# Check available metrics
print("=== Available Revenue Metrics ===")
query = """
SELECT DISTINCT concept
FROM financial_data
WHERE concept LIKE '%revenue%'
ORDER BY concept
"""
df = pd.read_sql_query(query, conn)
print(df)
print()

# Close the connection
conn.close()
