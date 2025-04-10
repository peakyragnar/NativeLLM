#!/usr/bin/env python3
"""
Check text blocks in the database.
"""

import psycopg2

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

# Create a cursor
cursor = conn.cursor()

# Check if there are any text blocks with content
cursor.execute("""
SELECT id, ticker, content 
FROM text_blocks 
WHERE content IS NOT NULL AND length(content) > 0 
LIMIT 5
""")

rows = cursor.fetchall()

if rows:
    print(f"Found {len(rows)} text blocks with content:")
    for row in rows:
        print(f"ID: {row[0]}, Ticker: {row[1]}")
        print(f"Content type: {type(row[2])}")
        print(f"Content length: {len(row[2])}")
        print(f"Content preview: {row[2][:100]}...")
        print()
else:
    print("No text blocks with content found")

# Close the connection
conn.close()
