#!/usr/bin/env python3
"""
Add vector embeddings to text blocks in the SEC filings database.
"""

import os
import sys
import argparse
import logging
from typing import List, Dict, Any

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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

def setup_vector_extension():
    """Set up the vector extension in the database."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        # Create the vector extension if it doesn't exist
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
        conn.commit()
        logger.info("Vector extension created or already exists")
    except Exception as e:
        logger.error(f"Error setting up vector extension: {e}")
    finally:
        conn.close()

def add_vector_column():
    """Add a vector column to the text_blocks table if it doesn't exist."""
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # Check if the column exists
        cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'text_blocks' AND column_name = 'content_embedding'
        """)

        if cursor.fetchone() is None:
            # Column doesn't exist, so add it
            cursor.execute("ALTER TABLE text_blocks ADD COLUMN content_embedding vector(1536)")
            conn.commit()
            logger.info("Added vector column content_embedding to text_blocks")
        else:
            logger.info("Vector column content_embedding already exists in text_blocks")
    except Exception as e:
        logger.error(f"Error adding vector column: {e}")
    finally:
        conn.close()

def create_vector_index():
    """Create an index for vector similarity search."""
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # Check if the index exists
        cursor.execute("""
        SELECT indexname
        FROM pg_indexes
        WHERE tablename = 'text_blocks' AND indexname = 'text_blocks_embedding_idx'
        """)

        if cursor.fetchone() is None:
            # Index doesn't exist, so create it
            cursor.execute("""
            CREATE INDEX text_blocks_embedding_idx ON text_blocks
            USING ivfflat (content_embedding vector_cosine_ops)
            """)
            conn.commit()
            logger.info("Created vector index text_blocks_embedding_idx")
        else:
            logger.info("Vector index text_blocks_embedding_idx already exists")
    except Exception as e:
        logger.error(f"Error creating vector index: {e}")
    finally:
        conn.close()

def get_openai_embeddings(texts: List[str], model="text-embedding-ada-002") -> List[List[float]]:
    """Get embeddings from OpenAI."""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    # Process in batches if needed
    if isinstance(texts, str):
        texts = [texts]

    response = client.embeddings.create(
        model=model,
        input=texts
    )

    return [item.embedding for item in response.data]

def get_text_blocks_without_embeddings(limit=100, ticker=None):
    """Get text blocks that don't have embeddings."""
    conn = get_connection()
    try:
        if ticker:
            query = """
            SELECT id, content
            FROM text_blocks
            WHERE content IS NOT NULL
            AND content_embedding IS NULL
            AND ticker = %s
            LIMIT %s
            """
            df = pd.read_sql_query(query, conn, params=(ticker, limit))
        else:
            query = """
            SELECT id, content
            FROM text_blocks
            WHERE content IS NOT NULL
            AND content_embedding IS NULL
            LIMIT %s
            """
            df = pd.read_sql_query(query, conn, params=(limit,))
        return df
    finally:
        conn.close()

def update_embeddings(batch_size=10, ticker=None):
    """Update embeddings for text blocks in batches."""
    # Get text blocks without embeddings
    df = get_text_blocks_without_embeddings(batch_size, ticker)

    if len(df) == 0:
        logger.info("No text blocks to update")
        return 0

    logger.info(f"Updating embeddings for {len(df)} text blocks")

    # Generate embeddings
    texts = df['content'].tolist()
    embeddings = get_openai_embeddings(texts)

    # Update the database
    conn = get_connection()
    try:
        cursor = conn.cursor()
        for i, row_id in enumerate(df['id']):
            embedding_array = embeddings[i]
            cursor.execute(
                "UPDATE text_blocks SET content_embedding = %s WHERE id = %s",
                (embedding_array, row_id)
            )

        conn.commit()
        logger.info(f"Updated embeddings for {len(df)} text blocks")
        return len(df)

    except Exception as e:
        logger.error(f"Error updating embeddings: {e}")
        return 0
    finally:
        conn.close()

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Add vector embeddings to text blocks in the SEC filings database.')
    parser.add_argument('--setup', action='store_true', help='Set up the vector extension and column')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for updating embeddings')
    parser.add_argument('--max-batches', type=int, default=10, help='Maximum number of batches to process')
    parser.add_argument('--ticker', type=str, help='Process only text blocks for a specific ticker (e.g., NVDA)')

    args = parser.parse_args()

    # Check if OpenAI API key is set
    if not os.environ.get("OPENAI_API_KEY"):
        logger.error("OPENAI_API_KEY environment variable not set")
        return 1

    # Set up the vector extension and column if requested
    if args.setup:
        setup_vector_extension()
        add_vector_column()
        create_vector_index()
        return 0

    # Update embeddings in batches
    total_updated = 0
    for _ in range(args.max_batches):
        updated = update_embeddings(args.batch_size, args.ticker)
        total_updated += updated

        if updated == 0:
            break

    if args.ticker:
        logger.info(f"Total text blocks updated for {args.ticker}: {total_updated}")
    else:
        logger.info(f"Total text blocks updated: {total_updated}")
    return 0

if __name__ == '__main__':
    sys.exit(main())
