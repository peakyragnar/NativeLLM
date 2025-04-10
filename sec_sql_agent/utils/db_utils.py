"""
Database utilities for the SEC SQL Agent.
"""

import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from pgvector.sqlalchemy import Vector
import numpy as np

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

def get_sqlalchemy_engine():
    """Get a SQLAlchemy engine for the database."""
    return create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

def execute_query(query, params=None):
    """Execute a query and return the results as a pandas DataFrame."""
    conn = get_connection()
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

def execute_statement(statement, params=None):
    """Execute a statement (INSERT, UPDATE, DELETE, etc.)."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(statement, params)
        conn.commit()
    finally:
        conn.close()

def get_table_schema(table_name):
    """Get the schema for a table."""
    query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = %s
    ORDER BY ordinal_position
    """
    return execute_query(query, (table_name,))

def get_table_sample(table_name, limit=5):
    """Get a sample of data from a table."""
    query = f"SELECT * FROM {table_name} LIMIT {limit}"
    return execute_query(query)

def setup_vector_extensions():
    """Set up the vector extensions in the database."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        # Create the vector extension if it doesn't exist
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
        conn.commit()
    except Exception as e:
        print(f"Error setting up vector extensions: {e}")
    finally:
        conn.close()

def add_vector_column_if_not_exists(table_name, column_name, dimensions=1536):
    """Add a vector column to a table if it doesn't exist."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        
        # Check if the column exists
        cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND column_name = '{column_name}'
        """)
        
        if cursor.fetchone() is None:
            # Column doesn't exist, so add it
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} vector({dimensions})")
            conn.commit()
            print(f"Added vector column {column_name} to {table_name}")
        else:
            print(f"Vector column {column_name} already exists in {table_name}")
    except Exception as e:
        print(f"Error adding vector column: {e}")
    finally:
        conn.close()

def create_vector_index(table_name, column_name, index_name=None):
    """Create an index for vector similarity search."""
    if index_name is None:
        index_name = f"{table_name}_{column_name}_idx"
    
    conn = get_connection()
    try:
        cursor = conn.cursor()
        
        # Check if the index exists
        cursor.execute(f"""
        SELECT indexname 
        FROM pg_indexes 
        WHERE tablename = '{table_name}' AND indexname = '{index_name}'
        """)
        
        if cursor.fetchone() is None:
            # Index doesn't exist, so create it
            cursor.execute(f"""
            CREATE INDEX {index_name} ON {table_name} 
            USING ivfflat ({column_name} vector_cosine_ops)
            """)
            conn.commit()
            print(f"Created vector index {index_name} on {table_name}.{column_name}")
        else:
            print(f"Vector index {index_name} already exists")
    except Exception as e:
        print(f"Error creating vector index: {e}")
    finally:
        conn.close()

def update_embeddings(table_name, column_name, text_column, embedding_function, batch_size=100):
    """Update embeddings for a text column in batches."""
    conn = get_connection()
    try:
        # Get rows with text but no embedding
        query = f"""
        SELECT id, {text_column} 
        FROM {table_name} 
        WHERE {text_column} IS NOT NULL 
        AND ({column_name} IS NULL OR {column_name} = '{{}}')
        LIMIT {batch_size}
        """
        
        df = pd.read_sql_query(query, conn)
        
        if len(df) == 0:
            print(f"No rows to update in {table_name}")
            return 0
        
        print(f"Updating embeddings for {len(df)} rows in {table_name}")
        
        # Generate embeddings
        texts = df[text_column].tolist()
        embeddings = embedding_function(texts)
        
        # Update the database
        cursor = conn.cursor()
        for i, row_id in enumerate(df['id']):
            embedding_array = embeddings[i]
            cursor.execute(
                f"UPDATE {table_name} SET {column_name} = %s WHERE id = %s",
                (embedding_array, row_id)
            )
        
        conn.commit()
        return len(df)
    
    except Exception as e:
        print(f"Error updating embeddings: {e}")
        return 0
    finally:
        conn.close()

def vector_search(table_name, column_name, query_embedding, limit=5):
    """Perform a vector similarity search."""
    query = f"""
    SELECT *, ({column_name} <=> %s) as distance
    FROM {table_name}
    ORDER BY {column_name} <=> %s
    LIMIT {limit}
    """
    
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, (query_embedding, query_embedding))
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        return pd.DataFrame(results, columns=columns)
    finally:
        conn.close()
