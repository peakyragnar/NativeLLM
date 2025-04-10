#!/usr/bin/env python3
"""
Load knowledge base for the SEC SQL Agent.

This script loads the knowledge base for the SEC SQL Agent, including table metadata,
rules, and sample queries.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path

import psycopg2
import pandas as pd
from openai import OpenAI

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

def setup_knowledge_table():
    """Set up the knowledge table in the database."""
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # Create the vector extension if it doesn't exist
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")

        # Create the knowledge table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sec_agent_knowledge (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            content_type VARCHAR(50) NOT NULL,
            content_embedding vector(1536),
            metadata JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Create an index for vector similarity search
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS sec_agent_knowledge_embedding_idx
        ON sec_agent_knowledge USING ivfflat (content_embedding vector_cosine_ops)
        """)

        conn.commit()
        logger.info("Knowledge table created or already exists")
    except Exception as e:
        logger.error(f"Error setting up knowledge table: {e}")
    finally:
        conn.close()

def get_openai_embeddings(texts, model="text-embedding-3-small"):
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

def load_table_metadata():
    """Load table metadata from JSON files."""
    knowledge_dir = Path(__file__).parent / "knowledge"
    metadata_files = list(knowledge_dir.glob("*.json"))

    conn = get_connection()
    try:
        cursor = conn.cursor()

        for file_path in metadata_files:
            with open(file_path, "r") as f:
                metadata = json.load(f)

            table_name = metadata.get("table_name")
            if not table_name:
                logger.warning(f"No table_name found in {file_path}")
                continue

            # Convert metadata to string for embedding
            metadata_str = json.dumps(metadata, indent=2)

            # Get embedding
            embedding = get_openai_embeddings(metadata_str)[0]

            # Check if this table metadata already exists
            cursor.execute(
                "SELECT id FROM sec_agent_knowledge WHERE content_type = 'table_metadata' AND metadata->>'table_name' = %s",
                (table_name,)
            )
            existing = cursor.fetchone()

            if existing:
                # Update existing metadata
                cursor.execute(
                    """UPDATE sec_agent_knowledge
                    SET content = %s, content_embedding = %s, metadata = %s
                    WHERE id = %s""",
                    (metadata_str, embedding, json.dumps(metadata), existing[0])
                )
                logger.info(f"Updated metadata for table {table_name}")
            else:
                # Insert new metadata
                cursor.execute(
                    """INSERT INTO sec_agent_knowledge
                    (content, content_type, content_embedding, metadata)
                    VALUES (%s, %s, %s, %s)""",
                    (metadata_str, "table_metadata", embedding, json.dumps(metadata))
                )
                logger.info(f"Inserted metadata for table {table_name}")

        conn.commit()
    except Exception as e:
        logger.error(f"Error loading table metadata: {e}")
    finally:
        conn.close()

def load_sample_queries():
    """Load sample queries from SQL files."""
    knowledge_dir = Path(__file__).parent / "knowledge"
    query_files = list(knowledge_dir.glob("*.sql"))

    conn = get_connection()
    try:
        cursor = conn.cursor()

        for file_path in query_files:
            with open(file_path, "r") as f:
                content = f.read()

            # Parse the queries
            queries = []
            current_description = ""
            current_query = ""
            in_description = False
            in_query = False

            for line in content.split("\n"):
                if line.strip() == "-- <query description>":
                    in_description = True
                    current_description = ""
                elif line.strip() == "-- </query description>":
                    in_description = False
                elif line.strip() == "-- <query>":
                    in_query = True
                    current_query = ""
                elif line.strip() == "-- </query>":
                    in_query = False
                    if current_description and current_query:
                        queries.append({
                            "description": current_description.strip(),
                            "query": current_query.strip()
                        })
                elif in_description:
                    current_description += line.lstrip("-- ").lstrip("--") + "\n"
                elif in_query:
                    current_query += line + "\n"

            # Insert each query into the knowledge base
            for query_data in queries:
                description = query_data["description"]
                query = query_data["query"]

                # Combine description and query for embedding
                content = f"Description: {description}\n\nQuery: {query}"

                # Get embedding
                embedding = get_openai_embeddings(content)[0]

                # Check if this query already exists
                cursor.execute(
                    "SELECT id FROM sec_agent_knowledge WHERE content_type = 'sample_query' AND metadata->>'description' = %s",
                    (description,)
                )
                existing = cursor.fetchone()

                metadata = {
                    "description": description,
                    "query": query,
                    "source": file_path.name
                }

                if existing:
                    # Update existing query
                    cursor.execute(
                        """UPDATE sec_agent_knowledge
                        SET content = %s, content_embedding = %s, metadata = %s
                        WHERE id = %s""",
                        (content, embedding, json.dumps(metadata), existing[0])
                    )
                    logger.info(f"Updated sample query: {description}")
                else:
                    # Insert new query
                    cursor.execute(
                        """INSERT INTO sec_agent_knowledge
                        (content, content_type, content_embedding, metadata)
                        VALUES (%s, %s, %s, %s)""",
                        (content, "sample_query", embedding, json.dumps(metadata))
                    )
                    logger.info(f"Inserted sample query: {description}")

        conn.commit()
    except Exception as e:
        logger.error(f"Error loading sample queries: {e}")
    finally:
        conn.close()

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Load knowledge base for the SEC SQL Agent.')
    parser.add_argument('--recreate', action='store_true', help='Recreate the knowledge table')

    args = parser.parse_args()

    # Check if OpenAI API key is set
    if not os.environ.get("OPENAI_API_KEY"):
        logger.error("OPENAI_API_KEY environment variable not set")
        return 1

    # Set up the knowledge table
    setup_knowledge_table()

    # If recreate is specified, clear the knowledge table
    if args.recreate:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM sec_agent_knowledge")
            conn.commit()
            logger.info("Knowledge table cleared")
        except Exception as e:
            logger.error(f"Error clearing knowledge table: {e}")
        finally:
            conn.close()

    # Load table metadata
    load_table_metadata()

    # Load sample queries
    load_sample_queries()

    logger.info("Knowledge base loaded successfully")
    return 0

if __name__ == "__main__":
    sys.exit(main())
