#!/usr/bin/env python3
"""
Simple Streamlit app for the SEC SQL Agent.

This is a simplified version of the app that doesn't use the Agno framework.
Instead, it uses a direct connection to the database and OpenAI/Anthropic APIs.
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional

import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from openai import OpenAI
from anthropic import Anthropic

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

# Custom CSS for dark mode support
CUSTOM_CSS = """
<style>
.main-title {
    font-size: 3rem !important;
    font-weight: 600 !important;
    margin-bottom: 0.5rem !important;
}
.subtitle {
    font-size: 1.5rem !important;
    font-weight: 400 !important;
    margin-bottom: 2rem !important;
}
</style>
"""

def get_connection():
    """Get a connection to the database."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def get_openai_response(messages, model="gpt-4o"):
    """Get a response from OpenAI."""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.0
    )

    return response.choices[0].message.content

def get_anthropic_response(messages, model="claude-3-sonnet-20240229"):
    """Get a response from Anthropic."""
    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    # Convert messages to Anthropic format
    anthropic_messages = []
    for message in messages:
        role = "user" if message["role"] == "user" else "assistant"
        anthropic_messages.append({"role": role, "content": message["content"]})

    response = client.messages.create(
        model=model,
        messages=anthropic_messages,
        temperature=0.0
    )

    return response.content[0].text

def generate_sql(question, provider="anthropic"):
    """Generate SQL from a natural language question."""
    # Get table metadata
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT content FROM sec_agent_knowledge WHERE content_type = 'table_metadata'"
        )
        table_metadata = "\n\n".join([row[0] for row in cursor.fetchall()])

        # Get sample queries
        cursor.execute(
            "SELECT content FROM sec_agent_knowledge WHERE content_type = 'sample_query' LIMIT 3"
        )
        sample_queries = "\n\n".join([row[0] for row in cursor.fetchall()])
    finally:
        conn.close()

    # Prepare the messages
    messages = [
        {"role": "system", "content": f"""You are a financial SQL expert specializing in SEC filings analysis.
Your task is to convert natural language questions into PostgreSQL queries.

Here is the database schema:
{table_metadata}

Here are some example queries:
{sample_queries}

Rules:
1. Always use standard PostgreSQL syntax
2. For financial metrics, use SUM() or AVG() as appropriate
3. For time-based analysis, prefer using period_end_date over fiscal_year/fiscal_period when possible
4. When comparing companies, join on ticker
5. Handle NULL values appropriately in financial data
6. For text analysis, use LIKE or ILIKE for simple pattern matching
7. Return only the SQL query without any explanation"""}
    ]

    # Add the user question
    messages.append({"role": "user", "content": f"Generate a SQL query for this question: {question}"})

    # Generate the response
    if provider == "anthropic":
        sql = get_anthropic_response(messages)
    else:
        sql = get_openai_response(messages)

    # Clean up the response
    sql = sql.strip()
    if sql.startswith("```sql"):
        sql = sql[7:]
    if sql.endswith("```"):
        sql = sql[:-3]

    return sql.strip()

def run_sql_query(sql):
    """Run a SQL query and return the results as a pandas DataFrame."""
    conn = get_connection()
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()

def analyze_results(question, results, provider="anthropic"):
    """Analyze the results of a SQL query."""
    # Convert results to string representation
    if hasattr(results, "to_string"):
        results_str = results.to_string()
    else:
        results_str = str(results)

    # Prepare the messages
    messages = [
        {"role": "system", "content": """You are a financial analyst specializing in SEC filings analysis.
Your task is to analyze the results of a SQL query and provide insights.

Guidelines:
1. Focus on key trends and patterns in the data
2. Highlight significant changes or anomalies
3. Provide context for financial metrics when possible
4. Be concise but thorough
5. Use bullet points for clarity when appropriate
6. If the data shows a clear trend, mention it explicitly
7. For financial data, consider growth rates, margins, and comparisons
8. If the results are empty or show an error, explain what might have happened"""}
    ]

    # Add the user question and results
    messages.append({"role": "user", "content": f"Question: {question}\n\nResults:\n{results_str}\n\nPlease analyze these results."})

    # Generate the response
    if provider == "anthropic":
        return get_anthropic_response(messages)
    else:
        return get_openai_response(messages)

def add_message(role, content):
    """Add a message to the session state."""
    if "messages" not in st.session_state:
        st.session_state["messages"] = []

    st.session_state["messages"].append({"role": role, "content": content})

def main():
    """Main function."""
    st.set_page_config(
        page_title="SEC Filings SQL Agent",
        page_icon="💰",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Load custom CSS
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    # App header
    st.markdown(
        "<h1 class='main-title'>SEC Filings SQL Agent</h1>", unsafe_allow_html=True
    )
    st.markdown(
        "<p class='subtitle'>Your intelligent SQL Agent for analyzing SEC filings data</p>",
        unsafe_allow_html=True,
    )

    # Sidebar
    st.sidebar.title("Settings")

    # Model selector
    provider = st.sidebar.selectbox(
        "Select a model provider",
        options=["anthropic", "openai"],
        index=0,
        key="provider_selector",
    )

    # Initialize messages
    if "messages" not in st.session_state:
        st.session_state["messages"] = []

    # Get user input
    if prompt := st.chat_input("👋 Ask me about SEC filings data!"):
        add_message("user", prompt)

    # Display chat history
    for message in st.session_state["messages"]:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Generate response for user message
    if st.session_state["messages"] and st.session_state["messages"][-1]["role"] == "user":
        question = st.session_state["messages"][-1]["content"]

        with st.chat_message("assistant"):
            with st.spinner("🤔 Thinking..."):
                try:
                    # Generate SQL
                    sql = generate_sql(question, provider)

                    # Run the query
                    results = run_sql_query(sql)

                    # Display the results
                    st.subheader("Generated SQL:")
                    st.code(sql, language="sql")

                    st.subheader("Query Results:")
                    st.dataframe(results)

                    # Try to create a chart if appropriate
                    if len(results) > 0 and results.select_dtypes(include=['number']).shape[1] > 0:
                        try:
                            # Find date-like columns
                            date_cols = [col for col in results.columns if 'date' in col.lower() or 'period' in col.lower()]

                            # Find numeric columns
                            numeric_cols = results.select_dtypes(include=['number']).columns.tolist()

                            if date_cols and numeric_cols:
                                st.subheader("Visualization:")
                                fig = px.line(results, x=date_cols[0], y=numeric_cols[0], title=f"{numeric_cols[0]} over time")
                                st.plotly_chart(fig)
                        except Exception as e:
                            logger.warning(f"Could not create chart: {e}")

                    # Analyze the results
                    analysis = analyze_results(question, results, provider)

                    st.subheader("Analysis:")
                    st.markdown(analysis)

                    # Add the response to the chat history
                    response = f"### Generated SQL:\n```sql\n{sql}\n```\n\n### Analysis:\n{analysis}"
                    add_message("assistant", response)

                except Exception as e:
                    error_message = f"Sorry, I encountered an error: {str(e)}"
                    st.error(error_message)
                    add_message("assistant", error_message)

    # About section
    st.sidebar.markdown("---")
    st.sidebar.title("About")
    st.sidebar.info(
        """
        This app uses a SQL Agent to analyze SEC filings data.

        It converts natural language questions into SQL queries,
        runs them against a PostgreSQL database, and analyzes the results.

        Built with Streamlit, OpenAI, and Anthropic APIs.
        """
    )

if __name__ == "__main__":
    main()
