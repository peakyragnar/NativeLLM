"""
LLM utilities for the SEC SQL Agent.
"""

import os
import json
from anthropic import Anthropic
from openai import OpenAI

def get_anthropic_client():
    """Get an Anthropic client."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    return Anthropic(api_key=api_key)

def get_openai_client():
    """Get an OpenAI client."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    return OpenAI(api_key=api_key)

def generate_anthropic_response(messages, model="claude-3-sonnet-20240229", temperature=0.0, max_tokens=4000):
    """Generate a response from Anthropic."""
    client = get_anthropic_client()
    
    response = client.messages.create(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens
    )
    
    return response.content[0].text

def generate_openai_response(messages, model="gpt-4", temperature=0.0, max_tokens=4000):
    """Generate a response from OpenAI."""
    client = get_openai_client()
    
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens
    )
    
    return response.choices[0].message.content

def generate_response(messages, provider="anthropic", model=None, temperature=0.0, max_tokens=4000):
    """Generate a response from the specified provider."""
    if provider.lower() == "anthropic":
        model = model or "claude-3-sonnet-20240229"
        return generate_anthropic_response(messages, model, temperature, max_tokens)
    elif provider.lower() == "openai":
        model = model or "gpt-4"
        return generate_openai_response(messages, model, temperature, max_tokens)
    else:
        raise ValueError(f"Unsupported provider: {provider}")

def generate_sql(question, table_metadata, sample_queries=None, provider="anthropic"):
    """Generate SQL from a natural language question."""
    # Prepare the messages
    messages = [
        {"role": "system", "content": f"""You are a financial SQL expert specializing in SEC filings analysis. 
Your task is to convert natural language questions into PostgreSQL queries.

Here is the database schema:
{table_metadata}

Rules:
1. Always use standard PostgreSQL syntax
2. For financial metrics, use SUM() or AVG() as appropriate
3. For time-based analysis, prefer using period_end_date over fiscal_year/fiscal_period when possible
4. When comparing companies, join on ticker
5. Handle NULL values appropriately in financial data
6. For text analysis, use LIKE or ILIKE for simple pattern matching
7. Return only the SQL query without any explanation
8. Do not use vector operations in your SQL unless specifically asked about text similarity"""}
    ]
    
    # Add sample queries if provided
    if sample_queries:
        messages[0]["content"] += f"\n\nHere are some example queries:\n{sample_queries}"
    
    # Add the user question
    messages.append({"role": "user", "content": f"Generate a SQL query for this question: {question}"})
    
    # Generate the response
    sql = generate_response(messages, provider=provider, temperature=0.0)
    
    # Clean up the response
    sql = sql.strip()
    if sql.startswith("```sql"):
        sql = sql[7:]
    if sql.endswith("```"):
        sql = sql[:-3]
    
    return sql.strip()

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
    return generate_response(messages, provider=provider, temperature=0.2)
