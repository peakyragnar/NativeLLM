# SEC Filings SQL Agent

This advanced example shows how to build a sophisticated text-to-SQL system that leverages Reasoning Agents to provide deep insights into SEC filings data. The system is designed to analyze financial metrics, trends, and textual content from SEC filings.

The agent uses Reasoning Agents to search for table metadata and rules, enabling it to write and run better SQL queries. This process, called `Dynamic Few Shot Prompting`, is a technique that allows the agent to dynamically search for few shot examples to improve its performance.

## Example Queries

- "Show NVIDIA's revenue growth over the last 4 quarters"
- "Compare gross margins between NVIDIA, Apple, and Tesla"
- "What was NVIDIA's quarter-over-quarter revenue growth in fiscal year 2025?"
- "Show me all mentions of AI or artificial intelligence in NVIDIA's filings"
- "Which company had the highest R&D expenses in 2023?"
- "Calculate NVIDIA's gross margin percentage over time"

## Database Schema

The SEC Filings database consists of three main tables:

1. **financial_data**: Contains financial metrics from SEC filings
2. **text_blocks**: Contains textual content from SEC filings
3. **companies**: Contains basic information about companies

### 1. Create a virtual environment

```shell
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install libraries

```shell
pip install -r sec_sql_agent/requirements.txt
```

### 3. Set up PostgreSQL with PgVector

We're using a Google Cloud SQL PostgreSQL instance with the pgvector extension. You'll need to set up the extension on your database:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

Then, add a vector column to the text_blocks table for semantic search:

```sql
ALTER TABLE text_blocks ADD COLUMN content_embedding vector(1536);
CREATE INDEX text_blocks_embedding_idx ON text_blocks USING ivfflat (content_embedding vector_cosine_ops);
```

### 4. Load the knowledge base

The knowledge base contains table metadata, rules and sample queries, which are used by the Agent to improve responses. This is a dynamic few shot prompting technique. This data, stored in the `sec_sql_agent/knowledge/` folder, is used by the Agent at run-time to search for sample queries and rules.

We've already added the following to the knowledge base:
  - Table metadata for `financial_data`, `text_blocks`, and `companies` tables
  - Table rules and column rules to guide the Agent in writing better queries
  - Sample SQL queries for common financial analyses

You can customize the knowledge base by:
  - Adding more `table_rules` and `column_rules` to the table metadata
  - Adding more sample SQL queries to the `sec_sql_agent/knowledge/sample_queries.sql` file

```shell
python sec_sql_agent/load_knowledge.py
```

### 5. Export API Keys

We recommend using claude-3-7-sonnet for this task, but you can use any Model you like.

```shell
export ANTHROPIC_API_KEY=***
```

Other API keys are optional, but if you'd like to test:

```shell
export OPENAI_API_KEY=***
```

### 6. Run SQL Agent

```shell
streamlit run sec_sql_agent/app.py
```

- Open [localhost:8501](http://localhost:8501) to view the SQL Agent.

### 7. Example Usage

Once the SQL Agent is running, you can ask questions like:

- "What was NVIDIA's revenue in the most recent quarter?"
- "Show me the trend of NVIDIA's gross margin over the last 4 quarters"
- "Compare the assets of NVIDIA, Apple, and Tesla in their most recent filings"
- "Find all mentions of AI in NVIDIA's management discussion"

The agent will:
1. Convert your question to SQL
2. Execute the query against the database
3. Analyze the results and provide insights
4. Show you the SQL query it used

### 8. Customization

You can customize the agent by:
1. Adding more sample queries to the knowledge base
2. Updating table metadata and rules
3. Adding more companies and filings to the database

