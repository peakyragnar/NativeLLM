# LLM-Native Financial Data Project

This project extracts financial data from SEC XBRL filings and converts it to an LLM-optimized format without imposing traditional financial frameworks or biases.

## Project Overview

The pipeline:
1. Retrieves raw XBRL data directly from SEC EDGAR
2. Preserves all data exactly as reported by companies
3. Converts it to a format optimized for LLM consumption
4. Avoids imposing traditional financial frameworks or interpretations

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Update the `USER_AGENT` in `src/config.py` with your information.

## Usage

1. Set up project directories:
```bash
python run_pipeline.py --setup
```

2. Process a single company to test:
```bash
python run_pipeline.py --company AAPL
```

3. Process initial companies from config:
```bash
python run_pipeline.py --initial
```

4. Process top N companies in parallel:
```bash
python run_pipeline.py --top 10 --workers 3
```

## LLM Queries

You can query processed filings with an LLM:
```bash
python query_llm.py --ticker AAPL --filing_type 10-Q --question "What was the revenue for the most recent quarter and how does it compare to the previous quarter?"
```

## Testing

Run the test pipeline:
```bash
python test_pipeline.py
```

Examine processed LLM files:
```bash
python test_llm_integration.py --list
python test_llm_integration.py --file data/processed/AAPL/AAPL_10-K_20230930_llm.txt
```

## Project Structure

- `src/`: Source code for the project
  - `edgar/`: SEC EDGAR access modules
  - `xbrl/`: XBRL processing modules
  - `formatter/`: LLM format generation
- `data/`: Data storage
  - `raw/`: Downloaded XBRL files
  - `processed/`: Generated LLM format files
- `run_pipeline.py`: Main entry point
- `test_pipeline.py`: Test the pipeline
- `test_llm_integration.py`: Test LLM integration
- `query_llm.py`: Query tool for LLMs