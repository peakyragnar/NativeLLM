"""
Test integrated context extraction with the LLM formatter
"""

import os
import logging
import json
from src2.formatter.llm_formatter import LLMFormatter
from src2.formatter.context_extractor import extract_contexts_from_html

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_integrated_extraction():
    """Test the integrated context extraction with the LLM formatter"""
    
    # Find a Tesla filing to test with
    test_filing_path = os.path.join(os.getcwd(), 'sec_processed/tmp/sec_downloads/TSLA/10-K/000162828024002390/tsla-20231231.htm')
    
    if not os.path.exists(test_filing_path):
        logging.error(f"Test filing not found: {test_filing_path}")
        return
    
    # Read the filing
    with open(test_filing_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Extract contexts using our enhanced extractor
    contexts = extract_contexts_from_html(html_content)
    logging.info(f"Extracted {len(contexts)} contexts from {test_filing_path}")
    
    # Create a simple parsed XBRL structure with the extracted contexts
    parsed_xbrl = {
        "contexts": contexts,
        "units": {"USD": "US Dollars"},
        "facts": []
    }
    
    # Add some dummy facts for testing
    for i, context_id in enumerate(list(contexts.keys())[:10]):
        parsed_xbrl["facts"].append({
            "concept": f"TestConcept{i}",
            "value": str(1000 * (i + 1)),
            "unit_ref": "USD",
            "decimals": "-6",  # In millions
            "context_ref": context_id
        })
    
    # Create metadata
    filing_metadata = {
        "ticker": "TSLA",
        "filing_type": "10-K",
        "company_name": "Tesla, Inc.",
        "cik": "0001318605",
        "filing_date": "2024-01-31",
        "period_end_date": "2023-12-31",
        "fiscal_year": "2023",
        "fiscal_period": "FY",
        "html_content": {
            "raw_html": html_content
        }
    }
    
    # Initialize the formatter
    formatter = LLMFormatter()
    
    # Generate LLM format
    llm_output = formatter.generate_llm_format(parsed_xbrl, filing_metadata)
    
    # Save the output to a file
    output_path = os.path.join(os.getcwd(), 'test_tsla_llm_output.txt')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(llm_output)
    
    logging.info(f"LLM output saved to {output_path}")
    
    # Check for context dictionary in the output
    context_section_start = llm_output.find("@DATA_DICTIONARY: CONTEXTS")
    context_section_end = llm_output.find("@UNITS_AND_SCALING")
    
    if context_section_start != -1 and context_section_end != -1:
        context_section = llm_output[context_section_start:context_section_end]
        context_count = context_section.count("c-")
        logging.info(f"Found {context_count} context entries in the LLM output")
        
        # Show a small sample
        context_lines = context_section.split('\n')[:20]
        logging.info("Sample of context entries in LLM output:")
        for line in context_lines:
            if line.strip():
                logging.info(f"  {line.strip()}")
    else:
        logging.error("Context section not found in LLM output")

if __name__ == "__main__":
    test_integrated_extraction()