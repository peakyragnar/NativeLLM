"""
Test script to verify that contexts are properly added to the LLM output for Tesla filings
"""

import os
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_context_in_llm_output():
    """Test that contexts are properly added to the LLM output for Tesla filings"""
    
    # Define paths for test files
    sec_processed_dir = os.path.join(os.getcwd(), 'sec_processed')
    tsla_dir = os.path.join(sec_processed_dir, 'TSLA')
    
    # Try to find Tesla LLM output files in the sec_processed directory
    tsla_llm_files = []
    for root, dirs, files in os.walk(tsla_dir):
        for file in files:
            if file.endswith('_llm.txt'):
                tsla_llm_files.append(os.path.join(root, file))
    
    if not tsla_llm_files:
        logging.warning("No Tesla LLM output files found in sec_processed directory")
        return
    
    # Process each Tesla LLM file
    for file_path in tsla_llm_files:
        logging.info(f"Testing context information in {file_path}")
        
        # Read the LLM file
        with open(file_path, 'r', encoding='utf-8') as f:
            llm_content = f.read()
        
        # Check for context dictionary section
        context_section_match = re.search(r'@DATA_DICTIONARY: CONTEXTS(.*?)@UNITS_AND_SCALING', llm_content, re.DOTALL)
        if not context_section_match:
            logging.error(f"Context dictionary section not found in {file_path}")
            continue
        
        context_section = context_section_match.group(1)
        
        # Count context entries
        context_count = len(re.findall(r'c-\d+', context_section))
        logging.info(f"Found {context_count} context entries in {file_path}")
        
        # Check for context entries with periods
        period_context_count = len(re.findall(r'Period:', context_section))
        instant_context_count = len(re.findall(r'Instant:', context_section))
        
        logging.info(f"  - Period contexts: {period_context_count}")
        logging.info(f"  - Instant contexts: {instant_context_count}")
        
        # Extract some sample context entries
        sample_contexts = re.findall(r'(c-\d+.*?)(?=c-\d+|\Z)', context_section, re.DOTALL)[:5]
        
        logging.info("Sample of context entries:")
        for context in sample_contexts:
            logging.info(f"  {context.strip()}")
        
        # Check financial data section to see if contexts are being used
        financial_section_match = re.search(r'@SECTION: FINANCIAL_DATA(.*?)@SECTION:', llm_content, re.DOTALL)
        if financial_section_match:
            financial_section = financial_section_match.group(1)
            context_refs = re.findall(r'(c-\d+):', financial_section)
            unique_context_refs = set(context_refs)
            
            logging.info(f"Found {len(unique_context_refs)} unique context references in financial data")
            logging.info(f"Total context references: {len(context_refs)}")
        else:
            logging.warning(f"Financial data section not found in {file_path}")

if __name__ == "__main__":
    test_context_in_llm_output()