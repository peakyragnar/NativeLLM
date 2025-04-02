"""
Test script to verify enhanced context extraction for Tesla filings
"""

import os
import json
import logging
from src2.formatter.context_extractor import extract_contexts_from_html

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_tesla_context_extraction():
    """Test context extraction on Tesla filings"""
    
    # Define paths for test files
    sec_processed_dir = os.path.join(os.getcwd(), 'sec_processed')
    tsla_dir = os.path.join(sec_processed_dir, 'TSLA')
    
    # Try to find Tesla HTML files in the sec_processed directory
    tsla_files = []
    for root, dirs, files in os.walk(tsla_dir):
        for file in files:
            if file.endswith('.htm'):
                tsla_files.append(os.path.join(root, file))
    
    if not tsla_files:
        logging.warning("No Tesla HTML files found in sec_processed directory")
        
        # Try to find files in the tmp/sec_downloads directory
        tmp_dir = os.path.join(sec_processed_dir, 'tmp', 'sec_downloads', 'TSLA')
        if os.path.exists(tmp_dir):
            for root, dirs, files in os.walk(tmp_dir):
                for file in files:
                    if file.endswith('.htm'):
                        tsla_files.append(os.path.join(root, file))
    
    if not tsla_files:
        logging.error("No Tesla HTML files found for testing")
        return
    
    # Process each Tesla file
    for file_path in tsla_files:
        logging.info(f"Testing context extraction on {file_path}")
        
        # Read the HTML file
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Extract contexts using our enhanced extractor
        contexts = extract_contexts_from_html(html_content)
        
        # Log results
        logging.info(f"Extracted {len(contexts)} contexts from {file_path}")
        
        # Save contexts to a JSON file for inspection
        base_name = os.path.basename(file_path).split('.')[0]
        output_path = os.path.join(os.getcwd(), f"tesla_contexts_{base_name}.json")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(contexts, f, indent=2)
        
        logging.info(f"Saved contexts to {output_path}")
        
        # Display a sample of contexts
        logging.info("Sample of extracted contexts:")
        sample_size = min(5, len(contexts))
        sample_contexts = list(contexts.items())[:sample_size]
        
        for context_id, context_info in sample_contexts:
            period_info = context_info.get("period", {})
            if "startDate" in period_info and "endDate" in period_info:
                logging.info(f"Context {context_id}: Period from {period_info['startDate']} to {period_info['endDate']}")
            elif "instant" in period_info:
                logging.info(f"Context {context_id}: Instant at {period_info['instant']}")

if __name__ == "__main__":
    test_tesla_context_extraction()