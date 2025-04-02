import sys
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)

# Add the project directory to the path
sys.path.append('/Users/michael/NativeLLM')

# Import our context extractor
from src2.formatter.context_extractor import extract_contexts_from_html

# Path to the TSLA filing
file_path = 'sec_processed/tmp/sec_downloads/TSLA/10-K/000162828024002390/tsla-20231231.htm'

# Test the extraction
if os.path.exists(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        html_content = f.read()
        
    contexts = extract_contexts_from_html(html_content, {'fiscal_year': '2023'})
    
    print(f"Found {len(contexts)} contexts")
    
    # Print details for a few contexts
    context_ids = list(contexts.keys())[:5]
    print(f"First few context IDs: {context_ids}")
    
    # Print details for c-1
    if 'c-1' in contexts:
        context = contexts['c-1']
        print("\nDetails for c-1:")
        for key, value in context.items():
            print(f"  {key}: {value}")
else:
    print(f"File not found: {file_path}")
