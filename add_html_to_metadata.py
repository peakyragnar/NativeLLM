#!/usr/bin/env python3
"""
Add the HTML content to metadata for context extraction.
Apply this fix to the src2/sec/pipeline.py file.
"""

import os
import sys
import re

def main():
    """
    Apply the fix to the pipeline.py file
    """
    pipeline_path = "src2/sec/pipeline.py"
    
    # Make sure the file exists
    if not os.path.exists(pipeline_path):
        print(f"Error: File {pipeline_path} not found")
        return 1
    
    # Read the file
    with open(pipeline_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Define the pattern and replacement
    pattern = r'with open\(doc_path, \'r\', encoding=\'utf-8\'\) as f:\s+html_content = f\.read\(\)\s+soup = BeautifulSoup\(html_content, \'html.parser\'\)'
    replacement = """with open(doc_path, 'r', encoding='utf-8') as f:
                                html_content = f.read()
                            
                            # Add raw HTML to metadata for context extraction
                            if 'html_content' not in metadata:
                                metadata['html_content'] = {}
                            metadata['html_content']['raw_html'] = html_content
                            
                            soup = BeautifulSoup(html_content, 'html.parser')"""
    
    # Apply the replacement
    new_content = re.sub(pattern, replacement, content)
    
    if new_content == content:
        print("No changes made - pattern not found")
        return 1
    
    # Write the file
    with open(pipeline_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"Successfully updated {pipeline_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())