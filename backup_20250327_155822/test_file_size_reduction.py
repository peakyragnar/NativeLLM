#!/usr/bin/env python
# Test file size reduction for HTML optimization
# This script measures the size reduction on a real file

import os
import sys
import re
import logging
import argparse
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.xbrl.xbrl_parser import extract_text_only_from_html

def test_file_size_reduction(file_path):
    """Test the size reduction on a real file"""
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return False
        
    try:
        # Only test LLM format files
        if not file_path.endswith('_llm.txt'):
            print(f"Error: Not an LLM format file: {file_path}")
            return False
            
        # Load the file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        original_size = len(content)
        print(f"Original file size: {original_size:,} bytes")
        
        # Find all @VALUE sections that might contain HTML
        html_pattern = r'(@VALUE: )(.*?)(\n@|$)'
        value_sections = re.findall(html_pattern, content, re.DOTALL)
        
        print(f"Found {len(value_sections)} value sections")
        
        # If no value sections, we can't proceed with the test
        if not value_sections:
            print("No value sections found in file. Skipping test.")
            return True  # Return success since there's nothing to optimize
        
        # Count how many contain HTML
        html_count = 0
        total_html_size = 0
        for match in value_sections:
            value = match[1]
            if '<' in value and '>' in value:
                html_count += 1
                total_html_size += len(value)
                
        print(f"HTML value sections: {html_count} ({html_count/len(value_sections)*100:.1f}% of all values)")
        
        # If no HTML content, we can't proceed with the test
        if total_html_size == 0:
            print("No HTML content found in value sections. Skipping test.")
            return True  # Return success since there's nothing to optimize
            
        print(f"Total HTML content size: {total_html_size:,} bytes ({total_html_size/original_size*100:.1f}% of file)")
        
        # Test optimization on HTML sections
        new_content = content
        cleaned_html_size = 0
        
        # Sample a subset of HTML values for the test
        sample_size = min(html_count, 50)  # Process up to 50 HTML sections
        html_sections = []
        
        for match in value_sections:
            prefix = match[0]
            value = match[1]
            suffix = match[2]
            
            if '<' in value and '>' in value:
                html_sections.append((prefix, value, suffix))
                
                if len(html_sections) >= sample_size:
                    break
        
        # Process the sampled HTML sections
        processed_count = 0
        size_reduction = 0
        
        for prefix, value, suffix in html_sections:
            processed = extract_text_only_from_html(value)
            cleaned_html_size += len(processed)
            size_reduction += len(value) - len(processed)
            processed_count += 1
            
        # Calculate percentage reduction
        if processed_count > 0:
            avg_reduction = size_reduction / processed_count
            html_reduction_pct = size_reduction / total_html_size * 100 if total_html_size > 0 else 0
            estimated_total_reduction = html_reduction_pct * total_html_size / original_size if original_size > 0 else 0
            
            print(f"\nProcessed {processed_count} HTML sections")
            print(f"Average size reduction per HTML section: {avg_reduction:.1f} bytes")
            print(f"HTML content reduction: {html_reduction_pct:.2f}%")
            print(f"Estimated total file reduction: {estimated_total_reduction:.2f}%")
            
            # Determine if the test passed - either significant reduction or no HTML to reduce
            result = html_reduction_pct >= 25.0 or total_html_size < 1000  # Require at least 25% HTML reduction unless minimal HTML
            
            if result:
                print("\n✅ Size reduction test PASSED")
                print(f"HTML reduction achieved ({html_reduction_pct:.2f}%) exceeds the minimum threshold (25%)")
            else:
                print("\n❌ Size reduction test FAILED")
                print(f"HTML reduction achieved ({html_reduction_pct:.2f}%) below the minimum threshold (25%)")
                
            return result
        else:
            print("\n⚠️ No HTML sections processed")
            return False
            
    except Exception as e:
        print(f"Error testing file size reduction: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

def main():
    parser = argparse.ArgumentParser(description="Test file size reduction for HTML optimization")
    parser.add_argument('file', help='File to test size reduction on')
    
    args = parser.parse_args()
    
    result = test_file_size_reduction(args.file)
    return 0 if result else 1

if __name__ == "__main__":
    sys.exit(main())