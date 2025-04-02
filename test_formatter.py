#!/usr/bin/env python3
"""
Test the LLMFormatter class
"""

import os
import sys
import traceback
from src2.formatter.llm_formatter import LLMFormatter

def main():
    """
    Main function to test the LLMFormatter
    """
    print("Testing LLMFormatter...")
    
    # Set up test data
    data = {
        'contexts': {},
        'units': {},
        'facts': []
    }
    
    metadata = {
        'ticker': 'TSLA',
        'filing_type': '10-K',
        'html_content': {
            'raw_html': '<html><body>Test</body></html>'
        }
    }
    
    try:
        formatter = LLMFormatter()
        result = formatter.generate_llm_format(data, metadata)
        print('Success!')
        print(f'Generated {len(result)} bytes of content')
    except Exception as e:
        print(f'Error: {type(e).__name__}: {str(e)}')
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())