#!/usr/bin/env python3
"""
Extract text from SEC iXBRL document using headless browser

This script demonstrates the extraction of text from SEC iXBRL documents
that require JavaScript rendering to access the content.
"""

import os
import sys
import logging
import argparse
import asyncio
from src2.processor.ixbrl_extractor import extract_text_from_ixbrl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def main():
    """Main function to run the script"""
    parser = argparse.ArgumentParser(description="Extract text from SEC iXBRL document")
    
    # Required argument for SEC URL
    parser.add_argument("url", help="SEC iXBRL URL to extract (e.g., https://www.sec.gov/ix?doc=/Archives/...)")
    
    # Optional arguments
    parser.add_argument("--output", "-o", help="Output file to save extracted text (defaults to stdout)")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode (default: headless)")
    parser.add_argument("--wait", type=int, default=15000, 
                      help="Wait time in milliseconds for iXBRL rendering (default: 15000)")
    
    args = parser.parse_args()
    
    # Print banner
    print(f"\n{'=' * 80}")
    print(f"SEC iXBRL Extractor".center(80))
    print(f"{'=' * 80}")
    print(f"URL: {args.url}")
    print(f"Mode: {'Visible browser' if args.visible else 'Headless mode'}")
    print(f"Wait time: {args.wait}ms")
    if args.output:
        print(f"Output: {args.output}")
    print(f"{'=' * 80}\n")
    
    try:
        # Run extraction
        print("Starting extraction...")
        extracted_text = extract_text_from_ixbrl(
            args.url,
            output_file=args.output,
            headless=not args.visible,
            wait_time=args.wait
        )
        
        # Output results
        if not args.output:
            print("\nExtracted text preview:\n")
            # Show first 30 lines
            preview_lines = extracted_text.split('\n')[:30]
            print('\n'.join(preview_lines))
            line_count = len(extracted_text.split('\n'))
            print(f"\n... (total {line_count} lines, {len(extracted_text)} characters)")
        
        print(f"\nExtraction complete! Extracted {len(extracted_text)} characters")
        if args.output:
            print(f"Full text saved to: {args.output}")
        
    except Exception as e:
        logging.error(f"Error extracting iXBRL: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())