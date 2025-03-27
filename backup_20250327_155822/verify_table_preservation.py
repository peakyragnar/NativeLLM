#!/usr/bin/env python3
# verify_table_preservation.py
import os
import sys
import re
import time
import logging
from bs4 import BeautifulSoup
import difflib
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.xbrl.xbrl_parser import extract_text_only_from_html, process_table_safely

def extract_table_content(table_html, include_structure=False):
    """Extract all text content from a table"""
    soup = BeautifulSoup(table_html, 'html.parser')
    
    # Get all words in the table
    all_words = re.findall(r'[A-Za-z]+', soup.get_text())
    
    # Get all numeric values
    all_numbers = re.findall(r'\b\d[\d,.]*\b|(?:\$[\d,.]+)', soup.get_text())
    
    # Get all header texts
    headers = []
    for th in soup.find_all('th'):
        headers.append(th.get_text(strip=True))
    
    # Get header-like td elements
    for td in soup.find_all('td', style=lambda s: s and ('bold' in s.lower() or 'font-weight' in s.lower())):
        headers.append(td.get_text(strip=True))
    
    # Get all rows as lists of cells for structural verification
    rows = []
    if include_structure:
        for tr in soup.find_all('tr'):
            row = []
            for cell in tr.find_all(['td', 'th']):
                row.append(cell.get_text(strip=True))
            if row:
                rows.append(row)
    
    return {
        'words': all_words,
        'numbers': all_numbers,
        'headers': headers,
        'rows': rows,
    }

def main():
    """Main function to verify table preservation"""
    print("=== Table Preservation Verification ===\n")
    
    # Try to load a sample file
    sample_file = "/Users/michael/NativeLLM/data/processed/AAPL/Apple_Inc_2024_FY_AAPL_10-K_20240928_llm.txt"
    if not os.path.exists(sample_file):
        print(f"Sample file not found: {sample_file}")
        return
    
    print(f"Using file: {sample_file}")
    print("-" * 60)
    
    try:
        with open(sample_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract all tables
        tables = re.findall(r'<table[\s\S]+?</table>', content)
        
        if not tables:
            print("No tables found in file")
            return
        
        print(f"Found {len(tables)} tables in file")
        
        # Data collection for summary
        total_original_size = 0
        total_processed_size = 0
        results = []
        
        # Process a sample of tables for detailed verification
        table_sample = tables[:10]  # Adjust number as needed
        
        for i, table in enumerate(table_sample):
            print(f"\nVerifying Table {i+1}/{len(table_sample)}")
            print("-" * 50)
            
            # Measure original table
            original_size = len(table)
            total_original_size += original_size
            
            # Extract original content
            original_content = extract_table_content(table, include_structure=True)
            
            # Process table
            start_time = time.time()
            processed = process_table_safely(table)
            processing_time = time.time() - start_time
            
            # Measure processed size
            processed_size = len(processed)
            total_processed_size += processed_size
            
            # Extract processed content (either from HTML or text)
            if '<' in processed and '>' in processed:
                # Result is still HTML
                processed_content = extract_table_content(processed, include_structure=True)
                was_preserved = True  # Original HTML was kept
            else:
                # Result is plain text format
                processed_text = processed
                
                # For text format, extract content differently
                processed_content = {
                    'words': re.findall(r'[A-Za-z]+', processed_text),
                    'numbers': re.findall(r'\b\d[\d,.]*\b|(?:\$[\d,.]+)', processed_text),
                    'headers': [],  # We'll populate from identified headers
                    'rows': []      # No row structure in text
                }
                
                # Try to reconstruct headers from the text format
                header_section = re.search(r'HEADERS:(.*?)(?:DATA:|$)', processed_text, re.DOTALL)
                if header_section:
                    header_lines = header_section.group(1).strip().split('\n')
                    for line in header_lines:
                        processed_content['headers'].extend([h.strip() for h in line.split('|') if h.strip()])
                
                was_preserved = False  # HTML was replaced with text
            
            # Calculate content preservation metrics
            words_original = set(original_content['words'])
            words_processed = set(processed_content['words'])
            numbers_original = set(original_content['numbers'])
            numbers_processed = set(processed_content['numbers'])
            headers_original = set(original_content['headers'])
            headers_processed = set(processed_content['headers'])
            
            # Calculate missing content
            missing_words = words_original - words_processed
            missing_numbers = numbers_original - numbers_processed
            missing_headers = headers_original - headers_processed
            
            # Calculate preservation percentages
            word_preservation = len(words_processed.intersection(words_original)) / len(words_original) if words_original else 1.0
            number_preservation = len(numbers_processed.intersection(numbers_original)) / len(numbers_original) if numbers_original else 1.0
            header_preservation = len(headers_processed.intersection(headers_original)) / len(headers_original) if headers_original else 1.0
            
            # Calculate overall integrity score (weighted)
            integrity_score = (word_preservation * 0.4 + 
                               number_preservation * 0.4 + 
                               header_preservation * 0.2)
            
            # Calculate size reduction
            size_reduction = 1 - (processed_size / original_size)
            
            # Display results
            print(f"Size: {original_size:,} → {processed_size:,} bytes ({size_reduction:.2%} reduction)")
            print(f"Processing time: {processing_time:.4f} seconds")
            print(f"HTML preserved: {'Yes' if was_preserved else 'No'}")
            print(f"Word preservation: {word_preservation:.2%} ({len(words_original)} → {len(words_processed)})")
            print(f"Number preservation: {number_preservation:.2%} ({len(numbers_original)} → {len(numbers_processed)})")
            print(f"Header preservation: {header_preservation:.2%} ({len(headers_original)} → {len(headers_processed)})")
            print(f"Overall integrity: {integrity_score:.2%}")
            
            if missing_words:
                print(f"Missing words ({len(missing_words)}): {list(missing_words)[:5]}")
            if missing_numbers:
                print(f"Missing numbers ({len(missing_numbers)}): {list(missing_numbers)[:5]}")
            if missing_headers:
                print(f"Missing headers ({len(missing_headers)}): {list(missing_headers)[:5]}")
            
            # Store results for summary
            results.append({
                'table_num': i + 1,
                'original_size': original_size,
                'processed_size': processed_size,
                'size_reduction': size_reduction,
                'word_preservation': word_preservation,
                'number_preservation': number_preservation,
                'header_preservation': header_preservation,
                'integrity_score': integrity_score,
                'html_preserved': was_preserved,
                'missing_words_count': len(missing_words),
                'missing_numbers_count': len(missing_numbers),
                'missing_headers_count': len(missing_headers),
            })
        
        # Display summary
        print("\n=== SUMMARY ===\n")
        print(f"{'Table':<8} {'Size Reduction':<15} {'Word Presrv':<12} {'Number Presrv':<14} {'Header Presrv':<14} {'Integrity':<10}")
        print("-" * 75)
        
        total_integrity = 0
        html_preserved_count = 0
        
        for res in results:
            print(f"{res['table_num']:<8} {res['size_reduction']:.2%}{' ':12} {res['word_preservation']:.2%}{' ':7} "
                  f"{res['number_preservation']:.2%}{' ':9} {res['header_preservation']:.2%}{' ':9} "
                  f"{res['integrity_score']:.2%}")
            
            total_integrity += res['integrity_score']
            if res['html_preserved']:
                html_preserved_count += 1
        
        overall_size_reduction = 1 - (total_processed_size / total_original_size)
        avg_integrity = total_integrity / len(results) if results else 0
        
        print("-" * 75)
        print(f"OVERALL: {overall_size_reduction:.2%} size reduction, {avg_integrity:.2%} avg integrity")
        print(f"HTML preserved for {html_preserved_count}/{len(results)} tables")
        
        # Process entire file and measure total reduction
        print("\n=== PROCESSING ENTIRE FILE ===\n")
        
        # Create a copy of the content
        processed_content = content
        table_count = 0
        processed_table_count = 0
        total_file_original_size = len(content)
        
        # Replace each table with its processed version
        for table in tables:
            table_count += 1
            processed_table = process_table_safely(table)
            
            if processed_table != table:
                processed_table_count += 1
                processed_content = processed_content.replace(table, processed_table)
        
        # Measure final size
        total_file_processed_size = len(processed_content)
        total_file_reduction = 1 - (total_file_processed_size / total_file_original_size)
        
        print(f"Original file: {total_file_original_size:,} bytes")
        print(f"Processed file: {total_file_processed_size:,} bytes")
        print(f"Total size reduction: {total_file_reduction:.2%}")
        print(f"Processed tables: {processed_table_count}/{table_count}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()