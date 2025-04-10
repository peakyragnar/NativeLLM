#!/usr/bin/env python3
"""
Optimize Tesla 10-K file by reducing the number of delimiters in financial tables.
This version uses a more efficient approach by processing the file line by line.
"""

import re
import os
import sys
import time

def optimize_tesla_file(input_file, output_file):
    """
    Optimize Tesla 10-K file by reducing the number of delimiters in financial tables.
    
    Args:
        input_file: Path to the input file
        output_file: Path to the output file
    """
    print(f'Optimizing {input_file}...')
    start_time = time.time()
    
    # Process the file line by line to avoid loading the entire file into memory
    with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
        original_size = 0
        optimized_size = 0
        line_count = 0
        
        for line in f_in:
            original_size += len(line)
            line_count += 1
            
            # Check if this is a financial data line with many |-| patterns
            if '|-|' in line and line.count('|-|') > 5:
                # Use regex to replace consecutive |-| patterns with a single |-|
                optimized_line = re.sub(r'(\|-\|)(\|-\|)+', r'|-|', line)
                f_out.write(optimized_line)
                optimized_size += len(optimized_line)
            else:
                # Keep non-financial lines as they are
                f_out.write(line)
                optimized_size += len(line)
    
    print(f'Processed {line_count} lines')
    print(f'Original file size: {original_size / 1024:.2f} KB')
    print(f'Optimized file size: {optimized_size / 1024:.2f} KB')
    
    # Calculate size reduction
    size_reduction = (original_size - optimized_size) / original_size * 100
    print(f'Size reduction: {size_reduction:.2f}%')
    
    end_time = time.time()
    print(f'Optimization completed in {end_time - start_time:.2f} seconds')
    print(f'Optimized file saved to {output_file}')

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python optimize_tesla_file_v2.py <input_file> <output_file>')
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    optimize_tesla_file(input_file, output_file)
