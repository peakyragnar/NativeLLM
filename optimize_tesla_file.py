#!/usr/bin/env python3
"""
Optimize Tesla 10-K file by reducing the number of delimiters in financial tables.
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

    # Read input file
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    original_size = len(content)
    print(f'Original file size: {original_size / 1024:.2f} KB')

    # Replace consecutive |-|-|-| patterns with a compact representation
    # This is a more efficient approach than using regex for large files
    optimized_content = ''
    i = 0
    while i < len(content):
        # Check if we have a |-|-|-| pattern
        if content[i:i+3] == '|-|':
            # Count how many |-| patterns we have
            j = i
            count = 0
            while j + 2 < len(content) and content[j:j+3] == '|-|':
                count += 1
                j += 2  # Skip to the next possible |-| pattern

            # Replace with a compact representation (just a dash)
            if count > 2:
                optimized_content += '|-|'  # Just use a single dash
                i = j + 1  # Skip past all the |-| patterns
            else:
                optimized_content += content[i:i+3]
                i += 3
        else:
            optimized_content += content[i]
            i += 1

    optimized_size = len(optimized_content)
    print(f'Optimized file size: {optimized_size / 1024:.2f} KB')

    # Calculate size reduction
    size_reduction = (original_size - optimized_size) / original_size * 100
    print(f'Size reduction: {size_reduction:.2f}%')

    # Write output file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(optimized_content)

    end_time = time.time()
    print(f'Optimization completed in {end_time - start_time:.2f} seconds')
    print(f'Optimized file saved to {output_file}')

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python optimize_tesla_file.py <input_file> <output_file>')
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    optimize_tesla_file(input_file, output_file)
