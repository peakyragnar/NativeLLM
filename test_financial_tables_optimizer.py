#!/usr/bin/env python3
"""
Test the financial tables optimizer on the Tesla 10-K file.
"""

import os
import sys
import logging
import time

def optimize_financial_tables(content: str) -> str:
    """
    Optimize financial tables by reducing excessive delimiters.
    This is particularly useful for files like Tesla's 10-K which have
    tables with many empty cells represented by |-|-|-| patterns.
    
    Args:
        content: The content to optimize
        
    Returns:
        The optimized content with reduced delimiters
    """
    print("Optimizing financial tables by reducing excessive delimiters")
    
    # Process the content line by line
    lines = content.split('\n')
    optimized_lines = []
    optimized_count = 0
    
    for line in lines:
        # Check if this is a financial data line with many |-| patterns
        if '|-|' in line and line.count('|-|') > 5:  # Threshold for optimization
            # Split the line by |
            parts = line.split('|')
            
            # Process each part
            new_parts = []
            i = 0
            while i < len(parts):
                if i < len(parts) - 1 and parts[i] == '-' and parts[i+1] == '-':
                    # Found a |-|-| pattern, skip consecutive dashes
                    new_parts.append('-')
                    i += 1
                    while i < len(parts) and parts[i] == '-':
                        i += 1
                else:
                    new_parts.append(parts[i])
                    i += 1
            
            # Join the parts back with | delimiter
            optimized_line = '|'.join(new_parts)
            optimized_lines.append(optimized_line)
            optimized_count += 1
        else:
            # Keep non-financial lines as they are
            optimized_lines.append(line)
    
    print(f"Optimized {optimized_count} financial table lines")
    
    # Join the lines back with newline delimiter
    return '\n'.join(optimized_lines)

def main():
    """
    Main function.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Check command line arguments
    if len(sys.argv) != 3:
        logger.error("Usage: python test_financial_tables_optimizer.py <input_file> <output_file>")
        return 1

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # Check if input file exists
    if not os.path.exists(input_file):
        logger.error(f"Input file {input_file} does not exist")
        return 1

    # Read input file
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Get original file size
    original_size = len(content)
    logger.info(f"Original file size: {original_size / 1024:.2f} KB")

    # Start timer
    start_time = time.time()

    # Optimize file
    optimized_content = optimize_financial_tables(content)

    # End timer
    end_time = time.time()
    logger.info(f"Optimization completed in {end_time - start_time:.2f} seconds")

    # Get optimized file size
    optimized_size = len(optimized_content)
    logger.info(f"Optimized file size: {optimized_size / 1024:.2f} KB")

    # Calculate size reduction
    size_reduction = (original_size - optimized_size) / original_size * 100
    logger.info(f"Size reduction: {size_reduction:.2f}%")

    # Write output file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(optimized_content)

    logger.info(f"Optimized file saved to {output_file}")
    return 0

if __name__ == '__main__':
    sys.exit(main())
