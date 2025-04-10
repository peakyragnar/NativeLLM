#!/usr/bin/env python3
"""
Test the regex-based financial tables optimizer on the Tesla 10-K file.
"""

import os
import sys
import re
import logging
import time

def optimize_financial_tables(content: str) -> str:
    """
    Optimize financial tables by reducing excessive delimiters.
    This works for all companies and maintains the original dash structure.
    
    Args:
        content: The content to optimize
        
    Returns:
        The optimized content with reduced delimiters
    """
    # Use a more efficient regex approach that works for all companies
    # This preserves the original dash structure while reducing file size
    
    # First, replace patterns like |-|-|-| with |-|
    optimized_content = re.sub(r'\|-\|-+\|', '|-|', content)
    
    # Then, replace patterns like |-|-|-|-|-| with |-|
    optimized_content = re.sub(r'\|-\|-+', '|-', optimized_content)
    
    # Return the optimized content
    return optimized_content

def main():
    """
    Main function.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Check command line arguments
    if len(sys.argv) != 3:
        logger.error("Usage: python test_regex_optimizer_v2.py <input_file> <output_file>")
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
