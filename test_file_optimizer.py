#!/usr/bin/env python3
"""
Test the file size optimizer on the Tesla 10-K file.
"""

import os
import sys
import logging
from src2.formatter.file_size_optimizer import FileSizeOptimizer

def main():
    """
    Main function.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Check command line arguments
    if len(sys.argv) != 3:
        logger.error("Usage: python test_file_optimizer.py <input_file> <output_file>")
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

    # Optimize file
    optimizer = FileSizeOptimizer()
    optimized_content = optimizer.optimize(content)

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
