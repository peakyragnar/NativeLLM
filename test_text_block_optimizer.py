#!/usr/bin/env python3
"""
Test script for the text block optimizer.
"""

import os
import sys
import argparse
import logging
from src2.formatter.text_block_optimizer import optimize_text_blocks

def main():
    """
    Main function.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test the text block optimizer')
    parser.add_argument('--input', required=True, help='Path to the input file')
    parser.add_argument('--output', required=True, help='Path to the output file')
    args = parser.parse_args()

    # Check if input file exists
    if not os.path.exists(args.input):
        logger.error(f"Input file {args.input} does not exist")
        return 1

    # Read input file
    with open(args.input, 'r', encoding='utf-8') as f:
        content = f.read()

    # Get original file size
    original_size = len(content)
    logger.info(f"Original file size: {original_size / 1024:.2f} KB")

    # Optimize text blocks
    optimized_content = optimize_text_blocks(content)

    # Get optimized file size
    optimized_size = len(optimized_content)
    logger.info(f"Optimized file size: {optimized_size / 1024:.2f} KB")

    # Calculate size reduction
    size_reduction = (original_size - optimized_size) / original_size * 100
    logger.info(f"Size reduction: {size_reduction:.2f}%")

    # Write output file
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(optimized_content)

    logger.info(f"Optimized file saved to {args.output}")
    return 0

if __name__ == '__main__':
    sys.exit(main())
