#!/usr/bin/env python3
"""
Extract XBRL Facts from HTML Files

This script extracts XBRL facts from HTML files with inline XBRL.
"""

import os
import sys
import json
import logging
import argparse
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_facts_from_html(html_file, output_file):
    """
    Extract XBRL facts from an HTML file with inline XBRL.

    Args:
        html_file: Path to the HTML file
        output_file: Path to the output file (JSON)
    """
    logging.info(f"Extracting facts from {html_file}")

    # Load HTML file
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all XBRL facts
    facts = []

    # Find all elements with contextRef attribute (numeric and non-numeric facts)
    fact_elements = soup.find_all(attrs={'contextref': True})

    for element in fact_elements:
        # Extract fact attributes
        name = element.name
        context_ref = element.get('contextref')
        unit_ref = element.get('unitref')
        decimals = element.get('decimals')
        scale = element.get('scale')
        format = element.get('format')

        # For inline XBRL, get the concept name from the name attribute
        concept = element.get('name')

        # Extract fact value
        value = element.get_text(strip=True)

        # Create fact object
        fact = {
            'name': concept if concept else name,  # Use concept name if available
            'element': name,  # Store the element name separately
            'contextRef': context_ref,
            'unitRef': unit_ref,
            'decimals': decimals,
            'scale': scale,
            'format': format,
            'value': value
        }

        # Remove None values
        fact = {k: v for k, v in fact.items() if v is not None}

        facts.append(fact)

    logging.info(f"Extracted {len(facts)} facts")

    # Save facts to output file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(facts, f, indent=2)

    logging.info(f"Facts saved to {output_file}")

    return facts

def main():
    parser = argparse.ArgumentParser(description="Extract XBRL facts from HTML files")
    parser.add_argument("--html", required=True, help="Path to the HTML file")
    parser.add_argument("--output", required=True, help="Path to the output file (JSON)")

    args = parser.parse_args()

    # Check if HTML file exists
    if not os.path.exists(args.html):
        logging.error(f"HTML file not found: {args.html}")
        return 1

    # Extract facts
    extract_facts_from_html(args.html, args.output)

    return 0

if __name__ == "__main__":
    sys.exit(main())
