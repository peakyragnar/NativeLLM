#!/usr/bin/env python3
"""
Export XBRL Data Structure

This script extracts XBRL data from a filing and exports it in a readable format
to help understand the structure of the data.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from bs4 import BeautifulSoup
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_inline_xbrl(html_path):
    """
    Extract inline XBRL data from an HTML document.

    Args:
        html_path: Path to the HTML file

    Returns:
        Dictionary with XBRL data structure
    """
    xbrl_data = {
        "contexts": {},
        "units": {},
        "facts": [],
        "metadata": {
            "file_path": html_path,
            "file_size": os.path.getsize(html_path)
        }
    }
    
    try:
        logging.info(f"Extracting inline XBRL from document: {html_path}")
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Try different parsers to handle various document formats
        parsers_to_try = ['lxml-xml', 'lxml', 'html.parser']
        soup = None
        ix_tags = []
        
        for parser in parsers_to_try:
            try:
                logging.info(f"Trying parser: {parser}")
                soup = BeautifulSoup(html_content, parser)
                
                # Find all relevant iXBRL tags
                ix_tags = soup.find_all(['ix:nonnumeric', 'ix:nonfraction'])
                
                if ix_tags:
                    logging.info(f"Found {len(ix_tags)} iXBRL tags with parser: {parser}")
                    break
                else:
                    # Try with namespace wildcard
                    ix_tags = soup.find_all(re.compile(r'.*:nonnumeric$|.*:nonfraction$'))
                    if ix_tags:
                        logging.info(f"Found {len(ix_tags)} iXBRL tags with namespace wildcard using parser: {parser}")
                        break
            except Exception as e:
                logging.warning(f"Error with parser {parser}: {str(e)}")
        
        if not ix_tags:
            logging.warning("No iXBRL tags found with any parser.")
            
            # Try to find any tags with namespaces
            if soup:
                all_tags = soup.find_all()
                namespace_tags = [tag for tag in all_tags if ':' in tag.name]
                if namespace_tags:
                    logging.info(f"Found {len(namespace_tags)} tags with namespaces. Examples: {[tag.name for tag in namespace_tags[:5]]}")
                    
                    # Try to find tags that might be XBRL-related
                    xbrl_related = [tag for tag in namespace_tags if any(x in tag.name.lower() for x in ['xbrl', 'ix', 'fact', 'context', 'unit'])]
                    if xbrl_related:
                        logging.info(f"Found {len(xbrl_related)} potentially XBRL-related tags. Examples: {[tag.name for tag in xbrl_related[:5]]}")
        
        # Extract facts
        for tag in ix_tags:
            fact = {
                'name': tag.get('name'),
                'contextRef': tag.get('contextref'),
                'unitRef': tag.get('unitref'),
                'scale': tag.get('scale'),
                'format': tag.get('format'),
                'value': tag.get_text(strip=True),
                'tag_name': tag.name  # e.g., 'ix:nonfraction'
            }
            
            # Add other attributes
            for attr, value in tag.attrs.items():
                if attr not in ['name', 'contextref', 'unitref', 'scale', 'format']:
                    fact[attr] = value
            
            xbrl_data["facts"].append(fact)
        
        # Extract contexts
        context_tags = soup.find_all(['xbrli:context', 'context'])
        if not context_tags:
            context_tags = soup.find_all(re.compile(r'.*:context$'))
        
        for context in context_tags:
            context_id = context.get('id')
            if not context_id:
                continue
                
            context_data = {'id': context_id}
            
            # Extract entity information
            entity = context.find(['xbrli:entity', 'entity'])
            if entity:
                entity_data = {}
                identifier = entity.find(['xbrli:identifier', 'identifier'])
                if identifier:
                    entity_data['identifier'] = {
                        'scheme': identifier.get('scheme'),
                        'value': identifier.get_text(strip=True)
                    }
                
                segment = entity.find(['xbrli:segment', 'segment'])
                if segment:
                    segment_data = {}
                    for member in segment.find_all():
                        dimension = member.get('dimension')
                        if dimension:
                            segment_data[dimension] = member.get_text(strip=True)
                    entity_data['segment'] = segment_data
                
                context_data['entity'] = entity_data
            
            # Extract period information
            period = context.find(['xbrli:period', 'period'])
            if period:
                period_data = {}
                instant = period.find(['xbrli:instant', 'instant'])
                if instant:
                    period_data['instant'] = instant.get_text(strip=True)
                else:
                    start_date = period.find(['xbrli:startdate', 'startdate'])
                    end_date = period.find(['xbrli:enddate', 'enddate'])
                    if start_date and end_date:
                        period_data['startDate'] = start_date.get_text(strip=True)
                        period_data['endDate'] = end_date.get_text(strip=True)
                
                context_data['period'] = period_data
            
            xbrl_data["contexts"][context_id] = context_data
        
        # Extract units
        unit_tags = soup.find_all(['xbrli:unit', 'unit'])
        if not unit_tags:
            unit_tags = soup.find_all(re.compile(r'.*:unit$'))
        
        for unit in unit_tags:
            unit_id = unit.get('id')
            if not unit_id:
                continue
                
            measure = unit.find(['xbrli:measure', 'measure'])
            if measure:
                xbrl_data["units"][unit_id] = {
                    'measure': measure.get_text(strip=True)
                }
        
        # Add summary statistics
        xbrl_data["metadata"]["num_facts"] = len(xbrl_data["facts"])
        xbrl_data["metadata"]["num_contexts"] = len(xbrl_data["contexts"])
        xbrl_data["metadata"]["num_units"] = len(xbrl_data["units"])
        
        # Add fact type statistics
        fact_types = {}
        for fact in xbrl_data["facts"]:
            name = fact.get('name', '')
            if ':' in name:
                prefix, local_name = name.split(':', 1)
                if prefix not in fact_types:
                    fact_types[prefix] = []
                if local_name not in fact_types[prefix]:
                    fact_types[prefix].append(local_name)
        
        xbrl_data["metadata"]["fact_types"] = {prefix: len(names) for prefix, names in fact_types.items()}
        
        return xbrl_data
    
    except Exception as e:
        logging.error(f"Error extracting inline XBRL: {str(e)}")
        xbrl_data["metadata"]["error"] = str(e)
        return xbrl_data

def find_filing_html(ticker, filing_type, year, base_dir="/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads"):
    """
    Find the HTML file for a specific filing.
    
    Args:
        ticker: Company ticker
        filing_type: Filing type (e.g., 10-K, 10-Q)
        year: Filing year
        base_dir: Base directory for SEC downloads
        
    Returns:
        Path to the HTML file
    """
    ticker_dir = os.path.join(base_dir, ticker, filing_type)
    if not os.path.exists(ticker_dir):
        logging.error(f"Directory not found: {ticker_dir}")
        return None
    
    # List all accession directories
    accession_dirs = [d for d in os.listdir(ticker_dir) if os.path.isdir(os.path.join(ticker_dir, d))]
    if not accession_dirs:
        logging.error(f"No accession directories found in {ticker_dir}")
        return None
    
    # Try to find a filing for the specified year
    for accession in accession_dirs:
        accession_dir = os.path.join(ticker_dir, accession)
        
        # Look for HTML files
        html_files = [f for f in os.listdir(accession_dir) if f.endswith('.htm') or f.endswith('.html')]
        if not html_files:
            continue
        
        # Check if any HTML file contains the year in its name
        for html_file in html_files:
            if str(year) in html_file or f"{ticker.lower()}-{year}" in html_file.lower():
                return os.path.join(accession_dir, html_file)
        
        # If no file with year in name, just return the first HTML file that's not index.htm
        for html_file in html_files:
            if html_file != 'index.htm':
                return os.path.join(accession_dir, html_file)
    
    # If no suitable file found, return None
    logging.error(f"No suitable HTML file found for {ticker} {filing_type} {year}")
    return None

def main():
    parser = argparse.ArgumentParser(description="Export XBRL data structure from a filing")
    parser.add_argument("--ticker", required=True, help="Company ticker")
    parser.add_argument("--filing-type", default="10-K", help="Filing type (default: 10-K)")
    parser.add_argument("--year", required=True, help="Filing year")
    parser.add_argument("--output", help="Output file path (default: ticker_filing-type_year_xbrl_structure.json)")
    parser.add_argument("--html-path", help="Direct path to HTML file (overrides ticker/filing-type/year)")
    
    args = parser.parse_args()
    
    # Determine HTML file path
    html_path = args.html_path
    if not html_path:
        html_path = find_filing_html(args.ticker, args.filing_type, args.year)
    
    if not html_path or not os.path.exists(html_path):
        logging.error(f"HTML file not found: {html_path}")
        sys.exit(1)
    
    # Extract XBRL data
    xbrl_data = extract_inline_xbrl(html_path)
    
    # Determine output file path
    output_path = args.output
    if not output_path:
        output_path = f"{args.ticker}_{args.filing_type}_{args.year}_xbrl_structure.json"
    
    # Save XBRL data
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(xbrl_data, f, indent=2)
    
    logging.info(f"XBRL data structure saved to {output_path}")
    
    # Print summary
    print("\nXBRL Data Structure Summary:")
    print(f"  File: {html_path}")
    print(f"  Facts: {xbrl_data['metadata']['num_facts']}")
    print(f"  Contexts: {xbrl_data['metadata']['num_contexts']}")
    print(f"  Units: {xbrl_data['metadata']['num_units']}")
    
    if 'fact_types' in xbrl_data['metadata']:
        print("\nFact Types:")
        for prefix, count in xbrl_data['metadata']['fact_types'].items():
            print(f"  {prefix}: {count} unique concepts")
    
    # Print example facts
    if xbrl_data['facts']:
        print("\nExample Facts:")
        for i, fact in enumerate(xbrl_data['facts'][:5]):
            print(f"  Fact {i+1}:")
            print(f"    Name: {fact.get('name', 'N/A')}")
            print(f"    Value: {fact.get('value', 'N/A')}")
            print(f"    Context: {fact.get('contextRef', 'N/A')}")
            print(f"    Unit: {fact.get('unitRef', 'N/A')}")
    
    # Print example contexts
    if xbrl_data['contexts']:
        print("\nExample Contexts:")
        contexts = list(xbrl_data['contexts'].items())
        for i, (context_id, context_data) in enumerate(contexts[:3]):
            print(f"  Context {i+1}: {context_id}")
            period = context_data.get('period', {})
            if 'instant' in period:
                print(f"    Period: Instant {period['instant']}")
            elif 'startDate' in period and 'endDate' in period:
                print(f"    Period: {period['startDate']} to {period['endDate']}")
            
            entity = context_data.get('entity', {})
            if entity:
                identifier = entity.get('identifier', {})
                if identifier:
                    print(f"    Entity: {identifier.get('value', 'N/A')}")
                
                segment = entity.get('segment', {})
                if segment:
                    print(f"    Segment: {segment}")

if __name__ == "__main__":
    main()
