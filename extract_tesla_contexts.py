#\!/usr/bin/env python3
"""
Extract Context Information from Tesla Filings

This standalone script extracts and displays context information from Tesla XBRL filings
without requiring the full NativeLLM pipeline to be working.
"""

import os
import sys
import re
import json
import logging
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_contexts_from_html(html_content, filing_metadata=None):
    """Extract context information from HTML content using regex patterns"""
    # Multiple patterns to find context elements with their period information
    # First try with exact XBRL namespace
    context_pattern = re.compile(r'<xbrli:context id="([^"]+)"[^>]*>.*?<xbrli:period>(.*?)<\/xbrli:period>', re.DOTALL)
    
    # If that fails, try with a more flexible approach that handles HTML encoding
    flex_context_pattern = re.compile(r'<xbrli:context\s+id="([^"]+)".*?>(.*?)<\/xbrli:context>', re.DOTALL)
    
    # Also check for context in the ix:resources section specifically
    ix_resources_pattern = re.compile(r'<ix:resources.*?>(.*?)<\/ix:resources>', re.DOTALL)
    
    # Patterns for period components
    instant_pattern = re.compile(r'<xbrli:instant>(.*?)<\/xbrli:instant>', re.DOTALL)
    startdate_pattern = re.compile(r'<xbrli:startDate>(.*?)<\/xbrli:startDate>', re.DOTALL)
    enddate_pattern = re.compile(r'<xbrli:endDate>(.*?)<\/xbrli:endDate>', re.DOTALL)
    
    # Find all context elements using standard pattern
    context_matches = context_pattern.findall(html_content)
    logging.info(f"Found {len(context_matches)} contexts via standard regex in HTML file")
    
    # If standard approach finds no contexts, try with flexible pattern
    if not context_matches:
        # Try to extract ix:resources section first
        ix_resources_match = ix_resources_pattern.search(html_content)
        if ix_resources_match:
            logging.info("Found ix:resources section, searching within it")
            resources_content = ix_resources_match.group(1)
            # Try standard pattern within resources
            context_matches = context_pattern.findall(resources_content)
            logging.info(f"Found {len(context_matches)} contexts in ix:resources section")
            
            # If still no matches, try flexible pattern
            if not context_matches:
                context_matches = flex_context_pattern.findall(resources_content)
                logging.info(f"Found {len(context_matches)} contexts with flexible pattern in ix:resources")
        
        # If still no matches, try flexible pattern on whole document
        if not context_matches:
            context_matches = flex_context_pattern.findall(html_content)
            logging.info(f"Found {len(context_matches)} contexts with flexible pattern in entire document")
    
    # Process all matches if any were found
    if context_matches:
        logging.info(f"Processing {len(context_matches)} context matches")
        
        # Parse all matches and create a dictionary
        extracted_contexts = {}
        for context_id, context_content in context_matches:
            period_info = {}
            
            # Extract instant date if present
            instant_match = instant_pattern.search(context_content)
            if instant_match:
                period_info["instant"] = instant_match.group(1).strip()
                logging.info(f"Found instant date for context {context_id}: {period_info['instant']}")
            
            # Extract start/end dates if present
            startdate_match = startdate_pattern.search(context_content)
            enddate_match = enddate_pattern.search(context_content)
            if startdate_match and enddate_match:
                period_info["startDate"] = startdate_match.group(1).strip()
                period_info["endDate"] = enddate_match.group(1).strip()
                logging.info(f"Found period for context {context_id}: {period_info['startDate']} to {period_info['endDate']}")
            
            # Store the context if we found period information
            if period_info:
                extracted_contexts[context_id] = {
                    "id": context_id,
                    "period": period_info
                }
        
        # If we found contexts but couldn't extract period info, try one more approach
        if not extracted_contexts and context_matches:
            logging.info("Found contexts but couldn't extract period info, trying direct approach")
            
            # Extract periods directly from full HTML content
            all_instant_matches = instant_pattern.findall(html_content)
            all_start_matches = startdate_pattern.findall(html_content)
            all_end_matches = enddate_pattern.findall(html_content)
            
            logging.info(f"Direct extraction found: {len(all_instant_matches)} instants, " +
                        f"{len(all_start_matches)} start dates, {len(all_end_matches)} end dates")
            
            # Pair up context IDs with periods by proximity in the original HTML
            for context_id, _ in context_matches:
                # Try to find period info near this context ID in the HTML
                context_pos = html_content.find(f'id="{context_id}"')
                if context_pos > 0:
                    # Look for period info within 500 chars of context ID
                    window = html_content[max(0, context_pos-200):min(len(html_content), context_pos+500)]
                    
                    period_info = {}
                    instant_match = instant_pattern.search(window)
                    if instant_match:
                        period_info["instant"] = instant_match.group(1).strip()
                    
                    startdate_match = startdate_pattern.search(window)
                    enddate_match = enddate_pattern.search(window)
                    if startdate_match and enddate_match:
                        period_info["startDate"] = startdate_match.group(1).strip()
                        period_info["endDate"] = enddate_match.group(1).strip()
                    
                    if period_info:
                        extracted_contexts[context_id] = {
                            "id": context_id,
                            "period": period_info
                        }
        
        # If we still have no context periods, create synthetic contexts based on common patterns
        if not extracted_contexts and context_matches:
            logging.info("No period information found, creating synthetic contexts based on common patterns")
            
            # Try to extract filing year from metadata
            filing_year = None
            if filing_metadata and "fiscal_year" in filing_metadata:
                filing_year = filing_metadata.get("fiscal_year")
            
            # Use fallback year if all else fails
            if not filing_year:
                try:
                    # Try to use the current year
                    filing_year = str(datetime.now().year)
                except:
                    filing_year = "2023"  # Default fallback
            
            # Common context ID patterns and their likely meanings
            context_patterns = {
                "c-1": {"type": "annual", "description": f"Current reporting year ({filing_year})"},
                "c-2": {"type": "instant", "description": f"End date for year {filing_year}"},
                "c-3": {"type": "instant", "description": f"Prior period end date ({int(filing_year)-1})"},
                "c-4": {"type": "instant", "description": "Current balance sheet date"},
                "c-5": {"type": "instant", "description": "Prior balance sheet date"}
            }
            
            # Create synthetic contexts based on pattern matching
            for context_id, _ in context_matches:
                # Check if this ID matches a common pattern
                pattern_match = None
                for pattern, info in context_patterns.items():
                    if context_id == pattern or context_id.startswith(pattern + "_"):
                        pattern_match = info
                        break
                
                # If no exact match, try numeric pattern
                if not pattern_match and re.match(r'^c-\d+$', context_id):
                    # Default for numeric context IDs
                    pattern_match = {"type": "unknown", "description": "Context with unknown period"}
                
                # Create synthetic period info based on the pattern
                if pattern_match:
                    period_info = {}
                    
                    if pattern_match["type"] == "annual":
                        # Create a full year period
                        period_info["startDate"] = f"{filing_year}-01-01"
                        period_info["endDate"] = f"{filing_year}-12-31"
                    elif pattern_match["type"] == "instant":
                        # Create an instant date at year end
                        period_info["instant"] = f"{filing_year}-12-31"
                    
                    # Store with explicit synthetic flag
                    extracted_contexts[context_id] = {
                        "id": context_id,
                        "period": period_info,
                        "synthetic": True,
                        "description": pattern_match["description"]
                    }
        
        return extracted_contexts
    else:
        logging.warning("No contexts found via any extraction method")
        return {}

def find_tesla_filings(base_dir="sec_processed", ticker="TSLA"):
    """Find all Tesla filing files"""
    tesla_filings = []
    
    # Look in the tmp directory for SEC downloads
    tmp_dir = os.path.join(base_dir, "tmp", "sec_downloads", ticker)
    if os.path.exists(tmp_dir):
        for root, dirs, files in os.walk(tmp_dir):
            for file in files:
                if file.endswith(".htm") and ticker.lower() in file.lower():
                    tesla_filings.append(os.path.join(root, file))
    
    # Also look directly in the ticker directory
    ticker_dir = os.path.join(base_dir, ticker)
    if os.path.exists(ticker_dir):
        for root, dirs, files in os.walk(ticker_dir):
            for file in files:
                if file.endswith(".htm") and ticker.lower() in file.lower():
                    tesla_filings.append(os.path.join(root, file))
    
    return tesla_filings

def extract_contexts_from_file(file_path, fiscal_year="2023"):
    """Extract contexts from a single file"""
    logging.info(f"Processing file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            html_content = f.read()
            
        # Extract filing year from file name if possible
        file_year = fiscal_year
        year_match = re.search(r'(\d{4})', os.path.basename(file_path))
        if year_match:
            file_year = year_match.group(1)
            
        contexts = extract_contexts_from_html(html_content, {'fiscal_year': file_year})
        
        logging.info(f"Found {len(contexts)} contexts in {file_path}")
        
        return contexts
    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")
        return {}

def save_context_mapping(contexts, output_file):
    """Save the context mapping to a file"""
    try:
        with open(output_file, 'w') as f:
            json.dump(contexts, f, indent=2)
        logging.info(f"Saved context mapping to {output_file}")
    except Exception as e:
        logging.error(f"Error saving context mapping: {str(e)}")

def print_context_summary(contexts):
    """Print a summary of the contexts"""
    print("\nContext Summary:")
    print(f"Total contexts: {len(contexts)}")
    
    # Count by type
    instant_count = 0
    duration_count = 0
    synthetic_count = 0
    
    for context_id, context in contexts.items():
        period_info = context.get("period", {})
        if "instant" in period_info:
            instant_count += 1
        elif "startDate" in period_info and "endDate" in period_info:
            duration_count += 1
            
        if context.get("synthetic", False):
            synthetic_count += 1
    
    print(f"Instant contexts: {instant_count}")
    print(f"Duration contexts: {duration_count}")
    print(f"Synthetic contexts: {synthetic_count}")
    
    # Print sample contexts
    print("\nSample Contexts:")
    sample_contexts = list(contexts.items())[:5]
    for context_id, context in sample_contexts:
        period_info = context.get("period", {})
        if "instant" in period_info:
            print(f"  {context_id}: Instant date {period_info['instant']}")
        elif "startDate" in period_info and "endDate" in period_info:
            print(f"  {context_id}: Period from {period_info['startDate']} to {period_info['endDate']}")
        else:
            print(f"  {context_id}: Unknown period type")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Extract context information from Tesla XBRL filings")
    parser.add_argument("--file", help="Path to a specific filing file")
    parser.add_argument("--output", help="Output file for the context mapping (JSON)")
    parser.add_argument("--fiscal-year", default="2023", help="Fiscal year for context generation")
    args = parser.parse_args()
    
    if args.file:
        # Process a specific file
        if os.path.exists(args.file):
            contexts = extract_contexts_from_file(args.file, args.fiscal_year)
            print_context_summary(contexts)
            
            if args.output:
                save_context_mapping(contexts, args.output)
        else:
            logging.error(f"File not found: {args.file}")
    else:
        # Find and process all Tesla filings
        tesla_filings = find_tesla_filings()
        
        if not tesla_filings:
            logging.error("No Tesla filings found")
            return
        
        logging.info(f"Found {len(tesla_filings)} Tesla filings")
        
        # Process the first filing
        if tesla_filings:
            logging.info(f"Processing the first filing: {tesla_filings[0]}")
            contexts = extract_contexts_from_file(tesla_filings[0], args.fiscal_year)
            print_context_summary(contexts)
            
            if args.output:
                save_context_mapping(contexts, args.output)

if __name__ == "__main__":
    main()
