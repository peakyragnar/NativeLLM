#!/usr/bin/env python3
"""
Verify LLM Completeness

This script compares the raw XBRL JSON data extracted from SEC filings
with the final LLM output file to ensure all XBRL facts were properly captured.
"""

import argparse
import json
import logging
import os
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def parse_llm_concepts(llm_content):
    """Parse @CONCEPT blocks from the llm.txt content."""
    concepts = []
    current_concept = None
    # Regex to find the start of a concept block and capture the concept name
    concept_start_re = re.compile(r"^@CONCEPT:\s*(.*)")
    # Regex to capture key-value pairs within a block
    attr_re = re.compile(r"^@(\w+):\s*(.*)")

    lines = llm_content.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        start_match = concept_start_re.match(line)

        if start_match:
            # Start of a new concept block
            if current_concept:
                concepts.append(current_concept)
            
            current_concept = {
                "CONCEPT": start_match.group(1).strip()
            }
            i += 1
            # Continue parsing attributes for this concept
            while i < len(lines) and lines[i].strip().startswith("@") and not concept_start_re.match(lines[i].strip()):
                attr_line = lines[i].strip()
                attr_match = attr_re.match(attr_line)
                if attr_match:
                    key = attr_match.group(1).upper()
                    value = attr_match.group(2).strip()
                    current_concept[key] = value
                i += 1
        else:
            # Line doesn't start a new concept, move on
            i += 1
    
    # Add the final concept if we were processing one
    if current_concept:
        concepts.append(current_concept)
    
    logging.info(f"Parsed {len(concepts)} @CONCEPT blocks from LLM file.")
    return concepts

def compare_values(raw_value, llm_value):
    """Compare two values, handling numeric types with tolerance."""
    # Basic string comparison
    if raw_value == llm_value:
        return True
        
    # Try numeric comparison for numbers
    try:
        raw_cleaned = raw_value.replace(",", "").strip()
        llm_cleaned = llm_value.replace(",", "").strip()
        
        raw_decimal = Decimal(raw_cleaned)
        llm_decimal = Decimal(llm_cleaned)
        
        # Exact match
        if raw_decimal == llm_decimal:
            return True
    except (InvalidOperation, ValueError, TypeError):
        # Not numeric values, and string comparison already failed
        pass
        
    return False

def main():
    parser = argparse.ArgumentParser(description="Verify LLM file completeness against raw XBRL JSON.")
    parser.add_argument("--temp-dir", help="Path to the temporary directory containing _xbrl_raw.json")
    parser.add_argument("--xbrl-file", help="Direct path to the XBRL JSON file")
    parser.add_argument("--llm-file", required=True, help="Path to the final llm.txt file")
    parser.add_argument("--downloads-dir", help="Path to the downloads directory (e.g., sec_processed/tmp/sec_downloads)")
    
    args = parser.parse_args()
    
    # Extract ticker and filing info from LLM filename
    llm_file_path = Path(args.llm_file)
    llm_basename = os.path.basename(args.llm_file)
    ticker = None
    filing_type = None
    
    # Try to extract ticker and filing type from filename (e.g., MSFT_10-K_2024_llm.txt)
    import re
    file_match = re.match(r'([A-Z]+)_(\d+-[A-Z])_.*', llm_basename)
    if file_match:
        ticker = file_match.group(1)
        filing_type = file_match.group(2)
        logging.info(f"Extracted ticker={ticker}, filing_type={filing_type} from filename")
    
    # Determine the XBRL file path
    raw_json_path = None
    
    # First try the specified XBRL file path if provided
    if args.xbrl_file:
        raw_json_path = Path(os.path.abspath(args.xbrl_file))
        
    # Then try the specified temp directory if provided
    elif args.temp_dir:
        raw_json_path = Path(os.path.abspath(args.temp_dir)) / "_xbrl_raw.json"
        
    # Then try to find XBRL file in downloads directory based on ticker and filing type
    elif args.downloads_dir and ticker and filing_type:
        downloads_base = os.path.abspath(args.downloads_dir)
        filing_dir = os.path.join(downloads_base, ticker, filing_type)
        
        if os.path.exists(filing_dir):
            # Find the most recent accession directory that has an XBRL file
            accession_dirs = []
            for entry in os.listdir(filing_dir):
                potential_dir = os.path.join(filing_dir, entry)
                potential_xbrl = os.path.join(potential_dir, "_xbrl_raw.json")
                if os.path.isdir(potential_dir) and os.path.exists(potential_xbrl):
                    accession_dirs.append((potential_dir, os.path.getmtime(potential_xbrl)))
            
            # Sort by modification time (latest first)
            if accession_dirs:
                accession_dirs.sort(key=lambda x: x[1], reverse=True)
                raw_json_path = Path(os.path.join(accession_dirs[0][0], "_xbrl_raw.json"))
                logging.info(f"Found XBRL file in downloads directory: {raw_json_path}")
    
    # If we still don't have a path, look for it in the standard downloads location
    if not raw_json_path and ticker and filing_type:
        # Try the standard downloads location
        std_downloads = os.path.join("sec_processed", "tmp", "sec_downloads", ticker, filing_type)
        if os.path.exists(std_downloads):
            # Find the most recent accession directory that has an XBRL file
            accession_dirs = []
            for entry in os.listdir(std_downloads):
                potential_dir = os.path.join(std_downloads, entry)
                potential_xbrl = os.path.join(potential_dir, "_xbrl_raw.json")
                if os.path.isdir(potential_dir) and os.path.exists(potential_xbrl):
                    accession_dirs.append((potential_dir, os.path.getmtime(potential_xbrl)))
            
            # Sort by modification time (latest first)
            if accession_dirs:
                accession_dirs.sort(key=lambda x: x[1], reverse=True)
                raw_json_path = Path(os.path.join(accession_dirs[0][0], "_xbrl_raw.json"))
                logging.info(f"Found XBRL file in standard downloads directory: {raw_json_path}")
    
    if not raw_json_path:
        logging.error("Could not determine XBRL file path. Provide --xbrl-file, --temp-dir, or --downloads-dir")
        return 1
    
    # Check if files exist
    if not raw_json_path.exists():
        logging.error(f"Raw XBRL JSON file not found: {raw_json_path}")
        return 1
        
    if not llm_file_path.exists():
        logging.error(f"LLM file not found: {llm_file_path}")
        return 1
    
    # Load raw XBRL data
    try:
        with open(raw_json_path, 'r', encoding='utf-8') as f:
            raw_xbrl_facts = json.load(f)
        logging.info(f"Loaded {len(raw_xbrl_facts)} facts from {raw_json_path}")
    except Exception as e:
        logging.error(f"Error loading raw XBRL JSON: {e}")
        return 1
    
    # Load LLM file content
    try:
        with open(llm_file_path, 'r', encoding='utf-8') as f:
            llm_content = f.read()
        logging.info(f"Loaded LLM file: {llm_file_path}")
    except Exception as e:
        logging.error(f"Error reading LLM file: {e}")
        return 1
    
    # Parse LLM concepts
    llm_concepts = parse_llm_concepts(llm_content)
    
    # Create dictionary of raw facts keyed by (name, contextRef, unitRef)
    raw_facts_dict = {}
    for fact in raw_xbrl_facts:
        key = (
            fact.get('name', ''),
            fact.get('contextRef', ''),
            fact.get('unitRef', '')
        )
        if key[0]:  # Skip entries with empty concept names
            raw_facts_dict[key] = fact.get('value', '')
    
    # Create dictionary of LLM concepts keyed by (concept, context_ref, unit_ref)
    llm_concepts_dict = {}
    for concept in llm_concepts:
        key = (
            concept.get('CONCEPT', ''),
            concept.get('CONTEXT_REF', ''),
            concept.get('UNIT_REF', '')
        )
        if key[0]:  # Skip entries with empty concept names
            llm_concepts_dict[key] = concept.get('VALUE', '')
    
    # Compare the dictionaries
    matched_count = 0
    missing_facts = []
    value_mismatches = []
    
    for key, raw_value in raw_facts_dict.items():
        if key in llm_concepts_dict:
            llm_value = llm_concepts_dict[key]
            if compare_values(raw_value, llm_value):
                matched_count += 1
            else:
                value_mismatches.append({
                    'key': key,
                    'raw_value': raw_value,
                    'llm_value': llm_value
                })
        else:
            missing_facts.append({
                'key': key,
                'raw_value': raw_value
            })
    
    # Check for extra facts in LLM that aren't in raw
    extra_facts = []
    for key in llm_concepts_dict:
        if key not in raw_facts_dict:
            extra_facts.append({
                'key': key,
                'llm_value': llm_concepts_dict[key]
            })
    
    # SECOND PASS: Try concept-name-only matching for missing facts
    # This helps identify if there's just a context/unit reference format issue
    name_only_matched = 0
    still_missing = []
    
    # Create concept-name-only dictionaries
    raw_name_only = {}
    for key, value in raw_facts_dict.items():
        concept_name = key[0]
        if concept_name in raw_name_only:
            raw_name_only[concept_name].append(value)
        else:
            raw_name_only[concept_name] = [value]
    
    llm_name_only = {}
    for key, value in llm_concepts_dict.items():
        concept_name = key[0]
        if concept_name in llm_name_only:
            llm_name_only[concept_name].append(value)
        else:
            llm_name_only[concept_name] = [value]
    
    # Check if missing facts exist by concept name only
    for fact in missing_facts:
        concept_name = fact['key'][0]
        raw_value = fact['raw_value']
        
        if concept_name in llm_name_only:
            # The concept exists in LLM, maybe with different context/unit
            matched_by_name = False
            for llm_value in llm_name_only[concept_name]:
                if compare_values(raw_value, llm_value):
                    matched_by_name = True
                    name_only_matched += 1
                    break
            
            if not matched_by_name:
                still_missing.append(fact)
        else:
            still_missing.append(fact)
    
    # Calculate completeness percentage
    total_raw_facts = len(raw_facts_dict)
    completeness_pct = (matched_count / total_raw_facts * 100) if total_raw_facts > 0 else 0
    
    # Including name-only matches
    adjusted_completeness = ((matched_count + name_only_matched) / total_raw_facts * 100) if total_raw_facts > 0 else 0
    
    # Print results
    print("\n=== Verification Results ===")
    print(f"Raw XBRL JSON: {raw_json_path}")
    print(f"LLM File: {llm_file_path}")
    print("-" * 40)
    print(f"Total Raw XBRL Facts: {total_raw_facts}")
    print(f"Total LLM Concepts: {len(llm_concepts_dict)}")
    print(f"Facts Matched (Exact): {matched_count}")
    print(f"Facts Matched (By Name Only): {name_only_matched}")
    print(f"Facts Missing in LLM: {len(missing_facts)}")
    print(f"Facts Still Missing After Name-Only Match: {len(still_missing)}")
    print(f"Facts with Value Mismatches: {len(value_mismatches)}")
    print(f"Extra Facts in LLM: {len(extra_facts)}")
    print("-" * 40)
    print(f"Completeness (Exact Match): {completeness_pct:.2f}%")
    print(f"Completeness (Including Name-Only Matches): {adjusted_completeness:.2f}%")
    
    # Show some examples of issues if they exist
    if still_missing:
        print("\n=== Sample Still Missing Facts ===")
        limit = min(5, len(still_missing))
        for i in range(limit):
            fact = still_missing[i]
            name, context, unit = fact['key']
            print(f"  {name} [Context: {context}, Unit: {unit}] = {fact['raw_value']}")
    
    if value_mismatches:
        print("\n=== Sample Value Mismatches ===")
        limit = min(5, len(value_mismatches))
        for i in range(limit):
            mismatch = value_mismatches[i]
            name, context, unit = mismatch['key']
            print(f"  {name} [Context: {context}, Unit: {unit}]")
            print(f"    Raw: {mismatch['raw_value']}")
            print(f"    LLM: {mismatch['llm_value']}")
    
    if extra_facts:
        print("\n=== Sample Extra Facts in LLM ===")
        limit = min(5, len(extra_facts))
        for i in range(limit):
            fact = extra_facts[i]
            name, context, unit = fact['key']
            print(f"  {name} [Context: {context}, Unit: {unit}] = {fact['llm_value']}")
    
    # Summary of unique concept names in each source
    raw_concept_names = set(key[0] for key in raw_facts_dict.keys())
    llm_concept_names = set(key[0] for key in llm_concepts_dict.keys())
    
    print("\n=== Concept Name Coverage ===")
    print(f"Unique Concept Names in Raw: {len(raw_concept_names)}")
    print(f"Unique Concept Names in LLM: {len(llm_concept_names)}")
    print(f"Concept Names in Raw but not LLM: {len(raw_concept_names - llm_concept_names)}")
    print(f"Concept Names in LLM but not Raw: {len(llm_concept_names - raw_concept_names)}")
    
    # Find specific concept name differences if they exist
    if raw_concept_names - llm_concept_names:
        print("\n=== Concept Names Missing in LLM ===")
        for name in sorted(list(raw_concept_names - llm_concept_names))[:10]:  # Show first 10
            print(f"  {name}")
        if len(raw_concept_names - llm_concept_names) > 10:
            print(f"  ... and {len(raw_concept_names - llm_concept_names) - 10} more")
    
    # Return success/failure code based on adjusted completeness
    return 0 if adjusted_completeness >= 99.5 else 1

if __name__ == "__main__":
    exit(main()) 