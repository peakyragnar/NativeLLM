#!/usr/bin/env python3
"""
Verify Context Completeness in LLM Output

This script checks if context information is properly represented in the LLM output
by comparing context IDs in raw XBRL data with context codes in the LLM output.
"""

import os
import sys
import re
import json
import logging
import argparse
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_context_ids_from_xbrl(xbrl_file):
    """
    Extract context IDs from raw XBRL JSON file
    
    Args:
        xbrl_file (str): Path to XBRL JSON file
        
    Returns:
        dict: Dictionary mapping context IDs to their info
    """
    try:
        with open(xbrl_file, 'r', encoding='utf-8') as f:
            xbrl_data = json.load(f)
        
        contexts = {}
        
        # Extract contexts
        if "contexts" in xbrl_data:
            contexts = xbrl_data["contexts"]
        
        logging.info(f"Extracted {len(contexts)} contexts from XBRL file")
        return contexts
    except Exception as e:
        logging.error(f"Error extracting contexts from XBRL: {str(e)}")
        return {}

def extract_context_map_from_llm(llm_file):
    """
    Extract context map from LLM output file
    
    Args:
        llm_file (str): Path to LLM output file
        
    Returns:
        dict: Dictionary mapping context codes to context IDs
    """
    try:
        with open(llm_file, 'r', encoding='utf-8') as f:
            llm_content = f.read()
        
        # Find the context dictionary section
        context_section_match = re.search(r'@DATA_DICTIONARY: CONTEXTS(.*?)@UNITS_AND_SCALING', 
                                         llm_content, re.DOTALL)
        
        if not context_section_match:
            logging.error("Context dictionary section not found in LLM file")
            return {}
        
        context_section = context_section_match.group(1)
        
        # Extract context codes and IDs
        context_map = {}
        context_pattern = re.compile(r'(c-\d+)\s*\|\s*ID:\s*([^\s|]+)', re.MULTILINE)
        
        for match in context_pattern.finditer(context_section):
            code = match.group(1)
            context_id = match.group(2)
            context_map[code] = context_id
        
        logging.info(f"Extracted {len(context_map)} contexts from LLM file")
        return context_map
    except Exception as e:
        logging.error(f"Error extracting contexts from LLM: {str(e)}")
        return {}

def find_context_references_in_llm(llm_file):
    """
    Find context references in the LLM output
    
    Args:
        llm_file (str): Path to LLM output file
        
    Returns:
        set: Set of context codes referenced in the LLM output
    """
    try:
        with open(llm_file, 'r', encoding='utf-8') as f:
            llm_content = f.read()
        
        # Find context references outside the context dictionary
        context_refs = set()
        
        # Skip the context dictionary section for this analysis
        content_without_dict = re.sub(r'@DATA_DICTIONARY: CONTEXTS.*?@UNITS_AND_SCALING', 
                                     '@UNITS_AND_SCALING', llm_content, flags=re.DOTALL)
        
        # Find c-X references
        ref_pattern = re.compile(r'(c-\d+)\s*(\||:)')
        for match in ref_pattern.finditer(content_without_dict):
            context_refs.add(match.group(1))
        
        logging.info(f"Found {len(context_refs)} unique context references in LLM content")
        return context_refs
    except Exception as e:
        logging.error(f"Error finding context references: {str(e)}")
        return set()

def extract_fact_contexts_from_xbrl(xbrl_file):
    """
    Extract context IDs used by facts in the raw XBRL
    
    Args:
        xbrl_file (str): Path to XBRL JSON file
        
    Returns:
        dict: Dictionary mapping context IDs to count of fact usage
    """
    try:
        with open(xbrl_file, 'r', encoding='utf-8') as f:
            xbrl_data = json.load(f)
        
        context_usage = {}
        
        # Count context usage in facts
        if "facts" in xbrl_data:
            for fact in xbrl_data["facts"]:
                context_ref = fact.get("context_ref", "")
                if context_ref:
                    if context_ref not in context_usage:
                        context_usage[context_ref] = 0
                    context_usage[context_ref] += 1
        
        logging.info(f"Found {len(context_usage)} contexts used in XBRL facts")
        return context_usage
    except Exception as e:
        logging.error(f"Error extracting fact contexts: {str(e)}")
        return {}

def verify_context_completeness(xbrl_file, llm_file):
    """
    Verify context completeness in LLM output
    
    Args:
        xbrl_file (str): Path to XBRL JSON file
        llm_file (str): Path to LLM output file
        
    Returns:
        dict: Completeness statistics
    """
    # Extract contexts from XBRL
    xbrl_contexts = extract_context_ids_from_xbrl(xbrl_file)
    
    # Extract context map from LLM
    llm_context_map = extract_context_map_from_llm(llm_file)
    
    # Extract context references in LLM content
    llm_context_refs = find_context_references_in_llm(llm_file)
    
    # Extract fact contexts from XBRL
    fact_contexts = extract_fact_contexts_from_xbrl(xbrl_file)
    
    # Reverse the LLM context map to get ID to code mapping
    id_to_code = {v: k for k, v in llm_context_map.items()}
    
    # Check which XBRL contexts are in the LLM context map
    contexts_in_map = set(id_to_code.keys())
    all_xbrl_contexts = set(xbrl_contexts.keys())
    
    # Check which fact contexts are in the LLM context map
    fact_context_ids = set(fact_contexts.keys())
    fact_contexts_in_map = fact_context_ids.intersection(contexts_in_map)
    
    # Check which context codes are actually referenced in the LLM content
    context_codes_used = llm_context_refs
    context_ids_used = set()
    for code in context_codes_used:
        if code in llm_context_map:
            context_ids_used.add(llm_context_map[code])
    
    # Calculate completeness metrics
    completeness = {
        "xbrl_context_count": len(all_xbrl_contexts),
        "fact_context_count": len(fact_context_ids),
        "llm_context_count": len(llm_context_map),
        "contexts_in_map": len(contexts_in_map),
        "contexts_missing": len(all_xbrl_contexts - contexts_in_map),
        "fact_contexts_in_map": len(fact_contexts_in_map),
        "fact_contexts_missing": len(fact_context_ids - fact_contexts_in_map),
        "context_codes_referenced": len(context_codes_used),
        "context_ids_referenced": len(context_ids_used),
        "context_map_coverage": round(len(contexts_in_map) / len(all_xbrl_contexts) * 100, 2) if all_xbrl_contexts else 0,
        "fact_context_coverage": round(len(fact_contexts_in_map) / len(fact_context_ids) * 100, 2) if fact_context_ids else 0,
    }
    
    return completeness

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Verify context completeness in LLM output")
    parser.add_argument("--xbrl-file", required=True, help="Path to raw XBRL JSON file")
    parser.add_argument("--llm-file", required=True, help="Path to LLM output file")
    parser.add_argument("--output", help="Path to save results JSON (optional)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.xbrl_file):
        logging.error(f"XBRL file not found: {args.xbrl_file}")
        return 1
    
    if not os.path.exists(args.llm_file):
        logging.error(f"LLM file not found: {args.llm_file}")
        return 1
    
    # Verify context completeness
    completeness = verify_context_completeness(args.xbrl_file, args.llm_file)
    
    # Print results
    print("\n=== Context Completeness Verification ===")
    print(f"XBRL File: {args.xbrl_file}")
    print(f"LLM File: {args.llm_file}")
    print("-" * 50)
    print(f"Total XBRL Contexts: {completeness['xbrl_context_count']}")
    print(f"Contexts Used in Facts: {completeness['fact_context_count']}")
    print(f"Contexts in LLM Map: {completeness['llm_context_count']}")
    print(f"XBRL Contexts in LLM Map: {completeness['contexts_in_map']}")
    print(f"XBRL Contexts Missing from LLM Map: {completeness['contexts_missing']}")
    print(f"Fact Contexts in LLM Map: {completeness['fact_contexts_in_map']}")
    print(f"Fact Contexts Missing from LLM Map: {completeness['fact_contexts_missing']}")
    print("-" * 50)
    print(f"Context Codes Referenced in LLM: {completeness['context_codes_referenced']}")
    print(f"Context IDs Referenced in LLM: {completeness['context_ids_referenced']}")
    print("-" * 50)
    print(f"Context Map Coverage: {completeness['context_map_coverage']}%")
    print(f"Fact Context Coverage: {completeness['fact_context_coverage']}%")
    
    # Save results if output file specified
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(completeness, f, indent=2)
            logging.info(f"Results saved to {args.output}")
        except Exception as e:
            logging.error(f"Error saving results: {str(e)}")
    
    # Return success if fact context coverage is high enough
    return 0 if completeness['fact_context_coverage'] >= 95 else 1

if __name__ == "__main__":
    sys.exit(main()) 