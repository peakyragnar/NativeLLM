#!/usr/bin/env python3
"""
Create LLM-Friendly Output from XBRL Mappings and Facts

This script creates an LLM-friendly output format that combines XBRL mappings
and facts, optimized for AI analysis in chatbots, RAG systems, and training inputs.
"""

import os
import sys
import json
import logging
import argparse
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_llm_friendly_output(mapping_file, facts_file, output_file):
    """
    Create an LLM-friendly output from XBRL mappings and facts.
    
    Args:
        mapping_file: Path to the mapping file (JSON)
        facts_file: Path to the facts file (JSON or raw XBRL)
        output_file: Path to the output file (JSON)
    """
    logging.info(f"Creating LLM-friendly output from {mapping_file} and {facts_file}")
    
    # Load mappings
    with open(mapping_file, 'r', encoding='utf-8') as f:
        mappings = json.load(f)
    
    # Load facts
    facts = []
    try:
        with open(facts_file, 'r', encoding='utf-8') as f:
            facts_data = json.load(f)
            
            # Check if it's a list of facts or a dictionary with a 'facts' key
            if isinstance(facts_data, list):
                facts = facts_data
            elif isinstance(facts_data, dict) and 'facts' in facts_data:
                facts = facts_data['facts']
            else:
                logging.warning(f"Unexpected facts format in {facts_file}")
    except Exception as e:
        logging.error(f"Error loading facts: {str(e)}")
    
    # Create LLM-friendly output
    output = create_output_structure(mappings, facts)
    
    # Save output
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    
    logging.info(f"LLM-friendly output saved to {output_file}")
    
    return output

def create_output_structure(mappings, facts):
    """
    Create the LLM-friendly output structure.
    
    Args:
        mappings: Dictionary containing presentation and calculation mappings
        facts: List of facts
        
    Returns:
        Dictionary containing the LLM-friendly output structure
    """
    # Extract presentation and calculation mappings
    presentation_mappings = mappings.get('presentation_mappings', [])
    calculation_mappings = mappings.get('calculation_mappings', [])
    
    # Group presentation mappings by role (financial statement)
    mappings_by_role = defaultdict(list)
    for mapping in presentation_mappings:
        role = mapping.get('role', '')
        mappings_by_role[role].append(mapping)
    
    # Identify financial statement types
    financial_statements = identify_financial_statements(mappings_by_role)
    
    # Build hierarchical structure for each financial statement
    hierarchies = {}
    for statement_type, roles in financial_statements.items():
        hierarchies[statement_type] = {}
        for role in roles:
            hierarchy = build_hierarchy(mappings_by_role[role])
            hierarchies[statement_type][role] = hierarchy
    
    # Group calculation mappings by role
    calc_by_role = defaultdict(list)
    for mapping in calculation_mappings:
        role = mapping.get('role', '')
        calc_by_role[role].append(mapping)
    
    # Build calculation relationships
    calculations = {}
    for statement_type, roles in financial_statements.items():
        calculations[statement_type] = {}
        for role in roles:
            calc_rels = build_calculation_relationships(calc_by_role[role])
            calculations[statement_type][role] = calc_rels
    
    # Group facts by concept
    facts_by_concept = defaultdict(list)
    for fact in facts:
        concept = fact.get('name', '')
        facts_by_concept[concept].append(fact)
    
    # Create the final output structure
    output = {
        "@DOCUMENT_TYPE": "XBRL_LLM_FRIENDLY",
        "@VERSION": "1.0",
        "@STRUCTURE": {
            "FINANCIAL_STATEMENTS": list(financial_statements.keys()),
            "SECTIONS": [
                "HIERARCHICAL_STRUCTURE",
                "CALCULATION_RELATIONSHIPS",
                "FACTS"
            ]
        },
        "HIERARCHICAL_STRUCTURE": hierarchies,
        "CALCULATION_RELATIONSHIPS": calculations,
        "FACTS": facts_by_concept
    }
    
    return output

def identify_financial_statements(mappings_by_role):
    """
    Identify financial statement types from role URIs.
    
    Args:
        mappings_by_role: Dictionary of mappings grouped by role
        
    Returns:
        Dictionary mapping statement types to lists of roles
    """
    financial_statements = {
        "BALANCE_SHEET": [],
        "INCOME_STATEMENT": [],
        "CASH_FLOW_STATEMENT": [],
        "STATEMENT_OF_EQUITY": [],
        "OTHER": []
    }
    
    for role in mappings_by_role.keys():
        role_lower = role.lower()
        
        if any(term in role_lower for term in ["balance", "financial position"]):
            financial_statements["BALANCE_SHEET"].append(role)
        elif any(term in role_lower for term in ["income", "operations", "earnings", "profit", "loss"]):
            financial_statements["INCOME_STATEMENT"].append(role)
        elif any(term in role_lower for term in ["cash flow", "cashflow"]):
            financial_statements["CASH_FLOW_STATEMENT"].append(role)
        elif any(term in role_lower for term in ["equity", "stockholder", "shareholder"]):
            financial_statements["STATEMENT_OF_EQUITY"].append(role)
        else:
            financial_statements["OTHER"].append(role)
    
    # Remove empty statement types
    return {k: v for k, v in financial_statements.items() if v}

def build_hierarchy(mappings):
    """
    Build a hierarchical structure from presentation mappings.
    
    Args:
        mappings: List of presentation mappings
        
    Returns:
        Dictionary representing the hierarchical structure
    """
    # Sort mappings by order
    sorted_mappings = sorted(mappings, key=lambda m: float(m.get('order', '0') or '0'))
    
    # Build parent-child relationships
    parent_to_children = defaultdict(list)
    for mapping in sorted_mappings:
        parent = mapping.get('parent', '')
        child = mapping.get('child', '')
        order = mapping.get('order', '0')
        
        if parent and child:
            parent_to_children[parent].append({
                "concept": child,
                "order": order
            })
    
    # Find root concepts (those that are parents but not children)
    all_parents = set(parent_to_children.keys())
    all_children = set()
    for children in parent_to_children.values():
        all_children.update(child['concept'] for child in children)
    
    root_concepts = all_parents - all_children
    
    # Build the hierarchy recursively
    hierarchy = {}
    for root in root_concepts:
        hierarchy[root] = build_subtree(root, parent_to_children)
    
    return hierarchy

def build_subtree(concept, parent_to_children):
    """
    Build a subtree of the hierarchy recursively.
    
    Args:
        concept: The concept to build the subtree for
        parent_to_children: Dictionary mapping parents to children
        
    Returns:
        Dictionary representing the subtree
    """
    children = parent_to_children.get(concept, [])
    
    if not children:
        return None
    
    subtree = {}
    for child in children:
        child_concept = child['concept']
        child_subtree = build_subtree(child_concept, parent_to_children)
        
        if child_subtree:
            subtree[child_concept] = child_subtree
        else:
            subtree[child_concept] = None
    
    return subtree

def build_calculation_relationships(mappings):
    """
    Build calculation relationships from calculation mappings.
    
    Args:
        mappings: List of calculation mappings
        
    Returns:
        Dictionary representing calculation relationships
    """
    # Group by parent concept
    parent_to_children = defaultdict(list)
    for mapping in mappings:
        parent = mapping.get('parent', '')
        child = mapping.get('child', '')
        weight = mapping.get('weight', '1')
        
        if parent and child:
            parent_to_children[parent].append({
                "concept": child,
                "weight": weight
            })
    
    return parent_to_children

def main():
    parser = argparse.ArgumentParser(description="Create LLM-friendly output from XBRL mappings and facts")
    parser.add_argument("--mappings", required=True, help="Path to the mapping file (JSON)")
    parser.add_argument("--facts", required=True, help="Path to the facts file (JSON or raw XBRL)")
    parser.add_argument("--output", required=True, help="Path to the output file (JSON)")
    
    args = parser.parse_args()
    
    # Check if mapping file exists
    if not os.path.exists(args.mappings):
        logging.error(f"Mapping file not found: {args.mappings}")
        return 1
    
    # Check if facts file exists
    if not os.path.exists(args.facts):
        logging.error(f"Facts file not found: {args.facts}")
        return 1
    
    # Create LLM-friendly output
    create_llm_friendly_output(args.mappings, args.facts, args.output)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
