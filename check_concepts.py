#!/usr/bin/env python3
"""
Check concepts in mappings and facts.
"""

import json

# Load mappings
with open('nvda_mappings.json', 'r') as f:
    mappings = json.load(f)

# Load facts
with open('nvda_facts.json', 'r') as f:
    facts = json.load(f)

# Extract balance sheet concepts from mappings
balance_sheet_concepts = set()
for mapping in mappings['presentation_mappings']:
    if 'balance' in mapping.get('role', '').lower():
        balance_sheet_concepts.add(mapping.get('parent', ''))
        balance_sheet_concepts.add(mapping.get('child', ''))

# Extract concepts from facts
fact_concepts = set(fact.get('name', '') for fact in facts)

# Find common concepts
common_concepts = balance_sheet_concepts.intersection(fact_concepts)

print(f'Balance sheet concepts: {len(balance_sheet_concepts)}')
print(f'Fact concepts: {len(fact_concepts)}')
print(f'Common concepts: {len(common_concepts)}')
print(f'Sample common concepts: {list(common_concepts)[:5]}')

# Check if concepts in mappings have namespace prefixes
mapping_has_prefix = any(':' in concept for concept in balance_sheet_concepts if concept)
print(f'Mapping concepts have namespace prefixes: {mapping_has_prefix}')

# Check if concepts in facts have namespace prefixes
fact_has_prefix = any(':' in concept for concept in fact_concepts if concept)
print(f'Fact concepts have namespace prefixes: {fact_has_prefix}')

# If there's a mismatch in namespace prefixes, try to normalize
if mapping_has_prefix != fact_has_prefix:
    print("Namespace prefix mismatch detected!")
    
    # If mappings have prefixes but facts don't, remove prefixes from mappings
    if mapping_has_prefix and not fact_has_prefix:
        normalized_mapping_concepts = set(concept.split(':')[-1] if ':' in concept else concept for concept in balance_sheet_concepts)
        common_without_prefix = normalized_mapping_concepts.intersection(fact_concepts)
        print(f'Common concepts after removing prefixes from mappings: {len(common_without_prefix)}')
        print(f'Sample: {list(common_without_prefix)[:5]}')
    
    # If facts have prefixes but mappings don't, remove prefixes from facts
    elif fact_has_prefix and not mapping_has_prefix:
        normalized_fact_concepts = set(concept.split(':')[-1] if ':' in concept else concept for concept in fact_concepts)
        common_without_prefix = balance_sheet_concepts.intersection(normalized_fact_concepts)
        print(f'Common concepts after removing prefixes from facts: {len(common_without_prefix)}')
        print(f'Sample: {list(common_without_prefix)[:5]}')
