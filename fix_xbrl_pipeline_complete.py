#!/usr/bin/env python3
"""
This script fixes the XBRL data structure in the pipeline.py file to
make it compatible with the LLM formatter's expectations.
"""

import os
import re

# Path to the pipeline.py file
PIPELINE_PATH = "/Users/michael/NativeLLM/src2/sec/pipeline.py"

# Create a backup of the original file
BACKUP_PATH = f"{PIPELINE_PATH}.bak2"
os.system(f"cp {PIPELINE_PATH} {BACKUP_PATH}")

# Read the file content
with open(PIPELINE_PATH, 'r', encoding='utf-8') as f:
    content = f.read()

# Step 1: Replace the XBRL data structure initialization
pattern1 = r'# Initialize XBRL data structure\s+xbrl_data = \{\s+"contexts": \{\},\s+"units": \{\},\s+"facts": \[\]\s+\}'
replacement1 = """# Initialize XBRL data structure
                    xbrl_data = {
                        "contexts": {},
                        "units": {},
                        "facts": {},  # Use a dictionary instead of a list
                        "concepts": {}  # Add concepts as expected by formatter
                    }
                    
                    # Create default context for basic facts
                    xbrl_data["contexts"]["AsOf"] = {
                        "entity": {"identifier": metadata.get("ticker", "")},
                        "period": {"instant": metadata.get("period_end_date", "")}
                    }
                    
                    # Helper function to add a fact
                    def add_fact(concept, value, context_ref="AsOf", unit_ref=None, decimals=None):
                        if context_ref not in xbrl_data["facts"]:
                            xbrl_data["facts"][context_ref] = {}
                        if concept not in xbrl_data["facts"][context_ref]:
                            xbrl_data["facts"][context_ref][concept] = []
                        
                        fact_data = {"value": value}
                        if unit_ref:
                            fact_data["unit"] = unit_ref
                        if decimals:
                            fact_data["decimals"] = decimals
                        
                        xbrl_data["facts"][context_ref][concept].append(fact_data)"""

content = re.sub(pattern1, replacement1, content)

# Step 2: Replace the basic document information section
pattern2 = r'# Start with basic document information\s+xbrl_data\["facts"\]\.append\(\{\s+"concept": "DocumentType",\s+"value": filing_type,\s+"context_ref": "AsOf"\s+\}\)\s+xbrl_data\["facts"\]\.append\(\{\s+"concept": "EntityRegistrantName",\s+"value": metadata\.get\("company_name", ""\),\s+"context_ref": "AsOf"\s+\}\)'
replacement2 = """# Start with basic document information
                    # Add document type
                    add_fact("DocumentType", filing_type)
                    # Add company name
                    add_fact("EntityRegistrantName", metadata.get("company_name", ""))"""

content = re.sub(pattern2, replacement2, content)

# Step 3: Replace the XBRL fact extraction logic
pattern3 = r'if ix_tag\.name == \'ix:nonfraction\':\s+fact = \{\s+"concept": ix_tag\.get\(\'name\', \'\'\),\s+"value": ix_tag\.text\.strip\(\),\s+"context_ref": ix_tag\.get\(\'contextref\', \'\'\)\s+\}\s+\s+# Add optional attributes\s+if ix_tag\.get\(\'unitref\'\):\s+fact\["unit_ref"\] = ix_tag\.get\(\'unitref\'\)\s+if ix_tag\.get\(\'decimals\'\):\s+fact\["decimals"\] = ix_tag\.get\(\'decimals\'\)\s+\s+xbrl_data\["facts"\]\.append\(fact\)'
replacement3 = """if ix_tag.name == 'ix:nonfraction':
                                    concept = ix_tag.get('name', '')
                                    value = ix_tag.text.strip()
                                    context_ref = ix_tag.get('contextref', '')
                                    unit_ref = ix_tag.get('unitref', None)
                                    decimals = ix_tag.get('decimals', None)
                                    
                                    # Add the fact using the helper function
                                    add_fact(concept, value, context_ref, unit_ref, decimals)"""

content = re.sub(pattern3, replacement3, content)

# Step 4: Replace the financial table extraction
pattern4 = r'xbrl_data\["facts"\]\.append\(\{\s+"concept": f"Table\{table_idx\}_\{label\.replace\(\' \', \'\'\)\}",\s+"value": value,\s+"context_ref": "AsOf"\s+\}\)'
replacement4 = """add_fact(f"Table{table_idx}_{label.replace(' ', '')}", value, "AsOf")"""

content = re.sub(pattern4, replacement4, content)

# Step 5: Fix the facts count reporting
pattern5 = r'logging\.info\(f"Extracted \{len\(xbrl_data\[\'contexts\'\]\)\} contexts, \{len\(xbrl_data\[\'units\'\]\)\} units, and \{len\(xbrl_data\[\'facts\'\]\)\} facts"\)'
replacement5 = """# Count total facts across all contexts
                        fact_count = sum(len(facts) for context_facts in xbrl_data['facts'].values() for facts in context_facts.values())
                        logging.info(f"Extracted {len(xbrl_data['contexts'])} contexts, {len(xbrl_data['units'])} units, and {fact_count} facts")"""

content = re.sub(pattern5, replacement5, content)

# Write the updated content back to the file
with open(PIPELINE_PATH, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"XBRL data structure in {PIPELINE_PATH} has been updated successfully.")
print(f"The original file has been backed up to {BACKUP_PATH}.")