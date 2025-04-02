#\!/usr/bin/env python3
"""
This script creates a clean fixed version of pipeline.py by running a search-and-replace
operation on specific patterns that need to be fixed for XBRL data handling.
"""

import re
import os

def fix_pipeline_file():
    """
    Make targeted fixes to the pipeline.py file for XBRL data handling
    """
    # Path to the pipeline file
    pipeline_path = "/Users/michael/NativeLLM/src2/sec/pipeline.py"
    
    # Create backup
    backup_path = f"{pipeline_path}.bak.final"
    os.system(f"cp {pipeline_path} {backup_path}")
    print(f"Created backup at {backup_path}")
    
    # Read the file
    with open(pipeline_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Fix 1: Update the XBRL data initialization
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
    
    # Fix 2: Update the document info section
    pattern2 = r'# Start with basic document information\s+xbrl_data\["facts"\]\.append\(\{\s+"concept": "DocumentType",\s+"value": filing_type,\s+"context_ref": "AsOf"\s+\}\)\s+xbrl_data\["facts"\]\.append\(\{\s+"concept": "EntityRegistrantName",\s+"value": metadata\.get\("company_name", ""\),\s+"context_ref": "AsOf"\s+\}\)'
    replacement2 = """# Start with basic document information
                    # Add document type
                    add_fact("DocumentType", filing_type)
                    # Add company name
                    add_fact("EntityRegistrantName", metadata.get("company_name", ""))"""
    content = re.sub(pattern2, replacement2, content)
    
    # Fix 3: Update the ix:nonfraction section
    pattern3 = r'if ix_tag\.name == \'ix:nonfraction\':[^}]+?xbrl_data\["facts"\]\.append\(fact\)'
    replacement3 = """if ix_tag.name == 'ix:nonfraction':
                                    concept = ix_tag.get('name', '')
                                    value = ix_tag.text.strip()
                                    context_ref = ix_tag.get('contextref', '')
                                    unit_ref = ix_tag.get('unitref', None)
                                    decimals = ix_tag.get('decimals', None)
                                    
                                    # Add the fact using the helper function
                                    add_fact(concept, value, context_ref, unit_ref, decimals)"""
    content = re.sub(pattern3, replacement3, content)
    
    # Fix 4: Update financial table extraction
    pattern4 = r'xbrl_data\["facts"\]\.append\(\{\s+"concept": f"Table\{table_idx\}_\{label\.replace\(\' \', \'\'\)\}",\s+"value": value,\s+"context_ref": "AsOf"\s+\}\)'
    replacement4 = """add_fact(f"Table{table_idx}_{label.replace(' ', '')}", value, "AsOf")"""
    content = re.sub(pattern4, replacement4, content)
    
    # Fix 5: Update the fact count log
    pattern5 = r'logging\.info\(f"Extracted \{len\(xbrl_data\[\'contexts\'\]\)\} contexts, \{len\(xbrl_data\[\'units\'\]\)\} units, and \{len\(xbrl_data\[\'facts\'\]\)\} facts"\)'
    replacement5 = """# Count total facts across all contexts
                        fact_count = sum(len(facts) for context_facts in xbrl_data['facts'].values() for facts in context_facts.values())
                        logging.info(f"Extracted {len(xbrl_data['contexts'])} contexts, {len(xbrl_data['units'])} units, and {fact_count} facts")"""
    content = re.sub(pattern5, replacement5, content)
    
    # Clean up any remaining corrupted sections
    # Fix 6: Remove any lingering old add code
    pattern6 = r'                        "value": metadata\.get\("company_name", ""\),\s+                        "context_ref": "AsOf"\s+                    \}\)'
    replacement6 = ""
    content = re.sub(pattern6, replacement6, content)
    
    pattern7 = r'                        fact\["decimals"\] = ix_tag\.get\(\'decimals\'\)'
    replacement7 = ""
    content = re.sub(pattern7, replacement7, content)
    
    # Write the fixed content back to the file
    with open(pipeline_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Applied fixes to {pipeline_path}")

if __name__ == "__main__":
    fix_pipeline_file()
