#\!/usr/bin/env python3
"""
This script checks if our XBRL data formatting fixes have been applied correctly.
"""

import re
import sys

def check_pipeline_fix():
    """
    Check if the pipeline.py file has been fixed correctly for XBRL data handling
    """
    # Path to the pipeline file
    pipeline_path = "/Users/michael/NativeLLM/src2/sec/pipeline.py"
    
    # Read the file
    with open(pipeline_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check 1: XBRL data initialization
    if re.search(r'xbrl_data = \{\s+"contexts": \{\},\s+"units": \{\},\s+"facts": \{\},', content):
        print("✅ XBRL data structure fixed: using dictionary for facts")
    else:
        print("❌ XBRL data structure not fixed: still using list for facts")
    
    # Check 2: Helper function
    if re.search(r'def add_fact\(concept, value, context_ref="AsOf", unit_ref=None, decimals=None\):', content):
        print("✅ Helper function add_fact is present")
    else:
        print("❌ Helper function add_fact is missing")
    
    # Check 3: Document info section
    if re.search(r'add_fact\("DocumentType", filing_type\)', content):
        print("✅ Document info section fixed: using add_fact")
    else:
        print("❌ Document info section not fixed: still using append")
    
    # Check 4: ix:nonfraction section
    pattern4 = r'if ix_tag\.name == \'ix:nonfraction\':[^}]+?add_fact\(concept, value, context_ref, unit_ref, decimals\)'
    if re.search(pattern4, content, re.DOTALL):
        print("✅ ix:nonfraction section fixed: using add_fact")
    else:
        print("❌ ix:nonfraction section not fixed: still using append")
    
    # Check 5: Fact count log
    if re.search(r'fact_count = sum\(len\(facts\) for context_facts in xbrl_data\[\'facts\'\]\.values\(\) for facts in context_facts\.values\(\)\)', content):
        print("✅ Fact count log fixed: properly counting dictionary facts")
    else:
        print("❌ Fact count log not fixed: still counting list facts")
    
    # Check for any remaining append calls
    remaining_appends = len(re.findall(r'xbrl_data\["facts"\]\.append\(', content))
    if remaining_appends == 0:
        print(f"✅ No remaining xbrl_data['facts'].append calls found")
    else:
        print(f"❌ Found {remaining_appends} remaining xbrl_data['facts'].append calls")
    
    # Overall check
    if re.search(r'xbrl_data = \{\s+"contexts": \{\},\s+"units": \{\},\s+"facts": \{\},', content) and \
       re.search(r'def add_fact\(concept, value, context_ref="AsOf", unit_ref=None, decimals=None\):', content) and \
       re.search(r'add_fact\("DocumentType", filing_type\)', content) and \
       remaining_appends == 0:
        print("\n✅ All XBRL data fixes have been applied successfully\!")
        return 0
    else:
        print("\n❌ Some XBRL data fixes are missing or incomplete. Please check the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(check_pipeline_fix())
