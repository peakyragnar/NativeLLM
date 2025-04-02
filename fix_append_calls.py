#\!/usr/bin/env python3
"""
This script fixes the remaining xbrl_data["facts"].append calls in the pipeline.py file
"""

import os
import re

# Path to the pipeline.py file
PIPELINE_PATH = "/Users/michael/NativeLLM/src2/sec/pipeline.py"

# Create a backup of the original file
BACKUP_PATH = f"{PIPELINE_PATH}.bak3"
os.system(f"cp {PIPELINE_PATH} {BACKUP_PATH}")

# Read the file content
with open(PIPELINE_PATH, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix corrupted lines by removing old append calls that weren't properly replaced
content = re.sub(r'                        "value": metadata\.get\("company_name", ""\),\s+                        "context_ref": "AsOf"\s+                    \}\)', '', content)

# Fix all remaining xbrl_data["facts"].append calls
content = re.sub(
    r'xbrl_data\["facts"\]\.append\(\{\s+"concept": f"Table\{table_idx\}_\{label\.replace\(\' \', \'\'\)\}",\s+"value": value,\s+"context_ref": "AsOf"\s+\}\)',
    'add_fact(f"Table{table_idx}_{label.replace(\' \', \'\')}", value, "AsOf")',
    content
)

# Fix the facts count reporting
content = re.sub(
    r'logging\.info\(f"Extracted \{len\(xbrl_data\[\'contexts\'\]\)\} contexts, \{len\(xbrl_data\[\'units\'\]\)\} units, and \{len\(xbrl_data\[\'facts\'\]\)\} facts"\)',
    '                        # Count total facts across all contexts\n                        fact_count = sum(len(facts) for context_facts in xbrl_data[\'facts\'].values() for facts in context_facts.values())\n                        logging.info(f"Extracted {len(xbrl_data[\'contexts\'])} contexts, {len(xbrl_data[\'units\'])} units, and {fact_count} facts")',
    content
)

# Write the updated content back to the file
with open(PIPELINE_PATH, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"Remaining xbrl_data[\"facts\"].append calls in {PIPELINE_PATH} have been fixed.")
print(f"The original file has been backed up to {BACKUP_PATH}.")
