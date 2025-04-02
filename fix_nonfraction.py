#\!/usr/bin/env python3
"""
This script fixes the ix:nonfraction tag handling in the pipeline.py file
"""

import os
import re

# Path to the pipeline.py file
PIPELINE_PATH = "/Users/michael/NativeLLM/src2/sec/pipeline.py"

# Create a backup of the original file
BACKUP_PATH = f"{PIPELINE_PATH}.bak4"
os.system(f"cp {PIPELINE_PATH} {BACKUP_PATH}")

# Read the file content
with open(PIPELINE_PATH, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the ix:nonfraction tag handling
pattern = re.compile(r'if ix_tag\.name == \'ix:nonfraction\':\s+fact = \{\s+"concept": ix_tag\.get\(\'name\', \'\'\),\s+"value": ix_tag\.text\.strip\(\),\s+"context_ref": ix_tag\.get\(\'contextref\', \'\'\)\s+\}\s+\s+# Add optional attributes\s+if ix_tag\.get\(\'unitref\'\):\s+fact\["unit_ref"\] = ix_tag\.get\(\'unitref\'\)\s+if ix_tag\.get\(\'decimals\'\):\s+fact\["decimals"\] = ix_tag\.get\(\'decimals\'\)', re.DOTALL)

replacement = """if ix_tag.name == 'ix:nonfraction':
                                    concept = ix_tag.get('name', '')
                                    value = ix_tag.text.strip()
                                    context_ref = ix_tag.get('contextref', '')
                                    unit_ref = ix_tag.get('unitref', None)
                                    decimals = ix_tag.get('decimals', None)
                                    
                                    # Add the fact using the helper function
                                    add_fact(concept, value, context_ref, unit_ref, decimals)"""

content = pattern.sub(replacement, content)

# Write the updated content back to the file
with open(PIPELINE_PATH, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"The ix:nonfraction tag handling in {PIPELINE_PATH} has been fixed.")
print(f"The original file has been backed up to {BACKUP_PATH}.")
