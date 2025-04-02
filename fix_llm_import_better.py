#!/usr/bin/env python3
"""
Fix LLM formatter import in pipeline.py (improved version)
"""

import re

# Read the pipeline.py file
with open("src2/sec/pipeline.py", "r") as f:
    content = f.read()

# Replace the import statements first
content = re.sub(
    r'from src2\.formatter\.llm_formatter import llm_formatter',
    'from src2.formatter import LLMFormatter',
    content
)

# Now fix all references to the imported name
processed_content = ""
in_processed_section = False
formatter_fixed = False

for line in content.split('\n'):
    # If we see the import, mark that we're now in the processed section
    if "from src2.formatter import LLMFormatter" in line:
        processed_content += line + "\n"
        in_processed_section = True
        continue
        
    # Only replace instances of llm_formatter that aren't inside LLMFormatter
    if in_processed_section and "llm_formatter" in line and not formatter_fixed:
        # Replace standalone instances of llm_formatter with LLMFormatter()
        line = re.sub(r'\bllm_formatter\b(?!\()', 'LLMFormatter()', line)
        
        # Special case for formatter instantiation
        if "LLMFormatter()" in line:
            formatter_fixed = True
            
    processed_content += line + "\n"

# Write the corrected file
with open("src2/sec/pipeline.py", "w") as f:
    f.write(processed_content)

print("✅ Fixed LLM formatter import in pipeline.py")