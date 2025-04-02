#!/usr/bin/env python3
"""
Fix LLM formatter import in pipeline.py
"""

import re

# Read the pipeline.py file
with open("src2/sec/pipeline.py", "r") as f:
    content = f.read()

# Fix the import statement
fixed_content = re.sub(
    r'from src2\.formatter\.llm_formatter import llm_formatter',
    'from src2.formatter import LLMFormatter',
    content
)

# Fix any references to llm_formatter
fixed_content = re.sub(
    r'llm_formatter\(',
    'LLMFormatter()(',
    fixed_content
)

# Fix other remaining references to llm_formatter as a variable
fixed_content = re.sub(
    r'llm_formatter',
    'LLMFormatter()',
    fixed_content
)

# Write the corrected file
with open("src2/sec/pipeline.py", "w") as f:
    f.write(fixed_content)

print("✅ Fixed LLM formatter import in pipeline.py")