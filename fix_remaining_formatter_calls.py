#!/usr/bin/env python3
"""
Fix remaining LLM formatter calls in pipeline.py
"""

import re

# Read the pipeline.py file
with open("src2/sec/pipeline.py", "r") as f:
    content = f.read()

# Fix any remaining references to llm_formatter
print("Checking for references to llm_formatter...")

# Patterns to look for
patterns = [
    r"llm_formatter\s*=\s*LLMFormatter\(\)",
    r"llm_content\s*=\s*llm_formatter.generate_llm_format\(",
    r"llm_content\s*=\s*LLMFormatter\(\).generate_llm_format\(",
]

# Check if any of the correct patterns exist
fixed_format_exists = any(re.search(pattern, content) for pattern in patterns)

if fixed_format_exists:
    print("Found correctly formatted LLMFormatter usage")
else:
    # Need to find and fix the problematic line
    print("Didn't find correct formatter usage, searching for problematic line...")
    
    # Look for the problematic line
    llm_content_lines = re.findall(r'(.*llm_content\s*=.*)', content)
    for line in llm_content_lines:
        print(f"Found potential issue: {line.strip()}")
    
    # Replace any remaining problematic patterns
    content = re.sub(
        r'llm_content\s*=\s*llm_formatter\.generate_llm_format\(',
        'llm_content = LLMFormatter().generate_llm_format(',
        content
    )
    
    # Write the corrected file
    with open("src2/sec/pipeline.py", "w") as f:
        f.write(content)
    
    print("✅ Fixed remaining LLM formatter references in pipeline.py")
    
# Fix missing datetime import
if "import datetime" not in content:
    # Add datetime import at the top of the imports section
    content = re.sub(
        r'import os\nimport re\nimport time\nimport logging',
        'import os\nimport re\nimport time\nimport logging\nimport datetime',
        content
    )
    
    # Write the corrected file
    with open("src2/sec/pipeline.py", "w") as f:
        f.write(content)
    
    print("✅ Added missing datetime import in pipeline.py")