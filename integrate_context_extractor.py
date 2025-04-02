import os
import sys
import shutil

# Copy our working context extractor to the formatter directory
src_file = 'test_context_extractor.py'
dest_file = 'src2/formatter/context_extractor.py'

print(f"Copying {src_file} to {dest_file}")
shutil.copy(src_file, dest_file)
print("File copied successfully")

# Add import of context_extractor to __init__.py
init_file = 'src2/formatter/__init__.py'
with open(init_file, 'r') as f:
    content = f.read()

if 'from .context_extractor import' not in content:
    print(f"Adding import to {init_file}")
    with open(init_file, 'w') as f:
        # Replace the existing import line with one that includes context_extractor
        new_content = content.replace(
            'from .llm_formatter import LLMFormatter',
            'from .llm_formatter import LLMFormatter\nfrom .context_extractor import extract_contexts_from_html'
        )
        f.write(new_content)
    print("Added import to formatter/__init__.py")

print("Integration complete")
