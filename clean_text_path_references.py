#!/usr/bin/env python3
"""
Clean up text_path and text.txt references across the codebase.
This script systematically removes references to the deprecated text.txt file format.
"""

import os
import re
import sys
from pathlib import Path

def process_file(file_path):
    """Process a single file to remove text_path references."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Keep track of changes
    original_content = content
    
    # Common patterns to fix
    replacements = [
        # Variables and assignments
        (r'text_path\s*=\s*.*?\n', ''),  # Remove text_path assignment lines
        (r'gcs_text_path\s*=\s*.*?\n', ''),  # Remove gcs_text_path assignment lines
        (r'new_text_path\s*=\s*.*?\n', ''),  # Remove new_text_path assignment lines
        
        # Parallel operations on text and llm paths
        (r'text_path.*?\n\s+llm_path', 'llm_path'),  # Keep just llm_path line
        
        # Dictionary entries
        (r'"text_path"\s*:\s*[^,}]+,?\s*\n', ''),  # Remove "text_path" dict entries
        (r"'text_path'\s*:\s*[^,}]+,?\s*\n", ''),  # Remove 'text_path' dict entries
        
        # If conditions checking text_path
        (r'if\s+(?:.*?and\s+)?text_path(?:\s+and.*?)?\s*:', 'if llm_path:'),  # if text_path -> if llm_path
        (r'if\s+(?:not\s+text_path\s+or\s+not\s+llm_path)', 'if not llm_path'),  # if not text_path or not llm_path -> if not llm_path
        
        # Copy/upload operations
        (r'if\s+os\.path\.exists\(text_path\):.*?\n(?:\s+.*?\n)*?(?=\s+if\s+os\.path\.exists\(llm_path\):)', 
         ''),  # Remove text_path copy blocks
        
        # text.txt strings
        (r'[\'"](/.*?|companies/.*?)text\.txt[\'"]', r'"\1llm.txt"'),  # Replace text.txt with llm.txt
    ]
    
    # Apply replacements
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    # Check if any changes were made
    if content != original_content:
        # Backup original file
        backup_path = f"{file_path}.bak"
        with open(backup_path, 'w') as f:
            f.write(original_content)
        
        # Write updated content
        with open(file_path, 'w') as f:
            f.write(content)
        
        return True
    
    return False

def main():
    """Main function to process all Python files in src2."""
    src_dir = Path('/Users/michael/NativeLLM/src2')
    
    # Files known to have text_path references
    key_files = [
        src_dir / 'config.py',
        src_dir / 'sec/batch_pipeline.py',
        src_dir / 'sec/pipeline.py',
        src_dir / 'storage/gcp_storage.py',
        src_dir / 'xbrl/html_text_extractor.py'
    ]
    
    # Process known files first
    print("Processing key files:")
    for file_path in key_files:
        if file_path.exists():
            changed = process_file(file_path)
            print(f"  {'UPDATED' if changed else 'UNCHANGED'}: {file_path}")
        else:
            print(f"  NOT FOUND: {file_path}")
    
    # Optionally process all Python files
    if len(sys.argv) > 1 and sys.argv[1] == '--all':
        print("\nProcessing all Python files:")
        for root, _, files in os.walk(src_dir):
            for file in files:
                if file.endswith('.py') and not file.endswith('.bak'):
                    file_path = Path(root) / file
                    if file_path not in key_files:  # Skip already processed files
                        changed = process_file(file_path)
                        if changed:
                            print(f"  UPDATED: {file_path}")

if __name__ == "__main__":
    main()