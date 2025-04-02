#\!/usr/bin/env python3
"""
Simple script to fix whitespace in LLM files
"""
import os
import re
import sys

def clean_whitespace(filename):
    """Clean whitespace in an LLM file"""
    print(f"Processing {filename}...")
    
    # Read the file
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Get original size
    original_size = len(content)
    original_lines = content.count('\n')
    
    # Save a backup
    backup_file = filename + '.backup'
    with open(backup_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    # Apply whitespace fixes
    # 1. Replace 2+ blank lines with a single blank line
    content = re.sub(r'\n{3,}', '\n\n', content)
    
    # 2. Ensure sections are separated by a single blank line
    content = re.sub(r'\n\n+(@SECTION)', r'\n\n\1', content)
    
    # 3. Remove empty lines in tables
    content = re.sub(r'(\| +-+ +\|)\n+', r'\1\n', content)
    content = re.sub(r'(\| +\|)\n+', r'\1\n', content)
    
    # 4. Condense whitespace in table cells
    content = re.sub(r'\| +([^|]*) +\|', r'| \1 |', content)
    
    # 5. Remove trailing whitespace on each line
    content = re.sub(r' +\n', '\n', content)
    
    # 6. Ensure there's a blank line before new section headers
    content = re.sub(r'([^\n])(\n@[A-Z])', r'\1\n\2', content)
    
    # Write the cleaned content
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    
    # Get new size
    new_size = len(content)
    new_lines = content.count('\n')
    
    # Calculate reduction
    size_reduction = original_size - new_size
    size_reduction_percent = (size_reduction / original_size) * 100
    line_reduction = original_lines - new_lines
    
    print(f"Original size: {original_size:,} bytes ({original_lines:,} lines)")
    print(f"New size: {new_size:,} bytes ({new_lines:,} lines)")
    print(f"Reduction: {size_reduction:,} bytes ({size_reduction_percent:.1f}%)")
    print(f"Line reduction: {line_reduction:,} lines")
    print(f"Backup saved to {backup_file}")

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python fix_whitespace.py file1.txt [file2.txt ...]")
        return
    
    for filename in sys.argv[1:]:
        if os.path.exists(filename) and filename.endswith('.txt'):
            clean_whitespace(filename)
        else:
            print(f"File not found or not a text file: {filename}")

if __name__ == "__main__":
    main()
