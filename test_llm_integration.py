# test_llm_integration.py
import os
import sys
import glob
import argparse

def read_llm_file(file_path):
    """Read an LLM format file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def display_llm_file_info(file_path):
    """Display information about an LLM format file"""
    content = read_llm_file(file_path)
    
    # Extract basic metadata
    lines = content.split('\n')
    metadata = {}
    
    for line in lines[:20]:  # Look in first 20 lines for metadata
        if line.startswith('@DOCUMENT:'):
            metadata['document'] = line.replace('@DOCUMENT:', '').strip()
        elif line.startswith('@FILING_DATE:'):
            metadata['filing_date'] = line.replace('@FILING_DATE:', '').strip()
        elif line.startswith('@COMPANY:'):
            metadata['company'] = line.replace('@COMPANY:', '').strip()
        elif line.startswith('@CIK:'):
            metadata['cik'] = line.replace('@CIK:', '').strip()
    
    # Count facts
    fact_count = content.count('@CONCEPT:')
    
    print(f"File: {os.path.basename(file_path)}")
    print(f"Size: {len(content):,} bytes")
    print(f"Document: {metadata.get('document', 'Unknown')}")
    print(f"Company: {metadata.get('company', 'Unknown')}")
    print(f"Filing Date: {metadata.get('filing_date', 'Unknown')}")
    print(f"Facts: {fact_count:,}")
    print()

def list_available_files():
    """List all available LLM format files"""
    from src.config import PROCESSED_DATA_DIR
    
    pattern = os.path.join(PROCESSED_DATA_DIR, "**", "*_llm.txt")
    files = glob.glob(pattern, recursive=True)
    
    if not files:
        print("No LLM format files found.")
        return []
    
    print(f"Found {len(files)} LLM format files:")
    for i, file_path in enumerate(files, 1):
        print(f"{i}. {os.path.basename(file_path)}")
    
    return files

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test LLM format files")
    parser.add_argument('--file', help='Path to a specific LLM format file to examine')
    parser.add_argument('--list', action='store_true', help='List all available LLM format files')
    
    args = parser.parse_args()
    
    if args.list:
        list_available_files()
    elif args.file:
        display_llm_file_info(args.file)
    else:
        files = list_available_files()
        if files and len(files) > 0:
            display_llm_file_info(files[0])  # Display the first file