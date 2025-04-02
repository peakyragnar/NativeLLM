#\!/usr/bin/env python3
"""
This script performs a direct cleanup of the remaining xbrl_data["facts"].append calls
"""

def cleanup_pipeline():
    # Path to the pipeline file
    pipeline_path = "/Users/michael/NativeLLM/src2/sec/pipeline.py"
    
    # Read the file
    with open(pipeline_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Remove the specific append lines
    cleaned_lines = []
    for line in lines:
        if "xbrl_data[\"facts\"].append(fact)" not in line:
            cleaned_lines.append(line)
    
    # Write back the cleaned file
    with open(pipeline_path, 'w', encoding='utf-8') as f:
        f.writelines(cleaned_lines)
    
    print("Removed remaining xbrl_data[\"facts\"].append calls")

if __name__ == "__main__":
    cleanup_pipeline()
