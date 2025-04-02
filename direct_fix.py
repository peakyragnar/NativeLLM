#!/usr/bin/env python3
"""
Direct fix for the pipeline.py file
"""

def apply_fix():
    """Apply the fix directly"""
    pipeline_path = "src2/sec/pipeline.py"
    
    # First, check if the file exists
    try:
        with open(pipeline_path, 'r') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading file: {e}")
        return False
    
    # Look for the upload_results initialization line
    insert_line = None
    for i, line in enumerate(lines):
        if "upload_results =" in line:
            insert_line = i
            break
    
    if insert_line is None:
        print("Could not find upload_results initialization")
        return False
    
    # Insert our fix line after the upload_results initialization
    lines.insert(insert_line + 1, "                llm_upload_result = None  # Initialize to prevent reference error\n")
    
    # Write the file back
    try:
        with open(pipeline_path, 'w') as f:
            f.writelines(lines)
        print(f"✅ Successfully fixed {pipeline_path}")
        return True
    except Exception as e:
        print(f"Error writing file: {e}")
        return False

if __name__ == "__main__":
    apply_fix()