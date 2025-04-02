#!/usr/bin/env python3
"""
Corrected fix for the pipeline.py file
"""

import os

def apply_fix():
    """Apply the fix correctly"""
    pipeline_path = "src2/sec/pipeline.py"
    
    # First, check if the file exists
    try:
        with open(pipeline_path, 'r') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading file: {e}")
        return False
    
    # Define the problematic section and the correct fix
    # We need to add the initialization AFTER the upload_results dictionary is fully defined
    old_section = '''                upload_results = {
                    "llm_upload": None,
                    "metadata": None
                }'''
    
    new_section = '''                upload_results = {
                    "llm_upload": None,
                    "metadata": None
                }
                llm_upload_result = None  # Initialize to prevent reference error'''
    
    # Apply the fix
    if old_section in content:
        new_content = content.replace(old_section, new_section)
        
        # Write the fixed content back
        try:
            with open(pipeline_path, 'w') as f:
                f.write(new_content)
            print(f"✅ Successfully fixed {pipeline_path}")
            return True
        except Exception as e:
            print(f"Error writing file: {e}")
            return False
    else:
        print(f"Could not find the target section in {pipeline_path}")
        return False

if __name__ == "__main__":
    apply_fix()