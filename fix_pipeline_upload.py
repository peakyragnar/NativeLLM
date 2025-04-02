#!/usr/bin/env python3
"""
Fix for the pipeline.py file to address the 'llm_upload_result' reference error
"""

import os

def apply_fix():
    """Apply the fix to pipeline.py"""
    try:
        # Path to the pipeline file
        pipeline_path = os.path.join('src2', 'sec', 'pipeline.py')
        
        # Ensure the file exists
        if not os.path.exists(pipeline_path):
            print(f"Error: {pipeline_path} not found")
            return False
        
        # Read the file
        with open(pipeline_path, 'r') as f:
            content = f.read()
        
        # Find the section with the upload_results initialization
        target_line = "                upload_results = {\n                    \"llm_upload\": None,"
        fix_line = "                upload_results = {\n                    \"llm_upload\": None,\n                llm_upload_result = None  # Initialize to prevent reference error"
        
        # Apply the fix
        if target_line in content:
            new_content = content.replace(target_line, fix_line)
            
            # Write the fixed content back
            with open(pipeline_path, 'w') as f:
                f.write(new_content)
            
            print(f"✅ Successfully fixed {pipeline_path}")
            return True
        else:
            print(f"Error: Could not find the target line in {pipeline_path}")
            return False
    
    except Exception as e:
        print(f"Error applying fix: {str(e)}")
        return False

if __name__ == "__main__":
    apply_fix()