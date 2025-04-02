#!/usr/bin/env python3
"""
Minimal fix for the INF issue in llm_formatter.py
"""

import os

def apply_minimal_fix():
    """Apply a minimal fix to handle 'INF' decimals"""
    
    file_path = "src2/formatter/llm_formatter.py"
    backup_path = "src2/formatter/llm_formatter.py.fixed"
    
    # First make a backup of our current file
    os.system(f"cp {file_path} {backup_path}")
    print(f"Created backup at {backup_path}")
    
    try:
        # First, add the safe_parse_decimals function
        function_def = '''
# Helper function to safely handle INF decimals values
def safe_parse_decimals(decimals):
    """Safely parse decimals value, handling INF special case"""
    if not decimals:
        return None
    if str(decimals).strip().upper() == 'INF':
        return float('inf')  # Return Python's infinity
    try:
        return int(decimals)
    except (ValueError, TypeError):
        return None  # Return None for unparseable values

'''

        # Add the function to the beginning of the file
        with open(file_path, 'r') as f:
            content = f.read()
            
        # Find the first actual code (after imports and docstring)
        import_section_end = content.find("class LLMFormatter")
        if import_section_end == -1:
            print("Could not find class LLMFormatter in the file")
            return False
            
        # Insert our function before the LLMFormatter class
        new_content = content[:import_section_end] + function_def + content[import_section_end:]
        
        # Now find and replace all instances of int(decimals) using a simpler approach
        new_content = new_content.replace("int(decimals)", "safe_parse_decimals(decimals)")
        
        # Write the modified content
        with open(file_path, 'w') as f:
            f.write(new_content)
            
        print(f"✅ Successfully applied minimal fix to {file_path}")
        return True
        
    except Exception as e:
        print(f"Error applying minimal fix: {str(e)}")
        # Restore from backup
        os.system(f"cp {backup_path} {file_path}")
        print(f"Restored original file after error")
        return False

if __name__ == "__main__":
    apply_minimal_fix()