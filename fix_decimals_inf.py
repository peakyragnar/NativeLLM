#!/usr/bin/env python3
"""
Fix the handling of 'INF' decimals values in the LLM formatter
"""

import os
import re

def fix_llm_formatter():
    """Fix the handling of INF values in the LLM formatter"""
    file_path = "src2/formatter/llm_formatter.py"
    
    try:
        # Check if the file exists
        if not os.path.exists(file_path):
            print(f"Error: {file_path} not found!")
            return False
        
        # Read the file content
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Find and fix instances of unsafe int(decimals) conversions
        # This pattern looks for int(decimals) without a check for INF
        pattern = r'(if\s+decimals\s+and\s+)int\(decimals\)(\s*==\s*-\d+)'
        replacement = r'\1safe_parse_decimals(decimals)\2'
        
        # Replace all occurrences
        content = re.sub(pattern, replacement, content)
        
        # Add a safe decimals parsing function at the top of the file
        # Find the first function definition
        first_function = re.search(r'def\s+\w+\(', content)
        if first_function:
            # Insert before the first function
            insert_pos = first_function.start()
            
            # Safe parse function to add
            safe_parse_function = """
def safe_parse_decimals(decimals):
    '''Safely parse decimals value, handling 'INF' special case'''
    if not decimals:
        return None
    if str(decimals).strip().upper() == 'INF':
        return float('inf')  # Return Python's infinity
    try:
        return int(decimals)
    except (ValueError, TypeError):
        return None  # Return None for unparseable values

"""
            
            # Insert the function before the first function
            content = content[:insert_pos] + safe_parse_function + content[insert_pos:]
            
            # Replace any other direct int(decimals) calls that might not be in the pattern
            # This is a more aggressive replacement, only do this if needed
            content = content.replace("int(decimals)", "safe_parse_decimals(decimals)")
            
            # Write the changes back to the file
            with open(file_path, 'w') as f:
                f.write(content)
            
            print(f"✅ Successfully fixed {file_path} to handle 'INF' decimals values")
            return True
        else:
            print("Error: Could not find a suitable insertion point for the safe_parse_decimals function")
            return False
        
    except Exception as e:
        print(f"Error fixing llm_formatter.py: {str(e)}")
        return False

if __name__ == "__main__":
    fix_llm_formatter()