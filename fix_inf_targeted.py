#!/usr/bin/env python3
"""
Targeted fix for handling INF decimals in llm_formatter.py
"""

import os

def fix_inf_error():
    """Fix the INF decimals issue with a more targeted approach"""
    
    file_path = "src2/formatter/llm_formatter.py"
    
    try:
        # First check if the file exists
        if not os.path.exists(file_path):
            print(f"Error: {file_path} not found!")
            return False
            
        # Read the file
        with open(file_path, 'r') as f:
            lines = f.readlines()
            
        # Add our helper function at the top of the file after imports
        # Find where the imports end (usually a blank line after the last import)
        insert_index = 0
        for i, line in enumerate(lines):
            if line.strip().startswith("import ") or line.strip().startswith("from "):
                insert_index = i + 1
                
        # Add a few lines of buffer to make sure we're past imports
        insert_index += 3
                
        # Create our helper function with proper indentation
        helper_function = [
            "def safe_parse_decimals(decimals):\n",
            "    '''Safely parse decimals value, handling INF special case'''\n",
            "    if not decimals:\n",
            "        return None\n",
            "    if str(decimals).strip().upper() == 'INF':\n",
            "        return float('inf')  # Return Python's infinity\n",
            "    try:\n",
            "        return int(decimals)\n",
            "    except (ValueError, TypeError):\n",
            "        return None  # Return None for unparseable values\n",
            "\n",
            "\n"  # Extra blank line for spacing
        ]
        
        # Insert our function
        lines = lines[:insert_index] + helper_function + lines[insert_index:]
        
        # Now find and replace all instances of int(decimals) with safe_parse_decimals(decimals)
        for i, line in enumerate(lines):
            if "int(decimals)" in line:
                lines[i] = line.replace("int(decimals)", "safe_parse_decimals(decimals)")
                
        # Write the modified file
        with open(file_path, 'w') as f:
            f.writelines(lines)
            
        print(f"✅ Successfully added safe_parse_decimals function and fixed int(decimals) calls")
        return True
        
    except Exception as e:
        print(f"Error fixing INF issue: {str(e)}")
        return False

if __name__ == "__main__":
    fix_inf_error()