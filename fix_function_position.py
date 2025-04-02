#!/usr/bin/env python3
"""
Fix the position of safe_parse_decimals function
"""

import os

def fix_function_position():
    """Move safe_parse_decimals function to the correct position"""
    
    file_path = "src2/formatter/llm_formatter.py"
    
    try:
        # Read the file line by line
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # Find the safe_parse_decimals function
        function_start = None
        function_end = None
        
        for i, line in enumerate(lines):
            if line.strip().startswith("def safe_parse_decimals"):
                function_start = i
                # Find the end of the function (next blank line or next def)
                for j in range(i + 1, len(lines)):
                    if lines[j].strip() == "" or lines[j].strip().startswith("def "):
                        function_end = j
                        break
                
                if not function_end:
                    function_end = len(lines)
                
                break
        
        if function_start is not None and function_end is not None:
            # Extract the function
            function_lines = lines[function_start:function_end]
            
            # Remove it from its current position
            new_lines = lines[:function_start] + lines[function_end:]
            
            # Find where to insert it (after imports, before class definition)
            insert_pos = 0
            for i, line in enumerate(new_lines):
                if line.strip().startswith("import ") or line.strip().startswith("from "):
                    insert_pos = i + 1
            
            # Look for a blank line after imports
            for i in range(insert_pos, len(new_lines)):
                if new_lines[i].strip() == "":
                    insert_pos = i + 1
                    break
            
            # Insert the function
            new_lines = new_lines[:insert_pos] + function_lines + new_lines[insert_pos:]
            
            # Write the file back
            with open(file_path, 'w') as f:
                f.writelines(new_lines)
            
            print(f"✅ Successfully moved safe_parse_decimals function to position {insert_pos}")
            return True
        else:
            print(f"Could not find safe_parse_decimals function")
            return False
    
    except Exception as e:
        print(f"Error moving function: {str(e)}")
        return False

if __name__ == "__main__":
    fix_function_position()