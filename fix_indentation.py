#!/usr/bin/env python3
"""
Fix indentation issues in llm_formatter.py
"""

import os

def fix_indentation():
    """Fix indentation issues in llm_formatter.py"""
    
    file_path = "src2/formatter/llm_formatter.py"
    
    try:
        # First, let's back up the file
        backup_path = file_path + ".bak"
        os.system(f"cp {file_path} {backup_path}")
        print(f"Created backup at {backup_path}")
        
        # Read the file with 'rU' (Universal newline mode)
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # Process each line to ensure consistent indentation
        fixed_lines = []
        for line in lines:
            # Replace tabs with 4 spaces
            line = line.replace('\t', '    ')
            fixed_lines.append(line)
        
        # Write the fixed content back
        with open(file_path, 'w') as f:
            f.writelines(fixed_lines)
        
        print(f"✅ Fixed indentation in {file_path}")
        return True
    
    except Exception as e:
        print(f"Error fixing indentation: {str(e)}")
        # Try to restore from backup if available
        if os.path.exists(backup_path):
            os.system(f"cp {backup_path} {file_path}")
            print(f"Restored from backup due to error")
        return False

if __name__ == "__main__":
    fix_indentation()