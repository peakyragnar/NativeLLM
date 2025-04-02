#!/usr/bin/env python3
"""
Fix the LLM formatter to use the safe_parse_decimals function
"""

import os
import re

def fix_llm_formatter_imports():
    """Update import statement in LLM formatter"""
    
    file_path = "src2/formatter/llm_formatter.py"
    
    try:
        # Read the file
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Find the import statement for normalize_value
        pattern = r'from\s+\.normalize_value\s+import\s+([^\n]+)'
        match = re.search(pattern, content)
        
        if match:
            # Add safe_parse_decimals to the import
            import_line = match.group(0)
            imports = match.group(1)
            
            if 'safe_parse_decimals' not in imports:
                if imports.endswith(','):
                    # Already has a trailing comma
                    new_imports = imports + ' safe_parse_decimals'
                else:
                    # Add comma and the new import
                    new_imports = imports + ', safe_parse_decimals'
                
                new_import_line = import_line.replace(imports, new_imports)
                content = content.replace(import_line, new_import_line)
                
                # Now find and replace int(decimals) with safe_parse_decimals(decimals)
                content = content.replace("int(decimals)", "safe_parse_decimals(decimals)")
                
                # Write the updated content back
                with open(file_path, 'w') as f:
                    f.write(content)
                
                print(f"✅ Successfully updated {file_path} to use safe_parse_decimals")
                return True
            else:
                print(f"safe_parse_decimals already in imports")
                return True
        else:
            print(f"Could not find import statement for normalize_value")
            return False
            
    except Exception as e:
        print(f"Error updating imports: {str(e)}")
        return False

if __name__ == "__main__":
    fix_llm_formatter_imports()