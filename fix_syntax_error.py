#!/usr/bin/env python3
"""
Fix syntax error in pipeline.py
"""

def fix_syntax_error():
    """Fix the syntax error in pipeline.py"""
    filepath = "src2/sec/pipeline.py"
    
    try:
        # Read the file
        with open(filepath, 'r') as f:
            lines = f.readlines()
        
        # Find and fix the problematic lines
        for i, line in enumerate(lines):
            if "upload_results = {" in line and i < len(lines) - 1:
                if "llm_upload_result = None" in lines[i+1]:
                    # The initialization line is in the wrong place, move it to after the dictionary
                    # Find the end of the dictionary
                    j = i + 1
                    while j < len(lines) and "}" not in lines[j]:
                        j += 1
                    
                    if j < len(lines) and "}" in lines[j]:
                        # Found the end of the dictionary, move initialization line to after this
                        initialization_line = lines.pop(i+1)  # Remove from current position
                        lines.insert(j+1, initialization_line)  # Insert after dictionary closing
                        break
        
        # Write back to the file
        with open(filepath, 'w') as f:
            f.writelines(lines)
        
        print(f"✅ Successfully fixed syntax error in {filepath}")
        return True
    
    except Exception as e:
        print(f"Error fixing syntax error: {e}")
        return False

if __name__ == "__main__":
    fix_syntax_error()