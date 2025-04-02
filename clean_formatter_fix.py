#!/usr/bin/env python3
"""
Clean fix for llm_formatter.py
"""

def fix_formatter():
    """Add import for safe_parse_decimals and replace int(decimals) calls"""
    
    file_path = "src2/formatter/llm_formatter.py"
    
    # Check if file exists
    try:
        with open(file_path, 'r') as f:
            content = f.read()
    except:
        print(f"Error: Could not open {file_path}")
        return False
    
    # Add import for safe_parse_decimals
    if "from .normalize_value import" in content:
        # There's already an import from normalize_value, add safe_parse_decimals
        import_line = None
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            if "from .normalize_value import" in line and "safe_parse_decimals" not in line:
                # Add safe_parse_decimals to the import
                lines[i] = line.rstrip()
                if lines[i].endswith(','):
                    lines[i] += " safe_parse_decimals"
                else:
                    lines[i] += ", safe_parse_decimals"
                import_line = lines[i]
                break
        
        if import_line:
            # Write the updated content
            with open(file_path, 'w') as f:
                f.write('\n'.join(lines))
            print(f"Added safe_parse_decimals to import statement: {import_line}")
        else:
            print("Could not find import statement for normalize_value")
            return False
    else:
        # No import from normalize_value yet, add it
        lines = content.split('\n')
        import_line = "from .normalize_value import normalize_value, safe_parse_decimals"
        
        # Find the last import line
        last_import = 0
        for i, line in enumerate(lines):
            if line.startswith("import ") or line.startswith("from "):
                last_import = i
        
        # Insert after the last import
        lines.insert(last_import + 1, import_line)
        
        # Write the updated content
        with open(file_path, 'w') as f:
            f.write('\n'.join(lines))
        print(f"Added new import statement: {import_line}")
    
    # Replace all int(decimals) calls with safe_parse_decimals(decimals)
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Replace int(decimals) with safe_parse_decimals(decimals)
    if "int(decimals)" in content:
        content = content.replace("int(decimals)", "safe_parse_decimals(decimals)")
        
        # Write the updated content
        with open(file_path, 'w') as f:
            f.write(content)
        print("Replaced int(decimals) with safe_parse_decimals(decimals)")
    else:
        print("No int(decimals) calls found in the file")
    
    print("✅ Fix completed")
    return True

if __name__ == "__main__":
    fix_formatter()