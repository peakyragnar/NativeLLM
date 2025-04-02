#\!/usr/bin/env python3
"""
This script makes a final targeted fix for the remaining XBRL data handling issues.
"""

import os

def fix_remaining_issues():
    """
    Fix remaining XBRL data handling issues by directly editing the specific lines
    """
    # Path to the pipeline file
    pipeline_path = "/Users/michael/NativeLLM/src2/sec/pipeline.py"
    
    # Create backup
    backup_path = f"{pipeline_path}.final_backup"
    os.system(f"cp {pipeline_path} {backup_path}")
    print(f"Created backup at {backup_path}")
    
    # Search for lines containing "xbrl_data["facts"].append"
    grep_cmd = "grep -n 'xbrl_data\[\"facts\"\].append' " + pipeline_path
    lines_with_append = os.popen(grep_cmd).read().strip().split('\n')
    
    # Open the file and read all lines
    with open(pipeline_path, 'r', encoding='utf-8') as f:
        all_lines = f.readlines()
    
    # Track lines to modify
    lines_to_fix = []
    
    for line in lines_with_append:
        if not line:
            continue
        line_num, content = line.split(':', 1)
        line_num = int(line_num)
        lines_to_fix.append(line_num)
        print(f"Found append at line {line_num}: {content.strip()}")
    
    # Fix the identified lines
    for line_num in lines_to_fix:
        # Get the context (5 lines before and after)
        start_idx = max(0, line_num - 6)
        end_idx = min(len(all_lines), line_num + 5)
        context_lines = all_lines[start_idx:end_idx]
        
        print(f"\nContext around line {line_num}:")
        for i, line in enumerate(context_lines):
            print(f"{start_idx + i}: {line.rstrip()}")
        
        # Look for ix:nonfraction pattern
        if 'ix:nonfraction' in '\n'.join(context_lines):
            print(f"Found ix:nonfraction pattern around line {line_num}")
            
            # Replace the entire section with the correct code
            # Find the start line (the if statement line)
            if_line_idx = None
            for i, line in enumerate(context_lines):
                if "if ix_tag.name ==" in line:
                    if_line_idx = start_idx + i
                    break
            
            if if_line_idx is not None:
                # Replace from if_line_idx to the append line with the correct code
                # First, backup the existing lines
                backup_lines = all_lines[if_line_idx:line_num+1]
                
                # Replace the section with the correct implementation
                replacement_lines = [
                    "                                if ix_tag.name == 'ix:nonfraction':\n",
                    "                                    concept = ix_tag.get('name', '')\n",
                    "                                    value = ix_tag.text.strip()\n",
                    "                                    context_ref = ix_tag.get('contextref', '')\n",
                    "                                    unit_ref = ix_tag.get('unitref', None)\n",
                    "                                    decimals = ix_tag.get('decimals', None)\n",
                    "                                    \n",
                    "                                    # Add the fact using the helper function\n",
                    "                                    add_fact(concept, value, context_ref, unit_ref, decimals)\n"
                ]
                
                # Replace the lines
                all_lines[if_line_idx:line_num+1] = replacement_lines
                print(f"Replaced lines {if_line_idx}-{line_num} with correct implementation")
        
        # Look for Table pattern
        elif "Table" in '\n'.join(context_lines) and "label.replace" in '\n'.join(context_lines):
            print(f"Found Table pattern around line {line_num}")
            
            # Replace the specific line with add_fact call
            all_lines[line_num-1] = all_lines[line_num-1].replace(
                'xbrl_data["facts"].append({',
                'add_fact(f"Table{table_idx}_{label.replace(\' \', \'\')}", value, "AsOf")'
            ).replace(
                '                        "concept": f"Table{table_idx}_{label.replace(\' \', \'\')}",\n',
                ''
            ).replace(
                '                        "value": value,\n',
                ''
            ).replace(
                '                        "context_ref": "AsOf"\n',
                ''
            ).replace(
                '                    })',
                ''
            )
            
            print(f"Replaced line {line_num-1} with add_fact call")
    
    # Write the modified lines back to the file
    with open(pipeline_path, 'w', encoding='utf-8') as f:
        f.writelines(all_lines)
    
    print(f"\nApplied final fixes to {pipeline_path}")
    print("Remember to run the check_fixes.py script again to verify all issues are fixed")

if __name__ == "__main__":
    fix_remaining_issues()
