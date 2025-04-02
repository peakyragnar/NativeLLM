import os
import sys
import re

def fix_llm_formatter():
    """Fix the LLM formatter to use the context extractor module"""
    # Path to the llm_formatter.py file
    llm_formatter_path = os.path.join('src2', 'formatter', 'llm_formatter.py')
    
    # Check if the file exists
    if not os.path.exists(llm_formatter_path):
        print(f"Error: {llm_formatter_path} not found")
        return False
    
    # Create a backup
    backup_path = llm_formatter_path + '.bak.context'
    try:
        with open(llm_formatter_path, 'r') as src:
            with open(backup_path, 'w') as dst:
                dst.write(src.read())
        print(f"Created backup at {backup_path}")
    except Exception as e:
        print(f"Error creating backup: {str(e)}")
        return False
    
    # Read the file
    with open(llm_formatter_path, 'r') as f:
        content = f.read()
    
    # Define patterns to find the code that needs to be modified
    pattern_start = r"# Try one more approach to extract context information directly from the HTML file.*?if \"filing_path\" in parsed_xbrl and os\.path\.exists\(parsed_xbrl\[\"filing_path\"\]\):.*?try:.*?with open\(parsed_xbrl\[\"filing_path\"\], 'r', encoding='utf-8', errors='replace'\) as f:.*?html_content = f\.read\(\)"
    pattern_end = r"# Update the output.*?output\[-1\] = f\"@NOTE: Found {num_contexts} contexts via direct HTML extraction\""
    
    # Find the start and end of the context extraction code
    match_start = re.search(pattern_start, content, re.DOTALL)
    if not match_start:
        print("Error: Could not find context extraction code start")
        return False
    
    start_pos = match_start.start()
    
    # Find the additional imports section
    import_pattern = r"import os\nimport logging\nimport json\nimport re\nimport datetime"
    import_match = re.search(import_pattern, content)
    if not import_match:
        print("Error: Could not find import section")
        return False
    
    # Add our module import after the existing imports
    import_pos = import_match.end()
    new_import = "\nfrom .context_extractor import extract_contexts_from_html"
    
    # Find where to insert the modified code
    pattern_try_open = r"try:.*?with open\(parsed_xbrl\[\"filing_path\"\], 'r', encoding='utf-8', errors='replace'\)"
    try_match = re.search(pattern_try_open, content, re.DOTALL)
    if not try_match:
        print("Error: Could not find file opening code")
        return False
    
    try_pos = try_match.start()
    
    # Find the end of the original context extraction code
    update_output_pattern = r"# Update the output.*?output\[-1\] = f\"@NOTE: Found {num_contexts} contexts via direct HTML extraction\""
    update_match = re.search(update_output_pattern, content, re.DOTALL)
    if not update_match:
        print("Error: Could not find update output code")
        return False
    
    update_end = update_match.end()
    
    # Create the new code to replace the old context extraction
    new_code = """try:
                    with open(parsed_xbrl["filing_path"], 'r', encoding='utf-8', errors='replace') as f:
                        html_content = f.read()
                    
                    # Use the context extractor module to extract contexts
                    extracted_contexts = extract_contexts_from_html(html_content, filing_metadata)
                    
                    if extracted_contexts:
                        # Update the contexts in parsed_xbrl
                        parsed_xbrl["contexts"] = extracted_contexts
                        num_contexts = len(extracted_contexts)
                        logging.info(f"Successfully extracted {num_contexts} contexts from HTML file")
                        
                        # Update the output
                        output[-1] = f"@NOTE: Found {num_contexts} contexts via direct HTML extraction\""""
    
    # Replace the original context extraction code with the new code
    modified_content = content[:import_pos] + new_import + content[import_pos:try_pos] + new_code + content[update_end:]
    
    # Write the modified content back to the file
    try:
        with open(llm_formatter_path, 'w') as f:
            f.write(modified_content)
        print(f"Successfully updated {llm_formatter_path}")
        return True
    except Exception as e:
        print(f"Error writing modified content: {str(e)}")
        return False

if __name__ == "__main__":
    fix_llm_formatter()
