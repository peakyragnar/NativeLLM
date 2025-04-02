with open('src2/sec/pipeline.py', 'r') as f:
    content = f.read()

replacement = """            # Save document sections and actual sections to metadata for LLM formatter
            metadata['html_content'] = {}
            
            # Add document sections
            if 'document_sections' in extract_result:
                metadata['html_content']['document_sections'] = extract_result['document_sections']
                logging.info(f"Added {len(extract_result['document_sections'])} document sections to metadata for LLM formatter")
            
            # Add actual sections from TOC
            if 'actual_sections' in extract_result:
                metadata['html_content']['actual_sections'] = extract_result['actual_sections']
                logging.info(f"Added {len(extract_result['actual_sections'])} actual sections from TOC to metadata")"""

# Replace only the first occurrence
index = content.find("            # Save document sections to metadata for LLM formatter")
if index >= 0:
    end_index = content.find("            if not extract_result.get(\"success\", False):", index)
    if end_index >= 0:
        new_content = content[:index] + replacement + content[end_index:]
        with open('src2/sec/pipeline.py', 'w') as f:
            f.write(new_content)
        print("Successfully updated file")
    else:
        print("Could not find end of section to replace")
else:
    print("Could not find section to replace")
