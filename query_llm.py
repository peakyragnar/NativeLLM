# query_llm.py
import os
import sys
import glob
import argparse

def find_structured_file(ticker, filing_type=None):
    """Find structured LLM format file for a specific company and filing type"""
    from src.config import PROCESSED_DATA_DIR
    
    if filing_type:
        pattern = os.path.join(PROCESSED_DATA_DIR, ticker, f"{ticker}_{filing_type}_*_llm.txt")
    else:
        pattern = os.path.join(PROCESSED_DATA_DIR, ticker, f"{ticker}_*_llm.txt")
    
    files = glob.glob(pattern)
    
    if not files:
        return None
    
    # Sort by modification time (newest first)
    return sorted(files, key=os.path.getmtime, reverse=True)[0]

def find_narrative_file(ticker, filing_type=None):
    """Find narrative text file for a specific company and filing type"""
    from src.config import PROCESSED_DATA_DIR
    
    # Using the single text file with section markers
    suffix = "_text.txt"
    
    if filing_type:
        pattern = os.path.join(PROCESSED_DATA_DIR, ticker, f"{ticker}_{filing_type}_*{suffix}")
    else:
        pattern = os.path.join(PROCESSED_DATA_DIR, ticker, f"{ticker}_*{suffix}")
    
    files = glob.glob(pattern)
    
    if not files:
        return None
    
    # Sort by modification time (newest first)
    return sorted(files, key=os.path.getmtime, reverse=True)[0]

def read_file(file_path):
    """Read a file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def prepare_structured_prompt(structured_content, question):
    """Prepare a prompt for the LLM using structured data"""
    prompt = f"""Below is structured financial data from an SEC filing in a special format optimized for AI analysis.
Each data point is presented with its concept name, value, and context reference.

{structured_content[:50000]}  # Limit content length to avoid token limits

Based on the structured financial data above, please answer the following question:
{question}

Provide your answer based solely on the data provided, with no additional information or assumptions.
"""
    return prompt

def extract_section_from_text(text, section=None):
    """
    Extract a specific section from the text using section markers
    
    Args:
        text: The full text with section markers
        section: The ID of the section to extract (e.g., 'ITEM_7_MD_AND_A', 'ITEM_1A_RISK_FACTORS')
                If None, returns the full text
        
    Returns:
        The extracted section text or full text if no section specified
    """
    if not section:
        return text
        
    # Define section patterns based on common section IDs
    if section.lower() == 'mda':
        section_ids = ['ITEM_7_MD_AND_A', 'ITEM_2_MD_AND_A', 'MANAGEMENT_DISCUSSION']
    elif section.lower() == 'risk_factors':
        section_ids = ['ITEM_1A_RISK_FACTORS', 'RISK_FACTORS']
    else:
        section_ids = [section]  # Use the provided section ID directly
    
    # Try each possible section ID
    for section_id in section_ids:
        # Define patterns to match the section start and end
        start_pattern = f"@SECTION_START: {section_id}"
        end_pattern = f"@SECTION_END: {section_id}"
        
        # Find the section in the text
        start_pos = text.find(start_pattern)
        if start_pos != -1:
            # Found the section
            section_start = text.find("\n", start_pos) + 1
            
            # Find the section end
            end_pos = text.find(end_pattern, start_pos)
            if end_pos != -1:
                # Return text between markers (exclude the markers themselves)
                return text[section_start:end_pos].strip()
            else:
                # If no end marker, take the rest of the text
                return text[section_start:].strip()
    
    # If section markers not found, check if we have section guide
    guide_start = text.find("@SECTION_GUIDE")
    if guide_start != -1:
        # Return the full text but mention we couldn't find the specific section
        return f"Note: Could not find the specific section '{section}' requested, returning full text.\n\n{text}"
    
    # If no markers found at all, just return the full text
    return text

def prepare_narrative_prompt(narrative_content, question, section=None):
    """
    Prepare a prompt for the LLM using narrative text
    
    Args:
        narrative_content: The full narrative text with section markers
        question: The question to answer
        section: The section to focus on (e.g., 'mda', 'risk_factors', or specific section ID)
    """
    # Extract the requested section if specified
    if section and section != 'full_text':
        extracted_content = extract_section_from_text(narrative_content, section)
        section_message = f" (focused on the {section.upper()} section)"
    else:
        extracted_content = narrative_content
        section_message = ""
    
    prompt = f"""Below is narrative text extracted from an SEC filing{section_message}.
This contains management's descriptions, analysis, and explanations in their own words.

{extracted_content[:50000]}  # Limit content length to avoid token limits

Based on the narrative text above, please answer the following question:
{question}

Provide your answer based solely on the text provided, with no additional information or assumptions.
"""
    return prompt

def prepare_combined_prompt(structured_content, narrative_content, question, section=None):
    """
    Prepare a prompt using both structured data and narrative text
    
    Args:
        structured_content: The structured financial data
        narrative_content: The narrative text (possibly already extracted for a specific section)
        question: The question to answer
        section: The section that was extracted (for informational purposes)
    """
    # Calculate content sizes to fit within token limits while keeping a reasonable balance
    total_limit = 80000  # Total character limit
    
    # Allocate 60% to structured data and 40% to narrative when both are available
    structured_limit = min(len(structured_content), int(total_limit * 0.6))
    narrative_limit = min(len(narrative_content), total_limit - structured_limit)
    
    # If one source is shorter than its allocation, give the extra space to the other
    if len(structured_content) < structured_limit:
        narrative_limit = min(len(narrative_content), total_limit - len(structured_content))
    
    if len(narrative_content) < narrative_limit:
        structured_limit = min(len(structured_content), total_limit - len(narrative_content))
    
    # Add information about any extracted section
    section_info = f" (focusing on the {section.upper()} section in the narrative text)" if section else ""
    
    prompt = f"""Below is financial information from an SEC filing{section_info} including:
1. Structured financial data in a special format optimized for AI analysis
2. Narrative text extracted from the same filing

STRUCTURED FINANCIAL DATA:
{structured_content[:structured_limit]}

NARRATIVE TEXT:
{narrative_content[:narrative_limit]}

Based on BOTH the structured data and narrative text above, please answer the following question:
{question}

When answering, consider both the quantitative information in the structured data and the qualitative context in the narrative text.
Provide your answer based solely on the information provided, with no additional information or assumptions.
"""
    return prompt

def main():
    parser = argparse.ArgumentParser(description="Query LLM with financial data")
    parser.add_argument('--ticker', required=True, help='Company ticker symbol')
    parser.add_argument('--filing_type', help='Filing type (10-K, 10-Q, etc.)')
    parser.add_argument('--question', required=True, help='Question to ask about the financial data')
    parser.add_argument('--data_type', choices=['structured', 'narrative', 'combined'], 
                       default='combined', help='Type of data to use for the prompt')
    parser.add_argument('--section', help='Section of text to extract (e.g., mda, risk_factors, or specific section ID)')
    
    args = parser.parse_args()
    
    # Find the appropriate files based on data type
    structured_file = None
    narrative_file = None
    
    if args.data_type in ['structured', 'combined']:
        structured_file = find_structured_file(args.ticker, args.filing_type)
        if not structured_file and args.data_type == 'structured':
            print(f"No structured data file found for {args.ticker}")
            return
    
    if args.data_type in ['narrative', 'combined']:
        narrative_file = find_narrative_file(args.ticker, args.filing_type)
        if not narrative_file and args.data_type == 'narrative':
            print(f"No narrative text file found for {args.ticker}")
            return
    
    # Handle combined case where one file type might be missing
    if args.data_type == 'combined' and not structured_file and not narrative_file:
        print(f"No files found for {args.ticker}")
        return
    elif args.data_type == 'combined' and not structured_file:
        print(f"No structured data file found for {args.ticker}, using narrative only")
        args.data_type = 'narrative'
    elif args.data_type == 'combined' and not narrative_file:
        print(f"No narrative text file found for {args.ticker}, using structured only")
        args.data_type = 'structured'
    
    # Read file content and prepare prompt based on data type
    prompt = None
    
    if args.data_type == 'structured':
        print(f"Using structured data file: {structured_file}")
        structured_content = read_file(structured_file)
        prompt = prepare_structured_prompt(structured_content, args.question)
    
    elif args.data_type == 'narrative':
        print(f"Using narrative text file: {narrative_file}")
        narrative_content = read_file(narrative_file)
        if args.section:
            print(f"Extracting section: {args.section}")
            prompt = prepare_narrative_prompt(narrative_content, args.question, args.section)
        else:
            prompt = prepare_narrative_prompt(narrative_content, args.question)
    
    elif args.data_type == 'combined':
        print(f"Using structured data file: {structured_file}")
        print(f"Using narrative text file: {narrative_file}")
        structured_content = read_file(structured_file)
        narrative_content = read_file(narrative_file)
        
        # If section is specified, extract it from the narrative content
        if args.section:
            print(f"Extracting section: {args.section}")
            narrative_content = extract_section_from_text(narrative_content, args.section)
            prompt = prepare_combined_prompt(structured_content, narrative_content, args.question, args.section)
        else:
            prompt = prepare_combined_prompt(structured_content, narrative_content, args.question)
    
    print("\nPrompt prepared. You can now use this prompt with an LLM API of your choice.")
    print(f"Prompt length: {len(prompt)} characters")
    
    # Here you would typically call an LLM API with the prompt
    # For this example, we'll just provide instructions
    print("\nTo use with Claude or GPT-4, copy the prompt and send it to the API.")
    
    # Optionally save the prompt to a file
    section_suffix = f"_{args.section}" if args.section else ""
    prompt_file = f"{args.ticker}_{args.data_type}{section_suffix}_prompt.txt"
    with open(prompt_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    
    print(f"Prompt saved to {prompt_file}")

if __name__ == "__main__":
    main()