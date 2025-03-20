# query_llm.py
import os
import sys
import glob
import argparse

def find_llm_file(ticker, filing_type=None):
    """Find LLM format file for a specific company and filing type"""
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

def read_llm_file(file_path):
    """Read an LLM format file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def prepare_prompt(llm_content, question):
    """Prepare a prompt for the LLM"""
    prompt = f"""Below is financial data from an SEC filing in a special format optimized for AI analysis.
Each data point is presented with its context and original value.

{llm_content[:50000]}  # Limit content length to avoid token limits

Based on the financial data above, please answer the following question:
{question}

Provide your answer based solely on the data provided, with no additional information or assumptions.
"""
    return prompt

def main():
    parser = argparse.ArgumentParser(description="Query LLM with financial data")
    parser.add_argument('--ticker', required=True, help='Company ticker symbol')
    parser.add_argument('--filing_type', help='Filing type (10-K, 10-Q, etc.)')
    parser.add_argument('--question', required=True, help='Question to ask about the financial data')
    
    args = parser.parse_args()
    
    # Find the LLM format file
    file_path = find_llm_file(args.ticker, args.filing_type)
    if not file_path:
        print(f"No LLM format file found for {args.ticker}")
        return
    
    print(f"Using file: {file_path}")
    
    # Read the file
    llm_content = read_llm_file(file_path)
    
    # Prepare the prompt
    prompt = prepare_prompt(llm_content, args.question)
    
    print("\nPrompt prepared. You can now use this prompt with an LLM API of your choice.")
    print(f"Prompt length: {len(prompt)} characters")
    
    # Here you would typically call an LLM API with the prompt
    # For this example, we'll just provide instructions
    print("\nTo use with Claude or GPT-4, copy the prompt and send it to the API.")
    
    # Optionally save the prompt to a file
    prompt_file = f"{args.ticker}_prompt.txt"
    with open(prompt_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    
    print(f"Prompt saved to {prompt_file}")

if __name__ == "__main__":
    main()