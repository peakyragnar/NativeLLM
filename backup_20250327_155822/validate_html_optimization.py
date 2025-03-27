#!/usr/bin/env python
# Validate HTML optimization before applying to production files
# This script tests our HTML optimization on files and verifies 100% data integrity

import os
import sys
import re
import logging
import argparse
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('html_optimization_validation.log')
    ]
)

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.xbrl.xbrl_parser import extract_text_only_from_html
from src.config import PROCESSED_DATA_DIR

def validate_optimization(file_path, show_details=False, dry_run=True):
    """
    Validate HTML optimization on a file without making changes
    
    Args:
        file_path: Path to the file to validate
        show_details: Whether to show detailed comparison for each HTML section
        dry_run: If False, will actually write changes (use with caution)
        
    Returns:
        Dictionary with validation results
    """
    try:
        if not os.path.exists(file_path):
            return {
                "file": file_path,
                "status": "error",
                "message": "File not found"
            }
            
        # Only process LLM formatted files
        if not file_path.endswith('_llm.txt'):
            return {
                "file": file_path,
                "status": "skipped",
                "message": "Not an LLM format file"
            }
            
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        original_size = len(content)
        
        # Find all @VALUE sections that might contain HTML
        html_pattern = r'(@VALUE: )(.*?)(\n@|$)'
        value_sections = re.findall(html_pattern, content, re.DOTALL)
        
        # Track validation stats
        total_values = len(value_sections)
        html_values = 0
        cleanable_values = 0
        unchanged_values = 0
        total_original_html_size = 0
        total_cleaned_html_size = 0
        
        # Structure for collecting detailed results
        detailed_results = []
        all_numbers_preserved = True
        
        # We'll rebuild the content by replacing HTML values
        new_content = content
        
        # Process each value section
        for match in value_sections:
            prefix = match[0]  # @VALUE: 
            value = match[1]   # The actual value
            suffix = match[2]  # \n@ or end of string
            
            full_match = prefix + value + suffix
            
            # Check if this contains HTML
            if '<' in value and '>' in value:
                html_values += 1
                original_len = len(value)
                total_original_html_size += original_len
                
                # Preserve original HTML for integrity check
                original_html = value
                
                # Extract all numbers from original HTML for verification
                soup = BeautifulSoup(original_html, 'html.parser')
                original_text = soup.get_text()
                original_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', original_text)
                original_number_set = set(original_numbers)
                
                # Extract tokens for secondary verification
                original_tokens = re.findall(r'\b[\w\d.,$%()-]+\b', original_text)
                original_token_set = set(original_tokens)
                
                # Process with our HTML cleaner
                cleaned_value = extract_text_only_from_html(value)
                cleaned_len = len(cleaned_value)
                total_cleaned_html_size += cleaned_len
                
                # Verify number preservation - our highest priority
                if cleaned_value != value:  # Only check if actually changed
                    # For text format, extract numbers directly
                    if '<' not in cleaned_value:
                        cleaned_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', cleaned_value)
                    else:
                        # For HTML format, extract text first
                        soup = BeautifulSoup(cleaned_value, 'html.parser')
                        cleaned_text = soup.get_text()
                        cleaned_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', cleaned_text)
                    
                    cleaned_number_set = set(cleaned_numbers)
                    
                    # Check for missing numbers
                    missing_numbers = original_number_set - cleaned_number_set
                    
                    # Also check tokens for general content preservation
                    if '<' not in cleaned_value:
                        cleaned_tokens = re.findall(r'\b[\w\d.,$%()-]+\b', cleaned_value)
                    else:
                        cleaned_tokens = re.findall(r'\b[\w\d.,$%()-]+\b', soup.get_text())
                    
                    cleaned_token_set = set(cleaned_tokens)
                    missing_tokens = original_token_set - cleaned_token_set
                    
                    # Record integrity verification
                    numbers_preserved = len(missing_numbers) == 0
                    token_preservation = 1 - (len(missing_tokens) / len(original_token_set)) if original_token_set else 1.0
                    
                    if not numbers_preserved:
                        all_numbers_preserved = False
                else:
                    # No change to value, so all numbers are preserved
                    missing_numbers = set()
                    numbers_preserved = True
                    token_preservation = 1.0
                
                # Track if this value would be cleaned or not
                if cleaned_len < original_len and numbers_preserved and token_preservation > 0.99:
                    cleanable_values += 1
                    size_reduction = original_len - cleaned_len
                    percentage_reduction = (size_reduction / original_len) * 100
                    
                    # In a real run, we would replace this value
                    if not dry_run:
                        new_content = new_content.replace(full_match, prefix + cleaned_value + suffix, 1)
                    
                    # For detailed reporting
                    if show_details:
                        detailed_results.append({
                            "status": "cleanable",
                            "original_size": original_len,
                            "cleaned_size": cleaned_len,
                            "reduction": percentage_reduction,
                            "numbers_preserved": numbers_preserved,
                            "token_preservation": token_preservation,
                            "original_snippet": original_html[:100] + "..." if len(original_html) > 100 else original_html,
                            "cleaned_snippet": cleaned_value[:100] + "..." if len(cleaned_value) > 100 else cleaned_value,
                            "missing_numbers": list(missing_numbers)[:5] if missing_numbers else []
                        })
                else:
                    unchanged_values += 1
                    
                    # For detailed reporting
                    if show_details and cleaned_len < original_len:
                        detailed_results.append({
                            "status": "unchanged",
                            "reason": "integrity concerns" if not numbers_preserved else "insufficient reduction",
                            "original_size": original_len,
                            "cleaned_size": cleaned_len,
                            "numbers_preserved": numbers_preserved,
                            "token_preservation": token_preservation,
                            "missing_numbers": list(missing_numbers)[:5] if missing_numbers else []
                        })
        
        # Calculate estimated total size reduction
        new_size = len(new_content)
        total_reduction = original_size - new_size
        percentage_reduction = (total_reduction / original_size) * 100 if original_size > 0 else 0
        html_reduction = (total_original_html_size - total_cleaned_html_size) / total_original_html_size * 100 if total_original_html_size > 0 else 0
        
        # Apply changes if not a dry run
        if not dry_run and new_size < original_size:
            # Create a backup of the original file
            backup_path = file_path + '.bak'
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
            # Write the optimized content
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            logging.info(f"Applied changes to {file_path}, backup saved to {backup_path}")
            status = "optimized"
        else:
            status = "validated" if dry_run else "unchanged"
            
        # Prepare result object
        result = {
            "file": file_path,
            "status": status,
            "original_size": original_size,
            "new_size": new_size,
            "total_values": total_values,
            "html_values": html_values,
            "cleanable_values": cleanable_values,
            "unchanged_values": unchanged_values,
            "size_reduction": total_reduction,
            "percentage_reduction": percentage_reduction,
            "html_reduction": html_reduction,
            "all_numbers_preserved": all_numbers_preserved,
            "detailed_results": detailed_results if show_details else []
        }
        
        return result
            
    except Exception as e:
        logging.error(f"Error validating {file_path}: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        
        return {
            "file": file_path,
            "status": "error",
            "message": str(e)
        }

def main():
    parser = argparse.ArgumentParser(description="Validate HTML optimization on processed files")
    parser.add_argument('file', help='File to validate')
    parser.add_argument('--details', action='store_true', help='Show detailed results for each HTML section')
    parser.add_argument('--apply', action='store_true', help='Actually apply changes (default is dry run)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.file):
        print(f"Error: File not found: {args.file}")
        sys.exit(1)
        
    print(f"Validating HTML optimization on: {args.file}")
    print(f"Mode: {'APPLY CHANGES' if args.apply else 'DRY RUN (no changes will be made)'}\n")
    
    result = validate_optimization(args.file, show_details=args.details, dry_run=not args.apply)
    
    if result["status"] == "error":
        print(f"Error: {result.get('message', 'Unknown error')}")
        sys.exit(1)
        
    if result["status"] == "skipped":
        print(f"File skipped: {result.get('message', 'Unknown reason')}")
        sys.exit(0)
        
    # Display summary
    print("===== OPTIMIZATION VALIDATION SUMMARY =====")
    print(f"File: {os.path.basename(result['file'])}")
    print(f"Total values: {result['total_values']}")
    print(f"HTML values: {result['html_values']} ({result['html_values']/result['total_values']*100:.1f}% of all values)")
    print(f"Cleanable HTML values: {result['cleanable_values']} ({result['cleanable_values']/result['html_values']*100:.1f}% of HTML values)")
    print(f"Original size: {result['original_size']:,} bytes")
    print(f"New size: {result['new_size']:,} bytes")
    print(f"Size reduction: {result['size_reduction']:,} bytes ({result['percentage_reduction']:.2f}%)")
    print(f"HTML content reduction: {result['html_reduction']:.2f}%")
    print(f"Data integrity: {'✅ All numbers preserved' if result['all_numbers_preserved'] else '❌ WARNING: Some numbers not preserved'}")
    
    # Show detailed breakdown if requested
    if args.details and result["detailed_results"]:
        print("\n===== DETAILED HTML SECTION ANALYSIS =====")
        
        for i, detail in enumerate(result["detailed_results"], 1):
            print(f"\nHTML Section {i}:")
            print(f"Status: {detail['status']}")
            if detail['status'] == 'unchanged' and 'reason' in detail:
                print(f"Reason: {detail['reason']}")
            print(f"Size: {detail['original_size']:,} → {detail.get('cleaned_size', 0):,} bytes")
            if 'reduction' in detail:
                print(f"Reduction: {detail['reduction']:.2f}%")
            
            if 'numbers_preserved' in detail:
                print(f"Numbers preserved: {'✅ Yes' if detail['numbers_preserved'] else '❌ No'}")
                if not detail['numbers_preserved'] and detail.get('missing_numbers'):
                    print(f"Missing numbers: {', '.join(detail['missing_numbers'])}")
            
            if 'token_preservation' in detail:
                print(f"Content preservation: {detail['token_preservation']*100:.2f}%")
            
            if args.details and 'original_snippet' in detail and 'cleaned_snippet' in detail:
                print("\nOriginal:")
                print(detail['original_snippet'])
                print("\nCleaned:")
                print(detail['cleaned_snippet'])
                print("-" * 50)
    
    # Final message
    if args.apply:
        if result["status"] == "optimized":
            print(f"\nSuccess: Applied HTML optimization to {args.file}")
            print(f"Backup saved to {args.file + '.bak'}")
        else:
            print(f"\nNo changes applied to {args.file} (insufficient reduction or integrity concerns)")
    else:
        if result["cleanable_values"] > 0:
            print("\nValidation successful. Run with --apply to actually apply changes.")
        else:
            print("\nNo cleanable HTML sections found.")
            
    # Write results to log file
    logging.info(f"Validation for {args.file}: {result['status']}, "
                f"{result['percentage_reduction']:.2f}% reduction, "
                f"integrity: {'preserved' if result['all_numbers_preserved'] else 'COMPROMISED'}")
    
if __name__ == "__main__":
    main()