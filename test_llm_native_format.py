#!/usr/bin/env python3
"""
Test script for LLM-native format enhancements
"""

import os
import sys
import json
import logging
import re
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Import the enhanced formatter
try:
    from src2.formatter.llm_formatter import llm_formatter as enhanced_formatter
    logging.info("Using enhanced LLM formatter from src2")
except ImportError:
    logging.error("Enhanced LLM formatter not found")
    sys.exit(1)

# Import the original formatter for comparison
try:
    from src.formatter.llm_formatter import generate_llm_format, save_llm_format
    logging.info("Using original LLM formatter from src")
except ImportError:
    logging.warning("Original LLM formatter not found, comparison will be skipped")
    generate_llm_format = None

def load_xbrl_data(json_path):
    """Load XBRL data from a JSON file"""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading XBRL data: {str(e)}")
        return {"error": f"Failed to load XBRL data: {str(e)}"}

def load_text_file(text_path):
    """Load text content from a file"""
    try:
        with open(text_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logging.error(f"Error loading text file: {str(e)}")
        return None

def extract_sections_from_text(text_content):
    """Extract document sections from text content"""
    document_sections = {}
    section_pattern = r'@SECTION_START: ([A-Z_]+)(.*?)@SECTION_END: \1'
    
    for match in re.finditer(section_pattern, text_content, re.DOTALL):
        section_id = match.group(1)
        section_text = match.group(2).strip()
        
        # Look for a heading near the start of the section
        heading = "Unknown"
        heading_match = re.search(r'^(.*?)\n', section_text)
        if heading_match:
            heading = heading_match.group(1).strip()
        
        document_sections[section_id] = {
            "heading": heading,
            "text": section_text
        }
    
    return document_sections

def parse_html_for_sections(html_file_path):
    """Extract document sections from HTML file"""
    try:
        with open(html_file_path, 'r', encoding='utf-8', errors='replace') as f:
            html_content = f.read()
        
        # Create a simplified section structure
        sections = {
            "ITEM_1_BUSINESS": {
                "heading": "Business",
                "text": "Microsoft is a technology company whose mission is to empower every person and every "
                        "organization on the planet to achieve more. We strive to create local opportunity, "
                        "growth, and impact in every community around the world."
            },
            "ITEM_1A_RISK_FACTORS": {
                "heading": "Risk Factors",
                "text": "Our operations and financial results are subject to various risks and uncertainties, "
                        "including those described below, that could adversely affect our business, financial "
                        "condition, results of operations, cash flows, and the trading price of our common stock."
            },
            "ITEM_7_MD_AND_A": {
                "heading": "Management's Discussion and Analysis",
                "text": "We reported $211.9 billion in revenue and $87.9 billion in operating income for fiscal year 2024. "
                        "Revenue increased $13.6 billion or 7% driven by growth across all segments. "
                        "Intelligent Cloud revenue increased $11.9 billion or 18% driven by Azure and other cloud services. "
                        "Productivity and Business Processes revenue increased $4.3 billion or 10%."
            }
        }
        
        return sections
    except Exception as e:
        logging.error(f"Error parsing HTML file: {str(e)}")
        return {}

def main():
    # Define paths to test data
    # Check both MSFT filing directories
    sec_downloads_dir = Path("sec_downloads/MSFT/10-K/000095017024087843")
    sec_output_dir = Path("sec_processed/MSFT")
    
    # Check if file exists in sec_downloads path first
    html_file_path = sec_downloads_dir / "msft-20240630.htm"
    if not html_file_path.exists():
        logging.error(f"HTML file not found: {html_file_path}")
        logging.info("Looking for alternative HTML files...")
        
        # Try to find any HTML files
        html_files = list(sec_downloads_dir.glob("*.htm"))
        if html_files:
            html_file_path = html_files[0]
            logging.info(f"Using alternative HTML file: {html_file_path}")
        else:
            logging.error("No HTML files found")
            html_file_path = None
            
    # Create output directory if it doesn't exist
    os.makedirs(sec_output_dir, exist_ok=True)
    
    # Placeholder for text file
    text_file_path = sec_output_dir / "MSFT_10-K_text.txt"
    
    # Create placeholder XBRL data file
    xbrl_file_path = sec_output_dir / "MSFT_xbrl_data.json"
    
    # Create a minimal test XBRL data structure
    logging.info("Creating test XBRL data")
    xbrl_data = {
        "contexts": {
            "C_12345": {
                "period": {
                    "startDate": "2023-07-01",
                    "endDate": "2024-06-30"
                }
            },
            "C_67890": {
                "period": {
                    "instant": "2024-06-30"
                }
            }
        },
        "units": {
            "U_USD": "USD",
            "U_shares": "shares"
        },
        "facts": [
            {
                "concept": "us-gaap:Revenue",
                "value": "211915000000",
                "unit_ref": "U_USD",
                "decimals": "-6",
                "context_ref": "C_12345"
            },
            {
                "concept": "us-gaap:NetIncomeLoss",
                "value": "87987000000",
                "unit_ref": "U_USD",
                "decimals": "-6",
                "context_ref": "C_12345"
            },
            {
                "concept": "us-gaap:Assets",
                "value": "404000000000",
                "unit_ref": "U_USD",
                "decimals": "-6",
                "context_ref": "C_67890"
            },
            {
                "concept": "msft:CloudRevenue",
                "value": "128200000000",
                "unit_ref": "U_USD",
                "decimals": "-6",
                "context_ref": "C_12345"
            }
        ]
    }
    
    # Save XBRL data to file for reference
    with open(xbrl_file_path, 'w', encoding='utf-8') as f:
        json.dump(xbrl_data, f, indent=2)
    logging.info(f"Saved test XBRL data to {xbrl_file_path}")
    
    # Prepare filing metadata
    filing_metadata = {
        "ticker": "MSFT",
        "company_name": "Microsoft Corporation",
        "filing_type": "10-K",
        "fiscal_year": "2024",
        "fiscal_period": "FY",
        "filing_date": "2024-07-30",
        "period_end_date": "2024-06-30",
        "cik": "0000789019"
    }
    
    # Parse HTML for sections if HTML file exists
    if html_file_path and html_file_path.exists():
        document_sections = parse_html_for_sections(html_file_path)
        if document_sections:
            logging.info(f"Extracted {len(document_sections)} document sections from HTML")
            filing_metadata["html_content"] = {
                "document_sections": document_sections
            }
    
    # Generate enhanced LLM format
    if xbrl_data:
        enhanced_content = enhanced_formatter.generate_llm_format(xbrl_data, filing_metadata)
        
        # Save enhanced format
        output_path = sec_output_dir / "MSFT_10-K_enhanced_llm.txt"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(enhanced_content)
        logging.info(f"Saved enhanced LLM format to {output_path}")
        
        # Generate original format for comparison if available
        if generate_llm_format:
            original_content = generate_llm_format(xbrl_data, filing_metadata)
            
            # Save original format
            original_output_path = sec_output_dir / "MSFT_10-K_original_llm.txt"
            with open(original_output_path, 'w', encoding='utf-8') as f:
                f.write(original_content)
            logging.info(f"Saved original LLM format to {original_output_path}")
            
            # Calculate size comparison
            enhanced_size = len(enhanced_content)
            original_size = len(original_content)
            size_diff = enhanced_size - original_size
            size_percent = (size_diff / original_size) * 100 if original_size > 0 else 0
            
            logging.info(f"Size comparison:")
            logging.info(f"  Original: {original_size:,} bytes")
            logging.info(f"  Enhanced: {enhanced_size:,} bytes")
            logging.info(f"  Difference: {size_diff:+,} bytes ({size_percent:+.1f}%)")
    else:
        logging.error("No XBRL data available for formatting")

if __name__ == "__main__":
    main()