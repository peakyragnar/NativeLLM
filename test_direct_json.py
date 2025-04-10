#!/usr/bin/env python3
"""
Test Direct JSON Output Format

This script tests the direct JSON output format for SEC filings.
"""

import os
import sys
import logging
import argparse
import json
from pathlib import Path
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def extract_facts_from_html(html_path, output_json_path=None):
    """
    Extract XBRL facts from an HTML file with inline XBRL.
    
    Args:
        html_path: Path to the HTML file
        output_json_path: Optional path to save extracted facts as JSON
        
    Returns:
        List of XBRL facts (dictionaries)
    """
    logging.info(f"Extracting facts from {html_path}")
    
    # Load HTML file
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all XBRL facts
    facts = []
    
    # Find all elements with contextRef attribute (numeric and non-numeric facts)
    fact_elements = soup.find_all(attrs={'contextref': True})
    
    for element in fact_elements:
        # Extract fact attributes
        name = element.name
        context_ref = element.get('contextref')
        unit_ref = element.get('unitref')
        decimals = element.get('decimals')
        scale = element.get('scale')
        format = element.get('format')
        
        # For inline XBRL, get the concept name from the name attribute
        concept = element.get('name')
        
        # Extract fact value
        value = element.get_text(strip=True)
        
        # Create fact object
        fact = {
            'name': concept if concept else name,  # Use concept name if available
            'element': name,  # Store the element name separately
            'contextRef': context_ref,
            'unitRef': unit_ref,
            'decimals': decimals,
            'scale': scale,
            'format': format,
            'value': value
        }
        
        # Remove None values
        fact = {k: v for k, v in fact.items() if v is not None}
        
        facts.append(fact)
    
    logging.info(f"Extracted {len(facts)} facts")
    
    # Save facts to output file if specified
    if output_json_path:
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(facts, f, indent=2)
        logging.info(f"Facts saved to {output_json_path}")
    
    return facts

def extract_document_sections(html_path):
    """
    Extract document sections from HTML content.
    
    Args:
        html_path: Path to the HTML file
        
    Returns:
        Dictionary of document sections
    """
    logging.info(f"Extracting document sections from {html_path}")
    
    # Load HTML file
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Define section patterns
    section_patterns = {
        "PART_I": r"PART\s+I",
        "PART_II": r"PART\s+II",
        "PART_III": r"PART\s+III",
        "PART_IV": r"PART\s+IV",
        "ITEM_1_BUSINESS": r"ITEM\s+1\.?\s+BUSINESS",
        "ITEM_1A_RISK_FACTORS": r"ITEM\s+1A\.?\s+RISK\s+FACTORS",
        "ITEM_1B_UNRESOLVED_COMMENTS": r"ITEM\s+1B\.?\s+UNRESOLVED\s+STAFF\s+COMMENTS",
        "ITEM_2_PROPERTIES": r"ITEM\s+2\.?\s+PROPERTIES",
        "ITEM_3_LEGAL_PROCEEDINGS": r"ITEM\s+3\.?\s+LEGAL\s+PROCEEDINGS",
        "ITEM_4_MINE_SAFETY": r"ITEM\s+4\.?\s+MINE\s+SAFETY\s+DISCLOSURES",
        "ITEM_5_MARKET": r"ITEM\s+5\.?\s+MARKET\s+FOR\s+REGISTRANT",
        "ITEM_6_SELECTED_FINANCIAL": r"ITEM\s+6\.?\s+SELECTED\s+FINANCIAL\s+DATA",
        "ITEM_7_MD_AND_A": r"ITEM\s+7\.?\s+MANAGEMENT'S\s+DISCUSSION",
        "ITEM_7A_MARKET_RISK": r"ITEM\s+7A\.?\s+QUANTITATIVE\s+AND\s+QUALITATIVE",
        "ITEM_8_FINANCIAL_STATEMENTS": r"ITEM\s+8\.?\s+FINANCIAL\s+STATEMENTS",
        "ITEM_9_DISAGREEMENTS": r"ITEM\s+9\.?\s+CHANGES\s+IN\s+AND\s+DISAGREEMENTS",
        "ITEM_9A_CONTROLS": r"ITEM\s+9A\.?\s+CONTROLS\s+AND\s+PROCEDURES",
        "ITEM_9B_OTHER_INFORMATION": r"ITEM\s+9B\.?\s+OTHER\s+INFORMATION",
        "ITEM_10_DIRECTORS": r"ITEM\s+10\.?\s+DIRECTORS",
        "ITEM_11_EXECUTIVE_COMPENSATION": r"ITEM\s+11\.?\s+EXECUTIVE\s+COMPENSATION",
        "ITEM_12_SECURITY_OWNERSHIP": r"ITEM\s+12\.?\s+SECURITY\s+OWNERSHIP",
        "ITEM_13_RELATIONSHIPS": r"ITEM\s+13\.?\s+CERTAIN\s+RELATIONSHIPS",
        "ITEM_14_ACCOUNTANT_FEES": r"ITEM\s+14\.?\s+PRINCIPAL\s+ACCOUNTANT\s+FEES",
        "ITEM_15_EXHIBITS": r"ITEM\s+15\.?\s+EXHIBITS",
        "BALANCE_SHEETS": r"(CONSOLIDATED\s+)?BALANCE\s+SHEETS?",
        "INCOME_STATEMENTS": r"(CONSOLIDATED\s+)?STATEMENTS?\s+OF\s+(OPERATIONS|INCOME|EARNINGS)",
        "CASH_FLOW_STATEMENTS": r"(CONSOLIDATED\s+)?STATEMENTS?\s+OF\s+CASH\s+FLOWS?",
        "EQUITY_STATEMENTS": r"(CONSOLIDATED\s+)?STATEMENTS?\s+OF\s+(STOCKHOLDERS'?|SHAREHOLDERS'?)\s+EQUITY"
    }
    
    # Find all headings
    headings = []
    for tag in ['h1', 'h2', 'h3', 'h4', 'strong', 'b', 'p', 'div']:
        elements = soup.find_all(tag)
        for element in elements:
            text = element.get_text().strip()
            if text and len(text) > 5 and len(text) < 100:  # Filter out too short or too long
                headings.append({
                    'text': text,
                    'element': element
                })
    
    # Match headings to section patterns
    sections = {}
    for heading in headings:
        heading_text = heading['text']
        for section_id, pattern in section_patterns.items():
            if re.search(pattern, heading_text, re.IGNORECASE):
                # Get the text content for this section
                section_text = ""
                element = heading['element']
                next_element = element.find_next_sibling()
                while next_element:
                    # Stop if we hit another heading
                    next_text = next_element.get_text().strip()
                    if next_text and len(next_text) > 5 and len(next_text) < 100:
                        for other_section_id, other_pattern in section_patterns.items():
                            if other_section_id != section_id and re.search(other_pattern, next_text, re.IGNORECASE):
                                break
                    
                    # Add text content
                    section_text += next_element.get_text() + "\n"
                    next_element = next_element.find_next_sibling()
                
                # Add section
                sections[section_id] = {
                    "heading": heading_text,
                    "text": section_text
                }
                break
    
    logging.info(f"Extracted {len(sections)} document sections")
    return sections

def organize_financial_statements(facts):
    """
    Organize XBRL facts into financial statements.
    
    Args:
        facts: List of XBRL facts
        
    Returns:
        Dictionary of financial statements
    """
    # Define statement types
    BALANCE_SHEET = "BALANCE_SHEET"
    INCOME_STATEMENT = "INCOME_STATEMENT"
    CASH_FLOW_STATEMENT = "CASH_FLOW_STATEMENT"
    EQUITY_STATEMENT = "EQUITY_STATEMENT"
    
    # Define section types
    ASSETS = "ASSETS"
    LIABILITIES = "LIABILITIES"
    EQUITY = "EQUITY"
    REVENUE = "REVENUE"
    EXPENSES = "EXPENSES"
    INCOME = "INCOME"
    OPERATING = "OPERATING"
    INVESTING = "INVESTING"
    FINANCING = "FINANCING"
    OTHER = "OTHER"
    
    # Initialize result
    statements = {}
    
    # Organize facts by statement type
    for fact in facts:
        concept = fact.get("name", "").lower()
        
        # Determine statement type
        statement_type = None
        section_type = OTHER
        
        # Balance sheet concepts
        if any(term in concept for term in ["asset", "liability", "equity", "debt", "loan", "deposit", "inventory"]):
            statement_type = BALANCE_SHEET
            if "asset" in concept:
                section_type = ASSETS
            elif "liability" in concept or "debt" in concept or "loan" in concept:
                section_type = LIABILITIES
            elif "equity" in concept:
                section_type = EQUITY
        
        # Income statement concepts
        elif any(term in concept for term in ["revenue", "income", "expense", "cost", "tax", "profit", "loss", "earning"]):
            statement_type = INCOME_STATEMENT
            if "revenue" in concept:
                section_type = REVENUE
            elif "expense" in concept or "cost" in concept:
                section_type = EXPENSES
            elif "income" in concept or "profit" in concept or "loss" in concept or "earning" in concept:
                section_type = INCOME
        
        # Cash flow statement concepts
        elif any(term in concept for term in ["cash", "payment", "proceed", "financing", "investing", "operating"]):
            statement_type = CASH_FLOW_STATEMENT
            if "operating" in concept:
                section_type = OPERATING
            elif "investing" in concept:
                section_type = INVESTING
            elif "financing" in concept:
                section_type = FINANCING
        
        # Equity statement concepts
        elif any(term in concept for term in ["equity", "stock", "share", "dividend", "capital"]):
            statement_type = EQUITY_STATEMENT
        
        # Add fact to statement
        if statement_type:
            if statement_type not in statements:
                statements[statement_type] = {
                    "type": statement_type,
                    "sections": {}
                }
            
            if section_type not in statements[statement_type]["sections"]:
                statements[statement_type]["sections"][section_type] = []
            
            statements[statement_type]["sections"][section_type].append(fact)
    
    # Convert to list format
    result = []
    for statement_type, statement_data in statements.items():
        sections = []
        for section_type, section_facts in statement_data["sections"].items():
            sections.append({
                "name": section_type,
                "facts": section_facts
            })
        
        result.append({
            "type": statement_type,
            "sections": sections
        })
    
    return result

def generate_direct_json(html_path, metadata):
    """
    Generate direct JSON format from HTML/XBRL document.
    
    Args:
        html_path: Path to HTML/XBRL document
        metadata: Filing metadata
        
    Returns:
        JSON-formatted content as a dictionary
    """
    # Initialize the JSON structure
    json_data = {
        "metadata": metadata,
        "document": {
            "sections": []
        },
        "financial_data": {
            "statements": []
        },
        "xbrl_data": {
            "facts": []
        },
        "data_integrity": {}
    }
    
    try:
        # Extract XBRL facts directly from HTML
        logging.info(f"Extracting XBRL facts from {html_path}")
        xbrl_facts = extract_facts_from_html(html_path)
        
        # Store XBRL facts
        json_data["xbrl_data"]["facts"] = xbrl_facts
        json_data["data_integrity"]["xbrl_facts_extracted"] = len(xbrl_facts)
        
        # Extract document sections directly from HTML
        logging.info(f"Extracting document sections from {html_path}")
        document_sections = extract_document_sections(html_path)
        
        # Convert document sections to list format
        sections_list = []
        for section_id, section_data in document_sections.items():
            # Skip financial statement sections - we'll get this data from XBRL
            if any(term in section_id.lower() for term in ["balance", "income", "cash", "equity", "financial_statements"]):
                continue
            
            # Create a clean section object
            section = {
                "id": section_id,
                "title": section_data.get("heading", section_id),
                "content": section_data.get("text", "")
            }
            
            sections_list.append(section)
        
        # Store narrative sections
        json_data["document"]["sections"] = sections_list
        json_data["data_integrity"]["narrative_sections_extracted"] = len(sections_list)
        
        # Organize financial statements
        logging.info("Organizing financial statements")
        financial_statements = organize_financial_statements(xbrl_facts)
        
        # Store financial statements
        json_data["financial_data"]["statements"] = financial_statements
        json_data["data_integrity"]["financial_statements_created"] = len(financial_statements)
        
    except Exception as e:
        logging.error(f"Error generating direct JSON format: {str(e)}")
    
    return json_data

def main():
    """
    Main function to test direct JSON output format
    """
    parser = argparse.ArgumentParser(description="Test direct JSON output format for SEC filings")
    parser.add_argument("--html-path", required=True, help="Path to HTML/XBRL document")
    parser.add_argument("--output", required=True, help="Output JSON file path")
    parser.add_argument("--ticker", default="UNKNOWN", help="Ticker symbol")
    parser.add_argument("--filing-type", default="UNKNOWN", help="Filing type")
    parser.add_argument("--year", default="UNKNOWN", help="Fiscal year")

    args = parser.parse_args()

    # Create metadata
    metadata = {
        "ticker": args.ticker,
        "filing_type": args.filing_type,
        "fiscal_year": args.year
    }

    # Generate direct JSON format
    json_data = generate_direct_json(args.html_path, metadata)

    # Save direct JSON format
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        
        # Save file
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2)
        
        logging.info(f"Successfully saved direct JSON format to {args.output}")
        
        # Print JSON structure summary
        print(f"\nDirect JSON file size: {os.path.getsize(args.output) / 1024:.2f} KB")
        print(f"JSON structure:")
        print(f"  - metadata: {list(json_data.get('metadata', {}).keys())}")
        
        # Print narrative sections
        sections = json_data.get('document', {}).get('sections', [])
        print(f"  - narrative sections: {len(sections)}")
        for i, section in enumerate(sections[:5]):
            print(f"    * {i+1}: {section.get('id', 'Unknown')} ({len(section.get('content', ''))} chars)")
        if len(sections) > 5:
            print(f"    * ... and {len(sections) - 5} more sections")
        
        # Print financial statements
        statements = json_data.get('financial_data', {}).get('statements', [])
        print(f"  - financial statements: {len(statements)}")
        for i, statement in enumerate(statements):
            print(f"    * {i+1}: {statement.get('type', 'Unknown')}")
            
            # Print sections
            sections = statement.get('sections', [])
            print(f"      - Sections: {len(sections)}")
            for j, section in enumerate(sections[:3]):
                print(f"        * {j+1}: {section.get('name', 'Unknown')} ({len(section.get('facts', []))} facts)")
            if len(sections) > 3:
                print(f"        * ... and {len(sections) - 3} more sections")
        
        # Print XBRL facts
        facts = json_data.get('xbrl_data', {}).get('facts', [])
        print(f"  - xbrl facts: {len(facts)}")
        for i, fact in enumerate(facts[:5]):
            print(f"    * {i+1}: {fact.get('name', 'Unknown')} = {fact.get('value', '')}")
        if len(facts) > 5:
            print(f"    * ... and {len(facts) - 5} more facts")
        
        # Print data integrity metrics
        print(f"  - data integrity: {json_data.get('data_integrity', {})}")
    except Exception as e:
        logging.error(f"Error saving direct JSON format: {str(e)}")

if __name__ == "__main__":
    import re  # Import re here to avoid import errors
    main()
