#!/usr/bin/env python3
"""
Efficient Financial Statement Mapper

This module creates a mapping between document structure and financial facts
without duplicating the fact data.
"""

import re
import logging
from collections import defaultdict
from typing import Dict, List, Any, Set

class EfficientFinancialStatementMapper:
    """
    Creates a mapping between document structure and financial facts
    without duplicating the fact data.
    """
    
    def __init__(self):
        """Initialize the financial statement mapper."""
        self.logger = logging.getLogger(__name__)
    
    def map_facts_to_structure(self, content: str) -> str:
        """
        Map financial facts to the document structure.
        
        Args:
            content: The LLM file content
            
        Returns:
            The LLM file content with a financial statements mapping section
        """
        logging.info("Creating efficient mapping between financial facts and document structure")
        
        # Extract sections from the content
        structure_section, facts_section = self._extract_sections(content)
        
        # If no structure section or facts section, return the original content
        if not structure_section or not facts_section:
            logging.warning("No structure section or facts section found")
            return content
        
        # Parse document structure
        structure_info = self._parse_structure(structure_section)
        
        # Parse facts
        facts = self._parse_facts(facts_section)
        
        # Create mapping between facts and structure
        mapping = self._create_mapping(facts, structure_info)
        
        # Generate mapping section
        mapping_section = self._generate_mapping_section(mapping, structure_info)
        
        # Find a good place to insert the mapping section
        # Look for the end of the contexts section
        contexts_end_pattern = r'(@CONTEXTS.*?)(?=\n\n@FACTS)'
        contexts_end_match = re.search(contexts_end_pattern, content, re.DOTALL)
        
        if contexts_end_match:
            # Insert after the contexts section
            insert_pos = contexts_end_match.end()
            
            # Insert the mapping section
            fixed_content = content[:insert_pos] + "\n\n" + mapping_section + content[insert_pos:]
        else:
            # If we can't find the contexts section, insert at the beginning
            fixed_content = mapping_section + "\n\n" + content
        
        return fixed_content
    
    def _extract_sections(self, content: str) -> tuple:
        """
        Extract sections from the content.
        
        Args:
            content: The LLM file content
            
        Returns:
            Tuple of (structure_section, facts_section)
        """
        # Extract structure section
        structure_pattern = r'@STRUCTURE:.*?(?=@CONTEXTS|\n\n@DATA_DICTIONARY)'
        structure_match = re.search(structure_pattern, content, re.DOTALL)
        structure_section = structure_match.group(0) if structure_match else ""
        
        # Extract facts section
        facts_pattern = r'@FACTS.*?(?=\n\n@SECTION:|\n\n@NARRATIVE_TEXT:|\n\n@FINANCIAL_STATEMENTS_SECTION|\Z)'
        facts_match = re.search(facts_pattern, content, re.DOTALL)
        facts_section = facts_match.group(0) if facts_match else ""
        
        return structure_section, facts_section
    
    def _parse_structure(self, structure_section: str) -> Dict[str, Any]:
        """
        Parse the document structure section.
        
        Args:
            structure_section: The document structure section
            
        Returns:
            Dictionary of structure information
        """
        structure_info = {}
        
        # Extract main categories
        main_categories_match = re.search(r'@MAIN_CATEGORIES: (.*)', structure_section)
        if main_categories_match:
            main_categories = main_categories_match.group(1).split(', ')
            structure_info['main_categories'] = main_categories
        
        # Extract statement types
        statement_types_match = re.search(r'@STATEMENT_TYPES: (.*)', structure_section)
        if statement_types_match:
            statement_types = statement_types_match.group(1).split(', ')
            structure_info['statement_types'] = statement_types
        
        # Extract document parts
        document_parts_match = re.search(r'@DOCUMENT_PARTS: (.*)', structure_section)
        if document_parts_match:
            document_parts = document_parts_match.group(1).split(', ')
            structure_info['document_parts'] = document_parts
        
        # Extract all sections
        all_sections_match = re.search(r'@ALL_SECTIONS: (.*)', structure_section)
        if all_sections_match:
            all_sections = all_sections_match.group(1).split(', ')
            structure_info['all_sections'] = all_sections
        
        # Extract subcategories
        subcategories = {}
        subcategories_pattern = r'@SUBCATEGORIES_([A-Z_]+): (.*)'
        subcategories_matches = re.findall(subcategories_pattern, structure_section)
        for category, subcategory_list in subcategories_matches:
            subcategories[category] = subcategory_list.split(', ')
        
        structure_info['subcategories'] = subcategories
        
        return structure_info
    
    def _parse_facts(self, facts_section: str) -> List[Dict[str, Any]]:
        """
        Parse facts from the facts section.
        
        Args:
            facts_section: The facts section
            
        Returns:
            List of facts
        """
        facts = []
        
        # Extract context blocks
        context_blocks = re.findall(r'@CONTEXT: ([^\n]+)(.*?)(?=\n@CONTEXT:|\Z)', facts_section, re.DOTALL)
        
        for context_ref, context_block in context_blocks:
            context_ref = context_ref.strip()
            
            # Extract prefix blocks
            prefix_blocks = re.findall(r'@PREFIX: ([^\n]+)(.*?)(?=\n@PREFIX:|\n@CONTEXT:|\Z)', context_block, re.DOTALL)
            
            if prefix_blocks:
                # Facts are grouped by prefix
                for prefix, prefix_block in prefix_blocks:
                    prefix = prefix.strip()
                    
                    # Parse facts in this prefix block
                    fact_lines = prefix_block.strip().split('\n')
                    for i, line in enumerate(fact_lines):
                        if not line or line.startswith('@'):
                            continue
                        
                        parts = line.split('|')
                        if len(parts) >= 2:
                            concept = parts[0].strip()
                            value = parts[1].strip()
                            unit = parts[2].strip() if len(parts) > 2 else ""
                            
                            # Add prefix to concept if not already there
                            if prefix and not concept.startswith(f"{prefix}:"):
                                concept = f"{prefix}:{concept}"
                            
                            fact = {
                                "concept": concept,
                                "value": value,
                                "unit": unit,
                                "context_ref": context_ref,
                                "location": f"c:{context_ref}|p:{prefix}|i:{i}"  # Location reference for mapping
                            }
                            
                            facts.append(fact)
            else:
                # Facts are not grouped by prefix
                fact_lines = context_block.strip().split('\n')
                for i, line in enumerate(fact_lines):
                    if not line or line.startswith('@'):
                        continue
                    
                    parts = line.split('|')
                    if len(parts) >= 2:
                        concept = parts[0].strip()
                        value = parts[1].strip()
                        unit = parts[2].strip() if len(parts) > 2 else ""
                        
                        fact = {
                            "concept": concept,
                            "value": value,
                            "unit": unit,
                            "context_ref": context_ref,
                            "location": f"c:{context_ref}|i:{i}"  # Location reference for mapping
                        }
                        
                        facts.append(fact)
        
        return facts
    
    def _create_mapping(self, facts: List[Dict[str, Any]], structure_info: Dict[str, Any]) -> Dict[str, Dict[str, List[str]]]:
        """
        Create mapping between facts and structure.
        
        Args:
            facts: List of facts
            structure_info: Dictionary of structure information
            
        Returns:
            Dictionary of mapping between facts and structure
        """
        # Define mapping of concepts to statement types and categories
        statement_type_mapping = {
            # Income Statement
            "Income_Statement": {
                "Revenues": [
                    "Revenue", "Sales", "Income", "Turnover"
                ],
                "Cost_of_Revenues": [
                    "CostOf", "Cost", "Expense"
                ],
                "Gross_Profit": [
                    "GrossProfit", "GrossMargin"
                ],
                "Operating_Expenses": [
                    "OperatingExpense", "ResearchAndDevelopment", "SellingGeneral", "Marketing"
                ],
                "Operating_Income": [
                    "OperatingIncome", "OperatingProfit", "OperatingEarnings"
                ],
                "Net_Income": [
                    "NetIncome", "NetEarnings", "NetProfit", "EarningsPerShare", "EPS"
                ]
            },
            
            # Balance Sheet
            "Balance_Sheet": {
                "Assets": [
                    "Asset", "Cash", "Receivable", "Inventory", "Property", "Equipment", "Investment"
                ],
                "Liabilities": [
                    "Liability", "Debt", "Payable", "Accrued", "Obligation"
                ],
                "Equity": [
                    "Equity", "Capital", "Stock", "Retained", "Earnings", "Shareholder", "Stockholder"
                ]
            },
            
            # Cash Flow Statement
            "Cash_Flow_Statement": {
                "Operating_Activities": [
                    "OperatingActivities", "CashFromOperations"
                ],
                "Investing_Activities": [
                    "InvestingActivities", "CashFromInvesting"
                ],
                "Financing_Activities": [
                    "FinancingActivities", "CashFromFinancing"
                ]
            },
            
            # Statement of Equity
            "Statement_Of_Equity": {
                "Common_Stock": [
                    "CommonStock", "Stock", "ShareCapital"
                ],
                "Additional_Paid_In_Capital": [
                    "AdditionalPaidInCapital", "PaidInCapital"
                ],
                "Retained_Earnings": [
                    "RetainedEarnings", "AccumulatedEarnings"
                ],
                "Accumulated_Other_Comprehensive_Income": [
                    "AccumulatedOtherComprehensiveIncome", "OtherComprehensiveIncome"
                ]
            }
        }
        
        # Use structure_info to customize the mapping if available
        if 'statement_types' in structure_info:
            # Keep only the statement types that are in the structure_info
            statement_type_mapping = {
                statement_type: categories
                for statement_type, categories in statement_type_mapping.items()
                if statement_type in structure_info['statement_types']
            }
        
        if 'main_categories' in structure_info:
            # Add any missing categories from structure_info
            for statement_type, categories in statement_type_mapping.items():
                for category in structure_info['main_categories']:
                    if category not in categories:
                        statement_type_mapping[statement_type][category] = [category]
        
        # Create mapping
        mapping = {}
        
        for statement_type, categories in statement_type_mapping.items():
            mapping[statement_type] = {}
            
            for category, keywords in categories.items():
                mapping[statement_type][category] = []
                
                # Find facts that match this category
                for fact in facts:
                    concept = fact["concept"].lower()
                    
                    if any(keyword.lower() in concept for keyword in keywords):
                        mapping[statement_type][category].append(fact["location"])
        
        return mapping
    
    def _generate_mapping_section(self, mapping: Dict[str, Dict[str, List[str]]], structure_info: Dict[str, Any]) -> str:
        """
        Generate mapping section.
        
        Args:
            mapping: Dictionary of mapping between facts and structure
            structure_info: Dictionary of structure information
            
        Returns:
            Mapping section
        """
        output = []
        output.append("@FINANCIAL_STATEMENTS_MAPPING")
        output.append("@DESCRIPTION: This section maps financial facts to the document structure defined in @STRUCTURE")
        output.append("@NOTE: Fact locations are references to facts in the @FACTS section")
        
        # Add each statement type
        for statement_type in structure_info.get('statement_types', []):
            if statement_type not in mapping:
                continue
            
            output.append(f"\n@STATEMENT_TYPE: {statement_type}")
            
            # Add each category
            for category in structure_info.get('main_categories', []):
                if category not in mapping[statement_type]:
                    continue
                
                fact_locations = mapping[statement_type][category]
                
                if not fact_locations:
                    continue
                
                output.append(f"@CATEGORY: {category}")
                
                # Check if there are subcategories for this category
                subcategories = structure_info.get('subcategories', {}).get(category.upper(), [])
                
                if subcategories:
                    # Organize facts by subcategory
                    for subcategory in subcategories:
                        subcategory_facts = []
                        subcategory_keywords = subcategory.split('_')
                        
                        # Find facts that match this subcategory
                        for fact_location in fact_locations:
                            # We don't have the concept here, so we'll just add all facts
                            # A more sophisticated approach would be to keep track of concepts
                            subcategory_facts.append(fact_location)
                        
                        if subcategory_facts:
                            output.append(f"@SUBCATEGORY: {subcategory}")
                            output.append(f"@FACT_LOCATIONS: {','.join(subcategory_facts)}")
                else:
                    # No subcategories, just add the fact locations
                    output.append(f"@FACT_LOCATIONS: {','.join(fact_locations)}")
        
        return "\n".join(output)
