#!/usr/bin/env python3
"""
Financial Statement Mapper

This module maps financial facts to the document structure defined in the @STRUCTURE section.
It creates a structured financial statements section that organizes facts by statement type and category.
"""

import re
import logging
from collections import defaultdict
from typing import Dict, List, Any, Set

class FinancialStatementMapper:
    """
    Maps financial facts to the document structure defined in the @STRUCTURE section.
    Creates a structured financial statements section that organizes facts by statement type and category.
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
            The LLM file content with a structured financial statements section
        """
        self.logger.info("Mapping financial facts to document structure")
        
        # Extract sections from the content
        structure_section, facts_section = self._extract_sections(content)
        
        # If no structure section or facts section, return the original content
        if not structure_section or not facts_section:
            self.logger.warning("No structure section or facts section found")
            return content
        
        # Parse document structure
        structure_info = self._parse_structure(structure_section)
        
        # Parse facts
        facts = self._parse_facts(facts_section)
        
        # Organize facts by financial statement type and category
        financial_statements = self._organize_facts_by_structure(facts, structure_info)
        
        # Generate structured financial statements section
        financial_statements_section = self._generate_structured_financial_statements(financial_statements, structure_info)
        
        # Remove any existing financial statements section
        fixed_content = re.sub(r'@FINANCIAL_STATEMENTS_SECTION.*?(?=\n\n@FACTS|\n\n@SECTION:|\n\n@NARRATIVE_TEXT:|\Z)', '', content, flags=re.DOTALL)
        
        # Find a good place to insert the financial statements section
        # Look for the end of the contexts section
        contexts_end_pattern = r'(@CONTEXTS.*?)(?=\n\n@FACTS)'
        contexts_end_match = re.search(contexts_end_pattern, fixed_content, re.DOTALL)
        
        if contexts_end_match:
            # Insert after the contexts section
            insert_pos = contexts_end_match.end()
            
            # Insert the financial statements section
            fixed_content = fixed_content[:insert_pos] + "\n\n" + financial_statements_section + fixed_content[insert_pos:]
        else:
            # If we can't find the contexts section, insert at the beginning
            fixed_content = financial_statements_section + "\n\n" + fixed_content
        
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
                    for line in fact_lines:
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
                                "context_ref": context_ref
                            }
                            
                            facts.append(fact)
            else:
                # Facts are not grouped by prefix
                fact_lines = context_block.strip().split('\n')
                for line in fact_lines:
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
                            "context_ref": context_ref
                        }
                        
                        facts.append(fact)
        
        return facts
    
    def _organize_facts_by_structure(self, facts: List[Dict[str, Any]], structure_info: Dict[str, Any]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """
        Organize facts by financial statement type and category.
        
        Args:
            facts: List of facts
            structure_info: Dictionary of structure information
            
        Returns:
            Dictionary of facts organized by statement type and category
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
        
        # Organize facts by statement type and category
        financial_statements = {}
        
        for statement_type, categories in statement_type_mapping.items():
            financial_statements[statement_type] = {}
            
            for category, keywords in categories.items():
                financial_statements[statement_type][category] = []
                
                # Find facts that match this category
                for fact in facts:
                    concept = fact["concept"].lower()
                    
                    if any(keyword.lower() in concept for keyword in keywords):
                        financial_statements[statement_type][category].append(fact)
        
        return financial_statements
    
    def _generate_structured_financial_statements(self, financial_statements: Dict[str, Dict[str, List[Dict[str, Any]]]], structure_info: Dict[str, Any]) -> str:
        """
        Generate structured financial statements that map to the document structure.
        
        Args:
            financial_statements: Dictionary of facts organized by statement type and category
            structure_info: Dictionary of structure information
            
        Returns:
            Structured financial statements section
        """
        output = []
        output.append("@FINANCIAL_STATEMENTS_SECTION")
        output.append("@MAPPING_TO_STRUCTURE: This section maps financial facts to the document structure defined in @STRUCTURE")
        
        # Add each statement type
        for statement_type in structure_info.get('statement_types', []):
            if statement_type not in financial_statements:
                continue
            
            output.append(f"\n@FINANCIAL_STATEMENT: {statement_type}")
            
            # Add each category
            for category in structure_info.get('main_categories', []):
                if category not in financial_statements[statement_type]:
                    continue
                
                facts = financial_statements[statement_type][category]
                
                if not facts:
                    continue
                
                output.append(f"\n@CATEGORY: {category}")
                
                # Check if there are subcategories for this category
                subcategories = structure_info.get('subcategories', {}).get(category.upper(), [])
                
                if subcategories:
                    # Organize facts by subcategory
                    for subcategory in subcategories:
                        subcategory_facts = []
                        subcategory_keywords = subcategory.split('_')
                        
                        for fact in facts:
                            concept = fact["concept"].lower()
                            if any(keyword.lower() in concept for keyword in subcategory_keywords):
                                subcategory_facts.append(fact)
                        
                        if subcategory_facts:
                            output.append(f"\n@SUBCATEGORY: {subcategory}")
                            
                            # Group facts by context
                            facts_by_context = defaultdict(list)
                            for fact in subcategory_facts:
                                context_ref = fact["context_ref"]
                                facts_by_context[context_ref].append(fact)
                            
                            # Add facts for each context
                            for context_ref, context_facts in facts_by_context.items():
                                output.append(f"@CONTEXT: {context_ref}")
                                
                                # Group facts by concept prefix
                                facts_by_prefix = defaultdict(list)
                                for fact in context_facts:
                                    concept = fact["concept"]
                                    prefix = concept.split(":")[0] if ":" in concept else ""
                                    facts_by_prefix[prefix].append(fact)
                                
                                # Add facts for each prefix
                                for prefix, prefix_facts in facts_by_prefix.items():
                                    if prefix:
                                        output.append(f"@PREFIX: {prefix}")
                                    
                                    for fact in prefix_facts:
                                        concept = fact["concept"]
                                        value = fact["value"]
                                        unit = fact["unit"]
                                        
                                        # Remove prefix from concept if it's already specified
                                        if prefix and concept.startswith(f"{prefix}:"):
                                            concept = concept[len(prefix)+1:]
                                        
                                        # Add fact in compact format
                                        fact_line = f"{concept}|{value}"
                                        if unit:
                                            fact_line += f"|{unit}"
                                        
                                        output.append(fact_line)
                else:
                    # No subcategories, just add the facts
                    # Group facts by context
                    facts_by_context = defaultdict(list)
                    for fact in facts:
                        context_ref = fact["context_ref"]
                        facts_by_context[context_ref].append(fact)
                    
                    # Add facts for each context
                    for context_ref, context_facts in facts_by_context.items():
                        output.append(f"@CONTEXT: {context_ref}")
                        
                        # Group facts by concept prefix
                        facts_by_prefix = defaultdict(list)
                        for fact in context_facts:
                            concept = fact["concept"]
                            prefix = concept.split(":")[0] if ":" in concept else ""
                            facts_by_prefix[prefix].append(fact)
                        
                        # Add facts for each prefix
                        for prefix, prefix_facts in facts_by_prefix.items():
                            if prefix:
                                output.append(f"@PREFIX: {prefix}")
                            
                            for fact in prefix_facts:
                                concept = fact["concept"]
                                value = fact["value"]
                                unit = fact["unit"]
                                
                                # Remove prefix from concept if it's already specified
                                if prefix and concept.startswith(f"{prefix}:"):
                                    concept = concept[len(prefix)+1:]
                                
                                # Add fact in compact format
                                fact_line = f"{concept}|{value}"
                                if unit:
                                    fact_line += f"|{unit}"
                                
                                output.append(fact_line)
        
        return "\n".join(output)
