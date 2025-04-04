#!/usr/bin/env python3
"""
Minimal Financial Mapper

This module creates a minimal mapping between document structure and financial facts
without duplicating the fact data.
"""

import re
import logging
from collections import defaultdict
from typing import Dict, List, Any, Set

class MinimalFinancialMapper:
    """
    Creates a minimal mapping between document structure and financial facts
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
            The LLM file content with a minimal financial statements mapping section
        """
        logging.info("Creating minimal mapping between financial facts and document structure")
        
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
        
        # Extract statement types
        statement_types_match = re.search(r'@STATEMENT_TYPES: (.*)', structure_section)
        if statement_types_match:
            statement_types = statement_types_match.group(1).split(', ')
            structure_info['statement_types'] = statement_types
        
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
                            
                            # Add prefix to concept if not already there
                            if prefix and not concept.startswith(f"{prefix}:"):
                                concept = f"{prefix}:{concept}"
                            
                            fact = {
                                "concept": concept
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
                        
                        fact = {
                            "concept": concept
                        }
                        
                        facts.append(fact)
        
        return facts
    
    def _create_mapping(self, facts: List[Dict[str, Any]], structure_info: Dict[str, Any]) -> Dict[str, Set[str]]:
        """
        Create mapping between facts and structure.
        
        Args:
            facts: List of facts
            structure_info: Dictionary of structure information
            
        Returns:
            Dictionary of mapping between facts and structure
        """
        # Define mapping of concepts to statement types
        statement_type_keywords = {
            "Income_Statement": [
                "Revenue", "Sales", "Income", "Expense", "Profit", "Loss", "Cost", "Tax", "Dividend", "EPS", "EBITDA",
                "Earnings", "Margin", "Operating"
            ],
            "Balance_Sheet": [
                "Asset", "Liability", "Equity", "Debt", "Cash", "Inventory", "Receivable", "Payable", "Property", 
                "Equipment", "Investment", "Capital", "Stock", "Retained", "Shareholder", "Stockholder"
            ],
            "Cash_Flow_Statement": [
                "Cash", "Flow", "Operating", "Investing", "Financing", "Activities"
            ],
            "Statement_Of_Equity": [
                "Equity", "Stock", "Capital", "Retained", "Earnings", "Comprehensive", "Shareholder", "Stockholder"
            ]
        }
        
        # Use structure_info to customize the mapping if available
        if 'statement_types' in structure_info:
            # Keep only the statement types that are in the structure_info
            statement_type_keywords = {
                statement_type: keywords
                for statement_type, keywords in statement_type_keywords.items()
                if statement_type in structure_info['statement_types']
            }
        
        # Create mapping
        mapping = {}
        
        for statement_type, keywords in statement_type_keywords.items():
            mapping[statement_type] = set()
            
            # Find facts that match this statement type
            for fact in facts:
                concept = fact["concept"].lower()
                
                if any(keyword.lower() in concept for keyword in keywords):
                    # Add the concept to the mapping
                    mapping[statement_type].add(fact["concept"])
        
        return mapping
    
    def _generate_mapping_section(self, mapping: Dict[str, Set[str]], structure_info: Dict[str, Any]) -> str:
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
        output.append("@DESCRIPTION: This section maps financial concepts to statement types defined in @STRUCTURE")
        
        # Add each statement type
        for statement_type in structure_info.get('statement_types', []):
            if statement_type not in mapping:
                continue
            
            concepts = mapping[statement_type]
            
            if not concepts:
                continue
            
            output.append(f"\n@STATEMENT_TYPE: {statement_type}")
            
            # Add concepts in a compact format
            # Group by prefix to make it more readable
            concepts_by_prefix = defaultdict(list)
            for concept in concepts:
                prefix = concept.split(":")[0] if ":" in concept else ""
                name = concept.split(":")[1] if ":" in concept else concept
                concepts_by_prefix[prefix].append(name)
            
            # Add concepts by prefix
            for prefix, prefix_concepts in concepts_by_prefix.items():
                if prefix:
                    output.append(f"@PREFIX: {prefix}")
                    output.append(f"@CONCEPTS: {','.join(sorted(prefix_concepts))}")
                else:
                    output.append(f"@CONCEPTS: {','.join(sorted(prefix_concepts))}")
        
        return "\n".join(output)
