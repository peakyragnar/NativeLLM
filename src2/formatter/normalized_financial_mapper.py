"""
Normalized Financial Statement Mapper

This module provides functionality to map financial statements in a normalized "long" format,
which is more efficient and reduces file size while maintaining all data.
"""

import re
import logging
from typing import Dict, List, Any, Set
from collections import defaultdict

class NormalizedFinancialMapper:
    """
    Maps financial statements in a normalized "long" format.
    
    Instead of wide tables with contexts as columns, this uses a normalized format:
    Line Item Concept Code | Value | Unit | Context Code
    """
    
    def __init__(self):
        """Initialize the normalized financial mapper."""
        self.logger = logging.getLogger(__name__)
    
    def map_facts_to_financial_statements(self, content: str) -> str:
        """
        Map facts to financial statements in a normalized format.
        
        Args:
            content: The content of the file
            
        Returns:
            The content with added normalized financial statements
        """
        # Check if the content already has financial statements
        if re.search(r'@NORMALIZED_FINANCIAL_STATEMENTS', content):
            self.logger.info("Content already has normalized financial statements")
            return content
        
        # Extract facts from the content
        facts = self._extract_facts(content)
        self.logger.info(f"Extracted {len(facts)} facts from content")
        
        if not facts:
            self.logger.warning("No facts extracted, returning original content")
            return content
        
        # Generate normalized financial statements
        financial_statements = self._generate_normalized_financial_statements(facts)
        self.logger.info(f"Generated normalized financial statements of length {len(financial_statements)}")
        
        if not financial_statements:
            self.logger.warning("No financial statements generated, returning original content")
            return content
        
        # Insert financial statements into the content
        return self._insert_financial_statements(content, financial_statements)
    
    def _extract_facts(self, content: str) -> List[Dict[str, Any]]:
        """
        Extract facts from the content.
        
        Args:
            content: The content of the file
            
        Returns:
            List of facts
        """
        facts = []
        
        # Look for @FACTS section
        facts_match = re.search(r'@FACTS.*?(?=\n\n@SECTION:|\n\n@NARRATIVE_TEXT:|\Z)', content, re.DOTALL)
        if facts_match:
            facts_section = facts_match.group(0)
            self.logger.info(f"Found @FACTS section of length {len(facts_section)}")
            
            # Extract context blocks
            context_blocks = re.findall(r'@CONTEXT: ([^\n]+)(.*?)(?=\n@CONTEXT:|\Z)', facts_section, re.DOTALL)
            
            for context_ref, context_block in context_blocks:
                # Extract prefix blocks
                prefix_blocks = re.findall(r'@PREFIX: ([^\n]+)(.*?)(?=\n@PREFIX:|\n@CONTEXT:|\Z)', context_block, re.DOTALL)
                
                if not prefix_blocks:
                    # No prefix specified, treat the whole context block as one prefix block
                    prefix_blocks = [("", context_block)]
                
                for prefix, prefix_block in prefix_blocks:
                    # Extract facts
                    fact_lines = prefix_block.strip().split('\n')
                    for line in fact_lines:
                        if line.startswith('@PREFIX:') or line.startswith('@CONTEXT:'):
                            continue
                        
                        # Parse fact
                        parts = line.split('|')
                        if len(parts) >= 2:
                            concept = parts[0].strip()
                            value = parts[1].strip()
                            
                            # Add prefix if specified
                            if prefix and not concept.startswith(f"{prefix}:"):
                                concept = f"{prefix}:{concept}"
                            
                            facts.append({
                                'concept': concept,
                                'value': value,
                                'context_ref': context_ref,
                                'unit': parts[2].strip() if len(parts) > 2 else ""
                            })
        
        # If no @FACTS section, look for @DATA_DICTIONARY: CONTEXTS section
        elif '@DATA_DICTIONARY: CONTEXTS' in content:
            self.logger.info("No @FACTS section found, looking for @DATA_DICTIONARY: CONTEXTS section")
            contexts_match = re.search(r'@DATA_DICTIONARY: CONTEXTS.*?(?=\n\n@SECTION:|\n\n@NARRATIVE_TEXT:|\Z)', content, re.DOTALL)
            if contexts_match:
                contexts_section = contexts_match.group(0)
                self.logger.info(f"Found @DATA_DICTIONARY: CONTEXTS section of length {len(contexts_section)}")
                
                # Extract context blocks
                context_blocks = re.findall(r'c-\d+ \| @CODE: ([^\n]+)(.*?)(?=c-\d+ \||\Z)', contexts_section, re.DOTALL)
                
                for _, context_block in context_blocks:
                    context_ref = f"c-{len(facts)}"
                    
                    # Extract facts from context block
                    fact_lines = context_block.strip().split('\n')
                    for line in fact_lines:
                        if '@SEGMENT:' in line or '@LABEL:' in line or '@DESCRIPTION:' in line:
                            continue
                        
                        # Try to extract concept and value
                        concept_value_match = re.search(r'([^|]+)\|([^|]+)', line)
                        if concept_value_match:
                            concept = concept_value_match.group(1).strip()
                            value = concept_value_match.group(2).strip()
                            
                            facts.append({
                                'concept': concept,
                                'value': value,
                                'context_ref': context_ref,
                                'unit': ""
                            })
        
        # Also look for existing financial statements to extract facts
        financial_statements_match = re.search(r'@FINANCIAL_STATEMENT: (Balance_Sheet|Income_Statement|Cash_Flow_Statement|Statement_Of_Equity).*?(?=\n\n@FINANCIAL_STATEMENT:|\n\n@SECTION:|\Z)', content, re.DOTALL)
        if financial_statements_match:
            self.logger.info("Found existing financial statements, extracting facts")
            
            # Find all financial statements
            financial_statements = re.findall(r'@FINANCIAL_STATEMENT: (Balance_Sheet|Income_Statement|Cash_Flow_Statement|Statement_Of_Equity).*?(?=\n\n@FINANCIAL_STATEMENT:|\n\n@SECTION:|\Z)', content, re.DOTALL)
            
            for statement in financial_statements:
                # Extract context blocks
                context_blocks = re.findall(r'@CONTEXT: ([^\n]+)(.*?)(?=\n@CONTEXT:|\n\n@FINANCIAL_STATEMENT:|\Z)', statement, re.DOTALL)
                
                for context_ref, context_block in context_blocks:
                    # Extract prefix blocks
                    prefix_blocks = re.findall(r'@PREFIX: ([^\n]+)(.*?)(?=\n@PREFIX:|\n@CONTEXT:|\n\n@FINANCIAL_STATEMENT:|\Z)', context_block, re.DOTALL)
                    
                    if not prefix_blocks:
                        # No prefix specified, treat the whole context block as one prefix block
                        prefix_blocks = [("", context_block)]
                    
                    for prefix, prefix_block in prefix_blocks:
                        # Extract facts
                        fact_lines = prefix_block.strip().split('\n')
                        for line in fact_lines:
                            if line.startswith('@PREFIX:') or line.startswith('@CONTEXT:'):
                                continue
                            
                            # Parse fact
                            parts = line.split('|')
                            if len(parts) >= 2:
                                concept = parts[0].strip()
                                value = parts[1].strip()
                                
                                # Add prefix if specified
                                if prefix and not concept.startswith(f"{prefix}:"):
                                    concept = f"{prefix}:{concept}"
                                
                                facts.append({
                                    'concept': concept,
                                    'value': value,
                                    'context_ref': context_ref,
                                    'unit': parts[2].strip() if len(parts) > 2 else ""
                                })
        
        return facts
    
    def _generate_normalized_financial_statements(self, facts: List[Dict[str, Any]]) -> str:
        """
        Generate normalized financial statements from facts.
        
        Args:
            facts: List of facts
            
        Returns:
            Normalized financial statements section
        """
        # Statement types
        statement_types = [
            "Balance_Sheet",
            "Income_Statement",
            "Cash_Flow_Statement",
            "Statement_Of_Equity"
        ]
        
        # Statement titles
        statement_titles = {
            "Balance_Sheet": "Consolidated Balance Sheets",
            "Income_Statement": "Consolidated Statements of Income",
            "Cash_Flow_Statement": "Consolidated Statements of Cash Flows",
            "Statement_Of_Equity": "Consolidated Statements of Stockholders' Equity"
        }
        
        # Mapping of concept patterns to statement types
        concept_to_statement_type = {
            r"(Asset|Liability|Equity|CurrentAsset|NonCurrentAsset|CurrentLiability|NonCurrentLiability)": "Balance_Sheet",
            r"(Revenue|Income|Expense|EarningsPerShare|GrossProfit|OperatingIncome|NetIncome)": "Income_Statement",
            r"(CashFlow|CashAndCashEquivalent|NetCashProvidedByUsedIn)": "Cash_Flow_Statement",
            r"(StockholdersEquity|ShareCapital|RetainedEarnings|AccumulatedOtherComprehensiveIncome)": "Statement_Of_Equity"
        }
        
        # Group facts by statement type
        facts_by_statement = defaultdict(list)
        
        for fact in facts:
            concept = fact.get('concept', '')
            
            # Skip non-financial concepts
            if not any(concept.startswith(prefix) for prefix in ['us-gaap:', 'ifrs-full:', 'gaap:', 'ifrs:']):
                continue
            
            # Determine statement type based on concept
            statement_type = self._determine_statement_type(concept, concept_to_statement_type)
            facts_by_statement[statement_type].append(fact)
        
        # Generate normalized financial statements
        financial_statements = "\n\n@NORMALIZED_FINANCIAL_STATEMENTS"
        
        # Add header row for the normalized format
        financial_statements += "\n\n@NORMALIZED_FORMAT: Concept | Value | Unit | Context"
        
        # Add facts for each statement type
        for statement_type in statement_types:
            if statement_type in facts_by_statement and facts_by_statement[statement_type]:
                financial_statements += f"\n\n@FINANCIAL_STATEMENT: {statement_type}"
                financial_statements += f"\n@STATEMENT_TITLE: {statement_titles.get(statement_type, statement_type.replace('_', ' '))}\n"
                
                # Add all facts for this statement type in normalized format
                for fact in facts_by_statement[statement_type]:
                    concept = fact['concept']
                    value = fact['value']
                    unit = fact['unit']
                    context_ref = fact['context_ref']
                    
                    # Add fact in normalized format
                    if unit:
                        financial_statements += f"{concept}|{value}|{unit}|{context_ref}\n"
                    else:
                        financial_statements += f"{concept}|{value}||{context_ref}\n"
        
        return financial_statements
    
    def _determine_statement_type(self, concept: str, concept_to_statement_type: Dict[str, str]) -> str:
        """
        Determine the statement type for a concept.
        
        Args:
            concept: The concept name
            concept_to_statement_type: Mapping of concept patterns to statement types
            
        Returns:
            The statement type
        """
        # Remove prefix
        if ":" in concept:
            concept_name = concept.split(":", 1)[1]
        else:
            concept_name = concept
        
        # Check against patterns
        for pattern, statement_type in concept_to_statement_type.items():
            if re.search(pattern, concept_name, re.IGNORECASE):
                return statement_type
        
        # Default to Balance Sheet if not determined
        return "Balance_Sheet"
    
    def _insert_financial_statements(self, content: str, financial_statements: str) -> str:
        """
        Insert financial statements into the content.
        
        Args:
            content: The content of the file
            financial_statements: The financial statements section
            
        Returns:
            The content with financial statements inserted
        """
        # Try to insert after @DATA_DICTIONARY: CONTEXTS section
        contexts_match = re.search(r'@DATA_DICTIONARY: CONTEXTS.*?(?=\n\n@SECTION:|\n\n@NARRATIVE_TEXT:|\Z)', content, re.DOTALL)
        if contexts_match:
            insert_pos = contexts_match.end()
            self.logger.info("Inserting financial statements after @DATA_DICTIONARY: CONTEXTS section")
            return content[:insert_pos] + "\n\n" + financial_statements + content[insert_pos:]
        
        # If no @DATA_DICTIONARY: CONTEXTS section, try to insert before @SECTION: ITEM_8_FINANCIAL_STATEMENTS
        section_match = re.search(r'\n\n@SECTION: ITEM_8_FINANCIAL_STATEMENTS', content)
        if section_match:
            insert_pos = section_match.start()
            self.logger.info("Inserting financial statements before @SECTION: ITEM_8_FINANCIAL_STATEMENTS")
            return content[:insert_pos] + "\n\n" + financial_statements + content[insert_pos:]
        
        # If no suitable position found, insert before the first @SECTION
        section_match = re.search(r'\n\n@SECTION:', content)
        if section_match:
            insert_pos = section_match.start()
            self.logger.info("Inserting financial statements before the first @SECTION")
            return content[:insert_pos] + "\n\n" + financial_statements + content[insert_pos:]
        
        # If no @SECTION found, append to the end
        self.logger.info("No suitable position found, appending financial statements to the end")
        return content + "\n\n" + financial_statements
