"""
Normalized Financial Statement Mapper

This module provides functionality to map financial statements in a normalized "long" format,
which is more efficient and reduces file size while maintaining all data.
"""

import re
import logging
import json
from typing import Dict, List, Any, Set, Optional, Tuple
from collections import defaultdict
from .financial_validator import FinancialValidator
from .xbrl_hierarchy import XBRLHierarchyExtractor

class NormalizedFinancialMapper:
    """
    Maps financial statements in a normalized "long" format.

    Instead of wide tables with contexts as columns, this uses a normalized format:
    Line Item Concept Code | Value | Unit | Context Code
    """

    def __init__(self):
        """Initialize the normalized financial mapper."""
        self.logger = logging.getLogger(__name__)
        self.validator = FinancialValidator()
        self.hierarchy_extractor = XBRLHierarchyExtractor()

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

        # Extract XBRL hierarchy from @CONCEPT sections
        raw_facts = self._extract_raw_facts_from_concepts(content)
        hierarchy = self.hierarchy_extractor.extract_hierarchy(raw_facts)
        self.logger.info(f"Extracted hierarchy with {len(hierarchy['top_level']['Balance_Sheet'])} top-level balance sheet concepts")

        # Debug: Check if we have balance sheet facts
        balance_sheet_facts = [f for f in facts if self._determine_statement_type(f.get('concept', ''), {}) == 'Balance_Sheet']
        self.logger.warning(f"Found {len(balance_sheet_facts)} balance sheet facts")

        # Validate and complete balance sheet using hierarchy
        self._validate_and_complete_balance_sheet_with_hierarchy(balance_sheet_facts, hierarchy)

        # Debug: Check if balance sheet facts were updated
        self.logger.warning(f"After validation, have {len(balance_sheet_facts)} balance sheet facts")

        # Generate normalized financial statements with hierarchy
        financial_statements = self._generate_normalized_financial_statements_with_hierarchy(facts, hierarchy)
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

    def _generate_normalized_financial_statements_with_hierarchy(self, facts: List[Dict[str, Any]], hierarchy: Dict[str, Any]) -> str:
        """
        Generate normalized financial statements from facts using hierarchy information.

        Args:
            facts: List of facts
            hierarchy: The XBRL hierarchy information

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

        # Group facts by statement type and level
        facts_by_statement = {}
        for statement_type in statement_types:
            facts_by_statement[statement_type] = {
                0: [],  # Top-level concepts
                1: [],  # Direct children of top-level concepts
                2: []   # Other concepts
            }

        # Categorize facts by statement type and level
        for fact in facts:
            concept = fact.get('concept', '')

            # Skip non-financial concepts
            if not any(concept.startswith(prefix) for prefix in ['us-gaap:', 'ifrs-full:', 'gaap:', 'ifrs:']):
                continue

            # Determine statement type and level based on hierarchy
            statement_type, level = self.hierarchy_extractor.get_concept_level(concept, hierarchy)

            # If statement type is unknown, determine it based on concept name
            if statement_type == "Unknown":
                statement_type = self._determine_statement_type(concept, {})

            # Add fact to the appropriate category
            if statement_type in facts_by_statement:
                # Ensure level is valid
                if level not in facts_by_statement[statement_type]:
                    level = 2  # Default to level 2 (other concepts)

                facts_by_statement[statement_type][level].append(fact)

        # Generate normalized financial statements
        financial_statements = "\n\n@NORMALIZED_FINANCIAL_STATEMENTS"

        # Add header row for the normalized format
        financial_statements += "\n\n@NORMALIZED_FORMAT: Statement|Concept|Value|Context|Context_Label"

        # Add facts for each statement type
        for statement_type in statement_types:
            if any(facts_by_statement[statement_type][level] for level in [0, 1, 2]):
                financial_statements += f"\n\n@STATEMENT: {statement_type}"

                # Add top-level concepts first (level 0)
                for fact in facts_by_statement[statement_type][0]:
                    concept = fact.get('concept', '').split(':')[-1]
                    value = fact.get('value', '')
                    unit = fact.get('unit', '')
                    context_ref = fact.get('context_ref', '')
                    context_label = fact.get('context_label', '')

                    # Add fact in normalized format
                    financial_statements += f"\n{statement_type}|{concept}|{value}|{context_ref}|{context_label}"

                # Add direct children of top-level concepts (level 1)
                for fact in facts_by_statement[statement_type][1]:
                    concept = fact.get('concept', '').split(':')[-1]
                    value = fact.get('value', '')
                    unit = fact.get('unit', '')
                    context_ref = fact.get('context_ref', '')
                    context_label = fact.get('context_label', '')

                    # Add fact in normalized format
                    financial_statements += f"\n{statement_type}|{concept}|{value}|{context_ref}|{context_label}"

                # Add other concepts (level 2)
                for fact in facts_by_statement[statement_type][2]:
                    concept = fact.get('concept', '').split(':')[-1]
                    value = fact.get('value', '')
                    unit = fact.get('unit', '')
                    context_ref = fact.get('context_ref', '')
                    context_label = fact.get('context_label', '')

                    # Add fact in normalized format
                    financial_statements += f"\n{statement_type}|{concept}|{value}|{context_ref}|{context_label}"

        return financial_statements

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

        # For balance sheet, ensure we have all required components
        if "Balance_Sheet" in facts_by_statement:
            self._validate_and_complete_balance_sheet(facts_by_statement["Balance_Sheet"])

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

    def _extract_raw_facts_from_concepts(self, content: str) -> List[Dict[str, Any]]:
        """
        Extract raw facts from @CONCEPT sections for hierarchy extraction.

        Args:
            content: The content of the file

        Returns:
            List of raw facts
        """
        raw_facts = []

        # Extract @CONCEPT blocks
        concept_blocks = re.finditer(r'@CONCEPT: ([^\n]+)\n@VALUE: ([^\n]+)\n@UNIT_REF: ([^\n]+)\n@CONTEXT_REF: ([^\n|]+)(?:\|@CONTEXT: ([^\n]+))?\n@DATE_TYPE: ([^\n]+)(?:\n@(?:DATE|START_DATE): ([^\n]+))?(?:\n@END_DATE: ([^\n]+))?', content)

        for match in concept_blocks:
            concept = match.group(1)
            value = match.group(2)
            context_ref = match.group(4)

            # Add to raw facts list for hierarchy extraction
            raw_facts.append({
                'name': concept,
                'value': value,
                'contextRef': context_ref
            })

        self.logger.info(f"Extracted {len(raw_facts)} raw facts from @CONCEPT sections")
        return raw_facts

    def _validate_and_complete_balance_sheet_with_hierarchy(self, balance_sheet_facts: List[Dict[str, Any]], hierarchy: Dict[str, Any]) -> None:
        """
        Validate and complete the balance sheet facts using hierarchy information.

        Args:
            balance_sheet_facts: List of balance sheet facts
            hierarchy: The XBRL hierarchy information
        """
        # Group facts by context reference
        facts_by_context = defaultdict(list)
        for fact in balance_sheet_facts:
            context_ref = fact.get('context_ref', '')
            facts_by_context[context_ref].append(fact)

        # Process each context separately
        for context_ref, facts in facts_by_context.items():
            # Extract key balance sheet components
            assets = None
            liabilities = None
            equity = None
            minority_interests = None
            total_liabilities_and_equity = None
            context_label = ""

            # Find the key components
            for fact in facts:
                concept = fact.get('concept', '')
                value_str = fact.get('value', '0').replace('$', '').replace(',', '')

                # Get context label if available
                if not context_label and 'context_label' in fact:
                    context_label = fact.get('context_label', '')

                try:
                    value = float(value_str)
                except ValueError:
                    continue

                # Check if this is a top-level concept based on hierarchy
                statement_type, level = self.hierarchy_extractor.get_concept_level(concept, hierarchy)

                # Only use top-level concepts (level 0) for balance sheet validation
                if statement_type == "Balance_Sheet" and level == 0:
                    # Check for assets
                    if 'us-gaap:Assets' in concept:
                        assets = value

                    # Check for liabilities
                    elif 'us-gaap:Liabilities' in concept:
                        liabilities = value

                    # Check for equity
                    elif 'us-gaap:StockholdersEquity' in concept:
                        equity = value

                    # Check for minority interests
                    elif 'us-gaap:MinorityInterest' in concept or 'us-gaap:RedeemableNoncontrollingInterest' in concept:
                        minority_interests = value

                    # Check for total liabilities and equity
                    elif 'us-gaap:LiabilitiesAndStockholdersEquity' in concept:
                        total_liabilities_and_equity = value

            # If we have assets and total liabilities and equity, but missing components, try to calculate them
            if assets is not None and total_liabilities_and_equity is not None:
                # If assets and total liabilities and equity don't match, log a warning
                if abs(assets - total_liabilities_and_equity) > 0.01 * max(assets, total_liabilities_and_equity):
                    self.logger.warning(f"Balance sheet doesn't balance for context {context_ref}: "
                                       f"Assets ({assets}) != Total Liabilities and Equity ({total_liabilities_and_equity})")

                # If we're missing liabilities but have equity, calculate liabilities
                if liabilities is None and equity is not None:
                    minority_interests = minority_interests or 0
                    liabilities = total_liabilities_and_equity - equity - minority_interests
                    facts.append({
                        'concept': 'us-gaap:Liabilities',
                        'value': f"${liabilities:,.0f}",
                        'context_ref': context_ref,
                        'unit': 'USD',
                        'statement_type': 'Balance_Sheet',
                        'level': 0,  # Top-level concept
                        'context_label': context_label
                    })
                    self.logger.info(f"Added calculated liabilities ({liabilities}) for context {context_ref}")

                # If we're missing equity but have liabilities, calculate equity
                elif equity is None and liabilities is not None:
                    minority_interests = minority_interests or 0
                    equity = total_liabilities_and_equity - liabilities - minority_interests
                    facts.append({
                        'concept': 'us-gaap:StockholdersEquity',
                        'value': f"${equity:,.0f}",
                        'context_ref': context_ref,
                        'unit': 'USD',
                        'statement_type': 'Balance_Sheet',
                        'level': 0,  # Top-level concept
                        'context_label': context_label
                    })
                    self.logger.info(f"Added calculated equity ({equity}) for context {context_ref}")

            # Validate the balance sheet
            if assets is not None and liabilities is not None and equity is not None:
                minority_interests = minority_interests or 0
                is_valid, error_message = self.validator.validate_balance_sheet(
                    assets, liabilities, equity, minority_interests
                )

                if not is_valid:
                    self.logger.warning(f"Balance sheet validation failed for context {context_ref}: {error_message}")

    def _validate_and_complete_balance_sheet(self, balance_sheet_facts: List[Dict[str, Any]]) -> None:
        """
        Validate and complete the balance sheet facts.

        Args:
            balance_sheet_facts: List of balance sheet facts
        """
        # Group facts by context reference
        facts_by_context = defaultdict(list)
        for fact in balance_sheet_facts:
            context_ref = fact.get('context_ref', '')
            facts_by_context[context_ref].append(fact)

        # Process each context separately
        for context_ref, facts in facts_by_context.items():
            # Extract key balance sheet components
            assets = None
            liabilities = None
            equity = None
            minority_interests = None
            total_liabilities_and_equity = None
            context_label = ""

            # Find the key components
            for fact in facts:
                concept = fact.get('concept', '')
                value_str = fact.get('value', '0').replace('$', '').replace(',', '')

                # Get context label if available
                if not context_label and 'context_label' in fact:
                    context_label = fact.get('context_label', '')

                try:
                    value = float(value_str)
                except ValueError:
                    continue

                # Check for assets
                if 'us-gaap:Assets' in concept:
                    assets = value

                # Check for liabilities
                elif 'us-gaap:Liabilities' in concept:
                    liabilities = value

                # Check for equity
                elif 'us-gaap:StockholdersEquity' in concept:
                    equity = value

                # Check for minority interests
                elif 'us-gaap:MinorityInterest' in concept or 'us-gaap:RedeemableNoncontrollingInterest' in concept:
                    minority_interests = value

                # Check for total liabilities and equity
                elif 'us-gaap:LiabilitiesAndStockholdersEquity' in concept:
                    total_liabilities_and_equity = value

            # If we have assets and total liabilities and equity, but missing components, try to calculate them
            if assets is not None and total_liabilities_and_equity is not None:
                # If assets and total liabilities and equity don't match, log a warning
                if abs(assets - total_liabilities_and_equity) > 0.01 * max(assets, total_liabilities_and_equity):
                    self.logger.warning(f"Balance sheet doesn't balance for context {context_ref}: "
                                       f"Assets ({assets}) != Total Liabilities and Equity ({total_liabilities_and_equity})")

                # If we're missing liabilities but have equity, calculate liabilities
                if liabilities is None and equity is not None:
                    minority_interests = minority_interests or 0
                    liabilities = total_liabilities_and_equity - equity - minority_interests
                    facts.append({
                        'concept': 'us-gaap:Liabilities',
                        'value': f"${liabilities:,.0f}",
                        'context_ref': context_ref,
                        'unit': 'USD'
                    })
                    self.logger.info(f"Added calculated liabilities ({liabilities}) for context {context_ref}")

                # If we're missing equity but have liabilities, calculate equity
                elif equity is None and liabilities is not None:
                    minority_interests = minority_interests or 0
                    equity = total_liabilities_and_equity - liabilities - minority_interests
                    facts.append({
                        'concept': 'us-gaap:StockholdersEquity',
                        'value': f"${equity:,.0f}",
                        'context_ref': context_ref,
                        'unit': 'USD'
                    })
                    self.logger.info(f"Added calculated equity ({equity}) for context {context_ref}")

            # Validate the balance sheet
            if assets is not None and liabilities is not None and equity is not None:
                minority_interests = minority_interests or 0
                is_valid, error_message = self.validator.validate_balance_sheet(
                    assets, liabilities, equity, minority_interests
                )

                if not is_valid:
                    self.logger.warning(f"Balance sheet validation failed for context {context_ref}: {error_message}")

            # Ensure all balance sheet components are included in the normalized format
            # This is critical for ensuring the balance sheet is complete in the output file
            normalized_data_added = False

            # Check if we have all the necessary components
            if assets is not None:
                # Find existing normalized data entries for this context
                normalized_entries = [f for f in balance_sheet_facts if f.get('statement_type') == 'Balance Sheet'
                                    and f.get('context_ref') == context_ref]

                # Check if we need to add normalized entries
                assets_entry = next((f for f in normalized_entries if f.get('concept') == 'Assets'), None)
                liabilities_entry = next((f for f in normalized_entries if f.get('concept') == 'Liabilities'), None)
                equity_entry = next((f for f in normalized_entries if f.get('concept') == 'Stockholders Equity'), None)
                minority_entry = next((f for f in normalized_entries if f.get('concept') == 'Minority Interest'), None)
                total_entry = next((f for f in normalized_entries if f.get('concept') == 'Liabilities And Stockholders Equity'), None)

                # If any component is missing, add all of them to ensure consistency
                if not assets_entry or not liabilities_entry or not equity_entry or not total_entry:
                    # Remove any existing entries to avoid duplication
                    balance_sheet_facts[:] = [f for f in balance_sheet_facts if not (f.get('statement_type') == 'Balance Sheet'
                                                                                and f.get('context_ref') == context_ref
                                                                                and f.get('concept') in ['Assets', 'Liabilities',
                                                                                                        'Stockholders Equity',
                                                                                                        'Minority Interest',
                                                                                                        'Liabilities And Stockholders Equity'])]

                    # Add assets
                    balance_sheet_facts.append({
                        'statement_type': 'Balance Sheet',
                        'concept': 'Assets',
                        'value': f"${assets:,.0f}",
                        'context_ref': context_ref,
                        'context_label': context_label,
                        'unit': 'USD'
                    })

                    # Add liabilities if available
                    if liabilities is not None:
                        balance_sheet_facts.append({
                            'statement_type': 'Balance Sheet',
                            'concept': 'Liabilities',
                            'value': f"${liabilities:,.0f}",
                            'context_ref': context_ref,
                            'context_label': context_label,
                            'unit': 'USD'
                        })

                    # Add equity if available
                    if equity is not None:
                        balance_sheet_facts.append({
                            'statement_type': 'Balance Sheet',
                            'concept': 'Stockholders Equity',
                            'value': f"${equity:,.0f}",
                            'context_ref': context_ref,
                            'context_label': context_label,
                            'unit': 'USD'
                        })

                    # Add minority interest if available
                    if minority_interests is not None and minority_interests > 0:
                        balance_sheet_facts.append({
                            'statement_type': 'Balance Sheet',
                            'concept': 'Minority Interest',
                            'value': f"${minority_interests:,.0f}",
                            'context_ref': context_ref,
                            'context_label': context_label,
                            'unit': 'USD'
                        })

                    # Add total liabilities and equity if available
                    if total_liabilities_and_equity is not None:
                        balance_sheet_facts.append({
                            'statement_type': 'Balance Sheet',
                            'concept': 'Liabilities And Stockholders Equity',
                            'value': f"${total_liabilities_and_equity:,.0f}",
                            'context_ref': context_ref,
                            'context_label': context_label,
                            'unit': 'USD'
                        })

                    normalized_data_added = True
                    self.logger.info(f"Added normalized balance sheet data for context {context_ref}")

            if normalized_data_added:
                self.logger.info(f"Completed balance sheet for context {context_ref}")

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
