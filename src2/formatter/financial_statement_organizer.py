#!/usr/bin/env python3
"""
Financial Statement Organizer

This module organizes financial facts into coherent financial statements
(balance sheet, income statement, cash flow statement, and statement of equity).
"""

import re
import logging
from typing import Dict, List, Any, Optional, Set, Tuple

class FinancialStatementOrganizer:
    """
    Organizes financial facts into coherent financial statements.
    """

    # Statement type definitions
    BALANCE_SHEET = "BALANCE_SHEET"
    INCOME_STATEMENT = "INCOME_STATEMENT"
    CASH_FLOW_STATEMENT = "CASH_FLOW_STATEMENT"
    EQUITY_STATEMENT = "EQUITY_STATEMENT"
    OTHER = "OTHER_FINANCIAL"

    # Concept patterns for identifying statement types
    STATEMENT_PATTERNS = {
        BALANCE_SHEET: [
            r'asset', r'liability', r'equity', r'debt', r'cash and', r'inventory',
            r'payable', r'receivable', r'goodwill', r'intangible', r'property',
            r'equipment', r'investment', r'deposit', r'loan', r'lease', r'accrued',
            r'deferred', r'prepaid', r'accumulated', r'treasury', r'stock', r'share',
            r'capital', r'retained earnings', r'deficit', r'surplus'
        ],
        INCOME_STATEMENT: [
            r'revenue', r'sales', r'income', r'earnings', r'eps', r'expense', r'cost',
            r'profit', r'loss', r'margin', r'tax', r'interest', r'depreciation',
            r'amortization', r'ebitda', r'operating', r'gross', r'net', r'dividend',
            r'per share', r'diluted', r'basic', r'continuing', r'discontinued'
        ],
        CASH_FLOW_STATEMENT: [
            r'cash flow', r'cashflow', r'financing', r'investing', r'operating',
            r'proceeds', r'payment', r'acquisition', r'disposal', r'purchase',
            r'sale of', r'dividend paid', r'repayment', r'borrowing', r'issuance',
            r'repurchase', r'effect of exchange', r'net increase', r'net decrease'
        ],
        EQUITY_STATEMENT: [
            r'equity', r'stockholder', r'shareholder', r'comprehensive income',
            r'accumulated other', r'retained earnings', r'treasury stock',
            r'common stock', r'preferred stock', r'additional paid', r'capital',
            r'contributed', r'noncontrolling', r'minority interest', r'deficit',
            r'surplus', r'reserve', r'share-based', r'stock-based', r'compensation'
        ]
    }

    # Concept keywords for identifying statement sections
    SECTION_KEYWORDS = {
        BALANCE_SHEET: {
            "ASSETS": [
                r'asset', r'cash', r'investment', r'receivable', r'inventory',
                r'prepaid', r'property', r'equipment', r'goodwill', r'intangible'
            ],
            "LIABILITIES": [
                r'liability', r'payable', r'debt', r'loan', r'lease', r'accrued',
                r'deferred', r'tax liability', r'provision', r'obligation'
            ],
            "EQUITY": [
                r'equity', r'stock', r'capital', r'retained earnings', r'treasury',
                r'accumulated other', r'noncontrolling', r'minority interest'
            ]
        },
        INCOME_STATEMENT: {
            "REVENUE": [
                r'revenue', r'sales', r'income from', r'fee', r'subscription'
            ],
            "EXPENSES": [
                r'expense', r'cost', r'research', r'development', r'selling',
                r'marketing', r'general', r'administrative', r'depreciation',
                r'amortization', r'impairment', r'restructuring'
            ],
            "INCOME": [
                r'income', r'earnings', r'profit', r'loss', r'ebitda', r'ebit',
                r'margin', r'per share', r'eps', r'diluted', r'basic'
            ]
        },
        CASH_FLOW_STATEMENT: {
            "OPERATING": [
                r'operating', r'operations', r'net income', r'depreciation',
                r'amortization', r'working capital', r'receivable', r'payable',
                r'inventory', r'accrued'
            ],
            "INVESTING": [
                r'investing', r'investment', r'purchase', r'acquisition',
                r'disposal', r'sale of', r'capital expenditure', r'property',
                r'equipment', r'intangible'
            ],
            "FINANCING": [
                r'financing', r'dividend', r'repurchase', r'issuance', r'repayment',
                r'borrowing', r'debt', r'stock', r'equity', r'loan'
            ]
        },
        EQUITY_STATEMENT: {
            "COMMON_STOCK": [
                r'common stock', r'share', r'issued', r'outstanding'
            ],
            "RETAINED_EARNINGS": [
                r'retained earnings', r'accumulated deficit', r'accumulated surplus'
            ],
            "OTHER_COMPREHENSIVE_INCOME": [
                r'comprehensive income', r'accumulated other', r'foreign currency',
                r'translation', r'unrealized', r'gain', r'loss'
            ],
            "TREASURY_STOCK": [
                r'treasury stock', r'repurchase', r'retirement'
            ]
        }
    }

    def __init__(self):
        """Initialize the financial statement organizer."""
        self.facts_by_statement = {
            self.BALANCE_SHEET: [],
            self.INCOME_STATEMENT: [],
            self.CASH_FLOW_STATEMENT: [],
            self.EQUITY_STATEMENT: [],
            self.OTHER: []
        }
        self.facts_by_section = {}
        self.statement_contexts = {}

    def organize_facts(self, facts: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Organize financial facts into statement types.

        Args:
            facts: List of financial facts from XBRL data

        Returns:
            Dictionary of facts organized by statement type
        """
        # Reset the organizer
        self.__init__()

        # First pass: Categorize facts by statement type
        for fact in facts:
            statement_type = self._determine_statement_type(fact)
            self.facts_by_statement[statement_type].append(fact)

        # Second pass: Identify contexts for each statement type
        self._identify_statement_contexts()

        # Third pass: Refine categorization based on contexts
        self._refine_categorization()

        # Fourth pass: Organize facts by section within each statement
        self._organize_by_section()

        return self.facts_by_statement

    def _determine_statement_type(self, fact: Dict[str, Any]) -> str:
        """
        Determine the statement type for a fact based on its concept name.

        Args:
            fact: A financial fact from XBRL data

        Returns:
            Statement type (BALANCE_SHEET, INCOME_STATEMENT, etc.)
        """
        concept = fact.get("concept", "").lower()

        # Check for statement type indicators in the concept name
        for statement_type, patterns in self.STATEMENT_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, concept):
                    return statement_type

        # If no match, check for common statement concepts
        if any(term in concept for term in ['asset', 'liability', 'equity']):
            return self.BALANCE_SHEET
        elif any(term in concept for term in ['revenue', 'expense', 'income']):
            return self.INCOME_STATEMENT
        elif any(term in concept for term in ['cash', 'flow']):
            return self.CASH_FLOW_STATEMENT
        elif any(term in concept for term in ['stockholder', 'shareholder']):
            return self.EQUITY_STATEMENT

        # Default to OTHER
        return self.OTHER

    def _identify_statement_contexts(self):
        """
        Identify contexts that are primarily used for each statement type.
        This helps with categorizing facts that could belong to multiple statements.
        """
        # Count context usage by statement type
        context_counts = {
            self.BALANCE_SHEET: {},
            self.INCOME_STATEMENT: {},
            self.CASH_FLOW_STATEMENT: {},
            self.EQUITY_STATEMENT: {}
        }

        # Count how many times each context is used in each statement type
        for statement_type, facts in self.facts_by_statement.items():
            if statement_type == self.OTHER:
                continue

            for fact in facts:
                # Handle different context reference field names (context_ref or contextRef)
                context_ref = fact.get("context_ref", fact.get("contextRef", ""))
                if not context_ref:
                    continue

                if context_ref not in context_counts[statement_type]:
                    context_counts[statement_type][context_ref] = 0
                context_counts[statement_type][context_ref] += 1

        # Determine primary statement type for each context
        all_contexts = set()
        for counts in context_counts.values():
            all_contexts.update(counts.keys())

        for context_ref in all_contexts:
            max_count = 0
            primary_statement = None

            for statement_type, counts in context_counts.items():
                count = counts.get(context_ref, 0)
                if count > max_count:
                    max_count = count
                    primary_statement = statement_type

            if primary_statement:
                if primary_statement not in self.statement_contexts:
                    self.statement_contexts[primary_statement] = set()
                self.statement_contexts[primary_statement].add(context_ref)

    def _refine_categorization(self):
        """
        Refine the categorization of facts based on context usage.
        Move facts to the appropriate statement type if their context
        is primarily used in a different statement type.
        """
        # Create a new categorization
        new_categorization = {
            self.BALANCE_SHEET: [],
            self.INCOME_STATEMENT: [],
            self.CASH_FLOW_STATEMENT: [],
            self.EQUITY_STATEMENT: [],
            self.OTHER: []
        }

        # Process facts in the OTHER category
        for fact in self.facts_by_statement[self.OTHER]:
            # Handle different context reference field names (context_ref or contextRef)
            context_ref = fact.get("context_ref", fact.get("contextRef", ""))
            if not context_ref:
                new_categorization[self.OTHER].append(fact)
                continue

            # Check if the context is primarily used in a specific statement type
            assigned = False
            for statement_type, contexts in self.statement_contexts.items():
                if context_ref in contexts:
                    new_categorization[statement_type].append(fact)
                    assigned = True
                    break

            if not assigned:
                new_categorization[self.OTHER].append(fact)

        # Keep the original categorization for facts already assigned to a statement type
        for statement_type in [self.BALANCE_SHEET, self.INCOME_STATEMENT, self.CASH_FLOW_STATEMENT, self.EQUITY_STATEMENT]:
            new_categorization[statement_type].extend(self.facts_by_statement[statement_type])

        # Update the categorization
        self.facts_by_statement = new_categorization

    def _organize_by_section(self):
        """
        Organize facts by section within each statement type.
        """
        self.facts_by_section = {
            self.BALANCE_SHEET: {
                "ASSETS": [],
                "LIABILITIES": [],
                "EQUITY": [],
                "OTHER": []
            },
            self.INCOME_STATEMENT: {
                "REVENUE": [],
                "EXPENSES": [],
                "INCOME": [],
                "OTHER": []
            },
            self.CASH_FLOW_STATEMENT: {
                "OPERATING": [],
                "INVESTING": [],
                "FINANCING": [],
                "OTHER": []
            },
            self.EQUITY_STATEMENT: {
                "COMMON_STOCK": [],
                "RETAINED_EARNINGS": [],
                "OTHER_COMPREHENSIVE_INCOME": [],
                "TREASURY_STOCK": [],
                "OTHER": []
            },
            self.OTHER: {
                "OTHER": []
            }
        }

        # Organize facts by section
        for statement_type, facts in self.facts_by_statement.items():
            for fact in facts:
                section = self._determine_section(fact, statement_type)
                self.facts_by_section[statement_type][section].append(fact)

    def _determine_section(self, fact: Dict[str, Any], statement_type: str) -> str:
        """
        Determine the section within a statement type for a fact.

        Args:
            fact: A financial fact from XBRL data
            statement_type: The statement type (BALANCE_SHEET, INCOME_STATEMENT, etc.)

        Returns:
            Section name within the statement type
        """
        if statement_type == self.OTHER:
            return "OTHER"

        concept = fact.get("concept", "").lower()

        # Check for section indicators in the concept name
        for section, patterns in self.SECTION_KEYWORDS.get(statement_type, {}).items():
            for pattern in patterns:
                if re.search(pattern, concept):
                    return section

        # Default to OTHER
        return "OTHER"

    def get_statement_facts(self, statement_type: str) -> List[Dict[str, Any]]:
        """
        Get facts for a specific statement type.

        Args:
            statement_type: The statement type (BALANCE_SHEET, INCOME_STATEMENT, etc.)

        Returns:
            List of facts for the statement type
        """
        return self.facts_by_statement.get(statement_type, [])

    def get_section_facts(self, statement_type: str, section: str) -> List[Dict[str, Any]]:
        """
        Get facts for a specific section within a statement type.

        Args:
            statement_type: The statement type (BALANCE_SHEET, INCOME_STATEMENT, etc.)
            section: The section name within the statement type

        Returns:
            List of facts for the section
        """
        return self.facts_by_section.get(statement_type, {}).get(section, [])

    def get_statement_contexts(self, statement_type: str) -> Set[str]:
        """
        Get contexts primarily used for a specific statement type.

        Args:
            statement_type: The statement type (BALANCE_SHEET, INCOME_STATEMENT, etc.)

        Returns:
            Set of context references primarily used for the statement type
        """
        return self.statement_contexts.get(statement_type, set())

    def format_statement(self, statement_type: str, contexts: List[str], xbrl_data: Dict[str, Any]) -> List[str]:
        """
        Format a financial statement as a table.

        Args:
            statement_type: The statement type (BALANCE_SHEET, INCOME_STATEMENT, etc.)
            contexts: List of context references to include in the statement
            xbrl_data: The XBRL data containing contexts and facts

        Returns:
            List of strings representing the formatted statement
        """
        output = []

        # Add statement header
        statement_name = statement_type.replace("_", " ").title()
        output.append(f"@FINANCIAL_STATEMENT: {statement_name}")

        # Add statement metadata
        if statement_type == self.BALANCE_SHEET:
            output.append("@STATEMENT_TYPE: Balance_Sheet")
        elif statement_type == self.INCOME_STATEMENT:
            output.append("@STATEMENT_TYPE: Income_Statement")
        elif statement_type == self.CASH_FLOW_STATEMENT:
            output.append("@STATEMENT_TYPE: Cash_Flow_Statement")
        elif statement_type == self.EQUITY_STATEMENT:
            output.append("@STATEMENT_TYPE: Statement_Of_Equity")

        # Add context information
        context_labels = []
        for context_ref in contexts:
            if context_ref in xbrl_data.get("contexts", {}):
                context_info = xbrl_data["contexts"][context_ref]
                period_info = context_info.get("period", {})

                if "instant" in period_info:
                    date_str = period_info["instant"]
                    context_labels.append(f"{context_ref} (As of {date_str})")
                elif "startDate" in period_info and "endDate" in period_info:
                    start_date = period_info["startDate"]
                    end_date = period_info["endDate"]
                    context_labels.append(f"{context_ref} (Period from {start_date} to {end_date})")
                else:
                    context_labels.append(context_ref)
            else:
                context_labels.append(context_ref)

        output.append(f"@CONTEXTS: {', '.join(contexts)}")
        output.append(f"@CONTEXT_LABELS: {', '.join(context_labels)}")
        output.append("")

        # Add table header
        header = "Line Item"
        for context_ref in contexts:
            header += f" | {context_ref}"
        output.append(header)

        # Add separator
        separator = "-" * len("Line Item")
        for _ in contexts:
            separator += " | " + "-" * 10
        output.append(separator)

        # Add sections
        for section, section_facts in self.facts_by_section.get(statement_type, {}).items():
            if not section_facts:
                continue

            # Add section header
            section_name = section.replace("_", " ").title()
            output.append(f"@SECTION: {section_name}")

            # Group facts by concept
            facts_by_concept = {}
            for fact in section_facts:
                concept = fact.get("concept", "")
                if ":" in concept:
                    # Get readable concept name
                    concept_name = concept.split(":")[-1]
                    # Make it more readable
                    readable_name = re.sub(r'([a-z])([A-Z])', r'\1 \2', concept_name).title()
                else:
                    readable_name = concept

                if readable_name not in facts_by_concept:
                    facts_by_concept[readable_name] = {}

                # Handle different context reference field names (context_ref or contextRef)
                context_ref = fact.get("context_ref", fact.get("contextRef", ""))
                if context_ref and context_ref in contexts:
                    facts_by_concept[readable_name][context_ref] = fact

            # Add facts to the table
            for concept_name, context_facts in sorted(facts_by_concept.items()):
                row = concept_name
                for context_ref in contexts:
                    if context_ref in context_facts:
                        fact = context_facts[context_ref]
                        value = fact.get("value", "")
                        # Add currency symbol if available
                        unit_ref = fact.get("unit_ref", "")
                        if unit_ref and unit_ref.lower() == "usd" and not value.startswith("$"):
                            value = f"${value}"
                        row += f" | {value}"
                    else:
                        row += " | -"
                output.append(row)

            # Add a blank line after each section
            output.append("")

        return output


def organize_financial_statements(xbrl_data: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Organize financial facts into coherent financial statements.

    Args:
        xbrl_data: The XBRL data containing contexts and facts

    Returns:
        Dictionary of formatted financial statements
    """
    organizer = FinancialStatementOrganizer()

    # Handle different XBRL data formats
    facts = []
    contexts = {}

    if isinstance(xbrl_data, list):
        # XBRL data is a list of facts
        facts = xbrl_data

        # Extract contexts from facts
        context_refs = set()
        for fact in facts:
            context_ref = fact.get("contextRef", "")
            if context_ref:
                context_refs.add(context_ref)

        # Create a minimal context structure
        for context_ref in context_refs:
            contexts[context_ref] = {}
    else:
        # XBRL data is a dictionary with facts and contexts
        facts = xbrl_data.get("facts", [])
        contexts = xbrl_data.get("contexts", {})

    # Create a wrapper for the XBRL data
    xbrl_wrapper = {
        "facts": facts,
        "contexts": contexts
    }

    # Organize facts by statement type
    organizer.organize_facts(facts)

    # Format each statement type
    statements = {}

    # Balance Sheet
    balance_sheet_contexts = list(organizer.get_statement_contexts(FinancialStatementOrganizer.BALANCE_SHEET))
    if balance_sheet_contexts:
        statements["BALANCE_SHEET"] = organizer.format_statement(
            FinancialStatementOrganizer.BALANCE_SHEET,
            balance_sheet_contexts,
            xbrl_wrapper
        )

    # Income Statement
    income_statement_contexts = list(organizer.get_statement_contexts(FinancialStatementOrganizer.INCOME_STATEMENT))
    if income_statement_contexts:
        statements["INCOME_STATEMENT"] = organizer.format_statement(
            FinancialStatementOrganizer.INCOME_STATEMENT,
            income_statement_contexts,
            xbrl_wrapper
        )

    # Cash Flow Statement
    cash_flow_contexts = list(organizer.get_statement_contexts(FinancialStatementOrganizer.CASH_FLOW_STATEMENT))
    if cash_flow_contexts:
        statements["CASH_FLOW_STATEMENT"] = organizer.format_statement(
            FinancialStatementOrganizer.CASH_FLOW_STATEMENT,
            cash_flow_contexts,
            xbrl_wrapper
        )

    # Statement of Equity
    equity_contexts = list(organizer.get_statement_contexts(FinancialStatementOrganizer.EQUITY_STATEMENT))
    if equity_contexts:
        statements["EQUITY_STATEMENT"] = organizer.format_statement(
            FinancialStatementOrganizer.EQUITY_STATEMENT,
            equity_contexts,
            xbrl_wrapper
        )

    return statements
