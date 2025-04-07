#!/usr/bin/env python3
"""
XBRL Mapper

This module extracts hierarchical relationships from XBRL linkbase files
and creates an LLM-friendly output format.
"""

import os
import sys
import json
import logging
import re
from pathlib import Path
from lxml import etree
from collections import defaultdict

class XBRLMapper:
    """
    XBRL Mapper for extracting hierarchical relationships from XBRL linkbase files
    and creating an LLM-friendly output format.
    """
    
    def __init__(self):
        """Initialize the XBRL mapper."""
        self.presentation_links = []
        self.calculation_links = []
        self.definition_links = []
        self.concepts = {}
        self.labels = {}
        
    def extract_mappings_from_linkbase(self, linkbase_path):
        """
        Extract mappings from an XBRL linkbase file.
        
        Args:
            linkbase_path: Path to the linkbase file
            
        Returns:
            Dictionary with extracted mappings
        """
        logging.info(f"Extracting mappings from {linkbase_path}")
        
        try:
            # Load the XBRL file
            tree = etree.parse(linkbase_path)
            ns = {
                "link": "http://www.xbrl.org/2003/linkbase",
                "xlink": "http://www.w3.org/1999/xlink"
            }
            
            # Build a locator map (label -> concept href)
            locators = {}
            for loc in tree.findall(".//link:loc", namespaces=ns):
                label = loc.get("{http://www.w3.org/1999/xlink}label")
                href = loc.get("{http://www.w3.org/1999/xlink}href")
                if label and href:
                    locators[label] = href
                    
            logging.info(f"Found {len(locators)} locators")
            
            # Extract presentation relationships
            presentation_mappings = []
            seen_pairs = set()  # To avoid loops or duplicates
            
            for arc in tree.findall(".//link:presentationArc", namespaces=ns):
                from_label = arc.get("{http://www.w3.org/1999/xlink}from")
                to_label = arc.get("{http://www.w3.org/1999/xlink}to")
                order = arc.get("order")
                
                # Find the parent link to get the role
                parent = arc.getparent()
                role = parent.get("{http://www.w3.org/1999/xlink}role") if parent is not None else None
                
                pair = (from_label, to_label)
                
                if pair not in seen_pairs and from_label in locators and to_label in locators:
                    # Extract concept names from hrefs
                    from_href = locators[from_label]
                    to_href = locators[to_label]
                    
                    # Extract concept names (remove namespace and fragment identifier)
                    from_concept = from_href.split('#')[-1]
                    to_concept = to_href.split('#')[-1]
                    
                    presentation_mappings.append({
                        "parent": from_concept,
                        "child": to_concept,
                        "role": role,
                        "order": order
                    })
                    seen_pairs.add(pair)
            
            logging.info(f"Extracted {len(presentation_mappings)} presentation relationships")
            
            # Extract calculation relationships
            calculation_mappings = []
            seen_pairs = set()  # Reset for calculation relationships
            
            for arc in tree.findall(".//link:calculationArc", namespaces=ns):
                from_label = arc.get("{http://www.w3.org/1999/xlink}from")
                to_label = arc.get("{http://www.w3.org/1999/xlink}to")
                weight = arc.get("weight")
                
                # Find the parent link to get the role
                parent = arc.getparent()
                role = parent.get("{http://www.w3.org/1999/xlink}role") if parent is not None else None
                
                pair = (from_label, to_label)
                
                if pair not in seen_pairs and from_label in locators and to_label in locators:
                    # Extract concept names from hrefs
                    from_href = locators[from_label]
                    to_href = locators[to_label]
                    
                    # Extract concept names (remove namespace and fragment identifier)
                    from_concept = from_href.split('#')[-1]
                    to_concept = to_href.split('#')[-1]
                    
                    calculation_mappings.append({
                        "parent": from_concept,
                        "child": to_concept,
                        "role": role,
                        "weight": weight
                    })
                    seen_pairs.add(pair)
            
            logging.info(f"Extracted {len(calculation_mappings)} calculation relationships")
            
            # Combine results
            result = {
                "presentation_mappings": presentation_mappings,
                "calculation_mappings": calculation_mappings
            }
            
            return result
        
        except Exception as e:
            logging.error(f"Error extracting mappings: {str(e)}")
            return {"presentation_mappings": [], "calculation_mappings": []}
    
    def extract_mappings_from_schema(self, schema_path):
        """
        Extract mappings from embedded linkbases in an XBRL schema file.
        
        Args:
            schema_path: Path to the schema file
            
        Returns:
            Dictionary with extracted mappings
        """
        logging.info(f"Extracting embedded mappings from {schema_path}")
        
        try:
            # Load the XBRL schema file
            tree = etree.parse(schema_path)
            ns = {
                "link": "http://www.xbrl.org/2003/linkbase",
                "xlink": "http://www.w3.org/1999/xlink"
            }
            
            # Find embedded linkbases
            linkbases = tree.findall(".//link:linkbase", namespaces=ns)
            logging.info(f"Found {len(linkbases)} embedded linkbases")
            
            presentation_mappings = []
            calculation_mappings = []
            
            for linkbase in linkbases:
                # Build a locator map for this linkbase
                locators = {}
                for loc in linkbase.findall(".//link:loc", namespaces=ns):
                    label = loc.get("{http://www.w3.org/1999/xlink}label")
                    href = loc.get("{http://www.w3.org/1999/xlink}href")
                    if label and href:
                        locators[label] = href
                
                # Process presentation links
                seen_pairs = set()
                for arc in linkbase.findall(".//link:presentationArc", namespaces=ns):
                    from_label = arc.get("{http://www.w3.org/1999/xlink}from")
                    to_label = arc.get("{http://www.w3.org/1999/xlink}to")
                    order = arc.get("order")
                    
                    # Find the parent link to get the role
                    parent = arc.getparent()
                    role = parent.get("{http://www.w3.org/1999/xlink}role") if parent is not None else None
                    
                    pair = (from_label, to_label)
                    
                    if pair not in seen_pairs and from_label in locators and to_label in locators:
                        # Extract concept names from hrefs
                        from_href = locators[from_label]
                        to_href = locators[to_label]
                        
                        # Extract concept names (remove namespace and fragment identifier)
                        from_concept = from_href.split('#')[-1]
                        to_concept = to_href.split('#')[-1]
                        
                        presentation_mappings.append({
                            "parent": from_concept,
                            "child": to_concept,
                            "role": role,
                            "order": order
                        })
                        seen_pairs.add(pair)
                
                # Process calculation links
                seen_pairs = set()
                for arc in linkbase.findall(".//link:calculationArc", namespaces=ns):
                    from_label = arc.get("{http://www.w3.org/1999/xlink}from")
                    to_label = arc.get("{http://www.w3.org/1999/xlink}to")
                    weight = arc.get("weight")
                    
                    # Find the parent link to get the role
                    parent = arc.getparent()
                    role = parent.get("{http://www.w3.org/1999/xlink}role") if parent is not None else None
                    
                    pair = (from_label, to_label)
                    
                    if pair not in seen_pairs and from_label in locators and to_label in locators:
                        # Extract concept names from hrefs
                        from_href = locators[from_label]
                        to_href = locators[to_label]
                        
                        # Extract concept names (remove namespace and fragment identifier)
                        from_concept = from_href.split('#')[-1]
                        to_concept = to_href.split('#')[-1]
                        
                        calculation_mappings.append({
                            "parent": from_concept,
                            "child": to_concept,
                            "role": role,
                            "weight": weight
                        })
                        seen_pairs.add(pair)
            
            logging.info(f"Extracted {len(presentation_mappings)} presentation relationships from embedded linkbases")
            logging.info(f"Extracted {len(calculation_mappings)} calculation relationships from embedded linkbases")
            
            # Combine results
            result = {
                "presentation_mappings": presentation_mappings,
                "calculation_mappings": calculation_mappings
            }
            
            return result
        
        except Exception as e:
            logging.error(f"Error extracting embedded mappings: {str(e)}")
            return {"presentation_mappings": [], "calculation_mappings": []}
    
    def create_llm_friendly_output(self, mapping_file, facts_file, output_file):
        """
        Create an LLM-friendly output from XBRL mappings and facts.
        
        Args:
            mapping_file: Path to the mapping file (JSON)
            facts_file: Path to the facts file (JSON)
            output_file: Path to the output file (TXT)
        """
        logging.info(f"Creating LLM-friendly output from {mapping_file} and {facts_file}")
        
        # Load mappings
        with open(mapping_file, 'r', encoding='utf-8') as f:
            mappings = json.load(f)
        
        # Load facts
        facts = []
        try:
            with open(facts_file, 'r', encoding='utf-8') as f:
                facts_data = json.load(f)
                
                # Check if it's a list of facts or a dictionary with a 'facts' key
                if isinstance(facts_data, list):
                    facts = facts_data
                elif isinstance(facts_data, dict) and 'facts' in facts_data:
                    facts = facts_data['facts']
                else:
                    logging.warning(f"Unexpected facts format in {facts_file}")
        except Exception as e:
            logging.error(f"Error loading facts: {str(e)}")
        
        # Group facts by context
        facts_by_context = defaultdict(list)
        for fact in facts:
            context = fact.get('contextRef', '')
            facts_by_context[context].append(fact)
        
        # Extract presentation and calculation mappings
        presentation_mappings = mappings.get('presentation_mappings', [])
        calculation_mappings = mappings.get('calculation_mappings', [])
        
        # Group presentation mappings by role (financial statement)
        mappings_by_role = defaultdict(list)
        for mapping in presentation_mappings:
            role = mapping.get('role', '')
            mappings_by_role[role].append(mapping)
        
        # Identify financial statement types
        financial_statements = self._identify_financial_statements(mappings_by_role)
        
        # Create the LLM-friendly output text
        output_text = self._create_output_text(financial_statements, mappings_by_role, calculation_mappings, facts_by_context)
        
        # Save output
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(output_text)
        
        logging.info(f"LLM-friendly output saved to {output_file}")
        
        return output_text
    
    def _identify_financial_statements(self, mappings_by_role):
        """
        Identify financial statement types from role URIs.
        
        Args:
            mappings_by_role: Dictionary of mappings grouped by role
            
        Returns:
            Dictionary mapping statement types to lists of roles
        """
        financial_statements = {
            "BALANCE_SHEET": [],
            "INCOME_STATEMENT": [],
            "CASH_FLOW_STATEMENT": [],
            "STATEMENT_OF_EQUITY": [],
            "OTHER": []
        }
        
        for role in mappings_by_role.keys():
            role_lower = role.lower()
            
            if any(term in role_lower for term in ["balance", "financial position"]):
                financial_statements["BALANCE_SHEET"].append(role)
            elif any(term in role_lower for term in ["income", "operations", "earnings", "profit", "loss"]):
                financial_statements["INCOME_STATEMENT"].append(role)
            elif any(term in role_lower for term in ["cash flow", "cashflow"]):
                financial_statements["CASH_FLOW_STATEMENT"].append(role)
            elif any(term in role_lower for term in ["equity", "stockholder", "shareholder"]):
                financial_statements["STATEMENT_OF_EQUITY"].append(role)
            else:
                financial_statements["OTHER"].append(role)
        
        # Remove empty statement types
        return {k: v for k, v in financial_statements.items() if v}
    
    def _create_output_text(self, financial_statements, mappings_by_role, calculation_mappings, facts_by_context):
        """
        Create the LLM-friendly output text.
        
        Args:
            financial_statements: Dictionary mapping statement types to lists of roles
            mappings_by_role: Dictionary of mappings grouped by role
            calculation_mappings: List of calculation mappings
            facts_by_context: Dictionary of facts grouped by context
            
        Returns:
            String containing the LLM-friendly output text
        """
        output = []
        
        # Add document header
        output.append("@DOCUMENT_TYPE: XBRL_LLM_FRIENDLY")
        output.append("@VERSION: 1.0")
        output.append("")
        
        # Add document structure
        output.append("@STRUCTURE")
        output.append("FINANCIAL_STATEMENTS:")
        for statement_type in financial_statements.keys():
            output.append(f"  - {statement_type}")
        output.append("SECTIONS:")
        output.append("  - HIERARCHICAL_STRUCTURE")
        output.append("  - CALCULATION_RELATIONSHIPS")
        output.append("  - FACTS")
        output.append("")
        
        # Add hierarchical structure for each financial statement
        output.append("@HIERARCHICAL_STRUCTURE")
        for statement_type, roles in financial_statements.items():
            output.append(f"{statement_type}:")
            
            for role in roles:
                # Extract role name from URI
                role_name = role.split('/')[-1] if '/' in role else role
                output.append(f"  ROLE: {role_name}")
                
                # Build parent-child relationships
                parent_to_children = defaultdict(list)
                for mapping in mappings_by_role[role]:
                    parent = mapping.get('parent', '')
                    child = mapping.get('child', '')
                    order = mapping.get('order', '0')
                    
                    if parent and child:
                        parent_to_children[parent].append({
                            "concept": child,
                            "order": order
                        })
                
                # Find root concepts (those that are parents but not children)
                all_parents = set(parent_to_children.keys())
                all_children = set()
                for children in parent_to_children.values():
                    all_children.update(child['concept'] for child in children)
                
                root_concepts = all_parents - all_children
                
                # Output the hierarchy
                for root in sorted(root_concepts):
                    self._output_hierarchy(root, parent_to_children, output, indent=2)
                
                output.append("")
        
        # Add calculation relationships for each financial statement
        output.append("@CALCULATION_RELATIONSHIPS")
        
        # Group calculation mappings by role
        calc_by_role = defaultdict(list)
        for mapping in calculation_mappings:
            role = mapping.get('role', '')
            calc_by_role[role].append(mapping)
        
        for statement_type, roles in financial_statements.items():
            output.append(f"{statement_type}:")
            
            for role in roles:
                # Extract role name from URI
                role_name = role.split('/')[-1] if '/' in role else role
                output.append(f"  ROLE: {role_name}")
                
                # Output calculation relationships
                for mapping in calc_by_role[role]:
                    parent = mapping.get('parent', '')
                    child = mapping.get('child', '')
                    weight = mapping.get('weight', '1')
                    
                    if parent and child:
                        output.append(f"    {parent} = {child} * {weight}")
                
                output.append("")
        
        # Find contexts for each financial statement type based on their content
        # This approach works across different context ID formats (NVDA, AAPL, MSFT, TSLA)
        contexts_by_fact_count = sorted([(ctx, len(facts)) for ctx, facts in facts_by_context.items()], 
                                       key=lambda x: x[1], reverse=True)
        
        # Find contexts for each financial statement type based on their content
        balance_sheet_contexts = []
        income_stmt_contexts = []
        cash_flow_contexts = []
        
        # Balance sheet indicator concepts
        balance_sheet_indicators = ['Assets', 'Liabilities', 'Equity', 'StockholdersEquity']
        
        # Income statement indicator concepts
        income_stmt_indicators = ['Revenue', 'Income', 'Expense', 'Earnings', 'EarningsPerShare', 'NetIncomeLoss']
        
        # Cash flow statement indicator concepts
        cash_flow_indicators = ['CashFlow', 'NetCashProvidedByUsedIn', 'CashAndCashEquivalentsPeriodIncreaseDecrease']
        
        # Examine each context with a significant number of facts
        for ctx, count in contexts_by_fact_count:
            if count < 10:  # Skip contexts with very few facts
                continue
                
            # Check for balance sheet facts
            has_balance_sheet_facts = False
            for fact in facts_by_context[ctx]:
                name = fact.get('name', '')
                if any(indicator in name for indicator in balance_sheet_indicators):
                    has_balance_sheet_facts = True
                    break
            if has_balance_sheet_facts:
                balance_sheet_contexts.append(ctx)
            
            # Check for income statement facts
            has_income_stmt_facts = False
            for fact in facts_by_context[ctx]:
                name = fact.get('name', '')
                if any(indicator in name for indicator in income_stmt_indicators):
                    has_income_stmt_facts = True
                    break
            if has_income_stmt_facts:
                income_stmt_contexts.append(ctx)
            
            # Check for cash flow statement facts
            has_cash_flow_facts = False
            for fact in facts_by_context[ctx]:
                name = fact.get('name', '')
                if any(indicator in name for indicator in cash_flow_indicators):
                    has_cash_flow_facts = True
                    break
            if has_cash_flow_facts:
                cash_flow_contexts.append(ctx)
        
        # Add facts section
        output.append("@FACTS")
        
        # Process balance sheet contexts
        if balance_sheet_contexts:
            # Sort by date (newest first)
            balance_sheet_contexts.sort(reverse=True)
            
            # Take the most recent context
            context = balance_sheet_contexts[0]
            facts = facts_by_context[context]
            
            output.append("BALANCE_SHEET_FACTS:")
            # Format context display based on format
            if '_I' in context:
                # NVDA-style context with date
                output.append(f"  CONTEXT: {context} (As of {context[-8:] if len(context) > 8 else context})")
            else:
                # AAPL/MSFT/TSLA-style context
                output.append(f"  CONTEXT: {context}")
            
            # Find balance sheet concepts
            balance_sheet_concepts = set()
            for role in financial_statements.get('BALANCE_SHEET', []):
                for mapping in mappings_by_role[role]:
                    balance_sheet_concepts.add(mapping.get('parent', ''))
                    balance_sheet_concepts.add(mapping.get('child', ''))
            
            # Skip if no facts found
            if not balance_sheet_facts:
                output.append("  No balance sheet facts found for this context")
            
            # Find facts for these concepts
            balance_sheet_facts = []
            for fact in facts:
                name = fact.get('name', '')
                # Try with and without namespace prefix
                name_without_prefix = name.split(':')[-1] if ':' in name else name
                
                # Check if the concept is in the balance sheet concepts
                in_concepts = False
                if name in balance_sheet_concepts:
                    in_concepts = True
                elif name_without_prefix in balance_sheet_concepts:
                    in_concepts = True
                elif f"us-gaap_{name_without_prefix}" in balance_sheet_concepts:
                    in_concepts = True
                
                if in_concepts:
                    balance_sheet_facts.append(fact)
            
            # Output facts in a normalized format
            for fact in sorted(balance_sheet_facts, key=lambda f: f.get('name', '')):
                name = fact.get('name', '')
                value = fact.get('value', '')
                unit = fact.get('unitRef', '')
                
                # Skip empty values
                if not value.strip():
                    continue
                    
                output.append(f"    {name} | {value} | {unit}")
            
            output.append("")
        
        # Process income statement contexts
        if income_stmt_contexts:
            # Sort by date (newest first)
            income_stmt_contexts.sort(reverse=True)
            
            # Take the most recent context
            context = income_stmt_contexts[0]
            facts = facts_by_context[context]
            
            output.append("INCOME_STATEMENT_FACTS:")
            # Format context display based on format
            if '_D' in context or '-' in context:
                # NVDA-style context with date range
                output.append(f"  CONTEXT: {context} (Period {context[-17:] if len(context) > 17 else context})")
            else:
                # AAPL/MSFT/TSLA-style context
                output.append(f"  CONTEXT: {context}")
            
            # Find income statement concepts
            income_stmt_concepts = set()
            for role in financial_statements.get('INCOME_STATEMENT', []):
                for mapping in mappings_by_role[role]:
                    income_stmt_concepts.add(mapping.get('parent', ''))
                    income_stmt_concepts.add(mapping.get('child', ''))
            
            # Find facts for these concepts
            income_stmt_facts = []
            for fact in facts:
                name = fact.get('name', '')
                # Try with and without namespace prefix
                name_without_prefix = name.split(':')[-1] if ':' in name else name
                
                # Check if the concept is in the income statement concepts
                in_concepts = False
                if name in income_stmt_concepts:
                    in_concepts = True
                elif name_without_prefix in income_stmt_concepts:
                    in_concepts = True
                elif f"us-gaap_{name_without_prefix}" in income_stmt_concepts:
                    in_concepts = True
                
                if in_concepts:
                    income_stmt_facts.append(fact)
            
            # Output facts in a normalized format
            for fact in sorted(income_stmt_facts, key=lambda f: f.get('name', '')):
                name = fact.get('name', '')
                value = fact.get('value', '')
                unit = fact.get('unitRef', '')
                
                # Skip empty values
                if not value.strip():
                    continue
                    
                output.append(f"    {name} | {value} | {unit}")
            
            output.append("")
        
        # Process cash flow statement contexts
        if cash_flow_contexts:
            # Sort by date (newest first)
            cash_flow_contexts.sort(reverse=True)
            
            # Take the most recent context
            context = cash_flow_contexts[0]
            facts = facts_by_context[context]
            
            output.append("CASH_FLOW_STATEMENT_FACTS:")
            # Format context display based on format
            if '_D' in context or '-' in context:
                # NVDA-style context with date range
                output.append(f"  CONTEXT: {context} (Period {context[-17:] if len(context) > 17 else context})")
            else:
                # AAPL/MSFT/TSLA-style context
                output.append(f"  CONTEXT: {context}")
            
            # Find cash flow statement concepts
            cash_flow_concepts = set()
            for role in financial_statements.get('CASH_FLOW_STATEMENT', []):
                for mapping in mappings_by_role[role]:
                    cash_flow_concepts.add(mapping.get('parent', ''))
                    cash_flow_concepts.add(mapping.get('child', ''))
            
            # Find facts for these concepts
            cash_flow_facts = []
            for fact in facts:
                name = fact.get('name', '')
                # Try with and without namespace prefix
                name_without_prefix = name.split(':')[-1] if ':' in name else name
                
                # Check if the concept is in the cash flow concepts
                in_concepts = False
                if name in cash_flow_concepts:
                    in_concepts = True
                elif name_without_prefix in cash_flow_concepts:
                    in_concepts = True
                elif f"us-gaap_{name_without_prefix}" in cash_flow_concepts:
                    in_concepts = True
                
                if in_concepts:
                    cash_flow_facts.append(fact)
            
            # Output facts in a normalized format
            for fact in sorted(cash_flow_facts, key=lambda f: f.get('name', '')):
                name = fact.get('name', '')
                value = fact.get('value', '')
                unit = fact.get('unitRef', '')
                
                # Skip empty values
                if not value.strip():
                    continue
                    
                output.append(f"    {name} | {value} | {unit}")
        
        return "\n".join(output)
    
    def _output_hierarchy(self, concept, parent_to_children, output, indent=0):
        """
        Output the hierarchy for a concept recursively.
        
        Args:
            concept: The concept to output
            parent_to_children: Dictionary mapping parents to children
            output: List to append output lines to
            indent: Current indentation level
        """
        # Output the concept
        output.append(f"{' ' * indent}{concept}")
        
        # Sort children by order
        children = sorted(parent_to_children.get(concept, []), key=lambda c: float(c.get('order', '0') or '0'))
        
        # Output children recursively
        for child in children:
            child_concept = child['concept']
            self._output_hierarchy(child_concept, parent_to_children, output, indent + 2)

# Create a singleton instance
xbrl_mapper = XBRLMapper()
