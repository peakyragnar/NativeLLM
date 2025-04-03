#!/usr/bin/env python3
"""
LLM Format Optimizer

This module optimizes the LLM file format by:
1. Using more compact tags
2. Grouping facts by context
3. Consolidating context definitions
4. Optimizing whitespace
5. Using a more efficient representation for tables
"""

import re
import logging
from collections import defaultdict
from typing import Dict, List, Any, Tuple, Set

class LLMFormatOptimizer:
    """
    Optimizes the LLM file format for reduced size while maintaining semantic structure.
    """

    def __init__(self):
        """Initialize the LLM format optimizer."""

    def optimize(self, content: str) -> str:
        """
        Optimize the LLM file format.

        Args:
            content: The original LLM file content

        Returns:
            The optimized LLM file content
        """
        logging.info("Optimizing LLM file format")

        # Extract sections from the original file
        header_section, structure_section, context_section, financial_statements_section, financial_mapping_section, facts_section, narrative_section, rest_section = self._extract_sections(content)

        # Parse context definitions
        context_defs = self._parse_context_definitions(context_section)

        # Consolidate context definitions
        consolidated_contexts = self._consolidate_contexts(context_defs)

        # Parse facts
        facts = self._parse_facts(facts_section)

        # Group facts by context
        facts_by_context = self._group_facts_by_context(facts)

        # Generate minimal financial mapping if it doesn't exist
        if not financial_mapping_section and structure_section:
            financial_mapping_section = self._generate_minimal_financial_mapping(structure_section, facts)

        # Optimize narrative section if it exists
        if narrative_section:
            narrative_section = self._optimize_narrative_section(narrative_section)

        # Generate optimized content
        optimized_content = self._generate_optimized_content(
            header_section,
            structure_section,
            consolidated_contexts,
            facts_by_context,
            financial_mapping_section,
            narrative_section,
            rest_section
        )

        # Calculate size reduction
        original_size = len(content)
        optimized_size = len(optimized_content)
        reduction_percentage = (original_size - optimized_size) / original_size * 100

        logging.info(f"Size reduction: {reduction_percentage:.2f}%")
        logging.info(f"Original size: {original_size / 1024:.2f} KB")
        logging.info(f"Optimized size: {optimized_size / 1024:.2f} KB")

        return optimized_content

    def _extract_sections(self, content: str) -> Tuple[str, str, str, str, str, str, str]:
        """
        Extract sections from the original file.

        Args:
            content: The original LLM file content

        Returns:
            Tuple of (header_section, structure_section, context_section, financial_statements_section, facts_section, narrative_section, rest_section)
        """
        # Define section patterns
        header_pattern = r'^.*?(?=@STRUCTURE:|@DATA_DICTIONARY: CONTEXTS|@CONTEXTS)'
        structure_pattern = r'(@STRUCTURE:.*?)(?=@DATA_DICTIONARY: CONTEXTS|@CONTEXTS)'
        context_pattern = r'(@DATA_DICTIONARY: CONTEXTS.*?|@CONTEXTS.*?)(?=\n\n@CONCEPT:|\n\n@FACTS|\n\n@FINANCIAL_STATEMENTS_SECTION|\n\n@FINANCIAL_MAPPING)'
        financial_statements_pattern = r'(\n\n@FINANCIAL_STATEMENTS_SECTION.*?)(?=\n\n@CONCEPT:|\n\n@FACTS|\n\n@SECTION:|\n\n@NARRATIVE_TEXT:|\Z)'
        financial_mapping_pattern = r'(\n\n@FINANCIAL_MAPPING.*?)(?=\n\n@CONCEPT:|\n\n@FACTS|\n\n@SECTION:|\n\n@NARRATIVE_TEXT:|\Z)'
        facts_pattern = r'(\n\n@CONCEPT:.*|\n\n@FACTS.*?)(?=\n\n@SECTION:|\n\n@NARRATIVE_TEXT:|\Z)'
        narrative_pattern = r'(\n\n@SECTION:.*|\n\n@NARRATIVE_TEXT:.*)\Z'

        # Extract sections
        header_match = re.search(header_pattern, content, re.DOTALL)
        header_section = header_match.group(0) if header_match else ""

        structure_match = re.search(structure_pattern, content, re.DOTALL)
        structure_section = structure_match.group(0) if structure_match else ""

        context_match = re.search(context_pattern, content, re.DOTALL)
        context_section = context_match.group(0) if context_match else ""

        financial_statements_match = re.search(financial_statements_pattern, content, re.DOTALL)
        financial_statements_section = financial_statements_match.group(0) if financial_statements_match else ""

        financial_mapping_match = re.search(financial_mapping_pattern, content, re.DOTALL)
        financial_mapping_section = financial_mapping_match.group(0) if financial_mapping_match else ""

        facts_match = re.search(facts_pattern, content, re.DOTALL)
        facts_section = facts_match.group(0) if facts_match else ""

        narrative_match = re.search(narrative_pattern, content, re.DOTALL)
        narrative_section = narrative_match.group(0) if narrative_match else ""

        # Get the rest of the content
        rest_section = content
        if header_section:
            rest_section = rest_section.replace(header_section, "", 1)
        if structure_section:
            rest_section = rest_section.replace(structure_section, "", 1)
        if context_section:
            rest_section = rest_section.replace(context_section, "", 1)
        if financial_statements_section:
            rest_section = rest_section.replace(financial_statements_section, "", 1)
        if financial_mapping_section:
            rest_section = rest_section.replace(financial_mapping_section, "", 1)
        if facts_section:
            rest_section = rest_section.replace(facts_section, "", 1)
        if narrative_section:
            rest_section = rest_section.replace(narrative_section, "", 1)

        return header_section, structure_section, context_section, financial_statements_section, financial_mapping_section, facts_section, narrative_section, rest_section

    def _parse_context_definitions(self, context_section: str) -> Dict[str, Dict[str, Any]]:
        """
        Parse context definitions from the context section.

        Args:
            context_section: The context section of the LLM file

        Returns:
            Dictionary of context definitions
        """
        context_defs = {}

        # Check if we're using the new format or the old format
        if context_section.startswith("@CONTEXTS"):
            # New format
            context_pattern = r'([a-zA-Z0-9_-]+)\|(Period|Instant)\|([^|]+)\|([^\n]+)(?:\n\s+ALIASES: ([^\n]+))?'
            context_matches = re.findall(context_pattern, context_section)

            for match in context_matches:
                context_id, period_type, date_range, label, aliases = match

                # Parse aliases
                alias_list = []
                if aliases:
                    alias_list = [alias.strip() for alias in aliases.split(",")]

                context_defs[context_id] = {
                    "period_type": period_type,
                    "date_range": date_range,
                    "label": label,
                    "aliases": alias_list
                }
        else:
            # Old format
            context_pattern = r'([a-zA-Z0-9_-]+) \| @CODE: ([^|]+) \| ([^|]+) \| (Period|Instant): ([^\n]+)'
            context_matches = re.findall(context_pattern, context_section)

            for match in context_matches:
                context_id, code, label, period_type, date_range = match

                context_defs[context_id] = {
                    "code": code,
                    "label": label,
                    "period_type": period_type,
                    "date_range": date_range,
                    "aliases": []
                }

        return context_defs

    def _consolidate_contexts(self, context_defs: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Consolidate context definitions by date range.

        Args:
            context_defs: Dictionary of context definitions

        Returns:
            Dictionary of consolidated context definitions
        """
        # Group contexts by date range
        contexts_by_date_range = defaultdict(list)
        for context_id, context_info in context_defs.items():
            date_range = context_info["date_range"]
            contexts_by_date_range[date_range].append((context_id, context_info))

        # Consolidate contexts
        consolidated_contexts = {}
        for date_range, contexts in contexts_by_date_range.items():
            # Sort contexts by ID
            contexts.sort(key=lambda x: x[0])

            # Use the first context as the representative
            representative_id, representative_info = contexts[0]

            # Add all context IDs to the representative
            all_context_ids = [context_id for context_id, _ in contexts]

            consolidated_contexts[representative_id] = {
                **representative_info,
                "all_context_ids": all_context_ids
            }

        return consolidated_contexts

    def _parse_facts(self, facts_section: str) -> List[Dict[str, Any]]:
        """
        Parse facts from the facts section.

        Args:
            facts_section: The facts section of the LLM file

        Returns:
            List of facts
        """
        facts = []

        # Check if we're using the new format or the old format
        if facts_section.startswith("@FACTS"):
            # New format - try to parse facts grouped by context
            context_blocks = re.findall(r'@CONTEXT: ([^\n]+)(.*?)(?=\n@CONTEXT:|\Z)', facts_section, re.DOTALL)

            for context_ref, context_block in context_blocks:
                # Parse facts in this context block
                prefix_blocks = re.findall(r'@PREFIX: ([^\n]+)(.*?)(?=\n@PREFIX:|\n@CONTEXT:|\Z)', context_block, re.DOTALL)

                if prefix_blocks:
                    # Facts are grouped by prefix
                    for prefix, prefix_block in prefix_blocks:
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
                                    "decimals": "",
                                    "context_ref": context_ref.strip(),
                                    "date_type": "",
                                    "start_date": "",
                                    "end_date": ""
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
                                "decimals": "",
                                "context_ref": context_ref.strip(),
                                "date_type": "",
                                "start_date": "",
                                "end_date": ""
                            }

                            facts.append(fact)

            # Check if we have any metadata fields (dei: prefix)
            metadata_fields = [fact for fact in facts if any(fact['concept'].lower().startswith(prefix) for prefix in ['dei:', 'invest:', 'srt:'])]
            if len(metadata_fields) < 10:  # We should have more metadata fields
                logging.warning(f"Only found {len(metadata_fields)} metadata fields in the new format. This may indicate missing data.")
        else:
            # Old format - try multiple patterns to catch all facts
            # Pattern 1: Standard fact pattern
            fact_pattern1 = r'@CONCEPT: ([^\n]+)\n@(?:VALUE|COMMON_NAME): ([^\n]+)(?:\n@UNIT(?:_REF)?: ([^\n]+))?(?:\n@DECIMALS: ([^\n]+))?\n@CONTEXT_REF: ([^\n|]+)(?:[^\n]*)?(?:\n@DATE_TYPE: ([^\n]+))?(?:\n@(?:DATE|START_DATE): ([^\n]+))?(?:\n@END_DATE: ([^\n]+))?'
            fact_matches1 = re.findall(fact_pattern1, facts_section)

            for match in fact_matches1:
                concept, value, unit, decimals, context_ref, date_type, start_date, end_date = match

                # Clean up context_ref
                if " | " in context_ref:
                    context_ref = context_ref.split(" | ")[0]

                fact = {
                    "concept": concept,
                    "value": value if value else "",
                    "unit": unit if unit else "",
                    "decimals": decimals if decimals else "",
                    "context_ref": context_ref,
                    "date_type": date_type if date_type else "",
                    "start_date": start_date if start_date else "",
                    "end_date": end_date if end_date else ""
                }

                facts.append(fact)

            # Pattern 2: Facts with empty values
            fact_pattern2 = r'@CONCEPT: ([^\n]+)\n@(?:VALUE|COMMON_NAME): \n@CONTEXT_REF: ([^\n|]+)'
            fact_matches2 = re.findall(fact_pattern2, facts_section)

            for match in fact_matches2:
                concept, context_ref = match

                # Clean up context_ref
                if " | " in context_ref:
                    context_ref = context_ref.split(" | ")[0]

                fact = {
                    "concept": concept,
                    "value": "",
                    "unit": "",
                    "decimals": "",
                    "context_ref": context_ref,
                    "date_type": "",
                    "start_date": "",
                    "end_date": ""
                }

                facts.append(fact)

            # Pattern 3: Facts with no unit
            fact_pattern3 = r'@CONCEPT: ([^\n]+)\n@(?:VALUE|COMMON_NAME): ([^\n]+)\n@CONTEXT_REF: ([^\n|]+)'
            fact_matches3 = re.findall(fact_pattern3, facts_section)

            for match in fact_matches3:
                concept, value, context_ref = match

                # Clean up context_ref
                if " | " in context_ref:
                    context_ref = context_ref.split(" | ")[0]

                # Check if this fact is already added
                duplicate = False
                for fact in facts:
                    if fact["concept"] == concept and fact["context_ref"] == context_ref:
                        duplicate = True
                        break

                if not duplicate:
                    fact = {
                        "concept": concept,
                        "value": value if value else "",
                        "unit": "",
                        "decimals": "",
                        "context_ref": context_ref,
                        "date_type": "",
                        "start_date": "",
                        "end_date": ""
                    }

                    facts.append(fact)

        return facts

    def _group_facts_by_context(self, facts: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group facts by context reference.

        Args:
            facts: List of facts

        Returns:
            Dictionary of facts grouped by context reference
        """
        facts_by_context = defaultdict(list)

        # Check if we have metadata fields (dei: prefix)
        metadata_fields = [fact for fact in facts if any(fact['concept'].lower().startswith(prefix) for prefix in ['dei:', 'invest:', 'srt:'])]
        logging.info(f"Found {len(metadata_fields)} metadata fields in the facts")

        # If we have very few metadata fields, this may indicate missing data
        if len(metadata_fields) < 10:
            logging.warning(f"Only found {len(metadata_fields)} metadata fields. This may indicate missing data.")

        for fact in facts:
            context_ref = fact["context_ref"]
            facts_by_context[context_ref].append(fact)

        return facts_by_context

    def _generate_minimal_financial_mapping(self, structure_section: str, facts: List[Dict[str, Any]]) -> str:
        """
        Generate a minimal financial mapping section.

        Args:
            structure_section: The document structure section
            facts: List of facts

        Returns:
            Minimal financial mapping section
        """
        # Extract statement types from structure section
        statement_types = []
        statement_types_match = re.search(r'@STATEMENT_TYPES: (.*)', structure_section)
        if statement_types_match:
            statement_types = statement_types_match.group(1).split(', ')

        if not statement_types:
            return ""

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

        # Keep only the statement types that are in the structure section
        statement_type_keywords = {
            statement_type: keywords
            for statement_type, keywords in statement_type_keywords.items()
            if statement_type in statement_types
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

        # Generate mapping section
        output = []
        output.append("@FINANCIAL_MAPPING")
        output.append("@DESCRIPTION: Maps financial concepts to statement types defined in @STRUCTURE")

        # Add each statement type
        for statement_type in statement_types:
            if statement_type not in mapping:
                continue

            concepts = mapping[statement_type]

            if not concepts:
                continue

            output.append(f"\n@STATEMENT: {statement_type}")

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
                    output.append(f"@CONCEPTS: {','.join(sorted(prefix_concepts)[:50])}")  # Limit to 50 concepts per prefix
                else:
                    output.append(f"@CONCEPTS: {','.join(sorted(prefix_concepts)[:50])}")  # Limit to 50 concepts per prefix

        return "\n".join(output)

    def _optimize_narrative_section(self, narrative_section: str) -> str:
        """
        Optimize the narrative section.

        Args:
            narrative_section: The narrative section

        Returns:
            Optimized narrative section
        """
        # Extract sections
        sections = []
        section_blocks = re.split(r'\n\n@SECTION: ', narrative_section)

        for section_block in section_blocks[1:]:  # Skip the first empty block
            section_name = section_block.split('\n')[0]
            section_content = section_block[len(section_name):].strip()

            sections.append({
                'name': section_name,
                'content': section_content
            })

        # Filter out less important sections
        important_sections = []
        for section in sections:
            # Keep only the most important sections
            if any(keyword in section['name'].lower() for keyword in ['financial', 'statement', 'balance', 'income', 'cash', 'equity', 'risk', 'management']):
                important_sections.append(section)

        # If we've filtered out too many sections, keep at least 3
        if len(important_sections) < 3 and len(sections) >= 3:
            important_sections = sections[:3]

        # Create optimized narrative section
        output = []

        for section in important_sections:
            output.append(f"@SECTION: {section['name']}")

            # Truncate section content if it's too long (more than 10KB)
            content = section['content']
            if len(content) > 10240:
                content = content[:10240] + "\n[... truncated for brevity ...]\n"

            output.append(content)

        return "\n\n".join(output)

    def _generate_optimized_content(
        self,
        header_section: str,
        structure_section: str,
        consolidated_contexts: Dict[str, Dict[str, Any]],
        facts_by_context: Dict[str, List[Dict[str, Any]]],
        financial_mapping_section: str,
        narrative_section: str,
        rest_section: str
    ) -> str:
        """
        Generate optimized content.

        Args:
            header_section: The header section of the LLM file
            structure_section: The document structure section
            consolidated_contexts: Dictionary of consolidated context definitions
            facts_by_context: Dictionary of facts grouped by context reference
            financial_mapping_section: The financial mapping section
            narrative_section: The narrative section
            rest_section: The rest of the LLM file content

        Returns:
            The optimized LLM file content
        """
        output = []

        # Add header section
        output.append(header_section.strip())

        # Add structure section
        if structure_section.strip():
            output.append(structure_section.strip())

        # Add consolidated context definitions
        output.append("\n@CONTEXTS")
        for context_id, context_info in consolidated_contexts.items():
            period_type = context_info.get("period_type", "")
            date_range = context_info.get("date_range", "")
            label = context_info.get("label", "")

            output.append(f"{context_id}|{period_type}|{date_range}|{label}")

            # Add aliases if any
            if "all_context_ids" in context_info and len(context_info["all_context_ids"]) > 1:
                output.append(f"  ALIASES: {', '.join(context_info['all_context_ids'][1:])}")
            elif "aliases" in context_info and context_info["aliases"]:
                output.append(f"  ALIASES: {', '.join(context_info['aliases'])}")

        # Add financial mapping section if it exists
        if financial_mapping_section and financial_mapping_section.strip():
            output.append("\n" + financial_mapping_section.strip())

        # Add facts grouped by context
        output.append("\n@FACTS")
        for context_id, facts in facts_by_context.items():
            output.append(f"\n@CONTEXT: {context_id}")

            # Group facts by concept prefix
            facts_by_prefix = defaultdict(list)
            for fact in facts:
                concept = fact["concept"]
                prefix = concept.split(":")[0] if ":" in concept else ""
                facts_by_prefix[prefix].append(fact)

            # Add facts by prefix
            for prefix, prefix_facts in sorted(facts_by_prefix.items()):
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

        # Add narrative section if it exists
        if narrative_section and narrative_section.strip():
            output.append("\n" + narrative_section.strip())

        # Add rest of the content
        if rest_section.strip():
            output.append("\n" + rest_section.strip())

        return "\n".join(output)


def optimize_llm_content(content: str) -> str:
    """
    Optimize the LLM file content.

    Args:
        content: The original LLM file content

    Returns:
        The optimized LLM file content
    """
    optimizer = LLMFormatOptimizer()
    return optimizer.optimize(content)
