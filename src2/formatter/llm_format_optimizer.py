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
        header_section, structure_section, context_section, facts_section, rest_section = self._extract_sections(content)

        # Parse context definitions
        context_defs = self._parse_context_definitions(context_section)

        # Consolidate context definitions
        consolidated_contexts = self._consolidate_contexts(context_defs)

        # Parse facts
        facts = self._parse_facts(facts_section)

        # Group facts by context
        facts_by_context = self._group_facts_by_context(facts)

        # Generate optimized content
        optimized_content = self._generate_optimized_content(
            header_section,
            structure_section,
            consolidated_contexts,
            facts_by_context,
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

    def _extract_sections(self, content: str) -> Tuple[str, str, str, str, str]:
        """
        Extract sections from the original file.

        Args:
            content: The original LLM file content

        Returns:
            Tuple of (header_section, structure_section, context_section, facts_section, rest_section)
        """
        # Define section patterns
        header_pattern = r'^.*?(?=@STRUCTURE:|@DATA_DICTIONARY: CONTEXTS|@CONTEXTS)'
        structure_pattern = r'(@STRUCTURE:.*?)(?=@DATA_DICTIONARY: CONTEXTS|@CONTEXTS)'
        context_pattern = r'(@DATA_DICTIONARY: CONTEXTS.*?|@CONTEXTS.*?)(?=\n\n@CONCEPT:|\n\n@FACTS)'
        facts_pattern = r'(\n\n@CONCEPT:.*|\n\n@FACTS.*?)(?=\n\n@SECTION:|\n\n@NARRATIVE_TEXT:|\Z)'

        # Extract sections
        header_match = re.search(header_pattern, content, re.DOTALL)
        header_section = header_match.group(0) if header_match else ""

        structure_match = re.search(structure_pattern, content, re.DOTALL)
        structure_section = structure_match.group(0) if structure_match else ""

        context_match = re.search(context_pattern, content, re.DOTALL)
        context_section = context_match.group(0) if context_match else ""

        facts_match = re.search(facts_pattern, content, re.DOTALL)
        facts_section = facts_match.group(0) if facts_match else ""

        # Get the rest of the content
        rest_section = content
        if header_section:
            rest_section = rest_section.replace(header_section, "", 1)
        if structure_section:
            rest_section = rest_section.replace(structure_section, "", 1)
        if context_section:
            rest_section = rest_section.replace(context_section, "", 1)
        if facts_section:
            rest_section = rest_section.replace(facts_section, "", 1)

        return header_section, structure_section, context_section, facts_section, rest_section

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

        for fact in facts:
            context_ref = fact["context_ref"]
            facts_by_context[context_ref].append(fact)

        return facts_by_context

    def _generate_optimized_content(
        self,
        header_section: str,
        structure_section: str,
        consolidated_contexts: Dict[str, Dict[str, Any]],
        facts_by_context: Dict[str, List[Dict[str, Any]]],
        rest_section: str
    ) -> str:
        """
        Generate optimized content.

        Args:
            header_section: The header section of the LLM file
            structure_section: The document structure section
            consolidated_contexts: Dictionary of consolidated context definitions
            facts_by_context: Dictionary of facts grouped by context reference
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
