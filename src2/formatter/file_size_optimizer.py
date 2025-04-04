"""
File Size Optimizer

This module provides functionality to optimize the size of LLM-formatted files
while maintaining data integrity.
"""

import re
import logging
import hashlib
from typing import Dict, List, Set, Tuple, Any
from collections import defaultdict

class FileSizeOptimizer:
    """
    Optimizes the size of LLM-formatted files by:
    1. Consolidating context definitions
    2. Optimizing tags
    3. Deduplicating narrative blocks
    """

    def __init__(self):
        """Initialize the file size optimizer."""
        self.logger = logging.getLogger(__name__)

        # Define tag mappings for optimization
        self.tag_mappings = {
            "@FINANCIAL_STATEMENT:": "@FS:",
            "@STATEMENT_TYPE:": "@ST:",
            "@NARRATIVE_TEXT:": "@NT:",
            "@CONTEXT_LABELS:": "@CL:",
            "@INDIVIDUAL_FACTS_SECTION": "@FACTS_SECTION",
            "@DATA_DICTIONARY: CONTEXTS": "@DD_CONTEXTS",
            "@SECTION:": "@SEC:",
        }

        # Initialize context mapping
        self.context_mapping = {}
        self.next_context_id = 1

        # Initialize text block mapping
        self.text_block_mapping = {}
        self.next_block_id = 1

    def optimize(self, content: str) -> str:
        """
        Apply all optimizations to the content.

        Args:
            content: The content to optimize

        Returns:
            The optimized content
        """
        # First, extract and consolidate contexts
        content = self._consolidate_contexts(content)

        # Then deduplicate text blocks
        content = self._deduplicate_text_blocks(content)

        # Normalize financial statements
        content = self._normalize_financial_statements(content)

        # Optimize tags
        content = self._optimize_tags(content)

        # Reduce whitespace
        content = self._reduce_whitespace(content)

        return content

    def _consolidate_contexts(self, content: str) -> str:
        """
        Consolidate context definitions and references.

        Args:
            content: The content to optimize

        Returns:
            The optimized content
        """
        self.logger.info("Consolidating contexts...")

        # Extract all context references
        context_refs = set(re.findall(r'i[0-9a-f]{32}_[DI][0-9]{8}(?:-[0-9]{8})?', content))
        self.logger.info(f"Found {len(context_refs)} unique context references")

        # Create a mapping from original context references to compact ones
        for i, context_ref in enumerate(sorted(context_refs), 1):
            self.context_mapping[context_ref] = f"c-{i}"

        # Create a new context dictionary section
        context_dict_section = "@DD_CONTEXTS\n"
        for orig_ref, compact_ref in self.context_mapping.items():
            # Try to extract the context label if available
            label_match = re.search(rf'{re.escape(orig_ref)} \(([^)]+)\)', content)
            label = label_match.group(1) if label_match else ""

            # Add to the dictionary
            context_dict_section += f"{compact_ref} | @CODE: {orig_ref}\n"
            if label:
                context_dict_section += f"     @LABEL: {label}\n"
            context_dict_section += "\n"

        # Replace all context references in the content
        for orig_ref, compact_ref in self.context_mapping.items():
            # Replace the reference but preserve the label on first occurrence
            label_pattern = rf'({re.escape(orig_ref)}) \(([^)]+)\)'
            content = re.sub(label_pattern, f"{compact_ref}", content)

            # Replace all other occurrences
            content = content.replace(orig_ref, compact_ref)

        # Remove the old context dictionary section if it exists
        content = re.sub(r'@DATA_DICTIONARY: CONTEXTS.*?(?=\n\n@|\Z)', '', content, flags=re.DOTALL)

        # Insert the new context dictionary section at the beginning of the document
        if "@DOCUMENT_METADATA" in content:
            # Insert after document metadata
            content = re.sub(r'(@DOCUMENT_METADATA.*?)(\n\n@)', r'\1\n\n' + context_dict_section + r'\2', content, flags=re.DOTALL)
        else:
            # Insert at the beginning
            content = context_dict_section + "\n\n" + content

        self.logger.info(f"Consolidated {len(self.context_mapping)} contexts")
        return content

    def _deduplicate_text_blocks(self, content: str) -> str:
        """
        Deduplicate narrative and policy text blocks.

        Args:
            content: The content to optimize

        Returns:
            The optimized content
        """
        self.logger.info("Deduplicating text blocks...")

        # Find all narrative text blocks
        narrative_blocks = re.findall(r'@NARRATIVE_TEXT:.*?(?=\n\n@|\Z)', content, flags=re.DOTALL)

        # Find all policy text blocks
        policy_blocks = re.findall(r'@POLICY_TEXT:.*?(?=\n\n@|\Z)', content, flags=re.DOTALL)

        # Combine all text blocks
        all_blocks = narrative_blocks + policy_blocks
        self.logger.info(f"Found {len(all_blocks)} text blocks")

        # Create a mapping from block content hash to block ID
        block_mapping = {}
        text_blocks_section = "@TEXT_BLOCKS\n\n"

        for block in all_blocks:
            # Extract the title and content
            title_match = re.match(r'@(?:NARRATIVE_TEXT|POLICY_TEXT): (.*?)(?:\n|$)', block)
            title = title_match.group(1) if title_match else "Untitled"

            # Get the content without the title
            content_without_title = re.sub(r'^@(?:NARRATIVE_TEXT|POLICY_TEXT): .*?(?:\n|$)', '', block, flags=re.DOTALL)

            # Hash the content to identify duplicates
            content_hash = hashlib.md5(content_without_title.encode()).hexdigest()

            if content_hash not in block_mapping:
                # This is a new unique block
                block_id = f"tb-{len(block_mapping) + 1}"
                block_mapping[content_hash] = block_id

                # Add to the text blocks section
                text_blocks_section += f"{block_id} | @TITLE: {title}\n"
                text_blocks_section += f"      @TEXT: {content_without_title.strip()}\n\n"

        # If we found duplicate blocks, replace them with references
        if len(block_mapping) < len(all_blocks):
            self.logger.info(f"Found {len(all_blocks) - len(block_mapping)} duplicate text blocks")

            # Add the text blocks section to the content
            if "@DOCUMENT_METADATA" in content:
                # Insert after document metadata and context dictionary
                content = re.sub(r'(@DD_CONTEXTS.*?)(\n\n@)', r'\1\n\n' + text_blocks_section + r'\2', content, flags=re.DOTALL)
            else:
                # Insert at the beginning
                content = text_blocks_section + "\n\n" + content

            # Replace text blocks with references
            for block in all_blocks:
                # Extract the title
                title_match = re.match(r'@(?:NARRATIVE_TEXT|POLICY_TEXT): (.*?)(?:\n|$)', block)
                title = title_match.group(1) if title_match else "Untitled"

                # Get the content without the title
                content_without_title = re.sub(r'^@(?:NARRATIVE_TEXT|POLICY_TEXT): .*?(?:\n|$)', '', block, flags=re.DOTALL)

                # Hash the content
                content_hash = hashlib.md5(content_without_title.encode()).hexdigest()

                # Get the block ID
                block_id = block_mapping[content_hash]

                # Replace the block with a reference
                block_ref = f"@TEXT_REF: {title} | {block_id}"
                content = content.replace(block, block_ref)

        return content

    def _optimize_tags(self, content: str) -> str:
        """
        Optimize tags to reduce verbosity.

        Args:
            content: The content to optimize

        Returns:
            The optimized content
        """
        self.logger.info("Optimizing tags...")

        # Replace tags with their optimized versions
        for old_tag, new_tag in self.tag_mappings.items():
            content = content.replace(old_tag, new_tag)

        return content

    def _normalize_financial_statements(self, content: str) -> str:
        """
        Convert financial statements to a true normalized format.

        Args:
            content: The content to optimize

        Returns:
            The optimized content
        """
        self.logger.info("Normalizing financial statements...")

        # Find all financial statement sections
        fs_sections = re.findall(r'@(?:FS|FINANCIAL_STATEMENT): ([^\n]+).*?(?=\n\n@(?:FS|FINANCIAL_STATEMENT|SEC|SECTION):|\Z)',
                                content, re.DOTALL)

        if not fs_sections:
            self.logger.info("No financial statement sections found")
            return content

        self.logger.info(f"Found {len(fs_sections)} financial statement sections")

        # Extract all financial data from the wide format tables
        normalized_data = []

        # Find all financial statement sections with their content
        fs_matches = re.finditer(r'@(?:FS|FINANCIAL_STATEMENT): ([^\n]+)(.*?)(?=\n\n@(?:FS|FINANCIAL_STATEMENT|SEC|SECTION):|\Z)',
                                content, re.DOTALL)

        for fs_match in fs_matches:
            statement_type = fs_match.group(1).strip()
            statement_content = fs_match.group(2)

            # Extract context labels if available
            context_labels = {}
            context_labels_match = re.search(r'@(?:CL|CONTEXT_LABELS): (.*?)(?=\n\n|\Z)', statement_content, re.DOTALL)
            if context_labels_match:
                # Parse context labels
                labels_text = context_labels_match.group(1)
                label_matches = re.finditer(r'(c-\d+) \(([^)]+)\)', labels_text)
                for label_match in label_matches:
                    context_id = label_match.group(1)
                    label = label_match.group(2)
                    context_labels[context_id] = label

            # Find the data table
            table_match = re.search(r'Line Item \|.*?(?=\n\n|\Z)', statement_content, re.DOTALL)
            if not table_match:
                continue

            table_content = table_match.group(0)

            # Extract header row to get context IDs
            header_match = re.search(r'Line Item \| (.*?)(?=\n)', table_content)
            if not header_match:
                continue

            header_row = header_match.group(1)
            contexts = [ctx.strip() for ctx in header_row.split('|')]

            # Extract data rows
            data_rows = re.findall(r'([^|\n]+)\|(.*?)(?=\n|$)', table_content)

            for row in data_rows:
                if 'Line Item' in row[0]:
                    continue  # Skip header row

                concept = row[0].strip()
                if not concept or concept == '---------':
                    continue  # Skip separator rows

                values = [val.strip() for val in row[1].split('|')]

                # Add each non-empty value to the normalized data
                for i, value in enumerate(values):
                    if i < len(contexts) and value and value != '-':
                        context_id = contexts[i]
                        normalized_data.append({
                            'statement_type': statement_type,
                            'concept': concept,
                            'value': value,
                            'context': context_id,
                            'context_label': context_labels.get(context_id, '')
                        })

        # If we found normalized data, replace the financial statement sections
        if normalized_data:
            self.logger.info(f"Extracted {len(normalized_data)} normalized data points")

            # Create a new normalized financial statements section
            new_fs_section = "@NORMALIZED_FINANCIAL_DATA\n\n"
            new_fs_section += "@FORMAT: Statement | Concept | Value | Context | Context_Label\n\n"

            # Group by statement type
            by_statement = {}
            for data in normalized_data:
                statement = data['statement_type']
                if statement not in by_statement:
                    by_statement[statement] = []
                by_statement[statement].append(data)

            # Add data for each statement type
            for statement, data_list in by_statement.items():
                new_fs_section += f"@STATEMENT: {statement}\n"
                for data in data_list:
                    new_fs_section += f"{statement}|{data['concept']}|{data['value']}|{data['context']}|{data['context_label']}\n"
                new_fs_section += "\n"

            # Remove the old financial statement sections
            for statement_type in fs_sections:
                content = re.sub(r'@(?:FS|FINANCIAL_STATEMENT): ' + re.escape(statement_type) +
                                r'.*?(?=\n\n@(?:FS|FINANCIAL_STATEMENT|SEC|SECTION):|\Z)',
                                '', content, flags=re.DOTALL)

            # Add the new normalized section
            if "@NORMALIZED_FORMAT:" in content:
                # Replace existing normalized format section
                content = re.sub(r'@NORMALIZED_FORMAT:.*?(?=\n\n@(?:SEC|SECTION):|\Z)',
                                new_fs_section, content, flags=re.DOTALL)
            else:
                # Add new section after context dictionary
                content = re.sub(r'(@DD_CONTEXTS.*?)(?=\n\n@)',
                                r'\1\n\n' + new_fs_section, content, flags=re.DOTALL)

        return content

    def _reduce_whitespace(self, content: str) -> str:
        """
        Reduce unnecessary whitespace in the content.

        Args:
            content: The content to optimize

        Returns:
            The optimized content
        """
        self.logger.info("Reducing whitespace...")

        # Replace multiple blank lines with a single blank line
        content = re.sub(r'\n{3,}', '\n\n', content)

        # Remove trailing whitespace from lines
        content = re.sub(r' +\n', '\n', content)

        # Compact lists of values
        content = re.sub(r'\| +', '|', content)
        content = re.sub(r' +\|', '|', content)

        return content
