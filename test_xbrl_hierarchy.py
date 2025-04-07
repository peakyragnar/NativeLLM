#!/usr/bin/env python3
"""
XBRL Hierarchy Test Script

This script extracts hierarchical relationships directly from XBRL taxonomy and linkbase files.
It's designed to test the approach before integrating it into the main codebase.
"""

import os
import sys
import json
import logging
import argparse
import re
import time
from pathlib import Path
from bs4 import BeautifulSoup
from collections import defaultdict
import requests
from urllib.parse import urljoin, urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class XBRLHierarchyTester:
    """
    Test class for extracting XBRL hierarchies from taxonomy and linkbase files.
    """

    def __init__(self):
        """Initialize the XBRL hierarchy tester."""
        self.namespaces = {
            'xsd': 'http://www.w3.org/2001/XMLSchema',
            'link': 'http://www.xbrl.org/2003/linkbase',
            'xlink': 'http://www.w3.org/1999/xlink',
            'xbrli': 'http://www.xbrl.org/2003/instance',
            'xbrldt': 'http://xbrl.org/2005/xbrldt',
            'us-gaap': 'http://fasb.org/us-gaap/2021',
            'dei': 'http://xbrl.sec.gov/dei/2021'
        }
        self.schema_refs = []
        self.linkbase_refs = []
        self.concepts = {}
        self.presentation_links = []
        self.calculation_links = []
        self.definition_links = []
        self.label_links = []
        self.hierarchy = {
            'presentation': defaultdict(list),
            'calculation': defaultdict(list),
            'definition': defaultdict(list)
        }
        self.labels = {}
        self.facts = []
        self.cache_dir = Path('xbrl_cache')
        self.cache_dir.mkdir(exist_ok=True)

    def extract_hierarchy_from_html(self, html_path):
        """
        Extract XBRL hierarchy from an HTML document with inline XBRL.

        Args:
            html_path: Path to the HTML file

        Returns:
            Dictionary with hierarchy information
        """
        logging.info(f"Extracting XBRL hierarchy from HTML: {html_path}")

        # Parse the HTML document
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        # Try different parsers
        try:
            soup = BeautifulSoup(html_content, 'lxml-xml')
        except Exception:
            soup = BeautifulSoup(html_content, 'lxml')

        # Extract schema references
        self._extract_schema_refs(soup, html_path)

        # Extract facts
        self._extract_facts(soup)

        # Process schema and linkbase files
        self._process_schema_files()

        # Build hierarchy
        self._build_hierarchy()

        # Map facts to hierarchy
        self._map_facts_to_hierarchy()

        return {
            'concepts': self.concepts,
            'presentation_hierarchy': dict(self.hierarchy['presentation']),
            'calculation_hierarchy': dict(self.hierarchy['calculation']),
            'definition_hierarchy': dict(self.hierarchy['definition']),
            'labels': self.labels,
            'facts': self.facts,
            'schema_refs': self.schema_refs,
            'linkbase_refs': self.linkbase_refs
        }

    def _extract_schema_refs(self, soup, html_path=None):
        """
        Extract schema references from the document.

        Args:
            soup: BeautifulSoup object
            html_path: Path to the HTML file (for resolving relative URLs)
        """
        # Look for schemaRef elements
        schema_refs = soup.find_all('link:schemaRef')
        if not schema_refs:
            schema_refs = soup.find_all(re.compile(r'.*:schemaRef'))

        for ref in schema_refs:
            href = ref.get('xlink:href') or ref.get('href')
            if href:
                # Resolve relative URL if html_path is provided
                if html_path and not href.startswith(('http://', 'https://')):
                    base_dir = os.path.dirname(html_path)
                    href = os.path.join(base_dir, href)
                    href = os.path.normpath(href)

                self.schema_refs.append(href)
                logging.info(f"Found schema reference: {href}")

                # If the schema file doesn't exist locally, try to construct a URL to the SEC website
                if not os.path.exists(href) and not href.startswith(('http://', 'https://')):
                    schema_filename = os.path.basename(href)

                    # Try to find filing_info.json in the same directory as the HTML file
                    html_dir = os.path.dirname(html_path)
                    filing_info_path = os.path.join(html_dir, 'filing_info.json')

                    if os.path.exists(filing_info_path):
                        try:
                            # Load filing info
                            with open(filing_info_path, 'r') as f:
                                filing_info = json.load(f)

                            # Extract CIK and accession number
                            cik = filing_info.get('cik')
                            accession_number = filing_info.get('accession_number')

                            if cik and accession_number:
                                # Format the accession number (remove dashes if present)
                                accession_number = accession_number.replace('-', '')

                                # Format the accession number with dashes (required by SEC)
                                formatted_accession = f"{accession_number[0:10]}-{accession_number[10:12]}-{accession_number[12:]}" if len(accession_number) > 12 else accession_number

                                # Construct SEC URL (direct format)
                                sec_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_accession}/{schema_filename}"
                                logging.info(f"Schema file not found locally, trying SEC URL: {sec_url}")
                                self.schema_refs.append(sec_url)

                                # Try the format from the SEC index page
                                index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_accession}/{schema_filename}"
                                logging.info(f"Also trying index URL: {index_url}")
                                self.schema_refs.append(index_url)

                                # Try the direct URL format without accession number
                                direct_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_accession}/{schema_filename}"
                                logging.info(f"Also trying direct URL: {direct_url}")
                                self.schema_refs.append(direct_url)

                                # Try the format we found in the curl command
                                curl_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_accession}/{schema_filename}"
                                logging.info(f"Also trying curl URL: {curl_url}")
                                self.schema_refs.append(curl_url)

                                # Try the direct format from the SEC website
                                sec_direct_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_accession.replace('-', '')}/{schema_filename}"
                                logging.info(f"Also trying SEC direct URL: {sec_direct_url}")
                                self.schema_refs.append(sec_direct_url)
                            else:
                                logging.warning(f"Could not extract CIK or accession number from filing_info.json")
                        except Exception as e:
                            logging.error(f"Error reading filing_info.json: {str(e)}")
                    else:
                        # Extract accession number from the path
                        path_parts = html_path.split(os.sep)
                        accession_number = None

                        # Look for accession number (12-digit number)
                        for part in path_parts:
                            if re.match(r'\d{12}', part):  # Accession number format
                                accession_number = part
                                break

                        if accession_number:
                            # Try to extract CIK from the HTML content
                            cik = None
                            try:
                                # Look for CIK in the HTML content
                                html_content = None
                                with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
                                    html_content = f.read()

                                # Try to find CIK in the HTML
                                cik_match = re.search(r'CIK=([0-9]+)', html_content)
                                if cik_match:
                                    cik = cik_match.group(1)
                                else:
                                    # Try another pattern
                                    cik_match = re.search(r'CIK="?([0-9]+)"?', html_content)
                                    if cik_match:
                                        cik = cik_match.group(1)
                            except Exception as e:
                                logging.error(f"Error extracting CIK from HTML: {str(e)}")

                            # If we found a CIK, construct the URL
                            if cik:
                                # Format the accession number with dashes (required by SEC)
                                formatted_accession = f"{accession_number[0:10]}-{accession_number[10:12]}-{accession_number[12:]}" if len(accession_number) > 12 else accession_number

                                # Construct SEC URL (direct format)
                                sec_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_accession}/{schema_filename}"
                                logging.info(f"Schema file not found locally, trying SEC URL: {sec_url}")
                                self.schema_refs.append(sec_url)

                                # Try the format from the SEC index page
                                index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_accession}/{schema_filename}"
                                logging.info(f"Also trying index URL: {index_url}")
                                self.schema_refs.append(index_url)

                                # Try the direct URL format without accession number
                                direct_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_accession}/{schema_filename}"
                                logging.info(f"Also trying direct URL: {direct_url}")
                                self.schema_refs.append(direct_url)

                                # Try the format we found in the curl command
                                curl_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_accession}/{schema_filename}"
                                logging.info(f"Also trying curl URL: {curl_url}")
                                self.schema_refs.append(curl_url)

                                # Try the direct format from the SEC website
                                sec_direct_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_accession.replace('-', '')}/{schema_filename}"
                                logging.info(f"Also trying SEC direct URL: {sec_direct_url}")
                                self.schema_refs.append(sec_direct_url)
                            else:
                                logging.warning(f"Could not extract CIK for {html_path}, cannot construct SEC URL")

        # Also look for schema references in the HTML head
        link_tags = soup.find_all('link', rel='schema.xbrl')
        for link in link_tags:
            href = link.get('href')
            if href:
                # Resolve relative URL if html_path is provided
                if html_path and not href.startswith(('http://', 'https://')):
                    base_dir = os.path.dirname(html_path)
                    href = os.path.join(base_dir, href)
                    href = os.path.normpath(href)

                self.schema_refs.append(href)
                logging.info(f"Found schema reference in HTML head: {href}")

    def _extract_facts(self, soup):
        """
        Extract facts from the document.

        Args:
            soup: BeautifulSoup object
        """
        # Try multiple approaches to extract facts

        # 1. Look for ix:nonNumeric and ix:nonFraction elements (inline XBRL)
        ix_tags = soup.find_all(['ix:nonnumeric', 'ix:nonfraction'])
        if not ix_tags:
            ix_tags = soup.find_all(re.compile(r'.*:nonnumeric$|.*:nonfraction$'))

        for tag in ix_tags:
            name = tag.get('name')
            context_ref = tag.get('contextref')
            unit_ref = tag.get('unitref')
            value = tag.get_text(strip=True)

            if name:
                self.facts.append({
                    'name': name,
                    'context_ref': context_ref,
                    'unit_ref': unit_ref,
                    'value': value
                })

        # 2. Look for standard XBRL facts
        xbrl_tags = []
        for tag_name in ['xbrli:measure', 'measure', 'xbrli:context', 'context']:
            xbrl_tags.extend(soup.find_all(tag_name))

        # Also look for any tag with a contextRef attribute
        for tag in soup.find_all(attrs={'contextRef': True}):
            name = tag.name
            context_ref = tag.get('contextRef')
            unit_ref = tag.get('unitRef')
            value = tag.get_text(strip=True)

            if name:
                self.facts.append({
                    'name': name,
                    'context_ref': context_ref,
                    'unit_ref': unit_ref,
                    'value': value
                })

        # 3. Look for any element with a namespace prefix (potential XBRL element)
        for tag in soup.find_all():
            if ':' in tag.name and not tag.name.startswith(('html:', 'xhtml:', 'link:')):
                # Skip already processed tags
                if tag in ix_tags or tag in xbrl_tags:
                    continue

                name = tag.name
                context_ref = tag.get('contextRef')
                unit_ref = tag.get('unitRef')
                value = tag.get_text(strip=True)

                if name and context_ref:  # Only include if it has a context reference
                    self.facts.append({
                        'name': name,
                        'context_ref': context_ref,
                        'unit_ref': unit_ref,
                        'value': value
                    })

        logging.info(f"Extracted {len(self.facts)} facts from document")

    def _process_schema_files(self):
        """Process schema files and extract linkbase references."""
        for schema_ref in self.schema_refs:
            try:
                # Download or use cached schema file
                schema_content = self._get_file_content(schema_ref)
                if not schema_content:
                    continue

                # Parse schema file
                schema_soup = BeautifulSoup(schema_content, 'lxml-xml')

                # Extract linkbase references
                self._extract_linkbase_refs(schema_soup, schema_ref)

                # Extract concepts
                self._extract_concepts(schema_soup)

                # Process linkbase files
                for linkbase_ref in self.linkbase_refs:
                    self._process_linkbase_file(linkbase_ref)

            except Exception as e:
                logging.error(f"Error processing schema file {schema_ref}: {str(e)}")

    def _extract_linkbase_refs(self, schema_soup, base_url):
        """
        Extract linkbase references from schema file.

        Args:
            schema_soup: BeautifulSoup object for schema file
            base_url: Base URL for resolving relative URLs
        """
        # Look for linkbaseRef elements
        linkbase_refs = schema_soup.find_all('link:linkbaseRef')
        if not linkbase_refs:
            linkbase_refs = schema_soup.find_all(re.compile(r'.*:linkbaseRef'))

        for ref in linkbase_refs:
            href = ref.get('xlink:href') or ref.get('href')
            role = ref.get('xlink:role') or ref.get('role')

            if href:
                # Resolve relative URL
                if not href.startswith(('http://', 'https://')):
                    href = urljoin(base_url, href)

                self.linkbase_refs.append({
                    'href': href,
                    'role': role
                })
                logging.info(f"Found linkbase reference: {href} (role: {role})")

    def _extract_concepts(self, schema_soup):
        """
        Extract concepts from schema file.

        Args:
            schema_soup: BeautifulSoup object for schema file
        """
        # Look for element definitions
        elements = schema_soup.find_all('xsd:element')
        if not elements:
            elements = schema_soup.find_all('element')

        for element in elements:
            name = element.get('name')
            id = element.get('id')
            type = element.get('type')
            substitution_group = element.get('substitutionGroup')

            if name:
                self.concepts[name] = {
                    'id': id,
                    'name': name,
                    'type': type,
                    'substitution_group': substitution_group,
                    'balance': element.get('xbrli:balance'),
                    'period_type': element.get('xbrli:periodType')
                }

        logging.info(f"Extracted {len(self.concepts)} concepts from schema")

    def _process_linkbase_file(self, linkbase_ref):
        """
        Process a linkbase file.

        Args:
            linkbase_ref: Dictionary with linkbase reference information
        """
        href = linkbase_ref['href']
        role = linkbase_ref['role']

        try:
            # Download or use cached linkbase file
            linkbase_content = self._get_file_content(href)
            if not linkbase_content:
                return

            # Parse linkbase file
            linkbase_soup = BeautifulSoup(linkbase_content, 'lxml-xml')

            # Process based on role
            if 'presentationLinkbase' in role:
                self._process_presentation_linkbase(linkbase_soup)
            elif 'calculationLinkbase' in role:
                self._process_calculation_linkbase(linkbase_soup)
            elif 'definitionLinkbase' in role:
                self._process_definition_linkbase(linkbase_soup)
            elif 'labelLinkbase' in role:
                self._process_label_linkbase(linkbase_soup)
            else:
                logging.info(f"Skipping linkbase with unknown role: {role}")

        except Exception as e:
            logging.error(f"Error processing linkbase file {href}: {str(e)}")

    def _process_presentation_linkbase(self, linkbase_soup):
        """
        Process a presentation linkbase file.

        Args:
            linkbase_soup: BeautifulSoup object for linkbase file
        """
        # Look for presentationLink elements
        presentation_links = linkbase_soup.find_all('link:presentationLink')
        if not presentation_links:
            presentation_links = linkbase_soup.find_all(re.compile(r'.*:presentationLink'))

        for link in presentation_links:
            role = link.get('xlink:role') or link.get('role')

            # Look for presentationArc elements
            arcs = link.find_all('link:presentationArc')
            if not arcs:
                arcs = link.find_all(re.compile(r'.*:presentationArc'))

            for arc in arcs:
                from_attr = arc.get('xlink:from') or arc.get('from')
                to_attr = arc.get('xlink:to') or arc.get('to')
                order = arc.get('order')

                if from_attr and to_attr:
                    # Look up the actual concept names
                    from_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': from_attr})
                    to_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': to_attr})

                    if from_loc and to_loc:
                        from_href = from_loc.get('xlink:href') or from_loc.get('href')
                        to_href = to_loc.get('xlink:href') or to_loc.get('href')

                        # Extract concept names from hrefs
                        from_concept = self._extract_concept_from_href(from_href)
                        to_concept = self._extract_concept_from_href(to_href)

                        if from_concept and to_concept:
                            self.presentation_links.append({
                                'role': role,
                                'from': from_concept,
                                'to': to_concept,
                                'order': order
                            })

        logging.info(f"Extracted {len(self.presentation_links)} presentation relationships")

    def _process_calculation_linkbase(self, linkbase_soup):
        """
        Process a calculation linkbase file.

        Args:
            linkbase_soup: BeautifulSoup object for linkbase file
        """
        # Look for calculationLink elements
        calculation_links = linkbase_soup.find_all('link:calculationLink')
        if not calculation_links:
            calculation_links = linkbase_soup.find_all(re.compile(r'.*:calculationLink'))

        for link in calculation_links:
            role = link.get('xlink:role') or link.get('role')

            # Look for calculationArc elements
            arcs = link.find_all('link:calculationArc')
            if not arcs:
                arcs = link.find_all(re.compile(r'.*:calculationArc'))

            for arc in arcs:
                from_attr = arc.get('xlink:from') or arc.get('from')
                to_attr = arc.get('xlink:to') or arc.get('to')
                weight = arc.get('weight')

                if from_attr and to_attr:
                    # Look up the actual concept names
                    from_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': from_attr})
                    to_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': to_attr})

                    if from_loc and to_loc:
                        from_href = from_loc.get('xlink:href') or from_loc.get('href')
                        to_href = to_loc.get('xlink:href') or to_loc.get('href')

                        # Extract concept names from hrefs
                        from_concept = self._extract_concept_from_href(from_href)
                        to_concept = self._extract_concept_from_href(to_href)

                        if from_concept and to_concept:
                            self.calculation_links.append({
                                'role': role,
                                'from': from_concept,
                                'to': to_concept,
                                'weight': weight
                            })

        logging.info(f"Extracted {len(self.calculation_links)} calculation relationships")

    def _process_definition_linkbase(self, linkbase_soup):
        """
        Process a definition linkbase file.

        Args:
            linkbase_soup: BeautifulSoup object for linkbase file
        """
        # Look for definitionLink elements
        definition_links = linkbase_soup.find_all('link:definitionLink')
        if not definition_links:
            definition_links = linkbase_soup.find_all(re.compile(r'.*:definitionLink'))

        for link in definition_links:
            role = link.get('xlink:role') or link.get('role')

            # Look for definitionArc elements
            arcs = link.find_all('link:definitionArc')
            if not arcs:
                arcs = link.find_all(re.compile(r'.*:definitionArc'))

            for arc in arcs:
                from_attr = arc.get('xlink:from') or arc.get('from')
                to_attr = arc.get('xlink:to') or arc.get('to')
                arcrole = arc.get('xlink:arcrole') or arc.get('arcrole')

                if from_attr and to_attr:
                    # Look up the actual concept names
                    from_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': from_attr})
                    to_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': to_attr})

                    if from_loc and to_loc:
                        from_href = from_loc.get('xlink:href') or from_loc.get('href')
                        to_href = to_loc.get('xlink:href') or to_loc.get('href')

                        # Extract concept names from hrefs
                        from_concept = self._extract_concept_from_href(from_href)
                        to_concept = self._extract_concept_from_href(to_href)

                        if from_concept and to_concept:
                            self.definition_links.append({
                                'role': role,
                                'from': from_concept,
                                'to': to_concept,
                                'arcrole': arcrole
                            })

        logging.info(f"Extracted {len(self.definition_links)} definition relationships")

    def _process_label_linkbase(self, linkbase_soup):
        """
        Process a label linkbase file.

        Args:
            linkbase_soup: BeautifulSoup object for linkbase file
        """
        # Look for labelLink elements
        label_links = linkbase_soup.find_all('link:labelLink')
        if not label_links:
            label_links = linkbase_soup.find_all(re.compile(r'.*:labelLink'))

        for link in label_links:
            # Look for labelArc elements
            arcs = link.find_all('link:labelArc')
            if not arcs:
                arcs = link.find_all(re.compile(r'.*:labelArc'))

            for arc in arcs:
                from_attr = arc.get('xlink:from') or arc.get('from')
                to_attr = arc.get('xlink:to') or arc.get('to')

                if from_attr and to_attr:
                    # Look up the concept
                    from_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': from_attr})

                    if from_loc:
                        from_href = from_loc.get('xlink:href') or from_loc.get('href')
                        concept = self._extract_concept_from_href(from_href)

                        # Look up the label
                        label_element = link.find(['link:label', 'label'], attrs={'xlink:label': to_attr})

                        if concept and label_element:
                            label_role = label_element.get('xlink:role') or label_element.get('role')
                            label_text = label_element.get_text(strip=True)

                            if concept not in self.labels:
                                self.labels[concept] = {}

                            self.labels[concept][label_role] = label_text

        logging.info(f"Extracted labels for {len(self.labels)} concepts")

    def _extract_concept_from_href(self, href):
        """
        Extract concept name from href attribute.

        Args:
            href: The href attribute value

        Returns:
            The concept name or None if not found
        """
        if not href:
            return None

        # Extract the fragment identifier
        fragment = href.split('#')[-1]

        # Check if it's a direct concept reference
        if fragment in self.concepts:
            return fragment

        # Check if it's an ID reference
        for name, concept in self.concepts.items():
            if concept.get('id') == fragment:
                return name

        # Try to extract the concept name from the fragment
        # Common patterns: _ConceptName_id, ConceptName, etc.
        parts = fragment.split('_')
        for part in parts:
            if part in self.concepts:
                return part

        return None

    def _build_hierarchy(self):
        """Build hierarchical relationships from extracted links."""
        # Process presentation links
        for link in self.presentation_links:
            parent = link['from']
            child = link['to']
            order = link.get('order', '0')
            role = link.get('role', '')

            # Determine statement type from role
            statement_type = self._determine_statement_type_from_role(role)

            # Add to hierarchy
            self.hierarchy['presentation'][parent].append({
                'child': child,
                'order': order,
                'role': role,
                'statement_type': statement_type
            })

        # Process calculation links
        for link in self.calculation_links:
            parent = link['from']
            child = link['to']
            weight = link.get('weight', '1')
            role = link.get('role', '')

            # Determine statement type from role
            statement_type = self._determine_statement_type_from_role(role)

            # Add to hierarchy
            self.hierarchy['calculation'][parent].append({
                'child': child,
                'weight': weight,
                'role': role,
                'statement_type': statement_type
            })

        # Process definition links
        for link in self.definition_links:
            parent = link['from']
            child = link['to']
            arcrole = link.get('arcrole', '')
            role = link.get('role', '')

            # Add to hierarchy
            self.hierarchy['definition'][parent].append({
                'child': child,
                'arcrole': arcrole,
                'role': role
            })

    def _map_facts_to_hierarchy(self):
        """Map facts to the hierarchy."""
        # Create a map of concept names to facts
        concept_facts = defaultdict(list)
        for fact in self.facts:
            name = fact['name']
            concept_facts[name].append(fact)

        # Add hierarchy information to facts
        for fact in self.facts:
            name = fact['name']

            # Find the concept in the hierarchy
            statement_type = "Unknown"
            level = -1

            # Check if it's a top-level concept
            for parent, children in self.hierarchy['presentation'].items():
                for child_info in children:
                    if child_info['child'] == name:
                        statement_type = child_info.get('statement_type', 'Unknown')
                        level = 1  # Direct child of a parent
                        fact['parent'] = parent
                        break

                if parent == name:
                    statement_type = self._determine_statement_type_from_concept(name)
                    level = 0  # Top-level concept
                    break

            # If not found, try to determine from concept name
            if statement_type == "Unknown":
                statement_type = self._determine_statement_type_from_concept(name)

            # Add hierarchy information to fact
            fact['statement_type'] = statement_type
            fact['level'] = level

            # Add label if available
            if name in self.labels:
                standard_label = self.labels[name].get('http://www.xbrl.org/2003/role/label')
                if standard_label:
                    fact['label'] = standard_label

    def _determine_statement_type_from_role(self, role):
        """
        Determine statement type from role URI.

        Args:
            role: The role URI

        Returns:
            The statement type
        """
        role_lower = role.lower()

        if any(term in role_lower for term in ["balance", "financial position", "statement of financial position"]):
            return "Balance_Sheet"
        elif any(term in role_lower for term in ["income", "operations", "profit", "loss", "comprehensive income"]):
            return "Income_Statement"
        elif any(term in role_lower for term in ["cash flow", "cashflow", "statement of cash flows"]):
            return "Cash_Flow_Statement"
        elif any(term in role_lower for term in ["equity", "stockholder", "shareholder", "changes in equity"]):
            return "Statement_Of_Equity"
        else:
            return "Unknown"

    def _determine_statement_type_from_concept(self, concept):
        """
        Determine statement type from concept name.

        Args:
            concept: The concept name

        Returns:
            The statement type
        """
        concept_lower = concept.lower()

        if any(term in concept_lower for term in ["asset", "liability", "equity", "stockholdersequity"]):
            return "Balance_Sheet"
        elif any(term in concept_lower for term in ["revenue", "income", "expense", "earnings", "profit", "loss"]):
            return "Income_Statement"
        elif any(term in concept_lower for term in ["cashflow", "cash", "operating", "investing", "financing"]):
            return "Cash_Flow_Statement"
        elif any(term in concept_lower for term in ["equity", "stockholder", "shareholder", "retained"]):
            return "Statement_Of_Equity"
        else:
            return "Unknown"

    def _get_file_content(self, url):
        """
        Get file content from URL, local file, or cache.

        Args:
            url: The URL or file path to fetch

        Returns:
            The file content or None if not found
        """
        # Check if it's a local file
        if os.path.exists(url):
            logging.info(f"Reading local file: {url}")
            try:
                with open(url, 'r', encoding='utf-8', errors='ignore') as f:
                    return f.read()
            except Exception as e:
                logging.error(f"Error reading local file {url}: {str(e)}")
                return None

        # Generate cache filename for remote URLs
        if url.startswith(('http://', 'https://')):
            parsed_url = urlparse(url)
            cache_filename = self.cache_dir / f"{parsed_url.netloc}_{Path(parsed_url.path).name}"

            # Check if file exists in cache
            if cache_filename.exists():
                logging.info(f"Using cached file: {cache_filename}")
                with open(cache_filename, 'r', encoding='utf-8', errors='ignore') as f:
                    return f.read()

            # Download file with SEC-compliant headers
            try:
                logging.info(f"Downloading file: {url}")

                # Set up SEC-compliant headers
                headers = {
                    "User-Agent": "NativeLLM_XBRLHierarchyTester (info@exascale.capital)",
                    "Accept-Encoding": "gzip, deflate",
                    "Host": "www.sec.gov" if "sec.gov" in url else None,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Connection": "keep-alive"
                }

                # Add rate limiting
                time.sleep(0.1)  # Simple rate limiting - 10 requests per second max

                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()

                # Save to cache
                with open(cache_filename, 'w', encoding='utf-8') as f:
                    f.write(response.text)

                return response.text
            except Exception as e:
                logging.error(f"Error downloading file {url}: {str(e)}")
                return None
        else:
            logging.error(f"File not found and not a valid URL: {url}")
            return None

def main():
    parser = argparse.ArgumentParser(description="Test XBRL hierarchy extraction")
    parser.add_argument("--html", required=True, help="Path to HTML file with inline XBRL")
    parser.add_argument("--output", help="Output file path (default: hierarchy_output.json)")

    args = parser.parse_args()

    # Check if HTML file exists
    if not os.path.exists(args.html):
        logging.error(f"HTML file not found: {args.html}")
        return 1

    # Determine output file path
    output_path = args.output or "hierarchy_output.json"

    # Extract hierarchy
    tester = XBRLHierarchyTester()
    hierarchy = tester.extract_hierarchy_from_html(args.html)

    # Save output
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(hierarchy, f, indent=2)

    logging.info(f"Hierarchy saved to {output_path}")

    # Print summary
    print("\nXBRL Hierarchy Summary:")
    print(f"  Concepts: {len(hierarchy['concepts'])}")
    print(f"  Presentation Relationships: {sum(len(children) for children in hierarchy['presentation_hierarchy'].values())}")
    print(f"  Calculation Relationships: {sum(len(children) for children in hierarchy['calculation_hierarchy'].values())}")
    print(f"  Definition Relationships: {sum(len(children) for children in hierarchy['definition_hierarchy'].values())}")
    print(f"  Labels: {len(hierarchy['labels'])}")
    print(f"  Facts: {len(hierarchy['facts'])}")

    # Print sample hierarchy
    print("\nSample Presentation Hierarchy:")
    for i, (parent, children) in enumerate(hierarchy['presentation_hierarchy'].items()):
        if i >= 3:  # Limit to first 3 parents
            break

        parent_label = hierarchy['labels'].get(parent, {}).get('http://www.xbrl.org/2003/role/label', parent)
        print(f"  {parent_label} ({parent}):")

        for j, child_info in enumerate(children):
            if j >= 5:  # Limit to first 5 children per parent
                print(f"    ... and {len(children) - 5} more")
                break

            child = child_info['child']
            child_label = hierarchy['labels'].get(child, {}).get('http://www.xbrl.org/2003/role/label', child)
            print(f"    - {child_label} ({child})")

    return 0

if __name__ == "__main__":
    sys.exit(main())
