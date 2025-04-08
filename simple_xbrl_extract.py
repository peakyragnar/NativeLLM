#!/usr/bin/env python3
"""
Simple XBRL Hierarchy Extractor

This script extracts hierarchical relationships from XBRL linkbase files
without complex processing that might cause infinite loops.
"""

import os
import sys
import json
import logging
import argparse
import re
import requests
from bs4 import BeautifulSoup
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class SimpleXBRLExtractor:
    """Simple XBRL hierarchy extractor that avoids complex processing."""

    def __init__(self, html_path):
        """Initialize the extractor with the HTML file path."""
        self.html_path = html_path
        self.schema_refs = []
        self.linkbase_refs = []
        self.concepts = {}
        self.presentation_links = []
        self.calculation_links = []
        self.definition_links = []
        self.labels = {}
        self.facts = []
        self.cache_dir = "xbrl_cache"

        # Create cache directory if it doesn't exist
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def extract_hierarchy(self):
        """Extract XBRL hierarchy from HTML file."""
        logging.info(f"Extracting XBRL hierarchy from {self.html_path}")

        # Extract schema and linkbase references from HTML
        self._extract_refs_from_html()

        # Process schema files
        self._process_schema_files()

        # Extract facts from HTML
        self._extract_facts_from_html()

        # Build hierarchy
        hierarchy = self._build_hierarchy()

        return hierarchy

    def _extract_refs_from_html(self):
        """Extract schema and linkbase references from HTML file."""
        try:
            with open(self.html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find schema references
            schema_tags = soup.find_all('link', {'type': 'application/xml+xbrl'})
            for tag in schema_tags:
                href = tag.get('href')
                if href:
                    self.schema_refs.append(href)
                    logging.info(f"Found schema reference: {href}")

            # Find linkbase references
            linkbase_tags = soup.find_all('link', {'type': 'application/xml+linkbase'})
            for tag in linkbase_tags:
                href = tag.get('href')
                if href:
                    self.linkbase_refs.append(href)
                    logging.info(f"Found linkbase reference: {href}")

            # Try to find schema reference in script tags (NVDA specific)
            if not self.schema_refs:
                script_tags = soup.find_all('script')
                for script in script_tags:
                    script_text = script.get_text()
                    if 'linkbase' in script_text or 'schema' in script_text:
                        # Look for schema reference
                        schema_match = re.search(r'href=["\']([^"\']*.xsd)["\']', script_text)
                        if schema_match:
                            schema_ref = schema_match.group(1)
                            self.schema_refs.append(schema_ref)
                            logging.info(f"Found schema reference in script: {schema_ref}")

            # If still no schema refs, try to construct them from the filing info
            if not self.schema_refs:
                # Extract accession number from HTML path
                accession_match = re.search(r'/(\d+)/', self.html_path)
                if accession_match:
                    accession = accession_match.group(1)
                    # Extract ticker from HTML path
                    ticker_match = re.search(r'/([A-Z]+)/', self.html_path)
                    if ticker_match:
                        ticker = ticker_match.group(1).lower()
                        # Construct schema reference
                        schema_ref = f"https://www.sec.gov/Archives/edgar/data/1045810/{accession}/{ticker}-20230129.xsd"
                        self.schema_refs.append(schema_ref)
                        logging.info(f"Constructed schema reference: {schema_ref}")

                        # Construct linkbase references
                        for linkbase_type in ['pre', 'cal', 'def', 'lab']:
                            linkbase_ref = f"https://www.sec.gov/Archives/edgar/data/1045810/{accession}/{ticker}-20230129_{linkbase_type}.xml"
                            self.linkbase_refs.append(linkbase_ref)
                            logging.info(f"Constructed linkbase reference: {linkbase_ref}")

        except Exception as e:
            logging.error(f"Error extracting references from HTML: {str(e)}")

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
                linkbase_refs = schema_soup.find_all('link:linkbaseRef')
                if not linkbase_refs:
                    linkbase_refs = schema_soup.find_all(re.compile(r'.*:linkbaseRef'))

                for ref in linkbase_refs:
                    href = ref.get('xlink:href')
                    if href:
                        # Resolve relative URLs
                        if not href.startswith('http'):
                            base_url = '/'.join(schema_ref.split('/')[:-1])
                            href = f"{base_url}/{href}"

                        self.linkbase_refs.append(href)
                        logging.info(f"Found linkbase reference: {href}")

                # Extract concepts
                elements = schema_soup.find_all('xsd:element')
                if not elements:
                    elements = schema_soup.find_all('element')

                for element in elements:
                    name = element.get('name')
                    id = element.get('id')

                    if name:
                        self.concepts[name] = {
                            'id': id,
                            'name': name,
                            'type': element.get('type'),
                            'substitution_group': element.get('substitutionGroup'),
                            'balance': element.get('xbrli:balance'),
                            'period_type': element.get('xbrli:periodType')
                        }

                logging.info(f"Extracted {len(self.concepts)} concepts from schema")

            except Exception as e:
                logging.error(f"Error processing schema file {schema_ref}: {str(e)}")

        # Process linkbase files
        for linkbase_ref in self.linkbase_refs:
            self._process_linkbase_file(linkbase_ref)

    def _process_linkbase_file(self, linkbase_ref):
        """Process a linkbase file."""
        try:
            # Download or use cached linkbase file
            linkbase_content = self._get_file_content(linkbase_ref)
            if not linkbase_content:
                return

            # Parse linkbase file
            linkbase_soup = BeautifulSoup(linkbase_content, 'lxml-xml')

            # Determine linkbase type from filename
            if '_pre.xml' in linkbase_ref.lower():
                self._process_presentation_linkbase(linkbase_soup)
            elif '_cal.xml' in linkbase_ref.lower():
                self._process_calculation_linkbase(linkbase_soup)
            elif '_def.xml' in linkbase_ref.lower():
                self._process_definition_linkbase(linkbase_soup)
            elif '_lab.xml' in linkbase_ref.lower():
                self._process_label_linkbase(linkbase_soup)
            else:
                # Try to determine type from content
                if linkbase_soup.find('link:presentationLink') or linkbase_soup.find(re.compile(r'.*:presentationLink')):
                    self._process_presentation_linkbase(linkbase_soup)
                if linkbase_soup.find('link:calculationLink') or linkbase_soup.find(re.compile(r'.*:calculationLink')):
                    self._process_calculation_linkbase(linkbase_soup)
                if linkbase_soup.find('link:definitionLink') or linkbase_soup.find(re.compile(r'.*:definitionLink')):
                    self._process_definition_linkbase(linkbase_soup)
                if linkbase_soup.find('link:labelLink') or linkbase_soup.find(re.compile(r'.*:labelLink')):
                    self._process_label_linkbase(linkbase_soup)

        except Exception as e:
            logging.error(f"Error processing linkbase file {linkbase_ref}: {str(e)}")

    def _process_presentation_linkbase(self, linkbase_soup):
        """Process a presentation linkbase file."""
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

            # Find all locators in this link
            loc_elements = link.find_all(['link:loc', 'loc'])

            # Build a map of labels to hrefs
            label_to_href = {}
            for loc in loc_elements:
                label = loc.get('xlink:label')
                href = loc.get('xlink:href') or loc.get('href')
                if label and href:
                    label_to_href[label] = href

            for arc in arcs:
                from_attr = arc.get('xlink:from') or arc.get('from')
                to_attr = arc.get('xlink:to') or arc.get('to')
                order = arc.get('order')

                if from_attr and to_attr:
                    # Direct label lookup
                    from_href = label_to_href.get(from_attr)
                    to_href = label_to_href.get(to_attr)

                    # Traditional locator lookup if direct lookup fails
                    if not from_href or not to_href:
                        from_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': from_attr})
                        to_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': to_attr})

                        if from_loc and to_loc:
                            from_href = from_loc.get('xlink:href') or from_loc.get('href')
                            to_href = to_loc.get('xlink:href') or to_loc.get('href')

                    if from_href and to_href:
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
        """Process a calculation linkbase file."""
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

            # Find all locators in this link
            loc_elements = link.find_all(['link:loc', 'loc'])

            # Build a map of labels to hrefs
            label_to_href = {}
            for loc in loc_elements:
                label = loc.get('xlink:label')
                href = loc.get('xlink:href') or loc.get('href')
                if label and href:
                    label_to_href[label] = href

            for arc in arcs:
                from_attr = arc.get('xlink:from') or arc.get('from')
                to_attr = arc.get('xlink:to') or arc.get('to')
                weight = arc.get('weight')

                if from_attr and to_attr:
                    # Direct label lookup
                    from_href = label_to_href.get(from_attr)
                    to_href = label_to_href.get(to_attr)

                    # Traditional locator lookup if direct lookup fails
                    if not from_href or not to_href:
                        from_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': from_attr})
                        to_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': to_attr})

                        if from_loc and to_loc:
                            from_href = from_loc.get('xlink:href') or from_loc.get('href')
                            to_href = to_loc.get('xlink:href') or to_loc.get('href')

                    if from_href and to_href:
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
        """Process a definition linkbase file."""
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

            # Find all locators in this link
            loc_elements = link.find_all(['link:loc', 'loc'])

            # Build a map of labels to hrefs
            label_to_href = {}
            for loc in loc_elements:
                label = loc.get('xlink:label')
                href = loc.get('xlink:href') or loc.get('href')
                if label and href:
                    label_to_href[label] = href

            for arc in arcs:
                from_attr = arc.get('xlink:from') or arc.get('from')
                to_attr = arc.get('xlink:to') or arc.get('to')
                arcrole = arc.get('xlink:arcrole') or arc.get('arcrole')

                if from_attr and to_attr:
                    # Direct label lookup
                    from_href = label_to_href.get(from_attr)
                    to_href = label_to_href.get(to_attr)

                    # Traditional locator lookup if direct lookup fails
                    if not from_href or not to_href:
                        from_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': from_attr})
                        to_loc = link.find(['link:loc', 'loc'], attrs={'xlink:label': to_attr})

                        if from_loc and to_loc:
                            from_href = from_loc.get('xlink:href') or from_loc.get('href')
                            to_href = to_loc.get('xlink:href') or to_loc.get('href')

                    if from_href and to_href:
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
        """Process a label linkbase file."""
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
        """Extract concept name from href attribute."""
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

        # Try removing namespace prefixes
        for concept in self.concepts:
            concept_name = concept.split(':')[-1] if ':' in concept else concept
            if fragment.endswith(concept_name):
                return concept

        return None

    def _extract_facts_from_html(self):
        """Extract facts from HTML file."""
        try:
            with open(self.html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find all XBRL facts
            fact_tags = soup.find_all(attrs={'contextref': True})

            for tag in fact_tags:
                name = tag.name
                context_ref = tag.get('contextref')
                unit_ref = tag.get('unitref')
                decimals = tag.get('decimals')
                value = tag.get_text(strip=True)

                if name and context_ref:
                    fact = {
                        'name': name,
                        'context_ref': context_ref,
                        'unit_ref': unit_ref,
                        'decimals': decimals,
                        'value': value
                    }

                    # Add label if available
                    if name in self.labels:
                        standard_label = self.labels[name].get('http://www.xbrl.org/2003/role/label')
                        if standard_label:
                            fact['label'] = standard_label

                    self.facts.append(fact)

            logging.info(f"Extracted {len(self.facts)} facts from HTML")

        except Exception as e:
            logging.error(f"Error extracting facts from HTML: {str(e)}")

    def _build_hierarchy(self):
        """Build hierarchical relationships from extracted links."""
        # Initialize hierarchy
        hierarchy = {
            'concepts': self.concepts,
            'labels': self.labels,
            'facts': self.facts,
            'presentation_links': self.presentation_links,
            'calculation_links': self.calculation_links,
            'definition_links': self.definition_links,
            'presentation_hierarchy': defaultdict(list),
            'calculation_hierarchy': defaultdict(list),
            'definition_hierarchy': defaultdict(list)
        }

        # Process presentation links
        for link in self.presentation_links:
            parent = link['from']
            child = link['to']
            order = link.get('order', '0')
            role = link.get('role', '')

            # Determine statement type from role
            statement_type = self._determine_statement_type_from_role(role)

            # Add to hierarchy
            hierarchy['presentation_hierarchy'][parent].append({
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
            hierarchy['calculation_hierarchy'][parent].append({
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
            hierarchy['definition_hierarchy'][parent].append({
                'child': child,
                'arcrole': arcrole,
                'role': role
            })

        return hierarchy

    def _determine_statement_type_from_role(self, role):
        """Determine statement type from role URI."""
        if not role:
            return "Unknown"

        role_lower = role.lower()

        # Handle NVDA's specific role URIs
        if 'nvidia.com/role/consolidatedbalancesheet' in role_lower:
            return "Balance_Sheet"
        elif 'nvidia.com/role/consolidatedstatementsofincome' in role_lower:
            return "Income_Statement"
        elif 'nvidia.com/role/consolidatedstatementsofcashflows' in role_lower:
            return "Cash_Flow_Statement"
        elif 'nvidia.com/role/consolidatedstatementsofstockholdersequity' in role_lower:
            return "Statement_Of_Equity"

        # Handle standard role URI patterns
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

    def _get_file_content(self, url):
        """Get file content from URL or local cache."""
        # Check if it's a local file
        if os.path.exists(url):
            with open(url, 'r', encoding='utf-8') as f:
                return f.read()

        # Create cache filename
        cache_filename = os.path.join(self.cache_dir, url.replace('://', '_').replace('/', '_'))

        # Check if file is in cache
        if os.path.exists(cache_filename):
            with open(cache_filename, 'r', encoding='utf-8') as f:
                return f.read()

        # Download file
        try:
            headers = {
                'User-Agent': 'SimpleXBRLExtractor (info@exascale.capital)'
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            # Save to cache
            with open(cache_filename, 'w', encoding='utf-8') as f:
                f.write(response.text)

            return response.text
        except Exception as e:
            logging.error(f"Error downloading {url}: {str(e)}")
            return None

def main():
    parser = argparse.ArgumentParser(description="Simple XBRL hierarchy extraction")
    parser.add_argument("--html", required=True, help="Path to HTML file with inline XBRL")
    parser.add_argument("--output", help="Output file path (default: simple_hierarchy_output.json)")

    args = parser.parse_args()

    # Check if HTML file exists
    if not os.path.exists(args.html):
        logging.error(f"HTML file not found: {args.html}")
        return 1

    # Determine output file path
    output_path = args.output or "simple_hierarchy_output.json"

    try:
        # Extract hierarchy
        extractor = SimpleXBRLExtractor(args.html)
        hierarchy = extractor.extract_hierarchy()

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

        return 0
    except Exception as e:
        logging.error(f"Error extracting hierarchy: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
