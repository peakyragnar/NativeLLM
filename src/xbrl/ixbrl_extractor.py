"""
Module for extracting data from iXBRL documents.
"""

import os
import sys
import re
import json
from bs4 import BeautifulSoup

def is_ixbrl_document(soup):
    """Enhanced iXBRL detection covering more formats"""
    # Current checks for HTML tag namespace
    html_tag = soup.find('html')
    if html_tag:
        for attr_name, attr_value in html_tag.attrs.items():
            if 'xmlns:ix' in attr_name or (isinstance(attr_value, str) and 'inline' in attr_value.lower() and 'xbrl' in attr_value.lower()):
                return True
    
    # Check for ix: elements by name (more reliable than current approach)
    for tag_prefix in ['ix:', 'ix-', 'ix.']:
        elements = soup.find_all(lambda tag: tag.name and tag.name.startswith(tag_prefix))
        if elements:
            return True
    
    # Check for standard ix: elements
    ix_elements = soup.find_all(lambda tag: tag.name and ':' in tag.name and tag.name.split(':')[0] == 'ix')
    if len(ix_elements) > 0:
        return True
    
    # Check entire document string for namespace patterns
    doc_str = str(soup)
    namespace_patterns = [
        'http://www.xbrl.org/2013/inlineXBRL',
        'http://www.xbrl.org/inlineXBRL',
        'inline XBRL',
        'ix:nonfraction',
        'ix:nonnumeric',
        'ix:header',
        'ix:references',
        'ix:resources'
    ]
    
    for pattern in namespace_patterns:
        if pattern in doc_str:
            return True
            
    # Look for SEC XBRL viewer
    if "XBRL Viewer" in str(soup.title) or "Inline XBRL" in str(soup.title):
        return True
            
    return False

def extract_hidden_data(soup):
    """Extract hidden iXBRL data sections"""
    hidden_sections = []
    
    # Find the ix:hidden section
    hidden_section = soup.find('ix:hidden')
    if hidden_section:
        hidden_sections.append(hidden_section)
    
    # Find other hidden sections that might contain iXBRL data
    hidden_divs = soup.find_all('div', style=lambda style: style and 'display:none' in style)
    for div in hidden_divs:
        if div.find(lambda tag: tag.name and ':' in tag.name):
            hidden_sections.append(div)
    
    return hidden_sections

def extract_contexts(soup, hidden_sections=None):
    """Extract context definitions from the document"""
    contexts = {}
    
    # Look for context definitions in resources section
    resources = soup.find('ix:resources')
    if resources:
        context_elements = resources.find_all('xbrli:context')
        
        for context in context_elements:
            context_id = context.get('id')
            if not context_id:
                continue
            
            period = context.find('xbrli:period')
            entity = context.find('xbrli:entity')
            
            context_data = {
                'id': context_id,
                'period': {},
                'entity': {},
                'dimensions': {}
            }
            
            # Extract period information
            if period:
                instant = period.find('xbrli:instant')
                if instant and instant.text:
                    context_data['period']['instant'] = instant.text.strip()
                
                start_date = period.find('xbrli:startDate')
                end_date = period.find('xbrli:endDate')
                if start_date and end_date:
                    context_data['period']['startDate'] = start_date.text.strip()
                    context_data['period']['endDate'] = end_date.text.strip()
            
            # Extract entity information
            if entity:
                identifier = entity.find('xbrli:identifier')
                if identifier:
                    context_data['entity']['identifier'] = identifier.text.strip()
                    context_data['entity']['scheme'] = identifier.get('scheme', '')
                
                # Extract segment dimensions
                segment = entity.find('xbrli:segment')
                if segment:
                    for member in segment.find_all('xbrldi:explicitMember'):
                        dimension = member.get('dimension', '').split(':')[-1]
                        value = member.text.strip()
                        context_data['dimensions'][dimension] = value
            
            contexts[context_id] = context_data
    
    return contexts

def extract_units(soup, hidden_sections=None):
    """Extract unit definitions from the document"""
    units = {}
    
    # Look for unit definitions in resources section
    resources = soup.find('ix:resources')
    if resources:
        unit_elements = resources.find_all('xbrli:unit')
        
        for unit in unit_elements:
            unit_id = unit.get('id')
            if not unit_id:
                continue
            
            measure = unit.find('xbrli:measure')
            divide = unit.find('xbrli:divide')
            
            if measure:
                # Simple measure (e.g., USD)
                units[unit_id] = {
                    'id': unit_id,
                    'measure': measure.text.strip(),
                    'type': 'simple'
                }
            elif divide:
                # Divide measure (e.g., USD/shares for EPS)
                numerator = divide.find('xbrli:unitNumerator')
                denominator = divide.find('xbrli:unitDenominator')
                
                numerator_measure = numerator.find('xbrli:measure') if numerator else None
                denominator_measure = denominator.find('xbrli:measure') if denominator else None
                
                units[unit_id] = {
                    'id': unit_id,
                    'type': 'divide',
                    'numerator': numerator_measure.text.strip() if numerator_measure else '',
                    'denominator': denominator_measure.text.strip() if denominator_measure else ''
                }
    
    return units

def extract_facts(soup, contexts=None, units=None):
    """Extract facts from the document"""
    facts = []
    
    # Extract facts from ix:nonFraction elements (numerical values)
    for fact in soup.find_all('ix:nonfraction'):
        fact_data = {
            'type': 'nonFraction',
            'name': fact.get('name', ''),
            'value': fact.text.strip(),
            'context_ref': fact.get('contextref', ''),
            'unit_ref': fact.get('unitref', ''),
            'decimals': fact.get('decimals', ''),
            'scale': fact.get('scale', ''),
            'format': fact.get('format', '')
        }
        facts.append(fact_data)
    
    # Extract facts from ix:nonNumeric elements (text values)
    for fact in soup.find_all('ix:nonnumeric'):
        fact_data = {
            'type': 'nonNumeric',
            'name': fact.get('name', ''),
            'value': fact.text.strip(),
            'context_ref': fact.get('contextref', ''),
            'format': fact.get('format', '')
        }
        facts.append(fact_data)
    
    # Extract facts from hidden sections
    hidden_facts = soup.select('ix\:hidden ix\:nonfraction, ix\:hidden ix\:nonnumeric')
    for fact in hidden_facts:
        if fact.name == 'ix:nonfraction':
            fact_data = {
                'type': 'nonFraction',
                'name': fact.get('name', ''),
                'value': fact.text.strip(),
                'context_ref': fact.get('contextref', ''),
                'unit_ref': fact.get('unitref', ''),
                'decimals': fact.get('decimals', ''),
                'scale': fact.get('scale', ''),
                'format': fact.get('format', ''),
                'hidden': True
            }
        else:  # ix:nonNumeric
            fact_data = {
                'type': 'nonNumeric',
                'name': fact.get('name', ''),
                'value': fact.text.strip(),
                'context_ref': fact.get('contextref', ''),
                'format': fact.get('format', ''),
                'hidden': True
            }
        facts.append(fact_data)
    
    return facts

def extract_ixbrl_data(html_content, filing_metadata=None):
    """
    Extract iXBRL data from HTML content.
    
    Parameters:
    - html_content: The HTML content containing iXBRL data
    - filing_metadata: Optional metadata about the filing
    
    Returns:
    - Dictionary containing contexts, units, and facts
    """
    try:
        # Parse HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Check if this is an iXBRL document
        if not is_ixbrl_document(soup):
            return {"error": "Document does not appear to be an iXBRL document"}
        
        # Extract hidden data sections
        hidden_sections = extract_hidden_data(soup)
        
        # Extract contexts, units, and facts
        contexts = extract_contexts(soup, hidden_sections)
        units = extract_units(soup, hidden_sections)
        facts = extract_facts(soup, contexts, units)
        
        # Get document metadata
        document_info = {}
        
        if filing_metadata:
            document_info = filing_metadata.copy()
        
        # Add document level metadata if available in the document
        dei_facts = [f for f in facts if f.get('name', '').startswith('dei:')]
        for fact in dei_facts:
            if fact.get('name') == 'dei:DocumentType':
                document_info['document_type'] = fact.get('value')
            elif fact.get('name') == 'dei:DocumentPeriodEndDate':
                document_info['period_end_date'] = fact.get('value')
            elif fact.get('name') == 'dei:EntityRegistrantName':
                document_info['entity_name'] = fact.get('value')
            elif fact.get('name') == 'dei:EntityCentralIndexKey':
                document_info['cik'] = fact.get('value')
        
        return {
            "success": True,
            "document_info": document_info,
            "contexts": contexts,
            "units": units,
            "facts": facts,
            "fact_count": len(facts),
            "context_count": len(contexts),
            "unit_count": len(units)
        }
    except Exception as e:
        return {"error": f"Error extracting iXBRL data: {str(e)}"}

def process_ixbrl_file(file_path, filing_metadata=None):
    """Process an iXBRL file and extract its data"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        result = extract_ixbrl_data(html_content, filing_metadata)
        return result
    except Exception as e:
        return {"error": f"Error processing iXBRL file: {str(e)}"}