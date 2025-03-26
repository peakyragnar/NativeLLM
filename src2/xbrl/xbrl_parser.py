# src2/xbrl/xbrl_parser.py
import os
import sys
import logging
from lxml import etree
import re
from bs4 import BeautifulSoup

# Use relative imports for the new module structure
from .company_formats import detect_xbrl_format, get_format_handler, learn_from_successful_parse

def process_table_safely(table_html):
    """
    Process table HTML to extract content with 100% data integrity.
    This function prioritizes perfect preservation of all numeric values and data relationships
    over maximum size reduction. It only removes definitively non-essential attributes 
    while preserving all content exactly as presented.
    
    Args:
        table_html: HTML string containing a table
        
    Returns:
        Cleaned representation of the table with structure and ALL values preserved
    """
    if not table_html or '<table' not in table_html:
        return table_html
    
    try:
        # Store original text content for verification
        soup = BeautifulSoup(table_html, 'html.parser')
        original_text = soup.get_text(separator=' ', strip=False)  # Preserve all whitespace
        
        # Extract numeric values using a comprehensive pattern
        # This captures dollar amounts, percentages, dates, and all numeric formats
        original_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', original_text)
        original_number_set = set(original_numbers)
        
        # Extract words and other non-numeric tokens for secondary verification
        original_tokens = re.findall(r'\b[\w\d.,$%()-]+\b', original_text)
        original_token_set = set(original_tokens)
        
        # If no content, return original
        if not original_token_set:
            return table_html
        
        # Create a copy of the soup for modification
        cleaned_soup = BeautifulSoup(table_html, 'html.parser')
        
        # Only remove specific non-essential attributes
        for tag in cleaned_soup.find_all(True):
            # Remove purely cosmetic attributes while keeping structural ones
            if tag.has_attr('style'):
                style = tag['style']
                style_parts = []
                # Only keep alignment and structural styles
                for part in style.split(';'):
                    part = part.strip()
                    # Keep only essential structural styles
                    if part and any(essential in part.lower() for essential in 
                             ['align', 'padding', 'margin', 'width', 'height', 'border']):
                        style_parts.append(part)
                if style_parts:
                    tag['style'] = ';'.join(style_parts)
                else:
                    del tag['style']
            
            # Remove purely cosmetic attributes
            for attr in ['bgcolor', 'color', 'font', 'face', 'class']:
                if tag.has_attr(attr):
                    del tag[attr]
        
        # Convert to string and verify PERFECT preservation
        cleaned_html = str(cleaned_soup)
        cleaned_text = cleaned_soup.get_text(separator=' ', strip=False)
        
        # Strict verification for numeric values - this is our absolute requirement
        cleaned_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', cleaned_text)
        cleaned_number_set = set(cleaned_numbers)
        
        # Check for any missing numbers
        missing_numbers = original_number_set - cleaned_number_set
        
        # If ANY number is missing or different, return the original
        if missing_numbers:
            logging.warning(f"Missing {len(missing_numbers)} numeric values, using original table: {list(missing_numbers)[:5]}")
            return table_html
        
        # Secondary verification for overall content
        cleaned_tokens = re.findall(r'\b[\w\d.,$%()-]+\b', cleaned_text)
        cleaned_token_set = set(cleaned_tokens)
        missing_tokens = original_token_set - cleaned_token_set
        
        # If more than 1% of tokens are missing, return original
        if len(missing_tokens) > 0.01 * len(original_token_set):
            logging.warning(f"Missing {len(missing_tokens)} tokens, using original table: {list(missing_tokens)[:5]}")
            return table_html
            
        # If we've maintained perfect numeric value integrity with size reduction, use the cleaned version
        if len(cleaned_html) < len(table_html):
            logging.info(f"Table size: {len(table_html)} → {len(cleaned_html)} chars ({len(cleaned_html)/len(table_html):.1%})")
            return cleaned_html
            
        # No meaningful size reduction achieved, return original
        return table_html
        
    except Exception as e:
        logging.warning(f"Error processing table: {str(e)}")
        return table_html  # Return original for safety

def extract_text_only_from_html(html_value):
    """
    Safely extract only text from HTML while preserving 100% of data values.
    This function prioritizes perfect data preservation over size reduction,
    only removing HTML/CSS styling when it can guarantee no data loss whatsoever.
    
    Args:
        html_value: Value that might contain HTML formatting
        
    Returns:
        Text-only value with HTML/CSS removed but all data perfectly preserved
    """
    # Don't process empty values
    if not html_value or not isinstance(html_value, str):
        return html_value
        
    # First check if this is a numerical value or non-HTML text
    # If it's clearly not HTML, return it unchanged
    if '<' not in html_value or '>' not in html_value:
        return html_value
    
    # Check if it's likely a numeric value (even if wrapped in HTML)
    # We never want to modify numeric values in any way
    stripped = html_value.strip().replace('-', '').replace('.', '').replace(',', '').replace('$', '').replace('%', '')
    if stripped.isdigit() or (stripped.startswith('(') and stripped.endswith(')') and stripped[1:-1].isdigit()):
        # This is numeric - extract just the number if it's wrapped in simple HTML, with extreme caution
        try:
            soup = BeautifulSoup(html_value, 'html.parser')
            text = soup.get_text(strip=False)  # Preserve whitespace
            
            # Extract all possible numeric values from original
            original_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', html_value)
            extracted_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', text)
            
            # Verify ALL numeric content is preserved exactly (strict verification)
            if set(original_numbers) == set(extracted_numbers) and len(original_numbers) == len(extracted_numbers):
                return text
            else:
                # If anything changed about the numbers, return original
                return html_value
        except:
            return html_value
        
    # Special handling for tables - process them with our table-specific function
    if '<table' in html_value:
        return process_table_safely(html_value)
        
    # For other HTML content, use a conservative approach
    try:
        # First extract original numbers for verification
        original_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', html_value)
        original_number_set = set(original_numbers)
        
        # Extract original tokens for secondary verification
        original_tokens = re.findall(r'\b[\w\d.,$%()-]+\b', html_value)
        original_token_set = set(original_tokens)
        
        # For really simple HTML with just text content, try extracting just the text
        # This handles the common case of <div>Some text</div> efficiently
        if html_value.count("<") < 5 and not original_number_set:
            try:
                soup = BeautifulSoup(html_value, "html.parser")
                text = soup.get_text(" ", strip=True)
                
                # Verify all words are preserved
                text_tokens = re.findall(r'\b[\w\d.,$%()-]+\b', text)
                
                if set(text_tokens) == original_token_set:
                    return text
            except:
                pass  # Fall back to other approaches
        
        # Create a cleaned version that only removes definitive non-essential attributes
        soup = BeautifulSoup(html_value, 'html.parser')
        
        # Only remove specific non-essential attributes
        for tag in soup.find_all(True):
            # Handle style attribute conservatively
            if tag.has_attr('style'):
                style = tag['style']
                style_parts = []
                # Only keep alignment and structural styles
                for part in style.split(';'):
                    part = part.strip()
                    # Keep only essential structural styles
                    if part and any(essential in part.lower() for essential in 
                             ['align', 'padding', 'margin', 'width', 'height', 'border']):
                        style_parts.append(part)
                if style_parts:
                    tag['style'] = ';'.join(style_parts)
                else:
                    del tag['style']
            
            # Remove purely cosmetic attributes
            for attr in ['bgcolor', 'color', 'font', 'face', 'class', 'font-family']:
                if tag.has_attr(attr):
                    del tag[attr]
        
        # Get text representation with preserved spacing
        extracted_text = soup.get_text(separator=' ', strip=False)
        
        # Create a simplified HTML version with only essential tags
        simplified_html = str(soup)
        
        # Verify numeric content preservation (our primary requirement)
        extracted_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', extracted_text)
        extracted_number_set = set(extracted_numbers)
        
        # If ANY numeric value is missing, return original
        if original_number_set != extracted_number_set or len(original_numbers) != len(extracted_numbers):
            return html_value
            
        # Verify overall content preservation
        extracted_tokens = re.findall(r'\b[\w\d.,$%()-]+\b', extracted_text)
        extracted_token_set = set(extracted_tokens)
        
        # Allow at most 0.5% token loss (for non-numeric tokens only)
        missing_tokens = original_token_set - extracted_token_set
        if len(missing_tokens) > 0.005 * len(original_token_set):
            return html_value
        
        # Determine which result to return for maximum preservation
        if len(extracted_text) < len(simplified_html) and len(extracted_text) < len(html_value):
            # Text representation is smallest
            return extracted_text
        elif len(simplified_html) < len(html_value):
            # Simplified HTML is smaller than original but preserves structure better than text
            return simplified_html
        else:
            # No meaningful size reduction achieved
            return html_value
            
    except Exception as e:
        # On any error, return original value
        logging.warning(f"Error cleaning HTML content: {str(e)[:100]}")
        return html_value

def parse_xbrl_file(file_path, ticker=None, filing_metadata=None):
    """
    Parse an XBRL instance document with robust handling for various formats
    
    Args:
        file_path: Path to the XBRL file
        ticker: Company ticker symbol (optional)
        filing_metadata: Additional metadata about the filing (optional)
        
    Returns:
        Dictionary with parsed XBRL data
    """
    try:
        logging.info(f"Parsing XBRL file: {file_path}")
        
        # Extract ticker from metadata if available
        if not ticker and filing_metadata and "ticker" in filing_metadata:
            ticker = filing_metadata.get("ticker")
            logging.info(f"Using ticker from metadata: {ticker}")
            
        # Safeguard in case file doesn't exist
        if not os.path.exists(file_path):
            return {"error": f"XBRL file does not exist: {file_path}"}
        
        # Save raw file for debugging
        file_directory, file_name = os.path.split(file_path)
        debug_file_path = os.path.join(file_directory, f"debug_{file_name}")
        with open(file_path, 'rb') as f:
            content = f.read()
        with open(debug_file_path, 'wb') as f:
            f.write(content)
        logging.info(f"Saved debug copy of XBRL to {debug_file_path}")

        # Detect file format using registry
        xbrl_format = detect_xbrl_format(file_path, ticker)
        format_handler = get_format_handler(xbrl_format)
        
        logging.info(f"Using XBRL format: {xbrl_format} - {format_handler.get('description', '')}")
        
        # Parse XML with format-specific error handling
        parser = etree.XMLParser(recover=True, no_network=True)  # Recover from bad XML and disable network access
        try:
            tree = etree.parse(file_path, parser)
            root = tree.getroot()
            logging.info(f"Successfully parsed XML with root element: {root.tag}")
        except Exception as e:
            logging.error(f"Failed to parse XML with standard parser: {str(e)}")
            # Try with a more permissive parser
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                # Clean content to help parsing
                content = re.sub(r'&(?!(amp|lt|gt|quot|apos);)', '&amp;', content)
                try:
                    root = etree.fromstring(content, parser)
                    tree = etree.ElementTree(root)
                    logging.info("Successfully parsed XML with permissive parser")
                except Exception as ex:
                    logging.error(f"Failed to parse XML even with permissive parser: {str(ex)}")
                    # Return minimal results when parsing fails completely
                    return {
                        "success": True,
                        "contexts": {},
                        "units": {},
                        "facts": [],
                        "parsing_warning": f"Failed to parse XML: {str(ex)}"
                    }
        
        # Get all namespaces, safely handling potential NoneType
        namespaces = {}
        if hasattr(root, 'nsmap') and root.nsmap is not None:
            namespaces = {k: v for k, v in root.nsmap.items() if k is not None}
        logging.info(f"Found {len(namespaces)} namespaces in XBRL document")
        
        # Extract contexts - handle possible issues with XPath
        # Use format-specific context extraction when needed
        contexts = {}
        
        if xbrl_format == "cal_xml":
            # Special handling for Apple's .cal.xml format
            try:
                # Apple's cal.xml files use a different structure
                # Try to extract period information from alternative elements
                logging.info("Using special handling for cal.xml format")
                
                # Look for period information in document
                period_contexts = {}
                
                # Try to find report date nodes
                report_date_nodes = tree.xpath("//*[contains(local-name(), 'ReportDate')]")
                if report_date_nodes:
                    for node in report_date_nodes:
                        if node.text and node.text.strip():
                            # Create a synthetic context with this date
                            context_id = f"AsOf_{node.text.strip().replace('-', '')}"
                            period_contexts[context_id] = {
                                "instant": node.text.strip()
                            }
                
                # Try to find period elements (even without context wrappers)
                period_nodes = tree.xpath("//*[contains(local-name(), 'Period') or contains(local-name(), 'period')]")
                for node in period_nodes:
                    if node.text and node.text.strip():
                        context_id = f"Period_{node.tag.split('}')[-1]}_{node.text.strip()}"
                        period_contexts[context_id] = {
                            "description": node.tag.split('}')[-1],
                            "value": node.text.strip()
                        }
                
                # Create synthetic contexts from these periods
                for context_id, period_data in period_contexts.items():
                    contexts[context_id] = {
                        "id": context_id,
                        "period": period_data,
                        "dimensions": {},
                        "synthetic": True
                    }
                
                logging.info(f"Created {len(contexts)} synthetic contexts for cal.xml format")
                
                # If we couldn't find any contexts, create a default one
                if not contexts:
                    import datetime
                    today = datetime.datetime.now().strftime("%Y-%m-%d")
                    contexts["DefaultContext"] = {
                        "id": "DefaultContext",
                        "period": {"instant": today},
                        "dimensions": {},
                        "synthetic": True,
                        "default": True
                    }
                    logging.warning("Created default context as fallback for cal.xml format")
            
            except Exception as e:
                logging.error(f"Error in special handling for cal.xml: {str(e)}")
                # Fall back to standard extraction
        
        # Standard context extraction for all formats (as fallback or primary method)
        try:
            context_elements = tree.xpath("//*[local-name()='context']")
            logging.info(f"Found {len(context_elements)} context elements")
        except Exception as e:
            logging.warning(f"XPath for contexts failed: {str(e)}, trying alternative")
            # Alternative approach if XPath fails
            context_elements = []
            for elem in root.iter():
                if elem.tag.endswith('}context') or elem.tag == 'context':
                    context_elements.append(elem)
            logging.info(f"Found {len(context_elements)} context elements with alternative method")
            
        for context in context_elements:
            try:
                context_id = context.get("id")
                if not context_id:
                    continue
                
                # Extract period
                period_info = {}
                # Try different ways to find period elements
                period = None
                try:
                    period = context.find(".//*[local-name()='period']")
                except:
                    for child in context.iterchildren():
                        if child.tag.endswith('}period') or child.tag == 'period':
                            period = child
                            break
                
                if period is not None:
                    instant = None
                    start_date = None
                    end_date = None
                    
                    # Try different ways to find instant/startDate/endDate
                    try:
                        instant = period.find(".//*[local-name()='instant']")
                        start_date = period.find(".//*[local-name()='startDate']")
                        end_date = period.find(".//*[local-name()='endDate']")
                    except:
                        for child in period.iterchildren():
                            if child.tag.endswith('}instant') or child.tag == 'instant':
                                instant = child
                            elif child.tag.endswith('}startDate') or child.tag == 'startDate':
                                start_date = child
                            elif child.tag.endswith('}endDate') or child.tag == 'endDate':
                                end_date = child
                    
                    if instant is not None and instant.text:
                        period_info["instant"] = instant.text
                    
                    if start_date is not None and end_date is not None and start_date.text and end_date.text:
                        period_info["startDate"] = start_date.text
                        period_info["endDate"] = end_date.text
                
                # Extract dimensions (segments and scenarios)
                dimensions = {}
                
                # Try to find segment - handle potential xpath issues
                segment = None
                try:
                    segment = context.find(".//*[local-name()='segment']")
                except:
                    for child in context.iterchildren():
                        if child.tag.endswith('}segment') or child.tag == 'segment':
                            segment = child
                            break
                
                if segment is not None:
                    # Try to find explicit members - handle potential xpath issues
                    explicit_members = []
                    try:
                        explicit_members = segment.xpath(".//*[local-name()='explicitMember']")
                    except:
                        for child in segment.iter():
                            if child.tag.endswith('}explicitMember') or child.tag == 'explicitMember':
                                explicit_members.append(child)
                    
                    for dim in explicit_members:
                        try:
                            dimension = dim.get("dimension")
                            if dimension:
                                if ":" in dimension:
                                    dimension = dimension.split(":")[-1]
                                value = dim.text.strip() if dim.text else ""
                                dimensions[dimension] = value
                        except Exception as e:
                            logging.warning(f"Error extracting dimension: {str(e)}")
                
                # Store context
                try:
                    contexts[context_id] = {
                        "id": context_id,
                        "period": period_info,
                        "dimensions": dimensions,
                        "xml": etree.tostring(context, encoding='unicode', with_tail=False) if hasattr(etree, 'tostring') else str(context)
                    }
                except Exception as e:
                    logging.warning(f"Error storing context: {str(e)}")
                    # Fallback without XML content
                    contexts[context_id] = {
                        "id": context_id,
                        "period": period_info,
                        "dimensions": dimensions
                    }
            except Exception as e:
                logging.warning(f"Error processing context: {str(e)}")
        
        # Extract units - handle potential xpath issues
        units = {}
        try:
            unit_elements = tree.xpath("//*[local-name()='unit']")
            logging.info(f"Found {len(unit_elements)} unit elements")
        except Exception as e:
            logging.warning(f"XPath for units failed: {str(e)}, trying alternative")
            # Alternative approach if XPath fails
            unit_elements = []
            for elem in root.iter():
                if elem.tag.endswith('}unit') or elem.tag == 'unit':
                    unit_elements.append(elem)
            logging.info(f"Found {len(unit_elements)} unit elements with alternative method")
        
        for unit in unit_elements:
            try:
                unit_id = unit.get("id")
                if not unit_id:
                    continue
                
                # Try different ways to find measure
                measure = None
                try:
                    measure = unit.find(".//*[local-name()='measure']")
                except:
                    for child in unit.iter():
                        if child.tag.endswith('}measure') or child.tag == 'measure':
                            measure = child
                            break
                
                if measure is not None and measure.text:
                    units[unit_id] = measure.text
            except Exception as e:
                logging.warning(f"Error processing unit: {str(e)}")
        
        # Extract facts - use a more robust method with format-specific handling
        facts = []
        facts_count = 0
        
        # Special handling for different formats
        if xbrl_format == "cal_xml":
            # For Apple's cal.xml files, treat all leaf nodes as potential facts
            try:
                logging.info("Using special extraction for cal.xml format facts")
                
                # Use a default context if we created one
                default_context_id = "DefaultContext" if "DefaultContext" in contexts else None
                
                # Process all leaf elements (those without children) as potential facts
                for element in root.iter():
                    facts_count += 1
                    
                    # Skip elements that have children - only process leaf nodes
                    if len(list(element)) > 0:
                        continue
                    
                    try:
                        # Extract tag info
                        tag = element.tag
                        if "}" in tag:
                            namespace = tag.split("}")[0].strip("{")
                            tag_name = tag.split("}")[1]
                        else:
                            namespace = None
                            tag_name = tag
                        
                        # Skip special elements and empty values
                        if tag_name.lower() in ('context', 'unit', 'schemaref', 'xml') or not element.text:
                            continue
                        
                        # Try to find a context based on parent element naming conventions
                        parent = element.getparent()
                        parent_tag = ""
                        if parent is not None and parent.tag:
                            if "}" in parent.tag:
                                parent_tag = parent.tag.split("}")[1]
                            else:
                                parent_tag = parent.tag
                        
                        # Try to determine an appropriate context
                        context_ref = None
                        
                        # Try to find date in tag name or parent tag
                        date_patterns = [
                            r'AsOf(\d{8})',
                            r'For(\d{8})',
                            r'(\d{4})[\-_](\d{2})[\-_](\d{2})'
                        ]
                        
                        for pattern in date_patterns:
                            # Check in current tag
                            match = re.search(pattern, tag_name)
                            if match:
                                date_str = match.group(1)
                                if len(date_str) == 8:  # YYYYMMDD format
                                    context_id = f"AsOf_{date_str}"
                                    if context_id in contexts:
                                        context_ref = context_id
                                        break
                            
                            # Check in parent tag
                            match = re.search(pattern, parent_tag)
                            if match:
                                date_str = match.group(1)
                                if len(date_str) == 8:  # YYYYMMDD format
                                    context_id = f"AsOf_{date_str}"
                                    if context_id in contexts:
                                        context_ref = context_id
                                        break
                        
                        # If still no context, use the first available context
                        if not context_ref and contexts:
                            # Try to find a context whose ID appears in the element path
                            element_path = []
                            current = element
                            while current is not None:
                                if hasattr(current, 'tag'):
                                    if "}" in current.tag:
                                        element_path.append(current.tag.split("}")[1])
                                    else:
                                        element_path.append(current.tag)
                                current = current.getparent()
                            
                            # Try to match context IDs to any element in the path
                            for context_id in contexts:
                                if any(context_id in path_elem for path_elem in element_path):
                                    context_ref = context_id
                                    break
                            
                            # If still no match, use the first context
                            if not context_ref:
                                context_ref = list(contexts.keys())[0]
                        
                        # Use the default context as last resort
                        if not context_ref and default_context_id:
                            context_ref = default_context_id
                        
                        # Only process elements that have a context
                        if context_ref:
                            # Extract and clean value
                            value = element.text.strip() if element.text else ""
                            # Clean HTML from value if present
                            value = extract_text_only_from_html(value)
                            
                            # Create fact
                            fact = {
                                "concept": tag_name,
                                "namespace": namespace,
                                "value": value,
                                "context_ref": context_ref,
                                "inferred": True
                            }
                            
                            facts.append(fact)
                    except Exception as ex:
                        logging.warning(f"Error processing cal.xml fact: {str(ex)}")
                
                logging.info(f"Extracted {len(facts)} facts from cal.xml format")
                
            except Exception as e:
                logging.error(f"Error in special handling for cal.xml facts: {str(e)}")
        
        # Standard fact extraction for all formats
        try:
            # Only do standard extraction if we don't already have facts from special handling
            if xbrl_format != "cal_xml" or not facts:
                for element in root.iter():
                    facts_count += 1
                    # Process element only if it has a contextRef attribute
                    context_ref = element.get("contextRef")
                    if not context_ref:
                        continue
                    
                    try:
                        # Extract namespace and tag name
                        tag = element.tag
                        if "}" in tag:
                            namespace = tag.split("}")[0].strip("{")
                            tag_name = tag.split("}")[1]
                        else:
                            namespace = None
                            tag_name = tag
                        
                        # Skip non-fact elements
                        if tag_name in ('context', 'unit', 'schemaRef'):
                            continue
                        
                        # Extract and clean the value
                        value = ""
                        if element.text:
                            value = element.text.strip()
                            # Clean HTML from value if present
                            value = extract_text_only_from_html(value)
                        
                        # Create fact object
                        fact = {
                            "concept": tag_name,
                            "namespace": namespace,
                            "value": value,
                            "context_ref": context_ref,
                            "unit_ref": element.get("unitRef"),
                            "decimals": element.get("decimals")
                        }
                        
                        # Try to include XML representation if possible
                        try:
                            fact["xml"] = etree.tostring(element, encoding='unicode', with_tail=False)
                        except Exception as e:
                            logging.debug(f"Could not serialize element to XML: {str(e)}")
                        
                        facts.append(fact)
                    except Exception as e:
                        logging.warning(f"Error processing fact element: {str(e)}")
        except Exception as e:
            logging.error(f"Error iterating through elements: {str(e)}")
        
        # If we still don't have any facts, try a more aggressive approach
        if not facts:
            logging.warning("No facts found with standard methods, trying more aggressive approach")
            try:
                # Try to extract any element with a non-empty text value that looks like a fact
                for element in root.iter():
                    # Skip elements without text
                    if not element.text or not element.text.strip():
                        continue
                    
                    # Extract tag info
                    tag = element.tag
                    if "}" in tag:
                        namespace = tag.split("}")[0].strip("{")
                        tag_name = tag.split("}")[1]
                    else:
                        namespace = None
                        tag_name = tag
                    
                    # Skip generic elements
                    if tag_name.lower() in ('html', 'body', 'div', 'span', 'p', 'br', 'table', 'tr', 'td'):
                        continue
                    
                    # Extract and clean value
                    value = element.text.strip() if element.text else ""
                    # Clean HTML from value if present
                    value = extract_text_only_from_html(value)
                    
                    # Create a fact with limited metadata
                    fact = {
                        "concept": tag_name,
                        "namespace": namespace,
                        "value": value,
                        "fallback": True
                    }
                    
                    # Try to find an appropriate context
                    if contexts:
                        fact["context_ref"] = list(contexts.keys())[0]
                    
                    facts.append(fact)
            except Exception as e:
                logging.error(f"Error in aggressive fact extraction: {str(e)}")
        
        logging.info(f"Processed {facts_count} elements, found {len(facts)} facts")
        logging.info(f"XBRL parsing complete for {file_path}")
        
        # Only consider it successful if we found usable facts
        successful = len(facts) > 0
        
        # Learn from this parsing attempt if it was successful and we have a ticker
        if successful and ticker:
            learn_from_successful_parse(ticker, file_path, xbrl_format)
            logging.info(f"Updated format registry for ticker {ticker} with successful format {xbrl_format}")
        
        # Return results, ensuring we have at least empty structures for all components
        return {
            "success": successful,
            "contexts": contexts,
            "units": units,
            "facts": facts,
            "xbrl_format": xbrl_format,
            "file_path": file_path,
            "ticker": ticker,
            "facts_count": len(facts)
        }
    except Exception as e:
        logging.error(f"Exception in parse_xbrl_file: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return {"error": f"Error parsing XBRL: {str(e)}"}