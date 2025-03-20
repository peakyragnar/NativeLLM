# src/xbrl/xbrl_parser.py
import os
import sys
import logging
from lxml import etree
import re

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def parse_xbrl_file(file_path):
    """Parse an XBRL instance document"""
    try:
        logging.info(f"Parsing XBRL file: {file_path}")
        
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
        
        # Parse XML
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
                root = etree.fromstring(content, parser)
                tree = etree.ElementTree(root)
                logging.info("Successfully parsed XML with permissive parser")
        
        # Get all namespaces
        namespaces = {k: v for k, v in root.nsmap.items() if k is not None}
        logging.info(f"Found {len(namespaces)} namespaces in XBRL document")
        
        # Extract contexts - handle possible issues with XPath
        contexts = {}
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
        
        # Extract facts - use a more robust method
        facts = []
        facts_count = 0
        
        # Process all elements with contextRef attribute
        try:
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
        
        logging.info(f"Processed {facts_count} elements, found {len(facts)} facts")
        logging.info(f"XBRL parsing complete for {file_path}")
        
        return {
            "success": True,
            "contexts": contexts,
            "units": units,
            "facts": facts
        }
    except Exception as e:
        logging.error(f"Exception in parse_xbrl_file: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return {"error": f"Error parsing XBRL: {str(e)}"}