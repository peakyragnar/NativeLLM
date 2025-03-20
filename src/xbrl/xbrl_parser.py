# src/xbrl/xbrl_parser.py
import os
import sys
from lxml import etree

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def parse_xbrl_file(file_path):
    """Parse an XBRL instance document"""
    try:
        # Parse XML
        parser = etree.XMLParser(recover=True)  # Recover from bad XML
        tree = etree.parse(file_path, parser)
        root = tree.getroot()
        
        # Get all namespaces
        namespaces = {k: v for k, v in root.nsmap.items() if k is not None}
        
        # Extract contexts
        contexts = {}
        for context in tree.xpath("//*[local-name()='context']"):
            context_id = context.get("id")
            
            # Extract period
            period = context.find(".//*[local-name()='period']")
            period_info = {}
            if period is not None:
                instant = period.find(".//*[local-name()='instant']")
                if instant is not None:
                    period_info["instant"] = instant.text
                
                start_date = period.find(".//*[local-name()='startDate']")
                end_date = period.find(".//*[local-name()='endDate']")
                if start_date is not None and end_date is not None:
                    period_info["startDate"] = start_date.text
                    period_info["endDate"] = end_date.text
            
            # Extract dimensions (segments and scenarios)
            dimensions = {}
            segment = context.find(".//*[local-name()='segment']")
            if segment is not None:
                for dim in segment.xpath(".//*[local-name()='explicitMember']"):
                    dimension = dim.get("dimension").split(":")[-1]
                    value = dim.text
                    dimensions[dimension] = value
            
            # Store context
            contexts[context_id] = {
                "id": context_id,
                "period": period_info,
                "dimensions": dimensions,
                "xml": etree.tostring(context).decode('utf-8')
            }
        
        # Extract units
        units = {}
        for unit in tree.xpath("//*[local-name()='unit']"):
            unit_id = unit.get("id")
            measure = unit.find(".//*[local-name()='measure']")
            
            if measure is not None:
                units[unit_id] = measure.text
        
        # Extract facts
        facts = []
        for element in tree.xpath("//*"):
            context_ref = element.get("contextRef")
            if context_ref is not None:  # This is a fact
                # Extract namespace and tag name
                tag = element.tag
                if "}" in tag:
                    namespace = tag.split("}")[0].strip("{")
                    tag_name = tag.split("}")[1]
                else:
                    namespace = None
                    tag_name = tag
                
                # Create fact object
                fact = {
                    "concept": tag_name,
                    "namespace": namespace,
                    "value": element.text.strip() if element.text else "",
                    "context_ref": context_ref,
                    "unit_ref": element.get("unitRef"),
                    "decimals": element.get("decimals"),
                    "xml": etree.tostring(element).decode('utf-8')
                }
                
                facts.append(fact)
        
        return {
            "success": True,
            "contexts": contexts,
            "units": units,
            "facts": facts
        }
    except Exception as e:
        return {"error": f"Error parsing XBRL: {str(e)}"}