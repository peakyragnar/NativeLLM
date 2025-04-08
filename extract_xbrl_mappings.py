#!/usr/bin/env python3
"""
Simple XBRL Mapping Extractor

This script extracts hierarchical relationships from XBRL linkbase files
using a direct, focused approach.
"""

import os
import sys
import json
import logging
import argparse
from lxml import etree

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_mappings(file_path, output_path=None):
    """Extract mappings from an XBRL linkbase file."""
    logging.info(f"Extracting mappings from {file_path}")
    
    try:
        # Load the XBRL file
        tree = etree.parse(file_path)
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
        
        # Save output if requested
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
            logging.info(f"Mappings saved to {output_path}")
        
        return result
    
    except Exception as e:
        logging.error(f"Error extracting mappings: {str(e)}")
        return {"presentation_mappings": [], "calculation_mappings": []}

def extract_embedded_mappings(schema_file_path, output_path=None):
    """Extract mappings from embedded linkbases in an XBRL schema file."""
    logging.info(f"Extracting embedded mappings from {schema_file_path}")
    
    try:
        # Load the XBRL schema file
        tree = etree.parse(schema_file_path)
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
        
        # Save output if requested
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
            logging.info(f"Mappings saved to {output_path}")
        
        return result
    
    except Exception as e:
        logging.error(f"Error extracting embedded mappings: {str(e)}")
        return {"presentation_mappings": [], "calculation_mappings": []}

def main():
    parser = argparse.ArgumentParser(description="Extract XBRL mappings")
    parser.add_argument("--linkbase", help="Path to XBRL linkbase file")
    parser.add_argument("--schema", help="Path to XBRL schema file with embedded linkbases")
    parser.add_argument("--output", help="Output file path")
    
    args = parser.parse_args()
    
    if not args.linkbase and not args.schema:
        parser.error("Either --linkbase or --schema must be provided")
    
    if args.linkbase:
        # Check if linkbase file exists
        if not os.path.exists(args.linkbase):
            logging.error(f"Linkbase file not found: {args.linkbase}")
            return 1
        
        # Extract mappings from linkbase file
        result = extract_mappings(args.linkbase, args.output)
        
        # Print summary
        print("\nXBRL Mapping Summary:")
        print(f"  Presentation Relationships: {len(result['presentation_mappings'])}")
        print(f"  Calculation Relationships: {len(result['calculation_mappings'])}")
    
    if args.schema:
        # Check if schema file exists
        if not os.path.exists(args.schema):
            logging.error(f"Schema file not found: {args.schema}")
            return 1
        
        # Extract mappings from schema file
        result = extract_embedded_mappings(args.schema, args.output)
        
        # Print summary
        print("\nXBRL Mapping Summary (Embedded Linkbases):")
        print(f"  Presentation Relationships: {len(result['presentation_mappings'])}")
        print(f"  Calculation Relationships: {len(result['calculation_mappings'])}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
