"""
Specialized extractor for SEC iXBRL format documents.

This module is designed to handle the specific format of SEC iXBRL documents,
including those that are embedded in the SEC's iXBRL Viewer or directly accessible
as HTML files. It focuses on extracting XBRL data directly from the MetaLinks.json
file and other sources.
"""

import os
import sys
import json
import re
import time
from urllib.parse import urlparse, parse_qs, urljoin
from bs4 import BeautifulSoup
import requests

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.edgar.edgar_utils import sec_request
from src.config import RAW_DATA_DIR

def extract_document_from_viewer_url(viewer_url):
    """Extract the underlying document path from an SEC iXBRL Viewer URL"""
    try:
        parsed_url = urlparse(viewer_url)
        query_params = parse_qs(parsed_url.query)
        
        if 'doc' in query_params:
            doc_path = query_params['doc'][0]
            return doc_path
    except:
        pass
    
    return None

def fetch_metalinks_json(cik, accession_number):
    """Fetch the MetaLinks.json file for a filing"""
    clean_accn = accession_number.replace("-", "")
    # Remove leading zeros from CIK for URL construction
    cik_no_zeros = cik.lstrip('0')
    # Build URL with correct directory structure
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_no_zeros}/{clean_accn}/"
    metalinks_url = f"{base_url}MetaLinks.json"
    
    response = sec_request(metalinks_url)
    if response and response.status_code == 200:
        try:
            return response.json()
        except:
            pass
    
    return None

def fetch_document_from_metalinks(metalinks_json, cik, accession_number):
    """Fetch the primary document file from MetaLinks.json"""
    if not metalinks_json or not isinstance(metalinks_json, dict):
        return None, None
    
    clean_accn = accession_number.replace("-", "")
    # Remove leading zeros from CIK for URL construction
    cik_no_zeros = cik.lstrip('0')
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_no_zeros}/{clean_accn}/"
    
    # Look for HTML files in reportFiles
    html_files = []
    if "reportFiles" in metalinks_json:
        html_files = [f for f in metalinks_json["reportFiles"].keys() if f.endswith('.htm')]
    
    if not html_files:
        return None, None
    
    # Fetch the first HTML file (usually the primary document)
    html_url = f"{base_url}{html_files[0]}"
    response = sec_request(html_url)
    
    if response and response.status_code == 200:
        return html_files[0], response.content
    
    return None, None

def extract_facts_from_metalinks(metalinks_json):
    """Extract fact data from the MetaLinks.json file"""
    if not metalinks_json or not isinstance(metalinks_json, dict):
        return {}, {}, []
    
    # Extract contexts
    contexts = {}
    if "contextIdMap" in metalinks_json:
        for context_id, context_info in metalinks_json["contextIdMap"].items():
            period_info = {}
            
            if "startDate" in context_info and "endDate" in context_info:
                period_info["startDate"] = context_info["startDate"]
                period_info["endDate"] = context_info["endDate"]
            elif "instant" in context_info:
                period_info["instant"] = context_info["instant"]
            
            dimensions = {}
            if "dimensions" in context_info:
                for dim, value in context_info["dimensions"].items():
                    dimensions[dim] = value
            
            contexts[context_id] = {
                "id": context_id,
                "period": period_info,
                "dimensions": dimensions
            }
    
    # Extract units
    units = {}
    if "unitIdMap" in metalinks_json:
        for unit_id, unit_info in metalinks_json["unitIdMap"].items():
            if "measures" in unit_info:
                # Handle simple units with a single measure
                if isinstance(unit_info["measures"], list) and len(unit_info["measures"]) == 1:
                    units[unit_id] = unit_info["measures"][0]
                # Handle complex units (divide)
                elif "numerator" in unit_info and "denominator" in unit_info:
                    num = unit_info["numerator"][0] if isinstance(unit_info["numerator"], list) and unit_info["numerator"] else ""
                    denom = unit_info["denominator"][0] if isinstance(unit_info["denominator"], list) and unit_info["denominator"] else ""
                    units[unit_id] = f"{num}/{denom}"
    
    # Extract facts
    facts = []
    if "factIdMap" in metalinks_json:
        for fact_id, fact_info in metalinks_json["factIdMap"].items():
            concept = fact_info.get("name", "").split(":")[-1]
            context_ref = fact_info.get("contextId")
            unit_ref = fact_info.get("unitId")
            value = fact_info.get("value", "")
            decimals = fact_info.get("decimals")
            
            if concept and context_ref:
                fact_obj = {
                    "concept": concept,
                    "value": value,
                    "context_ref": context_ref
                }
                
                if unit_ref:
                    fact_obj["unit_ref"] = unit_ref
                
                if decimals:
                    fact_obj["decimals"] = decimals
                
                facts.append(fact_obj)
    
    return contexts, units, facts

def extract_facts_from_inline_json(html_content):
    """Extract fact data from JSON embedded in JavaScript within the HTML"""
    try:
        # Convert to string if needed
        if isinstance(html_content, bytes):
            html_text = html_content.decode('utf-8', errors='ignore')
        else:
            html_text = html_content
        
        # Look for the JSON data
        # Pattern 1: var ix*** = {...}
        json_match = re.search(r'var\s+ix\w+\s*=\s*(\{.+?\});\s*', html_text, re.DOTALL)
        if not json_match:
            # Pattern 2: window.ix*** = {...}
            json_match = re.search(r'window\.ix\w+\s*=\s*(\{.+?\});\s*', html_text, re.DOTALL)
        
        if not json_match:
            # Pattern 3: {"facts": {...}}
            json_match = re.search(r'({"facts":.+?}})', html_text, re.DOTALL)
        
        if json_match:
            json_str = json_match.group(1)
            json_data = json.loads(json_str)
            
            # Extract contexts
            contexts = {}
            if "contexts" in json_data:
                for context_id, context_info in json_data["contexts"].items():
                    period_info = {}
                    if "period" in context_info:
                        period = context_info["period"]
                        if "start" in period and "end" in period:
                            period_info["startDate"] = period["start"]
                            period_info["endDate"] = period["end"]
                        elif "instant" in period:
                            period_info["instant"] = period["instant"]
                    
                    dimensions = {}
                    if "dimensions" in context_info:
                        for dim, value in context_info["dimensions"].items():
                            dimensions[dim] = value
                    
                    contexts[context_id] = {
                        "id": context_id,
                        "period": period_info,
                        "dimensions": dimensions
                    }
            
            # Extract units
            units = {}
            if "units" in json_data:
                for unit_id, unit_info in json_data["units"].items():
                    if "measures" in unit_info:
                        measures = unit_info["measures"]
                        if isinstance(measures, list) and len(measures) == 1:
                            units[unit_id] = measures[0]
                        elif "numerator" in unit_info and "denominator" in unit_info:
                            num = unit_info["numerator"][0] if isinstance(unit_info["numerator"], list) and unit_info["numerator"] else ""
                            denom = unit_info["denominator"][0] if isinstance(unit_info["denominator"], list) and unit_info["denominator"] else ""
                            units[unit_id] = f"{num}/{denom}"
            
            # Extract facts
            facts = []
            if "facts" in json_data:
                for fact_id, fact_info in json_data["facts"].items():
                    concept = fact_info.get("name", "").split(":")[-1]
                    context_ref = fact_info.get("contextRef")
                    unit_ref = fact_info.get("unitRef")
                    value = fact_info.get("value", "")
                    decimals = fact_info.get("decimals")
                    
                    if concept and context_ref:
                        fact_obj = {
                            "concept": concept,
                            "value": value,
                            "context_ref": context_ref
                        }
                        
                        if unit_ref:
                            fact_obj["unit_ref"] = unit_ref
                        
                        if decimals:
                            fact_obj["decimals"] = decimals
                        
                        facts.append(fact_obj)
            
            return contexts, units, facts
        
        return {}, {}, []
    
    except Exception as e:
        print(f"Error extracting facts from inline JSON: {str(e)}")
        return {}, {}, []

def extract_sec_ixbrl_data(filing_metadata):
    """
    Extract iXBRL data from an SEC filing using SEC-specific extraction methods.
    
    This function uses multiple approaches specialized for SEC iXBRL viewer documents:
    1. Fetches the MetaLinks.json file and extracts data from it
    2. Downloads the actual HTML document and extracts embedded JSON data
    3. Falls back to traditional iXBRL extraction methods if needed
    """
    try:
        # Extract necessary data from metadata
        cik = filing_metadata.get("cik")
        accession_number = filing_metadata.get("accession_number")
        ticker = filing_metadata.get("ticker")
        filing_type = filing_metadata.get("filing_type")
        
        if not cik or not accession_number:
            return {
                "error": "Missing required metadata (CIK or accession number)",
                "contexts": {},
                "units": {},
                "facts": []
            }
        
        print(f"Extracting SEC iXBRL data for {ticker} {filing_type}")
        
        # Approach 1: Use MetaLinks.json
        print("Fetching MetaLinks.json...")
        metalinks_json = fetch_metalinks_json(cik, accession_number)
        
        if metalinks_json:
            print("Successfully fetched MetaLinks.json")
            
            # Try to extract data directly from MetaLinks.json
            contexts, units, facts = extract_facts_from_metalinks(metalinks_json)
            
            if facts:
                print(f"Successfully extracted {len(facts)} facts from MetaLinks.json")
                return {
                    "success": True,
                    "extraction_method": "metalinks_json",
                    "contexts": contexts,
                    "units": units,
                    "facts": facts
                }
            
            # If no facts in MetaLinks, try to get the actual document from it
            print("No facts found in MetaLinks.json, trying to fetch document...")
            file_name, html_content = fetch_document_from_metalinks(metalinks_json, cik, accession_number)
            
            if html_content:
                print(f"Successfully fetched document: {file_name}")
                
                # Try to extract data from embedded JSON
                inline_contexts, inline_units, inline_facts = extract_facts_from_inline_json(html_content)
                
                if inline_facts:
                    print(f"Successfully extracted {len(inline_facts)} facts from inline JSON")
                    return {
                        "success": True,
                        "extraction_method": "inline_json",
                        "contexts": inline_contexts,
                        "units": inline_units,
                        "facts": inline_facts
                    }
                
                # For now, we don't try traditional iXBRL extraction here
                # This would be a fallback option if needed
        
        # Approach 2: If we have an instance_url, try to extract from that
        instance_url = filing_metadata.get("instance_url")
        if instance_url and "ix?doc=" in instance_url:
            print("Trying to extract document from instance URL...")
            doc_path = extract_document_from_viewer_url(instance_url)
            
            if doc_path:
                # Correct URL construction
                if doc_path.startswith('/'):
                    actual_doc_url = f"https://www.sec.gov{doc_path}"
                else:
                    actual_doc_url = f"https://www.sec.gov/{doc_path}"
                print(f"Fetching actual document from: {actual_doc_url}")
                
                response = sec_request(actual_doc_url)
                if response and response.status_code == 200:
                    print("Successfully retrieved actual document")
                    
                    # Try to extract data from embedded JSON
                    doc_contexts, doc_units, doc_facts = extract_facts_from_inline_json(response.content)
                    
                    if doc_facts:
                        print(f"Successfully extracted {len(doc_facts)} facts from document JSON")
                        return {
                            "success": True,
                            "extraction_method": "document_json",
                            "contexts": doc_contexts,
                            "units": doc_units,
                            "facts": doc_facts
                        }
        
        # If we got here, we couldn't extract data from any source
        return {
            "error": "Could not extract iXBRL data using SEC-specific methods",
            "contexts": {},
            "units": {},
            "facts": []
        }
    
    except Exception as e:
        print(f"Error extracting SEC iXBRL data: {str(e)}")
        return {
            "error": f"Exception extracting SEC iXBRL data: {str(e)}",
            "contexts": {},
            "units": {},
            "facts": []
        }