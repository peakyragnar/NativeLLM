import logging
import re

logging.basicConfig(level=logging.INFO)

def extract_contexts_from_html(html_content, filing_metadata=None):
    """Extract context information from HTML content using regex patterns"""
    # Multiple patterns to find context elements with their period information
    # First try with exact XBRL namespace
    context_pattern = re.compile(r'<xbrli:context id="([^"]+)"[^>]*>.*?<xbrli:period>(.*?)<\/xbrli:period>', re.DOTALL)
    
    # If that fails, try with a more flexible approach that handles HTML encoding
    flex_context_pattern = re.compile(r'<xbrli:context\s+id="([^"]+)".*?>(.*?)<\/xbrli:context>', re.DOTALL)
    
    # Also check for context in the ix:resources section specifically
    ix_resources_pattern = re.compile(r'<ix:resources.*?>(.*?)<\/ix:resources>', re.DOTALL)
    
    # Patterns for period components
    instant_pattern = re.compile(r'<xbrli:instant>(.*?)<\/xbrli:instant>', re.DOTALL)
    startdate_pattern = re.compile(r'<xbrli:startDate>(.*?)<\/xbrli:startDate>', re.DOTALL)
    enddate_pattern = re.compile(r'<xbrli:endDate>(.*?)<\/xbrli:endDate>', re.DOTALL)
    
    # Find all context elements using standard pattern
    context_matches = context_pattern.findall(html_content)
    logging.info(f"Found {len(context_matches)} contexts via standard regex in HTML file")
    
    # If standard approach finds no contexts, try with flexible pattern
    if not context_matches:
        # Try to extract ix:resources section first
        ix_resources_match = ix_resources_pattern.search(html_content)
        if ix_resources_match:
            logging.info("Found ix:resources section, searching within it")
            resources_content = ix_resources_match.group(1)
            # Try standard pattern within resources
            context_matches = context_pattern.findall(resources_content)
            logging.info(f"Found {len(context_matches)} contexts in ix:resources section")
            
            # If still no matches, try flexible pattern
            if not context_matches:
                context_matches = flex_context_pattern.findall(resources_content)
                logging.info(f"Found {len(context_matches)} contexts with flexible pattern in ix:resources")
        
        # If still no matches, try flexible pattern on whole document
        if not context_matches:
            context_matches = flex_context_pattern.findall(html_content)
            logging.info(f"Found {len(context_matches)} contexts with flexible pattern in entire document")
    
    # Process all matches if any were found
    if context_matches:
        logging.info(f"Processing {len(context_matches)} context matches")
        
        # Parse all matches and create a dictionary
        extracted_contexts = {}
        for context_id, context_content in context_matches:
            period_info = {}
            
            # Extract instant date if present
            instant_match = instant_pattern.search(context_content)
            if instant_match:
                period_info["instant"] = instant_match.group(1).strip()
                logging.info(f"Found instant date for context {context_id}: {period_info['instant']}")
            
            # Extract start/end dates if present
            startdate_match = startdate_pattern.search(context_content)
            enddate_match = enddate_pattern.search(context_content)
            if startdate_match and enddate_match:
                period_info["startDate"] = startdate_match.group(1).strip()
                period_info["endDate"] = enddate_match.group(1).strip()
                logging.info(f"Found period for context {context_id}: {period_info['startDate']} to {period_info['endDate']}")
            
            # Store the context if we found period information
            if period_info:
                extracted_contexts[context_id] = {
                    "id": context_id,
                    "period": period_info
                }
        
        # If we found contexts but couldn't extract period info, try one more approach
        if not extracted_contexts and context_matches:
            logging.info("Found contexts but couldn't extract period info, trying direct approach")
            
            # Extract periods directly from full HTML content
            all_instant_matches = instant_pattern.findall(html_content)
            all_start_matches = startdate_pattern.findall(html_content)
            all_end_matches = enddate_pattern.findall(html_content)
            
            logging.info(f"Direct extraction found: {len(all_instant_matches)} instants, " +
                        f"{len(all_start_matches)} start dates, {len(all_end_matches)} end dates")
            
            # Pair up context IDs with periods by proximity in the original HTML
            for context_id, _ in context_matches:
                # Try to find period info near this context ID in the HTML
                context_pos = html_content.find(f'id="{context_id}"')
                if context_pos > 0:
                    # Look for period info within 500 chars of context ID
                    window = html_content[max(0, context_pos-200):min(len(html_content), context_pos+500)]
                    
                    period_info = {}
                    instant_match = instant_pattern.search(window)
                    if instant_match:
                        period_info["instant"] = instant_match.group(1).strip()
                    
                    startdate_match = startdate_pattern.search(window)
                    enddate_match = enddate_pattern.search(window)
                    if startdate_match and enddate_match:
                        period_info["startDate"] = startdate_match.group(1).strip()
                        period_info["endDate"] = enddate_match.group(1).strip()
                    
                    if period_info:
                        extracted_contexts[context_id] = {
                            "id": context_id,
                            "period": period_info
                        }
        
        # If we still have no context periods, create synthetic contexts based on common patterns
        if not extracted_contexts and context_matches:
            logging.info("No period information found, creating synthetic contexts based on common patterns")
            
            # Try to extract filing year from metadata
            filing_year = None
            if filing_metadata and "fiscal_year" in filing_metadata:
                filing_year = filing_metadata.get("fiscal_year")
            
            # Use fallback year if all else fails
            if not filing_year:
                try:
                    # Try to use the current year
                    import datetime
                    filing_year = str(datetime.datetime.now().year)
                except:
                    filing_year = "2023"  # Default fallback
            
            # Common context ID patterns and their likely meanings
            context_patterns = {
                "c-1": {"type": "annual", "description": f"Current reporting year ({filing_year})"},
                "c-2": {"type": "instant", "description": f"End date for year {filing_year}"},
                "c-3": {"type": "instant", "description": f"Prior period end date ({int(filing_year)-1})"},
                "c-4": {"type": "instant", "description": "Current balance sheet date"},
                "c-5": {"type": "instant", "description": "Prior balance sheet date"}
            }
            
            # Create synthetic contexts based on pattern matching
            for context_id, _ in context_matches:
                # Check if this ID matches a common pattern
                pattern_match = None
                for pattern, info in context_patterns.items():
                    if context_id == pattern or context_id.startswith(pattern + "_"):
                        pattern_match = info
                        break
                
                # If no exact match, try numeric pattern
                if not pattern_match and re.match(r'^c-\d+$', context_id):
                    # Default for numeric context IDs
                    pattern_match = {"type": "unknown", "description": "Context with unknown period"}
                
                # Create synthetic period info based on the pattern
                if pattern_match:
                    period_info = {}
                    
                    if pattern_match["type"] == "annual":
                        # Create a full year period
                        period_info["startDate"] = f"{filing_year}-01-01"
                        period_info["endDate"] = f"{filing_year}-12-31"
                    elif pattern_match["type"] == "instant":
                        # Create an instant date at year end
                        period_info["instant"] = f"{filing_year}-12-31"
                    
                    # Store with explicit synthetic flag
                    extracted_contexts[context_id] = {
                        "id": context_id,
                        "period": period_info,
                        "synthetic": True,
                        "description": pattern_match["description"]
                    }
        
        return extracted_contexts
    else:
        logging.warning("No contexts found via any extraction method")
        return {}

# Test the extraction on a real file
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 test_context_extractor.py <html_file_path>")
        sys.exit(1)
    
    html_file_path = sys.argv[1]
    
    try:
        with open(html_file_path, 'r', encoding='utf-8', errors='replace') as f:
            html_content = f.read()
            
        contexts = extract_contexts_from_html(html_content, {'fiscal_year': '2023'})
        
        print(f"Found {len(contexts)} contexts")
        
        # Print the first 5 context IDs
        context_ids = list(contexts.keys())
        print(f"Context IDs: {context_ids[:5]}")
        
        # Print details for the first context
        if context_ids:
            first_context = contexts[context_ids[0]]
            print("\nFirst context details:")
            for key, value in first_context.items():
                print(f"{key}: {value}")
    
    except Exception as e:
        print(f"Error: {str(e)}")
