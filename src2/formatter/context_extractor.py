#!/usr/bin/env python3
"""
Context Extractor Module

This module handles the extraction of context information from XBRL filings,
providing a standardized mapping between context IDs and time periods.
"""

import re
import logging
from datetime import datetime

def extract_contexts_from_html(html_content, filing_metadata=None):
    """
    Extract context information from HTML content using regex patterns
    
    Args:
        html_content (str): The HTML content containing XBRL data
        filing_metadata (dict): Metadata about the filing including company and date info
        
    Returns:
        dict: A dictionary of context IDs mapped to their period information
    """
    if not html_content or not isinstance(html_content, str):
        logging.warning(f"Invalid HTML content type: {type(html_content)}")
        return {}
        
    logging.info("Extracting contexts from HTML content")
    
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
    
    # Entity and segment pattern for dimensional information
    entity_pattern = re.compile(r'<xbrli:entity>(.*?)<\/xbrli:entity>', re.DOTALL)
    segment_pattern = re.compile(r'<xbrli:segment>(.*?)<\/xbrli:segment>', re.DOTALL)
    
    # For dimensions in segment
    explicit_member_pattern = re.compile(r'<xbrldi:explicitMember\s+dimension="([^"]+)">(.*?)<\/xbrldi:explicitMember>', re.DOTALL)
    
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
            segment_info = None
            
            # Extract entity and segment information if available
            entity_match = entity_pattern.search(context_content)
            if entity_match:
                entity_content = entity_match.group(1)
                segment_match = segment_pattern.search(entity_content)
                
                if segment_match:
                    segment_content = segment_match.group(1)
                    
                    # Extract dimension information
                    dimensions = {}
                    for dim_name, dim_value in explicit_member_pattern.findall(segment_content):
                        # Clean up namespace prefixes for readability
                        clean_dim_name = dim_name.split(":")[-1] if ":" in dim_name else dim_name
                        clean_dim_value = dim_value.split(":")[-1] if ":" in dim_value else dim_value
                        dimensions[clean_dim_name] = clean_dim_value
                    
                    if dimensions:
                        segment_info = dimensions
            
            # Extract instant date if present
            instant_match = instant_pattern.search(context_content)
            if instant_match:
                period_info["instant"] = instant_match.group(1).strip()
                logging.debug(f"Found instant date for context {context_id}: {period_info['instant']}")
            
            # Extract start/end dates if present
            startdate_match = startdate_pattern.search(context_content)
            enddate_match = enddate_pattern.search(context_content)
            if startdate_match and enddate_match:
                period_info["startDate"] = startdate_match.group(1).strip()
                period_info["endDate"] = enddate_match.group(1).strip()
                logging.debug(f"Found period for context {context_id}: {period_info['startDate']} to {period_info['endDate']}")
            
            # Store the context if we found period information
            if period_info:
                extracted_contexts[context_id] = {
                    "id": context_id,
                    "period": period_info
                }
                
                # Add segment/dimension information if available
                if segment_info:
                    extracted_contexts[context_id]["entity"] = {
                        "segment": segment_info
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
            elif filing_metadata and "period_end_date" in filing_metadata:
                # Try to extract year from period end date
                period_end = filing_metadata.get("period_end_date")
                if period_end and len(period_end) >= 4:
                    filing_year = period_end[:4]
            
            # Use fallback year if all else fails
            if not filing_year:
                try:
                    # Try to use the current year
                    filing_year = str(datetime.now().year)
                except:
                    filing_year = "2023"  # Default fallback
            
            # Common context ID patterns and their likely meanings
            for context_id, _ in context_matches:
                # Create synthetic context based on common patterns
                extracted_contexts[context_id] = {
                    "id": context_id,
                    "period": {
                        # Default to fiscal year period if we can't determine the actual period
                        "startDate": f"{filing_year}-01-01",
                        "endDate": f"{filing_year}-12-31"
                    },
                    "synthetic": True,
                    "description": f"Synthetic context for {context_id} (fiscal year {filing_year})"
                }
        
        # Enrich contexts with additional information
        for context_id, context_data in extracted_contexts.items():
            period_info = context_data.get("period", {})
            
            # Add a type field to easily identify instant vs. duration contexts
            if "instant" in period_info:
                context_data["type"] = "instant"
            elif "startDate" in period_info and "endDate" in period_info:
                context_data["type"] = "duration"
            else:
                context_data["type"] = "unknown"
            
            # Try to identify fiscal periods from dates
            if context_data["type"] == "duration":
                try:
                    start_date = period_info["startDate"]
                    end_date = period_info["endDate"]
                    start_year = start_date.split("-")[0]
                    end_year = end_date.split("-")[0]
                    end_month = int(end_date.split("-")[1])
                    
                    # Check if this is an annual period
                    if start_year == end_year:
                        # Same year - could be quarterly or annual
                        if end_month == 12 or (end_month in [3, 6, 9] and "10-Q" in filing_metadata.get("filing_type", "")):
                            # Map month to quarter
                            quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
                            if end_month in quarter_map:
                                context_data["fiscal_period"] = quarter_map[end_month]
                                context_data["fiscal_year"] = end_year
                        else:
                            # Likely annual period
                            context_data["fiscal_period"] = "FY"
                            context_data["fiscal_year"] = end_year
                    else:
                        # Multi-year period
                        context_data["fiscal_period"] = f"FY{start_year}-{end_year}"
                except Exception as e:
                    logging.warning(f"Error enriching context {context_id}: {str(e)}")
            
            # For instant contexts, try to identify if it's a balance sheet date
            if context_data["type"] == "instant":
                try:
                    instant_date = period_info["instant"]
                    year = instant_date.split("-")[0]
                    month = int(instant_date.split("-")[1])
                    
                    # Check if this is a common fiscal period end date
                    if month in [3, 6, 9, 12]:
                        context_data["balance_sheet_date"] = True
                        context_data["fiscal_year"] = year
                        # Map month to quarter/year end
                        quarter_map = {3: "Q1_END", 6: "Q2_END", 9: "Q3_END", 12: "FY_END"}
                        context_data["fiscal_period"] = quarter_map[month]
                except Exception as e:
                    logging.warning(f"Error processing instant date for {context_id}: {str(e)}")
        
        logging.info(f"Successfully extracted and processed {len(extracted_contexts)} contexts")
        return extracted_contexts
    else:
        logging.warning("No contexts found via any extraction method")
        return {}

def map_contexts_to_periods(contexts, filing_metadata=None):
    """
    Create a mapping from context IDs to standardized period labels
    
    Args:
        contexts (dict): Dictionary of extracted contexts
        filing_metadata (dict): Metadata about the filing
        
    Returns:
        dict: Mapping from context IDs to standardized period labels
    """
    context_map = {}
    filing_type = filing_metadata.get("filing_type", "") if filing_metadata else ""
    
    for context_id, context_data in contexts.items():
        period_info = context_data.get("period", {})
        context_type = context_data.get("type", "unknown")
        
        # Skip if no period info
        if not period_info:
            continue
        
        if context_type == "duration":
            # Handle duration contexts (income statement, cash flows)
            try:
                start_date = period_info.get("startDate", "")
                end_date = period_info.get("endDate", "")
                
                if not start_date or not end_date:
                    continue
                
                # Extract components
                start_year = start_date.split("-")[0]
                end_year = end_date.split("-")[0]
                end_month = int(end_date.split("-")[1])
                
                # Create a readable label
                if "fiscal_period" in context_data:
                    # Use the pre-identified fiscal period
                    if context_data["fiscal_period"] == "FY":
                        label = f"FY{end_year}"
                    elif context_data["fiscal_period"].startswith("Q"):
                        label = f"{end_year}_{context_data['fiscal_period']}"
                    else:
                        label = context_data["fiscal_period"]
                else:
                    # Determine label based on dates
                    if start_year == end_year:
                        # Same year - determine if annual or quarterly
                        if end_month == 12:
                            label = f"FY{end_year}"
                        else:
                            # Map month to quarter
                            quarter_map = {3: "Q1", 6: "Q2", 9: "Q3"}
                            label = f"{end_year}_{quarter_map.get(end_month, 'QTR')}"
                    else:
                        # Multi-year period
                        label = f"FY{start_year}-{end_year}"
                
                # Check for segment/dimension information
                if "entity" in context_data and "segment" in context_data["entity"]:
                    segment_data = context_data["entity"]["segment"]
                    if segment_data:
                        # Add segment info to label
                        segments = []
                        for dim_name, dim_value in segment_data.items():
                            # Simplify dimension name
                            simple_name = dim_name.replace("Dimension", "").replace("Segment", "")
                            segments.append(f"{simple_name}:{dim_value}")
                        
                        if segments:
                            label += f" ({', '.join(segments)})"
                
                context_map[context_id] = label
            except Exception as e:
                logging.warning(f"Error mapping duration context {context_id}: {str(e)}")
                context_map[context_id] = f"Period_{context_id}"
        
        elif context_type == "instant":
            # Handle instant contexts (balance sheet dates)
            try:
                instant_date = period_info.get("instant", "")
                if not instant_date:
                    continue
                
                # Extract components
                year = instant_date.split("-")[0]
                month = int(instant_date.split("-")[1])
                
                # Create a readable label
                if "fiscal_period" in context_data:
                    # Use the pre-identified fiscal period
                    label = f"{year}_{context_data['fiscal_period']}"
                else:
                    # Determine label based on date
                    # Map month to period end
                    period_map = {3: "Q1_END", 6: "Q2_END", 9: "Q3_END", 12: "FY_END"}
                    period_code = period_map.get(month, "DATE")
                    label = f"{year}_{period_code}"
                
                # Check for segment/dimension information
                if "entity" in context_data and "segment" in context_data["entity"]:
                    segment_data = context_data["entity"]["segment"]
                    if segment_data:
                        # Add segment info to label
                        segments = []
                        for dim_name, dim_value in segment_data.items():
                            # Simplify dimension name
                            simple_name = dim_name.replace("Dimension", "").replace("Segment", "")
                            segments.append(f"{simple_name}:{dim_value}")
                        
                        if segments:
                            label += f" ({', '.join(segments)})"
                
                context_map[context_id] = label
            except Exception as e:
                logging.warning(f"Error mapping instant context {context_id}: {str(e)}")
                context_map[context_id] = f"Date_{context_id}"
        
        else:
            # Unknown context type
            context_map[context_id] = f"Unknown_{context_id}"
    
    return context_map

def generate_context_dictionary(extracted_contexts, context_map=None):
    """
    Generate a dictionary of contexts with human-readable labels and period information
    
    Args:
        extracted_contexts (dict or list): Dictionary or list of extracted context information
        context_map (dict, optional): Mapping from context IDs to human-readable labels
        
    Returns:
        dict: Dictionary of context information with human-readable labels
    """
    context_dict = {}
    
    if not extracted_contexts:
        return context_dict
        
    # If no context map is provided, create an empty one
    if context_map is None:
        context_map = {}
        
    # Convert list to dictionary if needed
    if isinstance(extracted_contexts, list):
        logging.warning(f"Converting list of {len(extracted_contexts)} contexts to dictionary")
        contexts_dict = {}
        for i, ctx in enumerate(extracted_contexts):
            if isinstance(ctx, dict) and 'id' in ctx:
                contexts_dict[ctx['id']] = ctx
            else:
                contexts_dict[f"context_{i}"] = ctx
        extracted_contexts = contexts_dict
    
    # Handle the case where extracted_contexts still isn't a dictionary
    if not isinstance(extracted_contexts, dict):
        logging.warning(f"Invalid extracted_contexts type: {type(extracted_contexts)}, returning empty dict")
        return context_dict
        
    # Now process each context 
    for context_id, context_info in extracted_contexts.items():
        # Skip if context_info is None or not a dict
        if not context_info or not isinstance(context_info, dict):
            continue
            
        # Get period information, handling various format possibilities
        period = context_info.get('period', {})
        
        # For different format types coming from different extractors
        start_date = ""
        end_date = ""
        instant_date = ""
        
        # Handle period structure from HTML extractor
        if isinstance(period, dict):
            start_date = period.get('startDate', '')
            end_date = period.get('endDate', '')
            instant_date = period.get('instant', '')
        
        # Determine context type based on available date info
        if instant_date:
            ctx_type = 'instant'
        elif start_date and end_date:
            ctx_type = 'duration'
        else:
            # Skip if we can't determine context type
            continue
            
        # Get year information
        year = None
        if instant_date:
            year = instant_date.split('-')[0] if '-' in instant_date else ''
        elif end_date:
            year = end_date.split('-')[0] if '-' in end_date else ''
            
        # Check for segment info
        segment = ""
        if 'entity' in context_info and 'segment' in context_info['entity']:
            segment_data = context_info['entity']['segment']
            if isinstance(segment_data, dict):
                segment_parts = []
                for key, value in segment_data.items():
                    segment_parts.append(f"{key}:{value}")
                if segment_parts:
                    segment = ", ".join(segment_parts)
            elif isinstance(segment_data, str):
                segment = segment_data
                
        # Determine period and create semantic code
        period_str = None
        semantic_code = None
        fiscal_period = None
        
        if ctx_type == 'instant':
            period_str = instant_date
            semantic_code = f"{year}_END" if year else "DATE"
            if segment:
                semantic_code = f"{semantic_code}_{segment}"
        else:
            period_str = f"{start_date} to {end_date}"
            if start_date and end_date:
                start_year = start_date.split('-')[0] if '-' in start_date else ''
                end_year = end_date.split('-')[0] if '-' in end_date else ''
                
                if start_year == end_year and start_year:
                    # Same year period
                    semantic_code = f"FY{year}"
                    fiscal_period = "annual"
                elif start_year and end_year:
                    # Multi-year period
                    semantic_code = f"{start_year}_{end_year}"
                else:
                    semantic_code = "PERIOD"
                    
            if segment:
                semantic_code = f"{semantic_code}_{segment}"
                
        # Get code from context map or create a simple one
        code = context_map.get(context_id, f"c-{context_id}")
        
        # Create context dictionary entry
        context_dict[context_id] = {
            'code': code,
            'type': ctx_type,
            'period': period_str,
            'semantic_code': semantic_code,
            'start_date': start_date,
            'end_date': end_date,
            'instant_date': instant_date,
            'segment': segment,
            'year': year,
            'fiscal_period': fiscal_period,
            'description': f"{semantic_code or 'Unknown'} - {period_str or 'Unknown period'}"
        }
        
    return context_dict 