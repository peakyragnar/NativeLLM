#!/usr/bin/env python3
"""
Enhanced processor that handles both XBRL and iXBRL formats.

This module provides a unified approach to processing financial filings
that works with both traditional XBRL documents and modern iXBRL formats.
It detects the format automatically and applies the appropriate processing
strategy, returning parsed data in a consistent structure.
"""

import os
import sys
import time
import logging

# Import from src2 modules
from ..downloader.direct_edgar_downloader import DirectEdgarDownloader
from ..processor.ixbrl_extractor import process_ixbrl_file
from ..xbrl.xbrl_parser import parse_xbrl_file

def determine_preferred_format(filing_metadata):
    """
    Determine the preferred processing format for a filing.
    
    This function decides whether to process a filing using traditional XBRL
    or the newer iXBRL approach based on filing date and type. It uses heuristics
    to make an initial guess, but the actual processing will attempt multiple
    approaches if needed.
    """
    filing_date = filing_metadata.get("filing_date", "")
    
    # Most newer filings (especially after June 2019) are likely to be using iXBRL
    # as per SEC mandate for Inline XBRL
    if filing_date:
        try:
            filing_year = int(filing_date.split("-")[0])
            filing_month = int(filing_date.split("-")[1]) if len(filing_date.split("-")) > 1 else 1
            
            # SEC mandated Inline XBRL in phases:
            # - Large accelerated filers (June 2019)
            # - Accelerated filers (June 2020)
            # - All others (June 2021)
            if filing_year > 2021 or (filing_year == 2021 and filing_month >= 6):
                return "ixbrl"
            elif filing_year > 2020 or (filing_year == 2020 and filing_month >= 6):
                return "ixbrl_likely"
            elif filing_year > 2019 or (filing_year == 2019 and filing_month >= 6):
                return "ixbrl_possible"
        except:
            # If we can't parse the date, don't make assumptions
            pass
    
    # Default to trying both, traditional XBRL first for older filings
    return "hybrid"

def process_filing_ixbrl(filing_metadata, downloader=None, force_download=False):
    """
    Process a filing using the iXBRL approach.
    
    This function downloads and processes an iXBRL document, typically an HTML file
    that contains inline XBRL tags. It returns the extracted data in a format
    compatible with the main processing pipeline.
    """
    ticker = filing_metadata.get('ticker', 'UNKNOWN')
    filing_type = filing_metadata.get('filing_type', 'UNKNOWN')
    logging.info(f"Processing {ticker} {filing_type} using iXBRL approach")
    
    # If we have a document_url from the enhanced URL detection, use it
    if "document_url" in filing_metadata:
        logging.info(f"Using pre-discovered document URL: {filing_metadata['document_url']}")
        # Make sure this URL is used by the downloader
        filing_metadata["html_url"] = filing_metadata["document_url"]
    
    # Ensure we have a downloader instance
    if downloader is None:
        downloader = DirectEdgarDownloader()
    
    # Step 1: Download the HTML document
    download_result = downloader.download_filing(ticker, filing_type, filing_metadata)
    if "error" in download_result:
        logging.error(f"Error downloading iXBRL document: {download_result['error']}")
        return download_result
    
    file_path = download_result.get("file_path")
    
    # If the download discovered document URLs, add them back to metadata to share
    # Ensure both URL fields are set and synchronized
    if "document_url" in download_result or "primary_doc_url" in download_result:
        # Get the URL from either field
        discovered_url = download_result.get("document_url", "") or download_result.get("primary_doc_url", "")
        
        if discovered_url:
            # Update both fields to ensure synchronization
            filing_metadata["document_url"] = discovered_url
            filing_metadata["primary_doc_url"] = discovered_url
            logging.info(f"Adding discovered URL to both document_url and primary_doc_url: {discovered_url}")
    
    # Step 2: Extract data from the iXBRL document
    extracted_data = process_ixbrl_file(file_path, filing_metadata)
    if "error" in extracted_data:
        logging.error(f"Error extracting iXBRL data: {extracted_data['error']}")
        return extracted_data
    
    # Convert the extracted data into a format compatible with the main pipeline
    # (similar to what parse_xbrl_file returns)
    
    # Map contexts from iXBRL format to XBRL format expected by pipeline
    contexts = {}
    for context_id, context_data in extracted_data.get("contexts", {}).items():
        context_obj = {
            "id": context_id,
            "entity": context_data.get("entity", {}).get("identifier", ""),
            "period": context_data.get("period", {}),
            "dimensions": context_data.get("dimensions", {})
        }
        contexts[context_id] = context_obj
    
    # Map units from iXBRL format to XBRL format expected by pipeline
    units = {}
    for unit_id, unit_data in extracted_data.get("units", {}).items():
        unit_type = unit_data.get("type", "simple")
        if unit_type == "simple":
            units[unit_id] = unit_data.get("measure", "")
        else:
            # For divide units, format as numerator/denominator
            numerator = unit_data.get("numerator", "")
            denominator = unit_data.get("denominator", "")
            units[unit_id] = f"{numerator}/{denominator}"
    
    # Map facts from iXBRL format to XBRL format expected by pipeline
    facts = []
    for fact in extracted_data.get("facts", []):
        fact_obj = {
            "concept": fact.get("name", ""),
            "value": fact.get("value", ""),
            "context_ref": fact.get("context_ref", ""),
            "unit_ref": fact.get("unit_ref", ""),
            "decimals": fact.get("decimals", ""),
            "format": fact.get("format", "")
        }
        facts.append(fact_obj)
    
    # Add fact count for tracking and validation
    fact_count = len(facts)
    
    return {
        "success": True,
        "processing_path": "ixbrl",
        "contexts": contexts,
        "units": units,
        "facts": facts,
        "fact_count": fact_count,
        "file_path": file_path
    }

def process_filing_xbrl(filing_metadata, downloader=None, force_download=False):
    """
    Process a filing using the traditional XBRL approach.
    
    This function downloads and processes a traditional XBRL instance document,
    typically an XML file. It returns the parsed data in the format expected
    by the main processing pipeline.
    """
    ticker = filing_metadata.get('ticker', 'UNKNOWN')
    filing_type = filing_metadata.get('filing_type', 'UNKNOWN')
    logging.info(f"Processing {ticker} {filing_type} using traditional XBRL approach")
    
    # Check for either document_url or primary_doc_url from the enhanced URL detection
    document_url = filing_metadata.get("document_url", "") or filing_metadata.get("primary_doc_url", "")
    
    # Make sure both URL fields are present for consistency
    if document_url:
        logging.info(f"Note: document URL is available: {document_url}")
        
        # Ensure both fields are set consistently
        if not filing_metadata.get("document_url"):
            filing_metadata["document_url"] = document_url
            logging.info(f"Added missing document_url: {document_url}")
        
        if not filing_metadata.get("primary_doc_url"):
            filing_metadata["primary_doc_url"] = document_url
            logging.info(f"Added missing primary_doc_url: {document_url}")
        
        # Try to derive instance URL from document URL if not already present
        if "instance_url" not in filing_metadata:
            # Common pattern: .htm -> _htm.xml
            if document_url.endswith('.htm') or document_url.endswith('.html'):
                instance_url = document_url.rsplit('.', 1)[0] + '_htm.xml'
                logging.info(f"Derived possible instance URL from document URL: {instance_url}")
                # Don't overwrite an existing instance_url, just add as alternative
                filing_metadata["alternative_instance_url"] = instance_url
    
    # Ensure we have a downloader instance
    if downloader is None:
        downloader = DirectEdgarDownloader()
    
    # Step 1: Download XBRL instance
    download_result = downloader.download_filing(ticker, filing_type, filing_metadata, target_format="xbrl")
    if "error" in download_result:
        logging.error(f"Error downloading XBRL: {download_result['error']}")
        return download_result
    
    file_path = download_result.get("file_path")
    
    # If the download discovered document URLs, add them back to metadata to share
    # Ensure both URL fields are set and synchronized
    if "document_url" in download_result or "primary_doc_url" in download_result:
        # Get the URL from either field
        discovered_url = download_result.get("document_url", "") or download_result.get("primary_doc_url", "")
        
        if discovered_url:
            # Update both fields to ensure synchronization
            filing_metadata["document_url"] = discovered_url
            filing_metadata["primary_doc_url"] = discovered_url
            logging.info(f"Adding discovered URL to both document_url and primary_doc_url: {discovered_url}")
    
    # Step 2: Parse XBRL
    parsed_result = parse_xbrl_file(file_path)
    if "error" in parsed_result:
        logging.error(f"Error parsing XBRL: {parsed_result['error']}")
        return parsed_result
    
    # Add processing path information to the result
    parsed_result["processing_path"] = "xbrl"
    parsed_result["file_path"] = file_path
    
    # Add fact count for tracking
    parsed_result["fact_count"] = len(parsed_result.get("facts", []))
    
    return parsed_result

def process_company_filing(filing_metadata, downloader=None, force_download=False):
    """
    Process a filing with enhanced format detection.
    
    This function determines the appropriate processing approach (XBRL vs iXBRL)
    based on filing date and type information, then processes accordingly.
    It includes fallback logic to try alternative formats if the preferred
    approach fails.
    """
    ticker = filing_metadata.get("ticker", "UNKNOWN")
    filing_type = filing_metadata.get("filing_type", "UNKNOWN")
    
    # Ensure we have a downloader instance
    if downloader is None:
        downloader = DirectEdgarDownloader()
    
    # Handle special case for foreign companies using 20-F instead of 10-K
    foreign_filing_type = None
    if filing_type == '10-K':
        foreign_filing_type = '20-F'
    
    # Try with the original filing type first
    cik = filing_metadata.get("cik")
    if cik and foreign_filing_type:
        # First, let's check if we need to use the foreign filing type instead
        # Check if we can find the document with the current filing type
        original_check = downloader.check_filing_availability(ticker, filing_type, filing_metadata)
        
        if not original_check.get("available", False):
            logging.info(f"[{ticker} {filing_type}] No documents found, trying foreign filing type {foreign_filing_type}")
            
            # Try with the foreign filing type
            foreign_check = downloader.check_filing_availability(ticker, foreign_filing_type, filing_metadata)
            if foreign_check.get("available", False):
                # Update filing type and URLs
                original_filing_type = filing_type
                filing_type = foreign_filing_type
                filing_metadata["filing_type"] = foreign_filing_type
                filing_metadata["original_filing_type"] = original_filing_type
                
                # Update metadata with URL information from the check
                for key in ["document_url", "primary_doc_url", "instance_url", "documents_url", 
                            "accession_number", "filing_date", "period_end_date"]:
                    if key in foreign_check:
                        filing_metadata[key] = foreign_check[key]
                
                logging.info(f"[{ticker} {filing_type}] Successfully found foreign filing type documents")
    
    # Determine preferred format
    preferred_format = determine_preferred_format(filing_metadata)
    logging.info(f"[{ticker} {filing_type}] Determined preferred format: {preferred_format}")
    
    # Save original URLs for logging/debugging
    instance_url = filing_metadata.get("instance_url", "None")
    logging.info(f"[{ticker} {filing_type}] Instance URL: {instance_url}")
    
    # Process using the appropriate strategy based on the preferred format
    if preferred_format == "ixbrl":
        # For definitely iXBRL files (newer filings)
        logging.info(f"[{ticker} {filing_type}] Attempting iXBRL processing first (newer format)")
        result = process_filing_ixbrl(filing_metadata, downloader, force_download)
        
        # If iXBRL processing fails, fall back to traditional XBRL
        if "error" in result:
            error = result.get("error", "Unknown error")
            logging.warning(f"[{ticker} {filing_type}] iXBRL processing failed: {error}")
            logging.info(f"[{ticker} {filing_type}] Falling back to traditional XBRL")
            result = process_filing_xbrl(filing_metadata, downloader, force_download)
    
    elif preferred_format == "ixbrl_likely" or preferred_format == "ixbrl_possible":
        # For filings that are likely to be iXBRL but not certain
        logging.info(f"[{ticker} {filing_type}] Attempting iXBRL processing first (likely format)")
        # Try iXBRL first, but fall back to XBRL if it fails
        result = process_filing_ixbrl(filing_metadata, downloader, force_download)
        
        if "error" in result:
            error = result.get("error", "Unknown error")
            logging.warning(f"[{ticker} {filing_type}] iXBRL processing failed: {error}")
            logging.info(f"[{ticker} {filing_type}] Trying traditional XBRL")
            result = process_filing_xbrl(filing_metadata, downloader, force_download)
    
    elif preferred_format == "hybrid":
        # For ambiguous cases, try both formats
        logging.info(f"[{ticker} {filing_type}] Using hybrid approach - trying both formats")
        
        # For hybrid approach - actually try iXBRL first for SEC filings after 2020
        filing_date = filing_metadata.get("filing_date", "")
        try:
            filing_year = int(filing_date.split("-")[0]) if filing_date and "-" in filing_date else 0
            if filing_year >= 2020:
                logging.info(f"[{ticker} {filing_type}] Post-2020 filing, trying iXBRL first")
                result = process_filing_ixbrl(filing_metadata, downloader, force_download)
                
                if "error" in result:
                    error = result.get("error", "Unknown error")
                    logging.warning(f"[{ticker} {filing_type}] iXBRL processing failed: {error}")
                    logging.info(f"[{ticker} {filing_type}] Falling back to traditional XBRL")
                    result = process_filing_xbrl(filing_metadata, downloader, force_download)
            else:
                # Pre-2020, try traditional XBRL first
                result = process_filing_xbrl(filing_metadata, downloader, force_download)
                
                if "error" in result:
                    error = result.get("error", "Unknown error")
                    logging.warning(f"[{ticker} {filing_type}] Traditional XBRL processing failed: {error}")
                    logging.info(f"[{ticker} {filing_type}] Trying iXBRL format")
                    result = process_filing_ixbrl(filing_metadata, downloader, force_download)
        except:
            # If date parsing fails, try both formats
            logging.info(f"[{ticker} {filing_type}] Could not determine filing year, trying XBRL first")
            result = process_filing_xbrl(filing_metadata, downloader, force_download)
            
            if "error" in result:
                error = result.get("error", "Unknown error")
                logging.warning(f"[{ticker} {filing_type}] Traditional XBRL processing failed: {error}")
                logging.info(f"[{ticker} {filing_type}] Trying iXBRL format")
                result = process_filing_ixbrl(filing_metadata, downloader, force_download)
    
    else:
        # Default to XBRL with iXBRL fallback
        logging.info(f"[{ticker} {filing_type}] Using default format - trying XBRL first")
        result = process_filing_xbrl(filing_metadata, downloader, force_download)
        
        if "error" in result:
            error = result.get("error", "Unknown error")
            logging.warning(f"[{ticker} {filing_type}] Traditional XBRL processing failed: {error}")
            logging.info(f"[{ticker} {filing_type}] Trying iXBRL format")
            result = process_filing_ixbrl(filing_metadata, downloader, force_download)
    
    # Log the final result
    if "error" in result:
        logging.error(f"[{ticker} {filing_type}] Final result: ERROR - {result.get('error')}")
    else:
        logging.info(f"[{ticker} {filing_type}] Final result: SUCCESS - Using {result.get('processing_path', 'unknown')} approach")
        if "fact_count" in result:
            logging.info(f"[{ticker} {filing_type}] Extracted {result.get('fact_count', 0)} facts")
            
    return result

def process_and_format_filing(filing_metadata, downloader=None, force_download=False):
    """
    Process a filing and return the parsed data.
    
    This is a compatibility function that maintains the existing API
    but uses the enhanced processing capabilities.
    
    Note: This function does NOT format or save the data - it returns
    the parsed data for the main pipeline to format and save.
    """
    logging.info(f"Processing filing: {filing_metadata.get('ticker')} {filing_metadata.get('filing_type')}")
    
    # Process the filing using the enhanced processor
    result = process_company_filing(filing_metadata, downloader, force_download)
    
    if "error" in result:
        logging.error(f"Error processing filing: {result['error']}")
        return result
    
    logging.info(f"Successfully processed filing using {result.get('processing_path', 'unknown')} approach")
    logging.info(f"Source file: {result.get('file_path')}")
    logging.info(f"Facts extracted: {len(result.get('facts', []))}")
    
    return result