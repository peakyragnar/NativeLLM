# src/xbrl/xbrl_downloader.py
import os
import sys
import logging
from urllib.parse import urlparse, unquote

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.edgar.edgar_utils import sec_request
from src.config import RAW_DATA_DIR

def download_xbrl_instance(filing_metadata):
    """Download XBRL instance document for a filing"""
    # Create directory path
    ticker = filing_metadata.get("ticker", "unknown")
    filing_type = filing_metadata.get("filing_type", "unknown")
    accession_number = filing_metadata.get("accession_number", "unknown")
    
    logging.info(f"Downloading XBRL for {ticker} {filing_type} (accession: {accession_number})")
    
    # Create directory structure
    dir_path = os.path.join(RAW_DATA_DIR, ticker, filing_type)
    os.makedirs(dir_path, exist_ok=True)
    logging.info(f"Created directory: {dir_path}")
    
    # Download the instance document - check both instance_url and xbrl_url for compatibility
    instance_url = filing_metadata.get("instance_url") or filing_metadata.get("xbrl_url")
    if not instance_url:
        logging.error("No instance URL or xbrl URL provided in metadata")
        return {"error": "No instance URL provided"}
    
    try:
        logging.info(f"Downloading XBRL from {instance_url}")
        response = sec_request(instance_url)
        
        if response.status_code != 200:
            logging.error(f"Failed to download XBRL: HTTP {response.status_code}")
            # Try alternative URL formats
            if instance_url.endswith('_htm.xml'):
                # Some SEC files might use a different extension
                alt_url = instance_url.replace('_htm.xml', '.xml')
                logging.info(f"Trying alternative URL: {alt_url}")
                alt_response = sec_request(alt_url)
                if alt_response.status_code == 200:
                    logging.info("Alternative URL succeeded")
                    response = alt_response
                    instance_url = alt_url
                else:
                    logging.warning(f"Alternative URL also failed: HTTP {alt_response.status_code}")
        
        if response.status_code != 200:
            return {"error": f"Failed to download instance document: {response.status_code}"}
        
        # Generate a safe filename from the URL if needed
        url_parts = urlparse(instance_url)
        path_parts = url_parts.path.split('/')
        url_filename = path_parts[-1] if path_parts else "unknown.xml"
        url_filename = unquote(url_filename)  # Handle URL encoding
        
        if not accession_number or accession_number == "unknown":
            # Use a part of the URL path as the filename
            if len(path_parts) >= 2:
                accession_number = f"{path_parts[-2]}_{url_filename}"
            else:
                accession_number = url_filename
        
        # Save the file
        file_path = os.path.join(dir_path, f"{accession_number}_instance.xml")
        logging.info(f"Saving XBRL to {file_path}")
        
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        # Check if we received an actual XML file
        content_type = response.headers.get('Content-Type', '')
        content_size = len(response.content)
        logging.info(f"Downloaded {content_size:,} bytes, Content-Type: {content_type}")
        
        if content_size < 100:  # Too small to be a real XBRL file
            logging.warning(f"Downloaded file is suspiciously small ({content_size} bytes)")
            
            # Check if it looks like XML
            if b'<?xml' not in response.content and b'<xbrl' not in response.content and b'<html' in response.content:
                logging.error("Downloaded content appears to be HTML, not XML")
                return {"error": "Downloaded content is not valid XBRL/XML"}
        
        return {
            "success": True,
            "file_path": file_path,
            "size": content_size,
            "content_type": content_type
        }
    except Exception as e:
        logging.error(f"Exception downloading XBRL: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return {"error": f"Exception downloading instance document: {str(e)}"}