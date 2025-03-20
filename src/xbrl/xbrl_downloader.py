# src/xbrl/xbrl_downloader.py
import os
import sys

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
    
    # Create directory structure
    dir_path = os.path.join(RAW_DATA_DIR, ticker, filing_type)
    os.makedirs(dir_path, exist_ok=True)
    
    # Download the instance document
    instance_url = filing_metadata.get("instance_url")
    if not instance_url:
        return {"error": "No instance URL provided"}
    
    try:
        response = sec_request(instance_url)
        if response.status_code != 200:
            return {"error": f"Failed to download instance document: {response.status_code}"}
        
        # Save the file
        file_path = os.path.join(dir_path, f"{accession_number}_instance.xml")
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        return {
            "success": True,
            "file_path": file_path,
            "size": len(response.content)
        }
    except Exception as e:
        return {"error": f"Exception downloading instance document: {str(e)}"}