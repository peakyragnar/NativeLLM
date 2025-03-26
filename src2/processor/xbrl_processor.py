"""
XBRL Processor Module

Responsible for processing XBRL files.
"""

import os
import logging
from lxml import etree

class XBRLProcessor:
    """
    Process XBRL files from SEC filings with enhanced format detection
    """
    
    def __init__(self):
        """
        Initialize XBRL processor
        """
        pass
    
    def determine_format(self, file_path):
        """
        Determine if the file is traditional XBRL or iXBRL
        
        Args:
            file_path: Path to the XBRL or HTML+iXBRL file
            
        Returns:
            Format type ('xbrl' or 'ixbrl')
        """
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read(2000)  # Read just the beginning
                
            # Check for iXBRL indicators
            if 'xmlns:ix=' in content or '<ix:' in content or 'inline XBRL' in content.lower():
                return 'ixbrl'
            # Check for traditional XBRL indicators
            elif '<?xml' in content and ('xbrl' in content.lower() or '<xbrl' in content):
                return 'xbrl'
            # If it's HTML but might contain iXBRL
            elif '<html' in content.lower() or '<body' in content.lower():
                return 'ixbrl_possible'
            # Default to XBRL as fallback
            else:
                return 'xbrl'
                
        except Exception as e:
            logging.warning(f"Error determining file format: {str(e)}")
            # Default to XBRL if we can't determine
            return 'xbrl'
    
    def parse_xbrl_file(self, file_path, ticker=None):
        """
        Parse an XBRL file with automatic format detection
        
        Args:
            file_path: Path to the XBRL file
            ticker: Optional ticker symbol for context
            
        Returns:
            Dict with parsed XBRL data
        """
        logging.info(f"Parsing XBRL file: {file_path}")
        
        # Determine file format first
        format_type = self.determine_format(file_path)
        logging.info(f"Detected file format: {format_type}")
        
        # Use appropriate parser based on format
        if format_type == 'ixbrl' or format_type == 'ixbrl_possible':
            try:
                return self.parse_ixbrl_file(file_path, ticker)
            except Exception as e:
                logging.warning(f"Error parsing as iXBRL, falling back to traditional XBRL: {str(e)}")
                # Fall back to traditional XBRL parser
                return self.parse_traditional_xbrl(file_path, ticker)
        else:
            return self.parse_traditional_xbrl(file_path, ticker)
    
    def parse_traditional_xbrl(self, file_path, ticker=None):
        """
        Parse a traditional XBRL file
        
        Args:
            file_path: Path to the XBRL file
            ticker: Optional ticker symbol for context
            
        Returns:
            Dict with parsed XBRL data
        """
        logging.info(f"Parsing traditional XBRL file: {file_path}")
        
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
                "processing_path": "xbrl",
                "contexts": contexts,
                "units": units,
                "facts": facts,
                "fact_count": len(facts)
            }
        except Exception as e:
            logging.error(f"Error parsing XBRL: {str(e)}")
            return {"error": f"Error parsing XBRL: {str(e)}"}
    
    def parse_ixbrl_file(self, file_path, ticker=None):
        """
        Parse an inline XBRL (iXBRL) file
        
        Args:
            file_path: Path to the HTML file containing iXBRL
            ticker: Optional ticker symbol for context
            
        Returns:
            Dict with parsed XBRL data
        """
        logging.info(f"Parsing iXBRL file: {file_path}")
        
        try:
            # Read HTML file
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                html_content = f.read()
            
            # Extract iXBRL elements
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all ix tags
            ix_facts = soup.find_all(['ix:nonfraction', 'ix:nonfraction', 'ix:nonNumeric'])
            
            # Get contexts
            ix_contexts = soup.find_all(['ix:context', 'context'])
            
            # Get units
            ix_units = soup.find_all(['ix:unit', 'unit'])
            
            # Extract contexts
            contexts = {}
            for ctx in ix_contexts:
                context_id = ctx.get('id')
                if not context_id:
                    continue
                    
                # Extract period
                period_info = {}
                period_elem = ctx.find(['period', 'ix:period'])
                if period_elem:
                    instant = period_elem.find(['instant', 'ix:instant'])
                    if instant:
                        period_info['instant'] = instant.text.strip()
                    
                    start_date = period_elem.find(['startdate', 'ix:startdate'])
                    end_date = period_elem.find(['enddate', 'ix:enddate'])
                    if start_date and end_date:
                        period_info['startDate'] = start_date.text.strip()
                        period_info['endDate'] = end_date.text.strip()
                
                # Extract dimensions
                dimensions = {}
                segment = ctx.find(['segment', 'ix:segment'])
                if segment:
                    for member in segment.find_all(['explicitmember', 'ix:explicitmember']):
                        dimension = member.get('dimension', '').split(':')[-1]
                        value = member.text.strip()
                        dimensions[dimension] = value
                
                # Store context
                contexts[context_id] = {
                    'id': context_id,
                    'period': period_info,
                    'dimensions': dimensions
                }
            
            # Extract units
            units = {}
            for unit in ix_units:
                unit_id = unit.get('id')
                if not unit_id:
                    continue
                    
                # Get measure
                measure = unit.find(['measure', 'ix:measure'])
                if measure:
                    units[unit_id] = measure.text.strip()
            
            # Extract facts
            facts = []
            for fact in ix_facts:
                # Get basic attributes
                name = fact.get('name', '')
                context_ref = fact.get('contextref')
                unit_ref = fact.get('unitref')
                decimals = fact.get('decimals')
                format_attr = fact.get('format')
                
                # Get value - handle hidden facts
                if fact.get('style') == 'display:none':
                    value = fact.text.strip() if fact.text else ''
                else:
                    # For visible facts, get the transformed value if available
                    value = fact.get('value', fact.text or '').strip()
                
                # Create fact object
                fact_obj = {
                    'concept': name,
                    'value': value,
                    'context_ref': context_ref,
                    'unit_ref': unit_ref,
                    'decimals': decimals,
                    'format': format_attr
                }
                
                facts.append(fact_obj)
            
            return {
                'success': True,
                'processing_path': 'ixbrl',
                'contexts': contexts,
                'units': units,
                'facts': facts,
                'fact_count': len(facts)
            }
        except Exception as e:
            logging.error(f"Error parsing iXBRL: {str(e)}")
            return {"error": f"Error parsing iXBRL: {str(e)}"}
    
    def get_filing_metadata(self, parsed_xbrl):
        """
        Extract metadata from parsed XBRL
        
        Args:
            parsed_xbrl: Parsed XBRL data from parse_xbrl_file()
            
        Returns:
            Dict with filing metadata
        """
        if "error" in parsed_xbrl:
            return {"error": parsed_xbrl["error"]}
        
        try:
            metadata = {}
            
            # Add processing path information
            if "processing_path" in parsed_xbrl:
                metadata["processing_path"] = parsed_xbrl["processing_path"]
            
            # Find company name
            company_name_facts = [f for f in parsed_xbrl["facts"] 
                                  if f["concept"].lower() in 
                                  ["entityregistrantname", "entityname", "companyname"]]
            if company_name_facts:
                metadata["company_name"] = company_name_facts[0]["value"]
            
            # Find ticker
            ticker_facts = [f for f in parsed_xbrl["facts"] 
                            if f["concept"].lower() in 
                            ["tradingsymbol", "ticker", "symbolname"]]
            if ticker_facts:
                metadata["ticker"] = ticker_facts[0]["value"]
            
            # Find fiscal year
            fiscal_year_facts = [f for f in parsed_xbrl["facts"] 
                                if "fiscalyear" in f["concept"].lower()]
            if fiscal_year_facts:
                metadata["fiscal_year"] = fiscal_year_facts[0]["value"]
            
            # Find fiscal period
            fiscal_period_facts = [f for f in parsed_xbrl["facts"] 
                                  if "fiscalperiod" in f["concept"].lower()]
            if fiscal_period_facts:
                metadata["fiscal_period"] = fiscal_period_facts[0]["value"]
            
            # Find period end date
            period_end_facts = [f for f in parsed_xbrl["facts"] 
                               if f["concept"].lower() in 
                               ["periodenddateandtime", "periodenddate", "documentperiodenddate"]]
            if period_end_facts:
                metadata["period_end_date"] = period_end_facts[0]["value"]
            
            # Find filing date
            filing_date_facts = [f for f in parsed_xbrl["facts"] 
                                if f["concept"].lower() in 
                                ["documentdate", "filingdate", "documentperiodenddate"]]
            if filing_date_facts:
                metadata["filing_date"] = filing_date_facts[0]["value"]
            
            return metadata
        except Exception as e:
            logging.error(f"Error extracting XBRL metadata: {str(e)}")
            return {"error": f"Error extracting XBRL metadata: {str(e)}"}
    
    def process_company_filing(self, filing_metadata, file_path=None):
        """
        Process a company filing with format detection and fallback
        
        Args:
            filing_metadata: Metadata about the filing
            file_path: Path to the filing file (if already downloaded)
            
        Returns:
            Dict with parsed data
        """
        ticker = filing_metadata.get("ticker", "unknown")
        filing_type = filing_metadata.get("filing_type", "unknown")
        
        logging.info(f"Processing {ticker} {filing_type} filing")
        
        # If file was already downloaded and path provided, use it
        if file_path and os.path.exists(file_path):
            logging.info(f"Using provided file: {file_path}")
            return self.parse_xbrl_file(file_path, ticker)
        
        # Otherwise we would need to download it - notify caller
        return {"error": "File path not provided or file not found"}

# Create a singleton instance
xbrl_processor = XBRLProcessor()