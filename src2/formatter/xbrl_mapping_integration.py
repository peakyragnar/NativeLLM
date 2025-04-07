#!/usr/bin/env python3
"""
XBRL Mapping Integration

This module integrates the XBRL mapping solution into the LLMFormatter.
"""

import os
import logging
import json
import re
from pathlib import Path

class XBRLMappingIntegration:
    """
    Integrates XBRL mapping solution into the LLMFormatter.
    """
    
    def __init__(self):
        """Initialize the XBRL mapping integration."""
        self.logger = logging.getLogger(__name__)
    
    def integrate_xbrl_mapping(self, parsed_xbrl, filing_metadata):
        """
        Integrate XBRL mapping into the parsed XBRL data.
        
        Args:
            parsed_xbrl: Parsed XBRL data
            filing_metadata: Filing metadata
            
        Returns:
            Updated parsed XBRL data with hierarchical mapping
        """
        try:
            # Import the XBRL mapper
            from src2.xbrl.xbrl_mapper import xbrl_mapper
            
            # Check if we have the necessary data
            if not parsed_xbrl or not filing_metadata:
                self.logger.warning("Missing parsed XBRL data or filing metadata")
                return parsed_xbrl
            
            # Get the HTML file path
            html_path = None
            if "html_file" in filing_metadata:
                html_path = filing_metadata["html_file"]
            elif "doc_path" in filing_metadata:
                html_path = filing_metadata["doc_path"]
            
            if not html_path or not os.path.exists(html_path):
                self.logger.warning(f"HTML file not found: {html_path}")
                return parsed_xbrl
            
            # Get the directory containing the HTML file
            html_dir = Path(html_path).parent
            
            # Check if we have raw XBRL facts
            raw_facts_path = html_dir / "_xbrl_raw.json"
            if not os.path.exists(raw_facts_path):
                self.logger.warning(f"Raw XBRL facts not found: {raw_facts_path}")
                return parsed_xbrl
            
            # Find linkbase files
            linkbase_files = list(html_dir.glob("*_pre.xml"))
            if not linkbase_files:
                self.logger.warning(f"No linkbase files found in {html_dir}")
                
                # Try to find schema file with embedded linkbases
                schema_files = list(html_dir.glob("*.xsd"))
                if schema_files:
                    self.logger.info(f"Found schema file: {schema_files[0]}")
                    
                    # Extract mappings from schema file
                    mappings_path = html_dir / "_xbrl_mappings.json"
                    mappings = xbrl_mapper.extract_mappings_from_schema(schema_files[0])
                    
                    # Save mappings to file
                    with open(mappings_path, 'w', encoding='utf-8') as f:
                        json.dump(mappings, f, indent=2)
                    
                    self.logger.info(f"Extracted mappings from schema file and saved to {mappings_path}")
                else:
                    self.logger.warning(f"No schema files found in {html_dir}")
                    return parsed_xbrl
            else:
                self.logger.info(f"Found linkbase file: {linkbase_files[0]}")
                
                # Extract mappings from linkbase file
                mappings_path = html_dir / "_xbrl_mappings.json"
                mappings = xbrl_mapper.extract_mappings_from_linkbase(linkbase_files[0])
                
                # Save mappings to file
                with open(mappings_path, 'w', encoding='utf-8') as f:
                    json.dump(mappings, f, indent=2)
                
                self.logger.info(f"Extracted mappings from linkbase file and saved to {mappings_path}")
            
            # Create LLM-friendly output
            llm_output_path = html_dir / "_xbrl_llm.txt"
            xbrl_mapper.create_llm_friendly_output(mappings_path, raw_facts_path, llm_output_path)
            
            self.logger.info(f"Created LLM-friendly output and saved to {llm_output_path}")
            
            # Add hierarchical mapping to parsed XBRL data
            parsed_xbrl["hierarchical_mapping"] = {
                "mappings_path": str(mappings_path),
                "llm_output_path": str(llm_output_path)
            }
            
            # Read the LLM-friendly output
            with open(llm_output_path, 'r', encoding='utf-8') as f:
                llm_output = f.read()
            
            # Add the LLM-friendly output to the parsed XBRL data
            parsed_xbrl["llm_friendly_output"] = llm_output
            
            return parsed_xbrl
        
        except ImportError:
            self.logger.warning("XBRL mapper module not found")
            return parsed_xbrl
        except Exception as e:
            self.logger.error(f"Error integrating XBRL mapping: {str(e)}")
            return parsed_xbrl

# Create a singleton instance
xbrl_mapping_integration = XBRLMappingIntegration()
