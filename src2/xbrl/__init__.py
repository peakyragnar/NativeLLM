"""
XBRL and HTML Processing Module

This module contains improved processors for XBRL and HTML content.
"""

# Import HTML processing module
from .html_text_extractor import process_html_filing

# Import XBRL processing modules
# Make these imports available to other modules
try:
    # Try relative imports first
    from .xbrl_facts_extractor import extract_facts_from_html
    from .xbrl_mapper import xbrl_mapper
except ImportError:
    try:
        # Fall back to absolute imports if relative imports fail
        from src2.xbrl.xbrl_facts_extractor import extract_facts_from_html
        from src2.xbrl.xbrl_mapper import xbrl_mapper
    except ImportError:
        import logging
        logging.getLogger(__name__).warning("XBRL processing modules not available")

# Export these symbols
__all__ = ['process_html_filing', 'extract_facts_from_html', 'xbrl_mapper']