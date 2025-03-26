"""
SEC Filing Processor Package

This package provides processors for SEC filing data:
- XBRLProcessor: Process XBRL files
- HTMLProcessor: Process HTML files to extract text
- HTMLOptimizer: Optimize HTML content while preserving data integrity
"""

from .xbrl_processor import XBRLProcessor, xbrl_processor
from .html_processor import HTMLProcessor, html_processor
from .html_optimizer import HTMLOptimizer, html_optimizer