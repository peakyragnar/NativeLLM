"""
SEC Filing Processing Package

This package implements SEC-compliant downloading, rendering, and extraction
of financial filings with proper handling of iXBRL documents.
"""

from .downloader import SECDownloader
from .renderer import ArelleRenderer
from .extractor import SECExtractor
from .pipeline import SECFilingPipeline