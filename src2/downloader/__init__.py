"""
SEC EDGAR Downloader Package

This package provides a direct downloader for SEC EDGAR filings:
- DirectEdgarDownloader: Uses direct HTTP requests
"""

from .direct_edgar_downloader import DirectEdgarDownloader, test_direct_download

# Factory function to get the appropriate downloader
def get_downloader(strategy="direct", user_agent=None):
    """
    Get a downloader based on the specified strategy
    
    Args:
        strategy: Currently only 'direct' is supported
        user_agent: User agent for SEC EDGAR
        
    Returns:
        Downloader instance
    """
    if user_agent is None:
        raise ValueError("User agent is required")
    
    if strategy == "direct":
        return DirectEdgarDownloader(user_agent)
    else:
        raise ValueError(f"Unknown or unsupported strategy: {strategy}. Use 'direct' instead.")