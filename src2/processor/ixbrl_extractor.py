#!/usr/bin/env python3
"""
SEC iXBRL Document Extractor with Headless Browser

This module uses Playwright to render and extract content from SEC iXBRL viewer pages
that require JavaScript execution.
"""

import os
import sys
import logging
import time
import re
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

class IXBRLExtractor:
    """
    Class for extracting text from iXBRL SEC documents using Playwright
    """
    
    def __init__(self, headless=True, browser_type="chromium", timeout=60000, wait_time=10000):
        """
        Initialize the iXBRL extractor
        
        Args:
            headless: Whether to run browser in headless mode
            browser_type: Browser type (chromium, firefox, webkit)
            timeout: Page load timeout in milliseconds
            wait_time: Time to wait for iXBRL renderer in milliseconds
        """
        self.headless = headless
        self.browser_type = browser_type
        self.timeout = timeout
        self.wait_time = wait_time
        self.browser = None
        self.context = None
    
    async def extract_from_sec_url(self, sec_url):
        """
        Extract text content from an SEC iXBRL URL
        
        Args:
            sec_url: SEC iXBRL viewer URL (e.g., https://www.sec.gov/ix?doc=/Archives/...)
            
        Returns:
            Dictionary with extracted content and metadata
        """
        if not sec_url.startswith("https://www.sec.gov/ix?doc="):
            # Ensure URL is in correct format
            if "/Archives/edgar/data/" in sec_url:
                # Extract the path portion
                path_match = re.search(r'(/Archives/edgar/data/[^"&\s]+)', sec_url)
                if path_match:
                    sec_url = f"https://www.sec.gov/ix?doc={path_match.group(1)}"
                else:
                    return {"error": f"Invalid SEC URL format: {sec_url}"}
            else:
                return {"error": f"URL doesn't appear to be an SEC filing: {sec_url}"}
        
        try:
            async with async_playwright() as playwright:
                # Launch browser
                browser_engine = getattr(playwright, self.browser_type)
                self.browser = await browser_engine.launch(headless=self.headless)
                self.context = await self.browser.new_context(
                    viewport={"width": 1280, "height": 1600},
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
                )
                
                # Create page and navigate to URL
                page = await self.context.new_page()
                page.set_default_timeout(self.timeout)
                
                # Log the start of navigation
                logging.info(f"Navigating to SEC iXBRL URL: {sec_url}")
                
                # Navigate to the SEC URL
                response = await page.goto(sec_url)
                if not response or response.status >= 400:
                    return {"error": f"Failed to load SEC URL (status {response.status if response else 'unknown'})"}
                
                # Wait for iXBRL viewer to load and render
                logging.info(f"Waiting for iXBRL viewer to render ({self.wait_time/1000} seconds)...")
                
                try:
                    # Wait for the SEC iXBRL viewer to finish loading
                    # First, wait for the loading indicator to appear
                    await page.wait_for_selector(".ixds-loading", timeout=10000)
                    # Then wait for it to disappear
                    await page.wait_for_selector(".ixds-loading", state="hidden", timeout=self.timeout)
                    
                    # Wait for iXBRL content to be visible
                    await page.wait_for_selector(".ixds-document-root", state="visible", timeout=10000)
                    
                    # Additional wait for content to fully render
                    await page.wait_for_timeout(self.wait_time)
                    
                except Exception as e:
                    logging.warning(f"Timeout waiting for iXBRL viewer elements: {str(e)}")
                    logging.info("Continuing with extraction despite timeout")
                
                # Extract document metadata
                metadata = await self._extract_metadata(page)
                
                # Extract document sections
                sections = await self._extract_document_sections(page)
                
                # Extract full text content
                full_text = await self._extract_full_text(page)
                
                # Take a screenshot for debugging
                screenshot_path = f"ixbrl_screenshot_{int(time.time())}.png"
                await page.screenshot(path=screenshot_path)
                
                # Close browser
                await self.browser.close()
                
                return {
                    "success": True,
                    "metadata": metadata,
                    "sections": sections,
                    "full_text": full_text,
                    "screenshot_path": screenshot_path
                }
        
        except Exception as e:
            logging.error(f"Error extracting content from SEC iXBRL URL: {str(e)}")
            if self.browser:
                await self.browser.close()
            return {"error": f"Error extracting content: {str(e)}"}
    
    async def _extract_metadata(self, page):
        """Extract metadata from the SEC iXBRL document"""
        metadata = {}
        
        try:
            # Get document title
            title = await page.title()
            metadata["title"] = title
            
            # Extract company name, filing type and period
            # These are usually in specific SEC iXBRL elements
            
            # Try to get company name from the header
            company_name = await page.evaluate("""() => {
                const headerElement = document.querySelector('.ixds-header');
                return headerElement ? headerElement.innerText : '';
            }""")
            if company_name:
                metadata["company_name"] = company_name.strip()
            
            # Look for filing period
            period_text = await page.evaluate("""() => {
                const periodElements = Array.from(document.querySelectorAll('*[name*="DocumentPeriodEndDate"]'));
                return periodElements.length > 0 ? periodElements[0].innerText : '';
            }""")
            if period_text:
                metadata["period_end_date"] = period_text.strip()
            
            # Get filing type
            filing_type = await page.evaluate("""() => {
                const typeElements = Array.from(document.querySelectorAll('*[name*="DocumentType"]'));
                return typeElements.length > 0 ? typeElements[0].innerText : '';
            }""")
            if filing_type:
                metadata["filing_type"] = filing_type.strip()
            
            # Extract URL information
            metadata["source_url"] = page.url
            
            # Get document creation timestamp
            metadata["extraction_timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            
        except Exception as e:
            logging.error(f"Error extracting metadata: {str(e)}")
            metadata["error"] = f"Error extracting metadata: {str(e)}"
        
        return metadata
    
    async def _extract_document_sections(self, page):
        """Extract document sections from the SEC iXBRL document"""
        sections = {}
        
        try:
            # Find sections using common SEC document patterns
            section_data = await page.evaluate("""() => {
                // Helper to clean text
                const cleanText = (text) => text.replace(/\\s+/g, ' ').trim();
                
                // Find section headings
                const sectionMap = {};
                
                // Common 10-K/10-Q sections (look for Item X headings)
                const itemHeadings = Array.from(document.querySelectorAll('h1, h2, h3, h4, p, div, span'))
                    .filter(el => {
                        const text = el.innerText.trim();
                        return /^Item\\s+\\d+[A-Za-z]?[\\.:]?\\s+/i.test(text) || 
                               /^PART\\s+[IVX]+/i.test(text);
                    });
                
                // Extract sections
                itemHeadings.forEach((heading, index) => {
                    const text = cleanText(heading.innerText);
                    const itemMatch = text.match(/^Item\\s+(\\d+[A-Za-z]?)[\\.:]?\\s+(.+)/i);
                    const partMatch = text.match(/^PART\\s+([IVX]+)[\\.:]?\\s*(.+)?/i);
                    
                    if (itemMatch) {
                        const itemNumber = itemMatch[1];
                        const itemTitle = itemMatch[2] || '';
                        sectionMap[`ITEM_${itemNumber}`] = {
                            heading: text,
                            title: itemTitle
                        };
                    } else if (partMatch) {
                        const partNumber = partMatch[1];
                        const partTitle = partMatch[2] || '';
                        sectionMap[`PART_${partNumber}`] = {
                            heading: text,
                            title: partTitle
                        };
                    }
                });
                
                // Find financial statement sections
                const financialSections = [
                    {id: 'BALANCE_SHEET', patterns: ['Balance Sheet', 'Consolidated Balance Sheet']},
                    {id: 'INCOME_STATEMENT', patterns: ['Income Statement', 'Statement of Operations', 'Statement of Earnings']},
                    {id: 'CASH_FLOW', patterns: ['Cash Flow', 'Statement of Cash Flows']},
                    {id: 'EQUITY', patterns: ['Statement of Stockholders', 'Statement of Shareholders', 'Changes in Equity']},
                    {id: 'COMPREHENSIVE_INCOME', patterns: ['Comprehensive Income', 'Comprehensive Earnings']}
                ];
                
                financialSections.forEach(section => {
                    const elements = Array.from(document.querySelectorAll('h1, h2, h3, h4, p, div, span'))
                        .filter(el => {
                            const text = el.innerText.toLowerCase().trim();
                            return section.patterns.some(pattern => text.includes(pattern.toLowerCase()));
                        });
                    
                    if (elements.length > 0) {
                        sectionMap[section.id] = {
                            heading: cleanText(elements[0].innerText)
                        };
                    }
                });
                
                return sectionMap;
            }""")
            
            sections = section_data
            
        except Exception as e:
            logging.error(f"Error extracting document sections: {str(e)}")
            sections["error"] = f"Error extracting document sections: {str(e)}"
        
        return sections
    
    async def _extract_full_text(self, page):
        """Extract full text content from the SEC iXBRL document"""
        try:
            # Extract the main document text, prioritizing the content area
            full_text = await page.evaluate("""() => {
                // Helper function to clean text
                const cleanText = (text) => {
                    if (!text) return '';
                    return text
                        .replace(/[\\t\\f\\v]+/g, ' ')  // Replace tabs and other whitespace with space
                        .replace(/\\n{3,}/g, '\\n\\n')  // Replace 3+ newlines with just 2
                        .replace(/([^\\n])\\n([^\\n])/g, '$1 $2')  // Join lines that don't represent paragraphs
                        .replace(/\\s+/g, ' ')  // Replace multiple spaces with single space
                        .trim();
                };
                
                // First try to find the main iXBRL content container
                const contentRoot = document.querySelector('.ixds-document-root');
                if (contentRoot) {
                    return cleanText(contentRoot.innerText);
                }
                
                // If that fails, try the body
                const body = document.body;
                if (body) {
                    // Skip the headers and footers
                    let content = '';
                    const elementsToSkip = [
                        '.ixds-header', 
                        '.ixds-footer',
                        '.ixds-menu',
                        '.ixds-search',
                        '.ixds-toolbar'
                    ];
                    
                    // Clone body to manipulate without affecting the page
                    const bodyClone = body.cloneNode(true);
                    
                    // Remove elements we want to skip
                    elementsToSkip.forEach(selector => {
                        const elements = bodyClone.querySelectorAll(selector);
                        elements.forEach(el => el.remove());
                    });
                    
                    return cleanText(bodyClone.innerText);
                }
                
                // Last resort, get all text
                return cleanText(document.documentElement.innerText);
            }""")
            
            return full_text
            
        except Exception as e:
            logging.error(f"Error extracting full text: {str(e)}")
            return f"Error extracting text: {str(e)}"

async def extract_from_sec_url(sec_url, headless=True, wait_time=15000):
    """
    Helper function to extract text content from an SEC iXBRL URL
    
    Args:
        sec_url: SEC iXBRL viewer URL
        headless: Whether to run browser in headless mode
        wait_time: Time to wait for iXBRL renderer in milliseconds
        
    Returns:
        Extracted text content and metadata
    """
    extractor = IXBRLExtractor(headless=headless, wait_time=wait_time)
    return await extractor.extract_from_sec_url(sec_url)

def extract_text_from_ixbrl(sec_url, output_file=None, headless=True, wait_time=15000):
    """
    Synchronous function to extract text from SEC iXBRL URL
    
    Args:
        sec_url: SEC iXBRL viewer URL
        output_file: Optional file path to save the extracted text
        headless: Whether to run browser in headless mode
        wait_time: Time to wait for iXBRL renderer in milliseconds
        
    Returns:
        Extracted text content as string
    """
    result = asyncio.run(extract_from_sec_url(sec_url, headless=headless, wait_time=wait_time))
    
    if "error" in result:
        logging.error(f"Error extracting text from iXBRL: {result['error']}")
        return f"ERROR: {result['error']}"
    
    # Format the output as a text document
    text_output = []
    
    # Add metadata header
    metadata = result.get("metadata", {})
    text_output.append(f"@DOCUMENT: {metadata.get('filing_type', 'UNKNOWN')}")
    text_output.append(f"@COMPANY: {metadata.get('company_name', 'UNKNOWN')}")
    text_output.append(f"@PERIOD: {metadata.get('period_end_date', 'UNKNOWN')}")
    text_output.append(f"@SOURCE: {metadata.get('source_url', sec_url)}")
    text_output.append(f"@EXTRACTION_DATE: {metadata.get('extraction_timestamp', time.strftime('%Y-%m-%d %H:%M:%S'))}")
    text_output.append("")
    
    # Add sections guide if available
    sections = result.get("sections", {})
    if sections:
        text_output.append("@SECTIONS:")
        for section_id, section_info in sections.items():
            text_output.append(f"  {section_id}: {section_info.get('heading', 'Unknown')}")
        text_output.append("")
    
    # Add full text
    text_output.append(result.get("full_text", "No text extracted"))
    
    # Combine into a single string
    full_text = "\n".join(text_output)
    
    # Save to file if requested
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(full_text)
            logging.info(f"Extracted text saved to: {output_file}")
        except Exception as e:
            logging.error(f"Error saving to file: {str(e)}")
    
    return full_text

if __name__ == "__main__":
    # Simple CLI for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract text from SEC iXBRL documents")
    parser.add_argument("url", help="SEC iXBRL URL to extract text from")
    parser.add_argument("--output", "-o", help="Output file to save extracted text")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode")
    parser.add_argument("--wait", type=int, default=15000, help="Wait time in milliseconds")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Extract text
    extracted_text = extract_text_from_ixbrl(
        args.url,
        output_file=args.output,
        headless=not args.visible,
        wait_time=args.wait
    )
    
    # Print summary
    print(f"\nExtracted {len(extracted_text)} characters from {args.url}")
    if args.output:
        print(f"Text saved to: {args.output}")
    else:
        print("\nFirst 500 characters of extracted text:")
        print("----------------------------------------")
        print(extracted_text[:500] + "...")