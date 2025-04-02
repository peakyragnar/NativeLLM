import logging
import os
import glob
import sys
logging.basicConfig(level=logging.INFO)

# Add project root to path
sys.path.append("/Users/michael/NativeLLM")
from src2.processor.html_processor import HTMLProcessor
from bs4 import BeautifulSoup

# Create HTML processor instance
html_processor = HTMLProcessor()

# Try multiple Tesla filings to find one with more sections
tesla_files = [
    '/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/TSLA/10-K/000162828024002390/tsla-20231231.htm',
    '/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/TSLA/10-K/000162828025003063/tsla-20241231.htm',
    '/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/TSLA/10-K/000156459022016871/tsla-10ka_20211231.htm'
]

# Find first existing file
tesla_file = None
for file in tesla_files:
    if os.path.exists(file):
        tesla_file = file
        break

if not tesla_file:
    print("No Tesla SEC filings found!")
    exit(1)
print(f"Testing with Tesla file: {tesla_file}")

# Load the HTML file
with open(tesla_file, 'r', encoding='utf-8') as f:
    content = f.read()

# Parse HTML
soup = BeautifulSoup(content, 'html.parser')
main_content = html_processor.find_main_content(soup)

# Initialize sections
sections = {}

# Process sections
html_processor.identify_and_mark_sections(main_content, sections, '10-K')

# Check results
doc_sections = sections.get("document_sections", {})
print(f'Found {len(doc_sections)} sections')
print("Sections found:")
for section_id in sorted(doc_sections.keys()):
    missing = doc_sections[section_id].get("missing", False)
    status = "MISSING" if missing else "FOUND"
    heading = doc_sections[section_id].get("heading", "No heading")
    print(f"  - {section_id}: {status} - '{heading}'")

# Debug: Check for Item headings in the HTML that might not be detected
print("\nDebug: Searching for Item headings in HTML...")
potential_items = []
for tag in main_content.find_all(['h1', 'h2', 'h3', 'h4', 'div', 'p', 'span']):
    text = tag.get_text().strip()
    if text.startswith('Item ') or text.startswith('ITEM '):
        potential_items.append(f"{tag.name}: {text}")

print(f"Found {len(potential_items)} potential item headings in raw HTML:")
for item in potential_items[:20]:  # Show first 20 to avoid overwhelming output
    print(f"  - {item}")
