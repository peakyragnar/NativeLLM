import logging
logging.basicConfig(level=logging.INFO)

from src2.processor.html_processor import html_processor
from bs4 import BeautifulSoup

# Load the HTML file
with open('/Users/michael/NativeLLM/sec_downloads/MSFT/10-K/000095017024087843/msft-20240630.htm', 'r', encoding='utf-8') as f:
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
    print(f"  - {section_id}: {status}")
