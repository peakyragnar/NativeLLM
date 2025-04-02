import logging
logging.basicConfig(level=logging.INFO)

from src2.formatter.llm_formatter import llm_formatter
from src2.processor.html_processor import html_processor
from bs4 import BeautifulSoup

# Load the HTML file
with open('/Users/michael/NativeLLM/sec_downloads/MSFT/10-K/000095017024087843/msft-20240630.htm', 'r', encoding='utf-8') as f:
    content = f.read()

# Parse HTML
soup = BeautifulSoup(content, 'html.parser')
main_content = html_processor.find_main_content(soup)

# Extract sections
html_content = {}
html_processor.identify_and_mark_sections(main_content, html_content, '10-K')

# Print the sections found
doc_sections = html_content.get("document_sections", {})
print(f'Found {len(doc_sections)} sections')
print("Sections found:")
for section_id in sorted(doc_sections.keys()):
    missing = doc_sections[section_id].get("missing", False)
    status = "MISSING" if missing else "FOUND"
    print(f"  - {section_id}: {status}")

# Prepare metadata
filing_metadata = {
    "ticker": "MSFT",
    "filing_type": "10-K",
    "company_name": "Microsoft Corporation",
    "cik": "789019",
    "filing_date": "2024-07-30",
    "period_end_date": "2024-06-30",
    "fiscal_year": "2024",
    "fiscal_period": "annual",
    "html_content": html_content
}

# Use a minimal parsed_xbrl structure
parsed_xbrl = {"contexts": {}, "facts": [], "units": {}}

# Generate LLM format
llm_content = llm_formatter.generate_llm_format(parsed_xbrl, filing_metadata)

# Find and print document coverage section
lines = llm_content.split("\n")
in_coverage = False
coverage_lines = []

for line in lines:
    if line.startswith("@DOCUMENT_COVERAGE"):
        in_coverage = True
        coverage_lines.append(line)
    elif in_coverage and line.startswith("@"):
        in_coverage = False
    elif in_coverage:
        coverage_lines.append(line)

print("\nDocument Coverage Section:")
for line in coverage_lines:
    print(line)
