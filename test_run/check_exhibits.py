import sys
import os
sys.path.append('/Users/michael/NativeLLM')
from src2.formatter.llm_formatter import LLMFormatter
from src2.processor.html_processor import HTMLProcessor
from datetime import datetime

# Process file
processor = HTMLProcessor()
print('Testing EXHIBITS section detection with our HTML processor improvements')
result = processor.extract_text_from_filing('/Users/michael/NativeLLM/sec_downloads/MSFT/10-K/000095017024087843/msft-20240630.htm', 'MSFT', '10-K')
sections = result.get('document_sections', {})

# Check if we found the EXHIBITS section
print('ITEM_15_EXHIBITS found?', 'ITEM_15_EXHIBITS' in sections)
if 'ITEM_15_EXHIBITS' in sections:
    print('  Section Heading:', sections['ITEM_15_EXHIBITS'].get('heading'))

# Analyze found sections
print('\nSection coverage analysis:')
print('-------------------------')
all_sections = list(sections.keys())
print(f'Total sections found: {len(all_sections)}')

# Required sections from LLMFormatter
required_10k_sections = [
    'ITEM_1_BUSINESS',
    'ITEM_1A_RISK_FACTORS', 
    'ITEM_2_PROPERTIES',
    'ITEM_3_LEGAL_PROCEEDINGS',
    'ITEM_7_MD_AND_A',
    'ITEM_7A_MARKET_RISK',
    'ITEM_8_FINANCIAL_STATEMENTS',
    'ITEM_9A_CONTROLS',
    'ITEM_10_DIRECTORS',
    'ITEM_11_EXECUTIVE_COMPENSATION',
    'ITEM_15_EXHIBITS'
]

found_required = [s for s in all_sections if s in required_10k_sections]
required_coverage = (len(found_required) / len(required_10k_sections)) * 100
print(f'Required Coverage: {required_coverage:.1f}% ({len(found_required)}/{len(required_10k_sections)})')
print('Missing required:', [s for s in required_10k_sections if s not in all_sections])

print('\nTimestamp: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print('Our HTML processor improvement is working correctly\!' if 'ITEM_15_EXHIBITS' in sections else 'EXHIBITS section still not detected')
