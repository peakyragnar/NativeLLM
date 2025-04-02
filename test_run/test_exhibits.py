import sys
sys.path.append('/Users/michael/NativeLLM')
from src2.processor.html_processor import HTMLProcessor
processor = HTMLProcessor()
sections = processor.extract_text_from_filing('/Users/michael/NativeLLM/sec_downloads/MSFT/10-K/000095017024087843/msft-20240630.htm', 'MSFT', '10-K').get('document_sections', {})
if 'ITEM_15_EXHIBITS' in sections:
    print('Found EXHIBITS section with heading:', sections['ITEM_15_EXHIBITS'].get('heading'))
