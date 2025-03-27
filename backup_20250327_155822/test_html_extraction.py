#!/usr/bin/env python3
# test_html_extraction.py
import os
import sys
import re
import time
import logging
from bs4 import BeautifulSoup
import difflib

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.xbrl.xbrl_parser import extract_text_only_from_html, process_table_safely

def test_table_extraction():
    """Test table extraction with various test cases"""
    print("\n=== Testing Table Extraction ===\n")
    
    test_cases = [
        # Simple table case
        {
            "name": "Simple Table",
            "html": """
            <table>
                <tr>
                    <th>Header 1</th>
                    <th>Header 2</th>
                </tr>
                <tr>
                    <td>Value 1</td>
                    <td>Value 2</td>
                </tr>
            </table>
            """
        },
        # Styled table
        {
            "name": "Styled Table",
            "html": """
            <table style="width:100%; border:1px solid black; font-family:Arial;">
                <tr style="background-color:#f2f2f2;">
                    <td style="font-weight:bold; text-align:center;">Item</td>
                    <td style="font-weight:bold; text-align:center;">Amount</td>
                </tr>
                <tr>
                    <td style="padding:5px;">Revenue</td>
                    <td style="padding:5px;">$1,000,000</td>
                </tr>
                <tr>
                    <td style="padding:5px;">Expenses</td>
                    <td style="padding:5px;">$750,000</td>
                </tr>
            </table>
            """
        },
        # Complex table with nested elements
        {
            "name": "Complex Table",
            "html": """
            <table border="1" cellspacing="0" style="border-collapse:collapse;width:90%;">
                <tr>
                    <th colspan="2"><span style="font-weight:bold;">Financial Summary</span></th>
                </tr>
                <tr>
                    <td style="text-align:left;"><div>Revenue <span style="font-size:80%;">(in millions)</span></div></td>
                    <td style="text-align:right;"><span style="color:green;">$365.7</span></td>
                </tr>
                <tr>
                    <td style="text-align:left;"><div>Cost of <span style="text-decoration:underline;">Revenue</span></div></td>
                    <td style="text-align:right;"><span style="color:red;">$217.8</span></td>
                </tr>
                <tr>
                    <td style="text-align:left;"><strong>Gross Profit</strong></td>
                    <td style="text-align:right;"><strong>$147.9</strong></td>
                </tr>
            </table>
            """
        },
        # Table with numeric values
        {
            "name": "Numeric Table",
            "html": """
            <table>
                <tr>
                    <th>Quarter</th>
                    <th>Q1</th>
                    <th>Q2</th>
                    <th>Q3</th>
                    <th>Q4</th>
                </tr>
                <tr>
                    <td>Revenue</td>
                    <td>145.6</td>
                    <td>168.2</td>
                    <td>190.5</td>
                    <td>210.3</td>
                </tr>
                <tr>
                    <td>Growth %</td>
                    <td>5.2%</td>
                    <td>15.5%</td>
                    <td>13.3%</td>
                    <td>10.4%</td>
                </tr>
            </table>
            """
        },
        # Table with special characters
        {
            "name": "Table with Special Chars",
            "html": """
            <table>
                <tr>
                    <th>Symbol</th>
                    <th>Name</th>
                    <th>Value ($)</th>
                </tr>
                <tr>
                    <td>AAPL</td>
                    <td>Apple Inc.</td>
                    <td>$178.39</td>
                </tr>
                <tr>
                    <td>GOOGL</td>
                    <td>Alphabet Inc. (Google)</td>
                    <td>$139.61</td>
                </tr>
                <tr>
                    <td>AMZN</td>
                    <td>Amazon.com, Inc.</td>
                    <td>$144.57</td>
                </tr>
            </table>
            """
        }
    ]
    
    # Try to load an actual financial table if available
    sample_file = "/Users/michael/NativeLLM/data/processed/AAPL/Apple_Inc_2024_FY_AAPL_10-K_20240928_llm.txt"
    if os.path.exists(sample_file):
        try:
            with open(sample_file, 'r', encoding='utf-8') as f:
                content = f.read()
                # Look for a section with a table
                table_match = re.search(r'<table[\s\S]+?</table>', content)
                if table_match:
                    test_cases.append({
                        "name": "Real AAPL Financial Table",
                        "html": table_match.group(0)
                    })
        except Exception as e:
            print(f"Error loading sample file: {str(e)}")
    
    results = []
    
    for test_case in test_cases:
        name = test_case.get("name")
        html = test_case.get("html")
        
        print(f"\nTesting: {name}")
        print("-" * 50)
        
        # Get original text content for comparison
        soup = BeautifulSoup(html, 'html.parser')
        original_text = soup.get_text(strip=True)
        
        # Process with our functions
        start_time = time.time()
        table_result = process_table_safely(html)
        processing_time = time.time() - start_time
        
        # Extract text from result for comparison
        if '<' in table_result and '>' in table_result:
            # Result is still HTML, extract text
            result_soup = BeautifulSoup(table_result, 'html.parser')
            result_text = result_soup.get_text(strip=True)
        else:
            # Result is already text
            result_text = table_result
        
        # Check for content preservation
        original_tokens = set(re.findall(r'\b[\w\d.,$%()-]+\b', original_text))
        result_tokens = set(re.findall(r'\b[\w\d.,$%()-]+\b', result_text))
        missing_tokens = original_tokens - result_tokens
        
        # Calculate metrics
        size_original = len(html)
        size_result = len(table_result)
        size_reduction = 1 - (size_result / size_original)
        content_preservation = len(result_tokens) / len(original_tokens) if original_tokens else 1.0
        
        # Display results
        print(f"Original size: {size_original} bytes")
        print(f"Result size: {size_result} bytes")
        print(f"Size reduction: {size_reduction:.2%}")
        print(f"Content preservation: {content_preservation:.2%}")
        print(f"Processing time: {processing_time:.4f} seconds")
        
        if missing_tokens:
            print(f"Missing tokens ({len(missing_tokens)}): {list(missing_tokens)[:5]}")
        
        # Display sample of the result
        print("\nOriginal content (sample):")
        print(original_text[:200] + "..." if len(original_text) > 200 else original_text)
        
        print("\nResult content (sample):")
        print(result_text[:200] + "..." if len(result_text) > 200 else result_text)
        
        # Save result for summary
        results.append({
            "name": name,
            "size_original": size_original,
            "size_result": size_result,
            "size_reduction": size_reduction,
            "content_preservation": content_preservation,
            "missing_tokens": len(missing_tokens),
            "processing_time": processing_time
        })
    
    # Display summary
    print("\n\n=== Summary ===\n")
    print(f"{'Test Case':<30} {'Size Reduction':<15} {'Content Preserved':<20} {'Missing Tokens':<15}")
    print("-" * 80)
    
    total_original = 0
    total_result = 0
    
    for result in results:
        total_original += result["size_original"]
        total_result += result["size_result"]
        print(f"{result['name']:<30} {result['size_reduction']:.2%} {result['content_preservation']:.2%} {result['missing_tokens']}")
    
    overall_reduction = 1 - (total_result / total_original)
    print("-" * 80)
    print(f"{'OVERALL':<30} {overall_reduction:.2%} {'N/A':<20} {'N/A':<15}")

def test_generic_html_extraction():
    """Test HTML extraction on various HTML snippets"""
    print("\n=== Testing Generic HTML Extraction ===\n")
    
    test_cases = [
        # Simple HTML
        {
            "name": "Simple HTML",
            "html": "<p>This is a simple paragraph.</p>"
        },
        # HTML with styling
        {
            "name": "Styled HTML",
            "html": '<div style="font-family: Arial; color: blue; margin: 10px;"><span style="font-weight: bold;">Bold text</span> and <span style="font-style: italic;">italic text</span></div>'
        },
        # Nested divs with classes
        {
            "name": "Nested divs with classes",
            "html": '<div class="container"><div class="row"><div class="col">Column 1</div><div class="col">Column 2</div></div></div>'
        },
        # Complex formatting
        {
            "name": "Complex formatting",
            "html": '<p style="text-align: center;"><span style="color: red; font-size: 16px;">Important</span> information with <strong>bold</strong> and <em>italic</em> text.</p>'
        },
        # Numeric content
        {
            "name": "Numeric content",
            "html": '<div>The total is <span style="font-weight: bold;">$1,234.56</span></div>'
        }
    ]
    
    for test_case in test_cases:
        name = test_case.get("name")
        html = test_case.get("html")
        
        print(f"\nTesting: {name}")
        print("-" * 50)
        
        # Get original text content for comparison
        soup = BeautifulSoup(html, 'html.parser')
        original_text = soup.get_text(strip=True)
        
        # Process with our function
        start_time = time.time()
        result = extract_text_only_from_html(html)
        processing_time = time.time() - start_time
        
        # Calculate metrics
        size_original = len(html)
        size_result = len(result)
        size_reduction = 1 - (size_result / size_original)
        
        # Compare text content
        if '<' in result and '>' in result:
            # Result is still HTML, extract text
            result_soup = BeautifulSoup(result, 'html.parser')
            result_text = result_soup.get_text(strip=True)
        else:
            # Result is already text
            result_text = result
        
        content_match = result_text == original_text
        
        # Display results
        print(f"Original size: {size_original} bytes")
        print(f"Result size: {size_result} bytes")
        print(f"Size reduction: {size_reduction:.2%}")
        print(f"Content preserved exactly: {content_match}")
        print(f"Processing time: {processing_time:.4f} seconds")
        
        print("\nOriginal content:")
        print(original_text)
        
        print("\nResult content:")
        print(result_text)
        
        if not content_match:
            diff = difflib.ndiff(original_text.splitlines(), result_text.splitlines())
            print("\nDifferences:")
            print('\n'.join(diff))

def test_real_file_processing():
    """Process a real XBRL file if available"""
    print("\n=== Testing Real File Processing ===\n")
    
    sample_file = "/Users/michael/NativeLLM/data/processed/AAPL/Apple_Inc_2024_FY_AAPL_10-K_20240928_llm.txt"
    if not os.path.exists(sample_file):
        print(f"Sample file not found: {sample_file}")
        return
    
    print(f"Processing real file: {sample_file}")
    print("-" * 50)
    
    try:
        with open(sample_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Get original size
        original_size = len(content)
        print(f"Original file size: {original_size:,} bytes")
        
        # Count HTML tables
        table_count = content.count('<table')
        print(f"Number of tables found: {table_count}")
        
        # Extract all table sections
        tables = re.findall(r'<table[\s\S]+?</table>', content)
        
        if not tables:
            print("No tables found in the file")
            return
        
        print(f"Found {len(tables)} table elements")
        
        # Process each table
        total_original_size = 0
        total_processed_size = 0
        preserved_count = 0
        number_preservation_stats = []
        
        for i, table in enumerate(tables[:20]):  # Process more tables for better verification
            size_before = len(table)
            total_original_size += size_before
            
            # Process the table
            processed = process_table_safely(table)
            size_after = len(processed)
            total_processed_size += size_after
            
            # Check if content was preserved
            original_soup = BeautifulSoup(table, 'html.parser')
            original_text = original_soup.get_text(strip=False)  # Preserve whitespace
            
            if '<' in processed and '>' in processed:
                # Result is still HTML
                processed_soup = BeautifulSoup(processed, 'html.parser')
                processed_text = processed_soup.get_text(strip=False)  # Preserve whitespace
            else:
                # Result is already text
                processed_text = processed
            
            # Check basic token preservation
            original_tokens = set(re.findall(r'\b[\w\d.,$%()-]+\b', original_text))
            processed_tokens = set(re.findall(r'\b[\w\d.,$%()-]+\b', processed_text))
            missing_tokens = original_tokens - processed_tokens
            
            # Check numeric value preservation specifically
            original_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', original_text)
            processed_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', processed_text)
            
            original_number_set = set(original_numbers)
            processed_number_set = set(processed_numbers)
            missing_numbers = original_number_set - processed_number_set
            
            # Calculate preservation rates
            token_preservation = 1 - (len(missing_tokens) / len(original_tokens) if original_tokens else 0)
            number_preservation = 1 - (len(missing_numbers) / len(original_number_set) if original_number_set else 0)
            
            # Store stats for analysis
            number_preservation_stats.append({
                'table_index': i+1,
                'size_before': size_before,
                'size_after': size_after,
                'reduction': 1 - (size_after/size_before),
                'token_preservation': token_preservation,
                'number_preservation': number_preservation,
                'original_numbers': len(original_number_set),
                'missing_numbers': len(missing_numbers)
            })
            
            is_preserved = (len(missing_numbers) == 0 and token_preservation > 0.99)
            if is_preserved:
                preserved_count += 1
            
            print(f"Table {i+1}: {size_before:,} → {size_after:,} bytes ({1 - size_after/size_before:.2%} reduction)")
            print(f"  - Token preservation: {token_preservation:.2%}")
            print(f"  - Number preservation: {number_preservation:.2%}")
            
            if missing_numbers:
                print(f"  - Missing numbers ({len(missing_numbers)}): {list(missing_numbers)[:3]}...")
            
            # For any tables with missing numbers, print details for debugging
            if missing_numbers and len(missing_numbers) <= 5:
                print("\nDetailed missing number analysis:")
                for num in missing_numbers:
                    count_orig = original_numbers.count(num)
                    count_proc = processed_numbers.count(num)
                    print(f"  Number: '{num}' - Original count: {count_orig}, Processed count: {count_proc}")
        
        # Calculate overall metrics
        if total_original_size > 0:
            overall_reduction = 1 - (total_processed_size / total_original_size)
            avg_token_preservation = sum(s['token_preservation'] for s in number_preservation_stats) / len(number_preservation_stats)
            avg_number_preservation = sum(s['number_preservation'] for s in number_preservation_stats) / len(number_preservation_stats)
            
            print("\n=== Overall Metrics ===")
            print(f"Size reduction: {overall_reduction:.2%}")
            print(f"Token preservation: {avg_token_preservation:.2%}")
            print(f"Number preservation: {avg_number_preservation:.2%}")
            print(f"Tables with perfect preservation: {preserved_count}/{len(number_preservation_stats)}")
            
            # Count tables with 100% number preservation
            perfect_number_tables = sum(1 for s in number_preservation_stats if s['number_preservation'] == 1.0)
            print(f"Tables with 100% number preservation: {perfect_number_tables}/{len(number_preservation_stats)}")
        
        # Now try to process the whole file
        print("\nSimulating processing the entire file...")
        test_content = content
        
        # Process all tables in the content
        for table in tables:
            processed_table = process_table_safely(table)
            test_content = test_content.replace(table, processed_table)
        
        new_size = len(test_content)
        total_reduction = 1 - (new_size / original_size)
        
        print(f"Original size: {original_size:,} bytes")
        print(f"New size: {new_size:,} bytes")
        print(f"Total size reduction: {total_reduction:.2%}")
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")

def test_numeric_value_preservation():
    """Specifically test numeric value preservation"""
    print("\n=== Testing Numeric Value Preservation ===\n")
    
    test_cases = [
        {
            "name": "Simple Numeric Values",
            "html": """
            <table>
                <tr>
                    <th>Value</th>
                    <th>Format</th>
                </tr>
                <tr>
                    <td>$123,456.78</td>
                    <td>Currency</td>
                </tr>
                <tr>
                    <td>45.67%</td>
                    <td>Percentage</td>
                </tr>
                <tr>
                    <td>(1,234)</td>
                    <td>Negative Value</td>
                </tr>
            </table>
            """
        },
        {
            "name": "Complex Table with Numeric Values",
            "html": """
            <table style="border:1px solid black;font-family:Arial;width:100%">
                <tr style="background-color:#f2f2f2;font-weight:bold">
                    <th style="text-align:left">Item</th>
                    <th style="text-align:right">2023</th>
                    <th style="text-align:right">2022</th>
                    <th style="text-align:right">Change</th>
                </tr>
                <tr>
                    <td style="padding:5px;border-bottom:1px solid #ddd">Revenue</td>
                    <td style="padding:5px;border-bottom:1px solid #ddd;text-align:right">$95,281</td>
                    <td style="padding:5px;border-bottom:1px solid #ddd;text-align:right">$89,736</td>
                    <td style="padding:5px;border-bottom:1px solid #ddd;text-align:right">6.2%</td>
                </tr>
                <tr>
                    <td style="padding:5px;border-bottom:1px solid #ddd">Operating Income</td>
                    <td style="padding:5px;border-bottom:1px solid #ddd;text-align:right">$42,163</td>
                    <td style="padding:5px;border-bottom:1px solid #ddd;text-align:right">$38,145</td>
                    <td style="padding:5px;border-bottom:1px solid #ddd;text-align:right">10.5%</td>
                </tr>
                <tr style="font-weight:bold">
                    <td style="padding:5px">Net Income</td>
                    <td style="padding:5px;text-align:right">$106,572</td>
                    <td style="padding:5px;text-align:right">$99,803</td>
                    <td style="padding:5px;text-align:right">6.8%</td>
                </tr>
            </table>
            """
        },
        {
            "name": "Nested Complex Values",
            "html": """
            <div style="font-family:Arial;">
                <p style="margin:10px;">In fiscal year 2023, we reported <span style="font-weight:bold;color:green;">$123,456.78 million</span> in total revenue, an increase of <span style="font-style:italic;color:blue;">12.3%</span> from the previous year.</p>
                <p style="margin:10px;">Our market share increased to <span style="text-decoration:underline;">34.5%</span>, while operating costs were <span style="color:red">($87,654.32 million)</span>.</p>
            </div>
            """
        }
    ]
    
    for test_case in test_cases:
        name = test_case["name"]
        html = test_case["html"]
        
        print(f"\nTesting: {name}")
        print("-" * 50)
        
        # Extract original numeric values
        soup = BeautifulSoup(html, 'html.parser')
        original_text = soup.get_text(strip=False)
        original_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', original_text)
        original_number_set = set(original_numbers)
        
        print(f"Original numbers ({len(original_numbers)}): {original_numbers}")
        
        # Process with our functions
        start_time = time.time()
        if '<table' in html:
            result = process_table_safely(html)
        else:
            result = extract_text_only_from_html(html)
        processing_time = time.time() - start_time
        
        # Extract processed numeric values
        if '<' in result and '>' in result:
            # Result is still HTML
            result_soup = BeautifulSoup(result, 'html.parser')
            result_text = result_soup.get_text(strip=False)
        else:
            # Result is already text
            result_text = result
        
        result_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', result_text)
        result_number_set = set(result_numbers)
        
        print(f"Processed numbers ({len(result_numbers)}): {result_numbers}")
        
        # Check for missing numbers
        missing_numbers = original_number_set - result_number_set
        if missing_numbers:
            print(f"Missing numbers ({len(missing_numbers)}): {missing_numbers}")
        else:
            print("All numeric values perfectly preserved!")
        
        # Calculate metrics
        size_original = len(html)
        size_result = len(result)
        size_reduction = 1 - (size_result / size_original)
        number_preservation = 1 - (len(missing_numbers) / len(original_number_set) if original_number_set else 0)
        
        print(f"Original size: {size_original} bytes")
        print(f"Result size: {size_result} bytes")
        print(f"Size reduction: {size_reduction:.2%}")
        print(f"Number preservation: {number_preservation:.2%}")
        print(f"Processing time: {processing_time:.4f} seconds")

if __name__ == "__main__":
    test_table_extraction()
    test_generic_html_extraction()
    test_numeric_value_preservation()
    test_real_file_processing()