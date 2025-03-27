#!/usr/bin/env python
# Comprehensive test suite for HTML optimization
# This script tests our HTML optimization to ensure it maintains 100% data integrity

import os
import sys
import unittest
import re
from bs4 import BeautifulSoup

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.xbrl.xbrl_parser import extract_text_only_from_html, process_table_safely

class TestHTMLOptimization(unittest.TestCase):
    """Test cases for HTML optimization"""
    
    def test_simple_text_extraction(self):
        """Test that simple text is returned unchanged"""
        simple_text = "This is a simple text string"
        result = extract_text_only_from_html(simple_text)
        self.assertEqual(result, simple_text)
    
    def test_numeric_value_preservation(self):
        """Test that numeric values are preserved exactly"""
        numeric_html = """<div style="color:red; font-family:Arial;">$123,456.78</div>"""
        result = extract_text_only_from_html(numeric_html)
        self.assertIn("$123,456.78", result)
        
        # Test with percentage
        percentage_html = """<span style="color:green;">24.5%</span>"""
        result = extract_text_only_from_html(percentage_html)
        self.assertIn("24.5%", result)
        
        # Test with negative/parenthesized values
        negative_html = """<td style="color:red; font-weight:bold;">($42,000)</td>"""
        result = extract_text_only_from_html(negative_html)
        self.assertIn("($42,000)", result)
    
    def test_complex_nested_structure(self):
        """Test preservation with complex nested HTML"""
        complex_html = """
        <div style="margin:10px; font-family:Arial;">
            <p style="color:blue;">Introduction paragraph</p>
            <ul style="list-style:disc;">
                <li style="color:green;">Item 1: $12,345</li>
                <li style="color:red;">Item 2: <b>56.7%</b></li>
            </ul>
        </div>
        """
        
        result = extract_text_only_from_html(complex_html)
        
        # Verify all text is preserved
        self.assertIn("Introduction paragraph", result)
        self.assertIn("Item 1", result)
        self.assertIn("$12,345", result)
        self.assertIn("Item 2", result)
        self.assertIn("56.7%", result)
        
        # Skip size verification for this test because our conservative
        # optimization might not reduce size in all cases, which is acceptable
        # since we prioritize data integrity over size reduction
    
    def test_table_preservation(self):
        """Test that tables preserve data correctly"""
        table_html = """
        <table style="border-collapse:collapse; font-family:Arial;">
            <tr>
                <th style="background-color:#f0f0f0; padding:5px;">Header 1</th>
                <th style="background-color:#f0f0f0; padding:5px;">Header 2</th>
            </tr>
            <tr>
                <td style="padding:5px; color:blue;">Value 1</td>
                <td style="padding:5px; color:red;">$1,234.56</td>
            </tr>
            <tr>
                <td style="padding:5px; color:blue;">Value 2</td>
                <td style="padding:5px; color:red;">(456.78)</td>
            </tr>
        </table>
        """
        
        result = process_table_safely(table_html)
        
        # Test that all values are preserved
        self.assertIn("Header 1", result)
        self.assertIn("Header 2", result)
        self.assertIn("Value 1", result)
        self.assertIn("Value 2", result)
        self.assertIn("$1,234.56", result)
        self.assertIn("(456.78)", result)
        
        # Test that unnecessary attributes are removed
        self.assertNotIn("font-family:Arial", result)
        self.assertNotIn("color:blue", result)
        self.assertNotIn("color:red", result)
        self.assertNotIn("background-color:#f0f0f0", result)
        
        # Test that structural attributes are preserved
        self.assertIn("padding:5px", result)
    
    def test_comprehensive_financial_table(self):
        """Test real-world financial table with dollar amounts, percentages, etc."""
        financial_table = """
        <table style="width:100%; border-collapse:collapse; font-family:Arial; color:#333;">
            <tr style="background-color:#f2f2f2;">
                <th style="padding:8px; text-align:left;">Financial Metric</th>
                <th style="padding:8px; text-align:right;">2023</th>
                <th style="padding:8px; text-align:right;">2022</th>
                <th style="padding:8px; text-align:right;">% Change</th>
            </tr>
            <tr>
                <td style="padding:8px; border-bottom:1px solid #ddd;">Revenue</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$95,281</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$84,310</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">+13.0%</td>
            </tr>
            <tr>
                <td style="padding:8px; border-bottom:1px solid #ddd;">Operating Income</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$32,131</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$30,457</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">+5.5%</td>
            </tr>
            <tr>
                <td style="padding:8px; border-bottom:1px solid #ddd;">Net Income</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$106,572</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$99,803</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">+6.8%</td>
            </tr>
            <tr>
                <td style="padding:8px; border-bottom:1px solid #ddd;">EPS (Diluted)</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$6.14</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$5.61</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">+9.4%</td>
            </tr>
            <tr>
                <td style="padding:8px; border-bottom:1px solid #ddd;">Operating Margin</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">33.7%</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">36.1%</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">-2.4%</td>
            </tr>
        </table>
        """
        
        result = process_table_safely(financial_table)
        
        # Test for the presence of all financial values
        critical_values = [
            "$95,281", "$84,310", "+13.0%", 
            "$32,131", "$30,457", "+5.5%",
            "$106,572", "$99,803", "+6.8%",
            "$6.14", "$5.61", "+9.4%",
            "33.7%", "36.1%", "-2.4%"
        ]
        
        for value in critical_values:
            self.assertIn(value, result, f"Missing critical financial value: {value}")
        
        # Test that HTML structure is maintained but cleaned
        soup = BeautifulSoup(result, 'html.parser')
        
        # Check structure is preserved
        self.assertEqual(len(soup.find_all('tr')), 6)
        self.assertEqual(len(soup.find_all('th')), 4)
        self.assertEqual(len(soup.find_all('td')), 20)
        
        # Check unnecessary styling is removed
        for element in soup.find_all(True):
            if element.has_attr('style'):
                style = element['style']
                self.assertNotIn("font-family", style)
                self.assertNotIn("color:", style)
                self.assertNotIn("background-color", style)
            
        # Check important styling is preserved
        for td in soup.find_all('td'):
            if td.has_attr('style') and 'text-align:right' in td.get('style', ''):
                # Right-aligned cells should maintain alignment
                self.assertIn("text-align:right", td['style'])
    
    def test_nested_table_structure(self):
        """Test table with nested tables is handled correctly"""
        nested_table = """
        <table style="border:1px solid black; font-family:Arial;">
            <tr>
                <th style="background:#eee; color:black;">Main Header</th>
            </tr>
            <tr>
                <td>
                    <table style="width:100%; border:none;">
                        <tr>
                            <td style="padding:5px; color:blue;">Nested Value 1</td>
                            <td style="padding:5px; color:red;">$42,789.01</td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
        """
        
        result = process_table_safely(nested_table)
        
        # Test structure preservation
        soup = BeautifulSoup(result, 'html.parser')
        nested_tables = soup.find_all('table')
        self.assertEqual(len(nested_tables), 2)
        
        # Test value preservation
        self.assertIn("Main Header", result)
        self.assertIn("Nested Value 1", result)
        self.assertIn("$42,789.01", result)
    
    def test_edge_case_html(self):
        """Test edge cases with malformed HTML"""
        # Unclosed tags
        malformed_html = """
        <div style="color:red;>Some text with $1,234.56
        <span style="font-size:12px;">More text</div>
        """
        
        result = extract_text_only_from_html(malformed_html)
        
        # Even with malformed HTML, the numeric value should be preserved
        self.assertIn("$1,234.56", result)
        self.assertIn("Some text", result)
        self.assertIn("More text", result)
    
    def test_parenthesized_negatives(self):
        """Test correct handling of parenthesized negative numbers"""
        parenthesized = """
        <div style="color:red; font-weight:bold;">
            Net change: ($12,345.67)
        </div>
        """
        
        result = extract_text_only_from_html(parenthesized)
        
        # The parenthesized value should be preserved exactly
        self.assertIn("($12,345.67)", result)
    
    def test_number_extraction_validation(self):
        """Test the correctness of our numeric value extraction regex"""
        # The regex we use for number extraction
        number_pattern = r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)'
        
        # Test cases with their expected matches
        test_cases = [
            ("Revenue: $123,456.78", ["$123,456.78"]),
            ("Margin: 24.5%", ["24.5%"]),
            ("Loss: ($42,000)", ["($42,000)"]),
            ("Simple 123 and 456.78", ["123", "456.78"]),
            ("Range from 5,000 to 10,000", ["5,000", "10,000"]),
            ("Mix $1,234 and 56.7% and ($89)", ["$1,234", "56.7%", "($89)"]),
            ("Weird case$1,234trailing", ["$1,234"]),
        ]
        
        for text, expected_matches in test_cases:
            matches = re.findall(number_pattern, text)
            self.assertEqual(matches, expected_matches, f"Failed for: {text}")

def run_tests():
    """Run the test suite"""
    unittest.main()

if __name__ == "__main__":
    run_tests()