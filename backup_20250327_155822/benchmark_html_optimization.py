#!/usr/bin/env python
# Benchmark HTML optimization performance
# This script measures size reduction and execution time

import os
import sys
import time
import argparse
import random
import glob
import statistics
from bs4 import BeautifulSoup

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.xbrl.xbrl_parser import extract_text_only_from_html, process_table_safely
from src.config import PROCESSED_DATA_DIR

def benchmark_file(file_path, sample_size=100):
    """
    Benchmark HTML optimization on a file
    
    Args:
        file_path: Path to the file to benchmark
        sample_size: Number of HTML values to sample
        
    Returns:
        Dictionary with benchmark results
    """
    try:
        if not os.path.exists(file_path):
            return {"error": "File not found"}
            
        # Only benchmark LLM formatted files
        if not file_path.endswith('_llm.txt'):
            return {"error": "Not an LLM format file"}
            
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Find all HTML values
        import re
        html_pattern = r'@VALUE: (.*?)(\n@|$)'
        value_sections = re.findall(html_pattern, content, re.DOTALL)
        
        # Filter for HTML values
        html_values = []
        for value_match in value_sections:
            value = value_match[0].strip()
            if '<' in value and '>' in value:
                html_values.append(value)
        
        if not html_values:
            return {"error": "No HTML values found"}
        
        # Sample values for benchmarking
        if sample_size < len(html_values):
            sample_values = random.sample(html_values, sample_size)
        else:
            sample_values = html_values
            
        # Benchmark metrics
        results = {
            "file": file_path,
            "total_html_values": len(html_values),
            "sampled_values": len(sample_values),
            "extract_text_only_timing": [],
            "process_table_timing": [],
            "original_sizes": [],
            "extracted_sizes": [],
            "table_processed_sizes": [],
            "extract_size_reductions": [],
            "table_size_reductions": [],
            "number_preservation": [],
            "values": []
        }
        
        # Benchmark each value
        for i, html_value in enumerate(sample_values):
            value_result = {"original_size": len(html_value)}
            
            # Test extract_text_only_from_html
            start_time = time.time()
            extracted = extract_text_only_from_html(html_value)
            extract_time = time.time() - start_time
            
            # Record timing and size
            results["extract_text_only_timing"].append(extract_time)
            results["original_sizes"].append(len(html_value))
            results["extracted_sizes"].append(len(extracted))
            
            size_reduction = (len(html_value) - len(extracted)) / len(html_value) * 100 if len(html_value) > 0 else 0
            results["extract_size_reductions"].append(size_reduction)
            
            value_result["extracted_size"] = len(extracted)
            value_result["extracted_reduction"] = size_reduction
            
            # Check number preservation
            original_soup = BeautifulSoup(html_value, 'html.parser')
            original_text = original_soup.get_text()
            original_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', original_text)
            
            if '<' not in extracted:
                # Extract directly for text
                cleaned_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', extracted)
            else:
                # Extract from HTML
                cleaned_soup = BeautifulSoup(extracted, 'html.parser')
                cleaned_text = cleaned_soup.get_text()
                cleaned_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', cleaned_text)
                
            numbers_preserved = set(original_numbers) == set(cleaned_numbers)
            results["number_preservation"].append(numbers_preserved)
            value_result["numbers_preserved"] = numbers_preserved
            
            # Only benchmark process_table_safely for table content
            if '<table' in html_value:
                start_time = time.time()
                table_processed = process_table_safely(html_value)
                table_time = time.time() - start_time
                
                results["process_table_timing"].append(table_time)
                results["table_processed_sizes"].append(len(table_processed))
                
                table_reduction = (len(html_value) - len(table_processed)) / len(html_value) * 100 if len(html_value) > 0 else 0
                results["table_size_reductions"].append(table_reduction)
                
                value_result["table_processed_size"] = len(table_processed)
                value_result["table_reduction"] = table_reduction
                
                # Check if table processing preserved numbers
                if '<' not in table_processed:
                    table_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', table_processed)
                else:
                    table_soup = BeautifulSoup(table_processed, 'html.parser')
                    table_numbers = re.findall(r'\$?[\d,]+\.?\d*%?|\(\$?[\d,]+\.?\d*\)', table_soup.get_text())
                    
                table_preserved = set(original_numbers) == set(table_numbers)
                value_result["table_numbers_preserved"] = table_preserved
            
            # Add a sample of the value content
            value_result["sample"] = html_value[:100] + "..." if len(html_value) > 100 else html_value
            results["values"].append(value_result)
            
        # Calculate summary statistics
        extract_time_avg = statistics.mean(results["extract_text_only_timing"]) if results["extract_text_only_timing"] else 0
        extract_reduction_avg = statistics.mean(results["extract_size_reductions"]) if results["extract_size_reductions"] else 0
        table_time_avg = statistics.mean(results["process_table_timing"]) if results["process_table_timing"] else 0
        table_reduction_avg = statistics.mean(results["table_size_reductions"]) if results["table_size_reductions"] else 0
        number_preservation_pct = sum(results["number_preservation"]) / len(results["number_preservation"]) * 100 if results["number_preservation"] else 0
        
        results["summary"] = {
            "extract_time_avg_ms": extract_time_avg * 1000,
            "extract_reduction_avg": extract_reduction_avg,
            "table_time_avg_ms": table_time_avg * 1000,
            "table_reduction_avg": table_reduction_avg,
            "number_preservation_pct": number_preservation_pct,
            "table_count": len(results["process_table_timing"])
        }
        
        return results
            
    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }

def run_benchmark(file_path=None, sample_size=100):
    """
    Run benchmarks on a file or sample of files
    
    Args:
        file_path: Specific file to benchmark, or None to sample from all files
        sample_size: Number of HTML values to sample per file
        
    Returns:
        Dictionary with benchmark results
    """
    if file_path and os.path.exists(file_path):
        print(f"Benchmarking file: {file_path}")
        results = benchmark_file(file_path, sample_size)
        display_benchmark_results(results)
        return results
    
    # Sample from all files if no specific file provided
    all_files = glob.glob(os.path.join(PROCESSED_DATA_DIR, "*", "*_llm.txt"))
    if not all_files:
        print("No LLM files found")
        return {"error": "No files found"}
    
    # Sample files if there are many
    if len(all_files) > 3:
        benchmark_files = random.sample(all_files, 3)
    else:
        benchmark_files = all_files
    
    all_results = []
    for file_path in benchmark_files:
        print(f"Benchmarking file: {os.path.basename(file_path)}")
        result = benchmark_file(file_path, sample_size)
        all_results.append(result)
        display_benchmark_results(result)
        
    # Calculate overall statistics
    print("\n===== OVERALL BENCHMARK SUMMARY =====")
    
    extract_times = []
    extract_reductions = []
    table_times = []
    table_reductions = []
    number_preservation = []
    
    for result in all_results:
        if "error" not in result:
            extract_times.extend(result["extract_text_only_timing"])
            extract_reductions.extend(result["extract_size_reductions"])
            table_times.extend(result["process_table_timing"])
            table_reductions.extend(result["table_size_reductions"])
            number_preservation.extend(result["number_preservation"])
    
    if extract_times:
        overall_extract_time = statistics.mean(extract_times) * 1000  # to ms
        overall_extract_reduction = statistics.mean(extract_reductions)
        print(f"extract_text_only_from_html:")
        print(f"  Average time: {overall_extract_time:.2f}ms per value")
        print(f"  Average size reduction: {overall_extract_reduction:.2f}%")
    
    if table_times:
        overall_table_time = statistics.mean(table_times) * 1000  # to ms
        overall_table_reduction = statistics.mean(table_reductions)
        print(f"process_table_safely:")
        print(f"  Average time: {overall_table_time:.2f}ms per table")
        print(f"  Average size reduction: {overall_table_reduction:.2f}%")
    
    if number_preservation:
        overall_preservation = sum(number_preservation) / len(number_preservation) * 100
        print(f"Number preservation: {overall_preservation:.2f}%")
    
    return {
        "files": len(all_results),
        "extract_time_avg_ms": overall_extract_time if extract_times else 0,
        "extract_reduction_avg": overall_extract_reduction if extract_reductions else 0,
        "table_time_avg_ms": overall_table_time if table_times else 0,
        "table_reduction_avg": overall_table_reduction if table_reductions else 0,
        "number_preservation_pct": overall_preservation if number_preservation else 0,
        "file_results": all_results
    }

def display_benchmark_results(results):
    """Display benchmark results in a readable format"""
    if "error" in results:
        print(f"Error: {results['error']}")
        return
    
    print("\n===== BENCHMARK RESULTS =====")
    print(f"File: {os.path.basename(results['file'])}")
    print(f"Total HTML values: {results['total_html_values']}")
    print(f"Sampled values: {results['sampled_values']}")
    
    summary = results["summary"]
    
    print("\nPerformance:")
    print(f"  extract_text_only_from_html: {summary['extract_time_avg_ms']:.2f}ms per value")
    print(f"  process_table_safely: {summary['table_time_avg_ms']:.2f}ms per table (for {summary['table_count']} tables)")
    
    print("\nSize Reduction:")
    print(f"  extract_text_only_from_html: {summary['extract_reduction_avg']:.2f}%")
    print(f"  process_table_safely: {summary['table_reduction_avg']:.2f}%")
    
    print("\nData Integrity:")
    print(f"  Number preservation: {summary['number_preservation_pct']:.2f}%")
    
    if summary['number_preservation_pct'] < 100:
        # Show examples of values where numbers weren't preserved
        print("\nExamples where numbers weren't preserved:")
        issue_count = 0
        for i, value_result in enumerate(results["values"]):
            if not value_result.get("numbers_preserved", True):
                issue_count += 1
                if issue_count <= 3:  # Only show a few examples
                    print(f"\nHTML Value {i+1}:")
                    print(f"  Sample: {value_result['sample']}")

def main():
    parser = argparse.ArgumentParser(description="Benchmark HTML optimization performance")
    parser.add_argument('--file', help='Specific file to benchmark')
    parser.add_argument('--sample', type=int, default=100, help='Number of HTML values to sample per file')
    
    args = parser.parse_args()
    
    run_benchmark(file_path=args.file, sample_size=args.sample)

if __name__ == "__main__":
    main()