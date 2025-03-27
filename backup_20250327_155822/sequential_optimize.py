#\!/usr/bin/env python
# Process files sequentially without using parallel execution

import os
import sys
import glob
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('html_optimization_sequential.log')
    ]
)

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from validate_html_optimization import validate_optimization
from src.config import PROCESSED_DATA_DIR

def find_files(company=None):
    """Find files to optimize"""
    if company:
        # For a specific company
        pattern = os.path.join(PROCESSED_DATA_DIR, company, "*_llm.txt")
    else:
        # For all companies
        pattern = os.path.join(PROCESSED_DATA_DIR, "*", "*_llm.txt")
        
    files = glob.glob(pattern)
    return files

def process_file(file_path, min_reduction=1.0):
    """Process a single file with validation"""
    logging.info(f"Processing file: {file_path}")
    
    # First run in validation mode
    validation = validate_optimization(file_path, show_details=False, dry_run=True)
    
    if validation["status"] != "validated":
        logging.warning(f"File {file_path} validation status: {validation['status']}")
        return validation
        
    # Check if the reduction meets our threshold
    if validation["percentage_reduction"] < min_reduction:
        validation["status"] = "skipped"
        validation["message"] = f"Reduction ({validation['percentage_reduction']:.2f}%) below threshold ({min_reduction:.1f}%)"
        logging.info(f"Skipping {file_path}: {validation['message']}")
        return validation
        
    # Check data integrity
    if not validation["all_numbers_preserved"]:
        validation["status"] = "error"
        validation["message"] = "Data integrity concerns: some numbers not preserved"
        logging.error(f"Data integrity issue with {file_path}: {validation['message']}")
        return validation
        
    # If validation passes, apply changes
    result = validate_optimization(file_path, show_details=False, dry_run=False)
    logging.info(f"Optimized {file_path}: {result['percentage_reduction']:.2f}% reduction")
    return result

def main():
    # Find files to process
    files = find_files()
    
    if not files:
        print("No files to process")
        return
        
    print(f"Found {len(files)} files to process")
    
    # Ensure the user wants to proceed
    proceed = input("\nThis will modify files. Backups will be created but proceed with caution.\nDo you want to continue? (yes/no): ")
    if proceed.lower() not in ['yes', 'y']:
        print("Operation cancelled")
        return
    
    # Process files sequentially
    start_time = time.time()
    results = []
    total_original = 0
    total_optimized = 0
    
    for i, file_path in enumerate(files):
        print(f"Processing file {i+1}/{len(files)}: {os.path.basename(file_path)}")
        try:
            result = process_file(file_path)
            results.append(result)
            
            # Update totals for optimized files
            if result["status"] == "optimized":
                total_original += result["original_size"]
                total_optimized += result["new_size"]
                
        except Exception as e:
            logging.error(f"Exception processing {file_path}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            
            results.append({
                "file": file_path,
                "status": "error",
                "message": str(e)
            })
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Summarize results
    optimized_count = sum(1 for r in results if r["status"] == "optimized")
    skipped_count = sum(1 for r in results if r["status"] == "skipped")
    error_count = sum(1 for r in results if r["status"] == "error")
    
    print("\n===== OPTIMIZATION SUMMARY =====")
    print(f"Total files processed: {len(files)}")
    print(f"Optimized: {optimized_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Errors: {error_count}")
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
    
    if optimized_count > 0:
        total_reduction = (total_original - total_optimized) / total_original * 100
        print(f"\nTotal size reduction: {total_reduction:.2f}%")
        print(f"Total bytes saved: {total_original - total_optimized:,}")
        
        # Calculate average reduction per file
        avg_reduction = sum(r.get("percentage_reduction", 0) for r in results if r["status"] == "optimized") / optimized_count
        print(f"Average reduction per file: {avg_reduction:.2f}%")
        
    # Write detailed report
    report_path = "html_optimization_sequential_report.txt"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("HTML Optimization Sequential Report\n")
        f.write("=================================\n\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Files processed: {len(files)}\n")
        f.write(f"Files optimized: {optimized_count}\n")
        f.write(f"Time elapsed: {elapsed_time:.2f} seconds\n\n")
        
        # Write details of optimized files
        f.write("Optimized Files\n")
        f.write("==============\n\n")
        
        for result in sorted([r for r in results if r["status"] == "optimized"], 
                            key=lambda x: x.get("percentage_reduction", 0), 
                            reverse=True):
            f.write(f"File: {os.path.basename(result['file'])}\n")
            f.write(f"  Size: {result['original_size']:,} → {result['new_size']:,} bytes\n")
            f.write(f"  Reduction: {result['percentage_reduction']:.2f}%\n")
            f.write(f"  HTML values: {result['html_values']}/{result['total_values']}\n")
            f.write(f"  HTML content reduction: {result.get('html_reduction', 0):.2f}%\n\n")
        
        # Write details of skipped files
        f.write("\nSkipped Files\n")
        f.write("============\n\n")
        
        for result in [r for r in results if r["status"] == "skipped"]:
            f.write(f"File: {os.path.basename(result['file'])}\n")
            f.write(f"  Reason: {result.get('message', 'Unknown')}\n\n")
        
        # Write details of error files
        f.write("\nError Files\n")
        f.write("==========\n\n")
        
        for result in [r for r in results if r["status"] == "error"]:
            f.write(f"File: {os.path.basename(result['file'])}\n")
            f.write(f"  Error: {result.get('message', 'Unknown error')}\n\n")
    
    print(f"\nDetailed report written to {report_path}")

if __name__ == "__main__":
    main()
