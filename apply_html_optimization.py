#!/usr/bin/env python
# Apply HTML optimization to existing processed files
# This script will apply our HTML optimization with 100% data integrity

import os
import sys
import glob
import time
import logging
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('html_optimization.log')
    ]
)

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from validate_html_optimization import validate_optimization
from src.config import PROCESSED_DATA_DIR

def find_files(company=None):
    """
    Find files to optimize
    
    Args:
        company: Optional company ticker to limit processing
        
    Returns:
        List of file paths
    """
    if company:
        # For a specific company
        pattern = os.path.join(PROCESSED_DATA_DIR, company, "*_llm.txt")
    else:
        # For all companies
        pattern = os.path.join(PROCESSED_DATA_DIR, "*", "*_llm.txt")
        
    files = glob.glob(pattern)
    return files

def optimize_files(files, workers=4, min_reduction=1.0):
    """
    Optimize files with validation to ensure data integrity
    
    Args:
        files: List of files to process
        workers: Number of parallel workers
        min_reduction: Minimum percentage reduction to apply changes
        
    Returns:
        Dictionary with optimization results
    """
    if not files:
        print("No files to process")
        return {"status": "no_files"}
        
    print(f"Found {len(files)} files to process")
    print(f"Using {workers} parallel workers")
    print(f"Minimum required reduction: {min_reduction:.1f}%")
    
    # Ensure the user wants to proceed
    proceed = input("\nThis will modify files. Backups will be created but proceed with caution.\nDo you want to continue? (yes/no): ")
    if proceed.lower() not in ['yes', 'y']:
        print("Operation cancelled")
        return {"status": "cancelled"}
    
    # Process files in parallel
    results = []
    total_original = 0
    total_optimized = 0
    
    def process_file(file_path):
        """Process a single file with validation"""
        # First run in validation mode
        validation = validate_optimization(file_path, show_details=False, dry_run=True)
        
        if validation["status"] != "validated":
            return validation
            
        # Check if the reduction meets our threshold
        if validation["percentage_reduction"] < min_reduction:
            validation["status"] = "skipped"
            validation["message"] = f"Reduction ({validation['percentage_reduction']:.2f}%) below threshold ({min_reduction:.1f}%)"
            return validation
            
        # Check data integrity
        if not validation["all_numbers_preserved"]:
            validation["status"] = "error"
            validation["message"] = "Data integrity concerns: some numbers not preserved"
            return validation
            
        # If validation passes, apply changes
        result = validate_optimization(file_path, show_details=False, dry_run=False)
        return result
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_file, file_path): file_path for file_path in files}
        
        # Track progress with tqdm
        for future in tqdm(as_completed(futures), total=len(futures), desc="Optimizing files"):
            file_path = futures[future]
            try:
                result = future.result()
                results.append(result)
                
                # Update totals for optimized files
                if result["status"] == "optimized":
                    total_original += result["original_size"]
                    total_optimized += result["new_size"]
                    
                # Log individual file results
                if result["status"] == "optimized":
                    logging.info(f"Optimized {os.path.basename(file_path)}: {result['percentage_reduction']:.2f}% reduction")
                elif result["status"] == "error":
                    logging.error(f"Error processing {os.path.basename(file_path)}: {result.get('message', 'Unknown error')}")
                
            except Exception as e:
                logging.error(f"Exception processing {file_path}: {str(e)}")
                results.append({
                    "file": file_path,
                    "status": "error",
                    "message": str(e)
                })
    
    # Summarize results
    optimized_count = sum(1 for r in results if r["status"] == "optimized")
    skipped_count = sum(1 for r in results if r["status"] == "skipped")
    error_count = sum(1 for r in results if r["status"] == "error")
    
    print("\n===== OPTIMIZATION SUMMARY =====")
    print(f"Total files processed: {len(files)}")
    print(f"Optimized: {optimized_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Errors: {error_count}")
    
    if optimized_count > 0:
        total_reduction = (total_original - total_optimized) / total_original * 100
        print(f"\nTotal size reduction: {total_reduction:.2f}%")
        print(f"Total bytes saved: {total_original - total_optimized:,}")
        
        # Calculate average reduction per file
        avg_reduction = sum(r.get("percentage_reduction", 0) for r in results if r["status"] == "optimized") / optimized_count
        print(f"Average reduction per file: {avg_reduction:.2f}%")
        
    # Write detailed report
    report_path = "html_optimization_report.txt"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("HTML Optimization Report\n")
        f.write("======================\n\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Files processed: {len(files)}\n")
        f.write(f"Files optimized: {optimized_count}\n\n")
        
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
    
    return {
        "processed": len(files),
        "optimized": optimized_count,
        "skipped": skipped_count,
        "errors": error_count,
        "total_original": total_original,
        "total_optimized": total_optimized,
        "total_reduction": (total_original - total_optimized) / total_original * 100 if total_original > 0 else 0
    }

def main():
    parser = argparse.ArgumentParser(description="Apply HTML optimization to processed files")
    parser.add_argument('--company', help='Process only files for this company ticker')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--min-reduction', type=float, default=1.0, 
                        help='Minimum percentage reduction required to apply changes (default: 1.0%%)')
    parser.add_argument('--validate-only', action='store_true', 
                        help='Run in validation mode only, no changes will be made')
    
    args = parser.parse_args()
    
    if args.validate_only:
        print("Running in VALIDATION ONLY mode - no changes will be made")
    
    # Find files to process
    files = find_files(company=args.company)
    
    if not files:
        print(f"No matching files found for {'company ' + args.company if args.company else 'any company'}")
        return
    
    # If validate-only, just validate a few files
    if args.validate_only:
        sample_size = min(5, len(files))
        sample_files = files[:sample_size]
        
        print(f"Validating a sample of {sample_size} files...")
        
        for file in sample_files:
            print(f"\nValidating {os.path.basename(file)}...")
            result = validate_optimization(file, show_details=True, dry_run=True)
            
            if result["status"] == "validated":
                print(f"✅ Valid for optimization: {result['percentage_reduction']:.2f}% reduction possible")
                print(f"   Data integrity: {'✅ All numbers preserved' if result['all_numbers_preserved'] else '❌ Some numbers not preserved'}")
            else:
                print(f"❌ Not valid for optimization: {result.get('message', 'Unknown reason')}")
                
        return
        
    # Process the files
    optimize_files(files, workers=args.workers, min_reduction=args.min_reduction)

if __name__ == "__main__":
    main()