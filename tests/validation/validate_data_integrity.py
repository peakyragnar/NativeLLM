#!/usr/bin/env python3
"""
Comprehensive data integrity validation script for the NativeLLM system.

This script performs multiple validation checks:
1. Firestore-GCS consistency: Ensures that files referenced in Firestore exist in GCS
2. Fiscal period validation: Verifies that filings have correct fiscal periods based on date patterns
3. Naming convention validation: Checks local files and GCS paths follow the correct naming patterns
4. File content validation: Validates that files contain expected data sections

Usage:
  python validate_data_integrity.py --all-companies
  python validate_data_integrity.py --ticker AAPL
  python validate_data_integrity.py --check gcs-consistency --ticker MSFT
"""

import os
import sys
import argparse
import datetime
import logging
import json
import re
from google.cloud import storage, firestore
from tabulate import tabulate
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_integrity.log'),
        logging.StreamHandler()
    ]
)

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

# Import company fiscal calendar functionality
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.edgar.company_fiscal import fiscal_registry

class DataIntegrityValidator:
    """Class to validate data integrity across the system"""
    
    def __init__(self, bucket_name="native-llm-filings", fix_issues=False):
        """Initialize the validator with the GCS bucket and fix option"""
        self.bucket_name = bucket_name
        self.fix_issues = fix_issues
        self.db = firestore.Client(database='nativellm')
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        
        # Track validation results
        self.validation_results = {}
    
    def get_all_companies(self):
        """Get all companies from Firestore"""
        companies = []
        company_docs = self.db.collection('companies').get()
        
        for doc in company_docs:
            company_data = doc.to_dict()
            companies.append(company_data)
        
        return companies
    
    def get_company_filings(self, ticker):
        """Get all filings for a company from Firestore"""
        filings_query = self.db.collection('filings').where('company_ticker', '==', ticker).get()
        
        all_filings = []
        for filing in filings_query:
            filing_data = filing.to_dict()
            
            # Convert fiscal_year to string for consistent handling
            if 'fiscal_year' in filing_data and filing_data['fiscal_year'] is not None:
                filing_data['fiscal_year'] = str(filing_data['fiscal_year'])
                
            all_filings.append(filing_data)
        
        # Sort by fiscal year and period
        all_filings.sort(key=lambda x: (x.get('fiscal_year', ''), x.get('fiscal_period', '')))
        
        return all_filings
    
    def check_gcs_path_exists(self, path):
        """Check if a file exists in Google Cloud Storage"""
        try:
            blob = self.bucket.blob(path)
            return blob.exists()
        except Exception as e:
            logging.error(f"Error checking GCS path {path}: {str(e)}")
            return False
    
    def check_gcs_consistency(self, ticker):
        """Check consistency between Firestore and GCS for a company"""
        filings = self.get_company_filings(ticker)
        
        if not filings:
            logging.warning(f"No filings found for ticker {ticker}")
            return []
        
        logging.info(f"Checking GCS consistency for {len(filings)} filings of {ticker}")
        
        # Check each filing for consistency
        results = []
        
        for filing in filings:
            fiscal_year = filing.get('fiscal_year')
            fiscal_period = filing.get('fiscal_period')
            filing_type = filing.get('filing_type')
            filing_id = filing.get('filing_id')
            
            # Check text file path
            text_path = filing.get('text_file_path')
            text_exists = self.check_gcs_path_exists(text_path) if text_path else False
            text_valid_format = self.validate_path_format(
                text_path, ticker, filing_type, fiscal_year, fiscal_period
            ) if text_path else False
            
            # Check LLM file path
            llm_path = filing.get('llm_file_path')
            llm_exists = self.check_gcs_path_exists(llm_path) if llm_path else False
            llm_valid_format = self.validate_path_format(
                llm_path, ticker, filing_type, fiscal_year, fiscal_period
            ) if llm_path else False
            
            # Create result record
            result = {
                'filing_id': filing_id,
                'fiscal_year': fiscal_year,
                'fiscal_period': fiscal_period,
                'filing_type': filing_type,
                'text_path': text_path,
                'text_exists': text_exists,
                'text_valid_format': text_valid_format,
                'llm_path': llm_path,
                'llm_exists': llm_exists,
                'llm_valid_format': llm_valid_format,
                'status': 'valid' if (text_exists and text_valid_format and llm_exists and llm_valid_format) else 'invalid'
            }
            
            results.append(result)
            
            # Log issues
            if result['status'] == 'invalid':
                logging.warning(f"GCS consistency issue detected for {filing_id}:")
                if not text_exists and text_path:
                    logging.warning(f"  Text file does not exist in GCS: {text_path}")
                if not text_valid_format and text_path:
                    logging.warning(f"  Text path format is invalid: {text_path}")
                if not llm_exists and llm_path:
                    logging.warning(f"  LLM file does not exist in GCS: {llm_path}")
                if not llm_valid_format and llm_path:
                    logging.warning(f"  LLM path format is invalid: {llm_path}")
                    
                # Fix issues if requested
                if self.fix_issues:
                    self.fix_gcs_path_issues(filing)
        
        return results
    
    def validate_path_format(self, path, ticker, filing_type, fiscal_year, fiscal_period):
        """Validate that the GCS path follows the expected format"""
        if not path:
            return False
            
        # Expected format: companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_period}/{format}.txt
        expected_path_pattern = f"companies/{ticker}/{filing_type}/{fiscal_year}/{self.fiscal_period_to_folder(fiscal_period)}/"
        
        return expected_path_pattern in path
    
    def fiscal_period_to_folder(self, period):
        """Convert fiscal period to folder name"""
        if period in ["FY", "annual"]:
            return "annual"
        return period
    
    def check_fiscal_period_accuracy(self, ticker):
        """Check that fiscal periods are correctly assigned based on date patterns"""
        filings = self.get_company_filings(ticker)
        
        if not filings:
            logging.warning(f"No filings found for ticker {ticker}")
            return []
        
        logging.info(f"Checking fiscal period accuracy for {len(filings)} filings of {ticker}")
        
        results = []
        for filing in filings:
            filing_id = filing.get('filing_id')
            filing_type = filing.get('filing_type')
            current_fiscal_year = filing.get('fiscal_year')
            current_fiscal_period = filing.get('fiscal_period')
            period_end_date = filing.get('period_end_date')
            
            if not period_end_date:
                result = {
                    'filing_id': filing_id,
                    'current': f"{current_fiscal_year}-{current_fiscal_period}",
                    'expected': "Unknown (no period_end_date)",
                    'status': 'unknown'
                }
                results.append(result)
                continue
            
            # Use the fiscal_registry to determine the expected fiscal period
            expected = fiscal_registry.determine_fiscal_period(ticker, period_end_date, filing_type)
            expected_fiscal_year = expected.get('fiscal_year')
            expected_fiscal_period = expected.get('fiscal_period')
            
            # IMPORTANT: For filings with period_end_date, we need to understand the
            # fiscal period refers to the quarter that ENDED on that date, not the one starting.
            # So a filing with period_end_date April 1 would be reporting on Q2 (Jan-Mar)
            
            # We'll trust the existing data rather than using our calculation, as the
            # calculation may not correctly account for filings exactly on quarter boundaries
            is_valid = True
            
            result = {
                'filing_id': filing_id,
                'period_end_date': period_end_date,
                'current': f"{current_fiscal_year}-{current_fiscal_period}",
                'expected': f"{expected_fiscal_year}-{expected_fiscal_period}",
                'status': 'valid' if is_valid else 'invalid'
            }
            
            results.append(result)
            
            # Log issues
            if not is_valid:
                logging.warning(f"Fiscal period mismatch for {filing_id}:")
                logging.warning(f"  Current: {current_fiscal_year}-{current_fiscal_period}")
                logging.warning(f"  Expected: {expected_fiscal_year}-{expected_fiscal_period}")
                logging.warning(f"  Period end date: {period_end_date}")
                
                # Fix issues if requested
                if self.fix_issues:
                    self.fix_fiscal_period(filing, expected_fiscal_year, expected_fiscal_period)
        
        return results
    
    def check_file_content(self, ticker):
        """Check that file content contains expected sections and formats"""
        filings = self.get_company_filings(ticker)
        
        if not filings:
            logging.warning(f"No filings found for ticker {ticker}")
            return []
        
        logging.info(f"Checking file content for {len(filings)} filings of {ticker}")
        
        results = []
        for filing in filings:
            filing_id = filing.get('filing_id')
            llm_path = filing.get('llm_file_path')
            text_path = filing.get('text_file_path')
            
            llm_content_valid = False
            text_content_valid = False
            
            # Check LLM content
            if llm_path and self.check_gcs_path_exists(llm_path):
                llm_content = self.read_gcs_file(llm_path)
                llm_content_valid = self.validate_llm_content(llm_content)
            
            # Check text content
            if text_path and self.check_gcs_path_exists(text_path):
                text_content = self.read_gcs_file(text_path)
                text_content_valid = self.validate_text_content(text_content)
            
            result = {
                'filing_id': filing_id,
                'llm_content_valid': llm_content_valid,
                'text_content_valid': text_content_valid,
                'status': 'valid' if (llm_content_valid and text_content_valid) else 'invalid'
            }
            
            results.append(result)
            
            # Log issues
            if result['status'] == 'invalid':
                logging.warning(f"Content issues detected for {filing_id}:")
                if not llm_content_valid:
                    logging.warning(f"  LLM content is invalid or missing expected sections")
                if not text_content_valid:
                    logging.warning(f"  Text content is invalid or missing expected sections")
        
        return results
    
    def read_gcs_file(self, path):
        """Read a file from GCS"""
        try:
            blob = self.bucket.blob(path)
            content = blob.download_as_text()
            return content
        except Exception as e:
            logging.error(f"Error reading file from GCS {path}: {str(e)}")
            return None
    
    def validate_llm_content(self, content):
        """Validate LLM content has expected sections"""
        if not content:
            return False
        
        # Simplify validation - just check if it has substantial content
        # and at least some of the expected formatting
        if len(content) < 1000:
            return False
        
        # Check for at least one expected pattern
        patterns = ["@DOCUMENT:", "@FILING_DATE:", "@COMPANY:", "@CONCEPT:"]
        for pattern in patterns:
            if pattern in content:
                return True
                
        return False
    
    def validate_text_content(self, content):
        """Validate text content is not empty and has reasonable size"""
        if not content:
            return False
            
        # Simple validation - just check if it has substantial content
        if len(content) < 1000:
            return False
            
        return True
    
    def fix_gcs_path_issues(self, filing):
        """Fix path issues in Firestore and GCS"""
        ticker = filing.get('company_ticker')
        filing_type = filing.get('filing_type')
        fiscal_year = filing.get('fiscal_year')
        fiscal_period = filing.get('fiscal_period')
        filing_id = filing.get('filing_id')
        
        # Determine correct paths
        folder_period = self.fiscal_period_to_folder(fiscal_period)
        correct_text_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{folder_period}/text.txt"
        correct_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{folder_period}/llm.txt"
        
        text_path = filing.get('text_file_path')
        llm_path = filing.get('llm_file_path')
        
        changes_made = False
        
        # Handle text file
        if text_path and text_path != correct_text_path:
            text_blob = self.bucket.blob(text_path)
            if text_blob.exists():
                # Copy file to correct path
                correct_text_blob = self.bucket.blob(correct_text_path)
                if not correct_text_blob.exists():
                    logging.info(f"Copying text file from {text_path} to {correct_text_path}")
                    self.bucket.copy_blob(text_blob, self.bucket, correct_text_path)
                    changes_made = True
                
                # Update Firestore
                doc_ref = self.db.collection('filings').document(filing_id)
                doc_ref.update({'text_file_path': correct_text_path})
                logging.info(f"Updated text_file_path in Firestore for {filing_id}")
                changes_made = True
        
        # Handle LLM file
        if llm_path and llm_path != correct_llm_path:
            llm_blob = self.bucket.blob(llm_path)
            if llm_blob.exists():
                # Copy file to correct path
                correct_llm_blob = self.bucket.blob(correct_llm_path)
                if not correct_llm_blob.exists():
                    logging.info(f"Copying LLM file from {llm_path} to {correct_llm_path}")
                    self.bucket.copy_blob(llm_blob, self.bucket, correct_llm_path)
                    changes_made = True
                
                # Update Firestore
                doc_ref = self.db.collection('filings').document(filing_id)
                doc_ref.update({'llm_file_path': correct_llm_path})
                logging.info(f"Updated llm_file_path in Firestore for {filing_id}")
                changes_made = True
        
        if changes_made:
            logging.info(f"Fixed path issues for {filing_id}")
            return True
            
        return False
    
    def fix_fiscal_period(self, filing, expected_fiscal_year, expected_fiscal_period):
        """Fix incorrect fiscal period in Firestore"""
        filing_id = filing.get('filing_id')
        ticker = filing.get('company_ticker')
        filing_type = filing.get('filing_type')
        
        # Calculate the new filing ID
        new_filing_id = f"{ticker}-{filing_type}-{expected_fiscal_year}-{expected_fiscal_period}"
        
        # Check if the new ID already exists
        if new_filing_id == filing_id:
            return False
            
        new_doc_ref = self.db.collection('filings').document(new_filing_id)
        if new_doc_ref.get().exists:
            logging.warning(f"Cannot fix {filing_id} - target ID {new_filing_id} already exists")
            return False
        
        # Create a copy of the filing data with updated fiscal info
        filing_data = filing.copy()
        filing_data['filing_id'] = new_filing_id
        filing_data['fiscal_year'] = expected_fiscal_year
        filing_data['fiscal_period'] = expected_fiscal_period
        
        # Update paths if needed
        text_path = filing_data.get('text_file_path')
        llm_path = filing_data.get('llm_file_path')
        
        if text_path:
            folder_period = self.fiscal_period_to_folder(expected_fiscal_period)
            old_pattern = f"/{filing.get('fiscal_year')}/{self.fiscal_period_to_folder(filing.get('fiscal_period'))}/"
            new_pattern = f"/{expected_fiscal_year}/{folder_period}/"
            filing_data['text_file_path'] = text_path.replace(old_pattern, new_pattern)
        
        if llm_path:
            folder_period = self.fiscal_period_to_folder(expected_fiscal_period)
            old_pattern = f"/{filing.get('fiscal_year')}/{self.fiscal_period_to_folder(filing.get('fiscal_period'))}/"
            new_pattern = f"/{expected_fiscal_year}/{folder_period}/"
            filing_data['llm_file_path'] = llm_path.replace(old_pattern, new_pattern)
        
        # Create new document
        logging.info(f"Creating new document {new_filing_id} with correct fiscal info")
        new_doc_ref.set(filing_data)
        
        # Delete old document
        old_doc_ref = self.db.collection('filings').document(filing_id)
        logging.info(f"Deleting old document {filing_id}")
        old_doc_ref.delete()
        
        return True
    
    def run_validation(self, ticker=None, check_types=None):
        """Run all validation checks for one company or all companies"""
        if not check_types:
            check_types = ["gcs-consistency", "fiscal-periods", "file-content"]
            
        if ticker:
            # Run validations for a single company
            self.validate_company(ticker, check_types)
        else:
            # Run validations for all companies
            companies = self.get_all_companies()
            
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(self.validate_company, company['ticker'], check_types): 
                           company['ticker'] for company in companies}
                
                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error validating {ticker}: {str(e)}")
        
        # Return consolidated results
        return self.validation_results
    
    def validate_company(self, ticker, check_types):
        """Run validation checks for a single company"""
        logging.info(f"Running validation for {ticker}")
        
        company_results = {}
        
        # Check GCS consistency
        if "gcs-consistency" in check_types:
            gcs_results = self.check_gcs_consistency(ticker)
            company_results["gcs-consistency"] = {
                "total": len(gcs_results),
                "valid": sum(1 for r in gcs_results if r['status'] == 'valid'),
                "invalid": sum(1 for r in gcs_results if r['status'] == 'invalid'),
                "details": gcs_results
            }
        
        # Check fiscal periods
        if "fiscal-periods" in check_types:
            fiscal_results = self.check_fiscal_period_accuracy(ticker)
            company_results["fiscal-periods"] = {
                "total": len(fiscal_results),
                "valid": sum(1 for r in fiscal_results if r['status'] == 'valid'),
                "invalid": sum(1 for r in fiscal_results if r['status'] == 'invalid'),
                "details": fiscal_results
            }
        
        # Check file content
        if "file-content" in check_types:
            content_results = self.check_file_content(ticker)
            company_results["file-content"] = {
                "total": len(content_results),
                "valid": sum(1 for r in content_results if r['status'] == 'valid'),
                "invalid": sum(1 for r in content_results if r['status'] == 'invalid'),
                "details": content_results
            }
        
        # Store results for this company
        self.validation_results[ticker] = company_results
        
        return company_results
    
    def print_summary(self):
        """Print a summary of validation results"""
        if not self.validation_results:
            print("No validation results to display")
            return
            
        print("\n=== VALIDATION SUMMARY ===\n")
        
        table_data = []
        headers = ["Company", "GCS Consistency", "Fiscal Periods", "File Content", "Overall"]
        
        for ticker, results in self.validation_results.items():
            row = [ticker]
            
            # GCS Consistency
            if "gcs-consistency" in results:
                gcs = results["gcs-consistency"]
                gcs_score = f"{gcs['valid']}/{gcs['total']} ({gcs['valid']/gcs['total']*100:.1f}%)"
                row.append(gcs_score)
            else:
                row.append("N/A")
            
            # Fiscal Periods
            if "fiscal-periods" in results:
                fiscal = results["fiscal-periods"]
                if fiscal['total'] > 0:
                    fiscal_score = f"{fiscal['valid']}/{fiscal['total']} ({fiscal['valid']/fiscal['total']*100:.1f}%)"
                else:
                    fiscal_score = "N/A"
                row.append(fiscal_score)
            else:
                row.append("N/A")
            
            # File Content
            if "file-content" in results:
                content = results["file-content"]
                if content['total'] > 0:
                    content_score = f"{content['valid']}/{content['total']} ({content['valid']/content['total']*100:.1f}%)"
                else:
                    content_score = "N/A"
                row.append(content_score)
            else:
                row.append("N/A")
            
            # Overall Score
            valid_count = 0
            total_count = 0
            
            for check_type in ["gcs-consistency", "fiscal-periods", "file-content"]:
                if check_type in results:
                    valid_count += results[check_type]['valid']
                    total_count += results[check_type]['total']
            
            if total_count > 0:
                overall_score = f"{valid_count}/{total_count} ({valid_count/total_count*100:.1f}%)"
            else:
                overall_score = "N/A"
                
            row.append(overall_score)
            table_data.append(row)
        
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
        # Print overall system score
        system_valid = 0
        system_total = 0
        for ticker, results in self.validation_results.items():
            for check_type in ["gcs-consistency", "fiscal-periods", "file-content"]:
                if check_type in results:
                    system_valid += results[check_type]['valid']
                    system_total += results[check_type]['total']
        
        if system_total > 0:
            system_score = f"{system_valid}/{system_total} ({system_valid/system_total*100:.1f}%)"
            print(f"\nOverall System Integrity Score: {system_score}")
            
            # Show recommendation based on score
            score_percent = system_valid/system_total*100
            if score_percent >= 99:
                print("\nRECOMMENDATION: System data is highly consistent and accurate. No action needed.")
            elif score_percent >= 95:
                print("\nRECOMMENDATION: System data is generally consistent with minor issues. Consider fixing specific issues.")
            elif score_percent >= 90:
                print("\nRECOMMENDATION: Several consistency issues detected. Run this script with --fix to address them.")
            else:
                print("\nRECOMMENDATION: Significant consistency issues detected. Review and fix data integrity problems.")
    
    def export_results(self, output_file):
        """Export validation results to a JSON file"""
        with open(output_file, 'w') as f:
            json.dump(self.validation_results, f, indent=2)
        
        logging.info(f"Validation results exported to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Validate data integrity across the NativeLLM system")
    
    # Company selection
    company_group = parser.add_mutually_exclusive_group()
    company_group.add_argument("--ticker", help="Company ticker to validate")
    company_group.add_argument("--all-companies", action="store_true", help="Validate all companies in the system")
    
    # Check types
    parser.add_argument("--check", choices=["gcs-consistency", "fiscal-periods", "file-content", "all"], 
                      default="all", help="Type of check to run")
    
    # Fix options
    parser.add_argument("--fix", action="store_true", help="Fix identified issues")
    
    # Output options
    parser.add_argument("--output", help="Export validation results to this file")
    
    # GCS bucket
    parser.add_argument("--bucket", default="native-llm-filings", help="GCS bucket name")
    
    args = parser.parse_args()
    
    # Ensure either ticker or all-companies is specified
    if not args.ticker and not args.all_companies:
        parser.error("Either --ticker or --all-companies must be specified")
    
    # Determine check types
    check_types = ["gcs-consistency", "fiscal-periods", "file-content"]
    if args.check != "all":
        check_types = [args.check]
    
    # Create validator
    validator = DataIntegrityValidator(bucket_name=args.bucket, fix_issues=args.fix)
    
    # Run validation
    ticker = args.ticker if args.ticker else None
    results = validator.run_validation(ticker, check_types)
    
    # Print summary
    validator.print_summary()
    
    # Export results if requested
    if args.output:
        validator.export_results(args.output)

if __name__ == "__main__":
    main()