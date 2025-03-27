# run_pipeline.py
import os
import sys
import argparse
import time

def setup_directories():
    """Set up project directories"""
    from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
    
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    
    print(f"Set up directories: {RAW_DATA_DIR}, {PROCESSED_DATA_DIR}")

def run_initial_companies():
    """Run the pipeline for initial companies"""
    from src.process_companies import process_companies
    
    print("Processing initial companies...")
    result = process_companies()
    return result

def run_specific_company(ticker):
    """Run the pipeline for a specific company"""
    from src.process_company import process_company
    
    print(f"Processing company: {ticker}")
    result = process_company(ticker)
    return result

def run_parallel_processing(count, workers):
    """Run parallel processing for top N companies"""
    from src.parallel_processor import process_companies_parallel
    from src.company_list import get_top_companies
    
    companies = get_top_companies(count)
    tickers = [c["ticker"] for c in companies]
    
    print(f"Processing top {len(tickers)} companies with {workers} workers...")
    result = process_companies_parallel(tickers, workers)
    return result
    
def upload_to_gcp(company=None, all_companies=False):
    """Upload processed files to Google Cloud Platform"""
    try:
        import gcp_upload
        
        if company:
            print(f"Uploading files for company {company} to GCP...")
            count = gcp_upload.upload_company_files(company)
            print(f"Uploaded {count} files for {company}")
            return count
        elif all_companies:
            print("Uploading files for all companies to GCP...")
            
            # Import to get processed data directory
            from src.config import PROCESSED_DATA_DIR
            
            # Get all company directories
            companies = [d for d in os.listdir(PROCESSED_DATA_DIR) 
                        if os.path.isdir(os.path.join(PROCESSED_DATA_DIR, d))]
            
            total_uploaded = 0
            for ticker in companies:
                count = gcp_upload.upload_company_files(ticker)
                total_uploaded += count
                print(f"Uploaded {count} files for {ticker}")
            
            print(f"Total files uploaded: {total_uploaded}")
            return total_uploaded
        else:
            print("No company specified for GCP upload")
            return 0
    except ImportError:
        print("Error importing GCP upload module. Make sure gcp_upload.py exists.")
        return 0
    except Exception as e:
        print(f"Error uploading to GCP: {str(e)}")
        return 0

def optimize_existing_files(company=None, workers=4):
    """Run HTML optimization on existing processed files"""
    try:
        # Import only when needed to avoid circular imports
        from apply_html_optimization import optimize_files
        from src.config import PROCESSED_DATA_DIR
        import glob
        
        if company:
            # For a specific company
            pattern = os.path.join(PROCESSED_DATA_DIR, company, "*_llm.txt")
        else:
            # For all companies
            pattern = os.path.join(PROCESSED_DATA_DIR, "*", "*_llm.txt")
            
        files = glob.glob(pattern)
        
        if not files:
            print(f"No matching files found for {'company ' + company if company else 'any company'}")
            return
            
        print(f"Found {len(files)} files to process")
        optimize_files(files, workers=workers)
        
    except ImportError:
        print("Error importing optimization modules. Make sure apply_html_optimization.py exists.")
    except Exception as e:
        print(f"Error running optimization: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Run the SEC filing to LLM format pipeline")
    parser.add_argument('--setup', action='store_true', help='Set up project directories')
    parser.add_argument('--initial', action='store_true', help='Process initial companies from config')
    parser.add_argument('--company', help='Process a specific company by ticker')
    parser.add_argument('--top', type=int, help='Process top N companies in parallel')
    parser.add_argument('--workers', type=int, default=3, help='Number of parallel workers')
    parser.add_argument('--optimize', action='store_true', help='Run HTML optimization on existing processed files')
    parser.add_argument('--optimize-company', help='Run HTML optimization on files for a specific company')
    parser.add_argument('--validate-optimization', action='store_true', 
                       help='Validate HTML optimization without making changes')
    # Add GCP upload arguments
    parser.add_argument('--upload-gcp', action='store_true', help='Upload all processed files to GCP')
    parser.add_argument('--upload-company', help='Upload processed files for a specific company to GCP')
    parser.add_argument('--process-and-upload', action='store_true', 
                       help='Process companies and automatically upload to GCP')
    
    args = parser.parse_args()
    
    if args.setup:
        setup_directories()
    
    if args.initial:
        run_initial_companies()
        # Auto-upload if requested
        if args.process_and_upload:
            upload_to_gcp(all_companies=True)
    
    if args.company:
        run_specific_company(args.company)
        # Auto-upload if requested
        if args.process_and_upload:
            upload_to_gcp(company=args.company)
    
    if args.top:
        run_parallel_processing(args.top, args.workers)
        # Auto-upload if requested
        if args.process_and_upload:
            upload_to_gcp(all_companies=True)
    
    if args.optimize or args.optimize_company:
        company = args.optimize_company if args.optimize_company else None
        optimize_existing_files(company, args.workers)
    
    if args.validate_optimization:
        # Import and run validation only mode
        try:
            from apply_html_optimization import main as run_optimization
            import sys
            sys.argv = [sys.argv[0], '--validate-only']
            if args.optimize_company:
                sys.argv.extend(['--company', args.optimize_company])
            run_optimization()
        except ImportError:
            print("Error importing optimization validation. Make sure apply_html_optimization.py exists.")
        except Exception as e:
            print(f"Error running optimization validation: {str(e)}")
    
    # Handle GCP upload requests
    if args.upload_gcp:
        upload_to_gcp(all_companies=True)
    
    if args.upload_company:
        upload_to_gcp(company=args.upload_company)
    
    # If no action specified, show help
    if not (args.setup or args.initial or args.company or args.top or 
           args.optimize or args.optimize_company or args.validate_optimization or
           args.upload_gcp or args.upload_company):
        parser.print_help()

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")