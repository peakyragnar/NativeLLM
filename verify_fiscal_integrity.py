#!/usr/bin/env python3
"""
Fiscal Period Integrity Verifier

This script verifies that all periods from the Firestore database
are correctly mapped according to our single source of truth in company_fiscal.py.
"""

import argparse
import logging
import sys
from google.cloud import firestore

def setup_firestore_client():
    """Setup the Firestore client for the nativellm database."""
    try:
        return firestore.Client(database='nativellm')
    except Exception as e:
        logging.error(f"Error connecting to Firestore: {str(e)}")
        sys.exit(1)

def verify_period_end_dates(client, ticker):
    """
    Verify period end dates for a specific ticker in Firestore.
    
    Args:
        client: Firestore client
        ticker: Company ticker to verify
        
    Returns:
        dict: Results of the verification
    """
    # Import the fiscal registry as single source of truth
    from src2.sec.fiscal.company_fiscal import fiscal_registry
    
    filings_ref = client.collection("filings")
    
    # Query all filings for this ticker
    query = filings_ref.where("company_ticker", "==", ticker.upper())
    docs = query.stream()
    
    results = {
        "ticker": ticker,
        "total_filings": 0,
        "valid_filings": 0,
        "invalid_filings": 0,
        "issues": []
    }
    
    for doc in docs:
        filing_data = doc.to_dict()
        results["total_filings"] += 1
        
        # Skip records with no period_end_date
        if not filing_data.get("period_end_date"):
            results["issues"].append({
                "filing_id": doc.id,
                "issue": "Missing period_end_date", 
                "current_values": {
                    "fiscal_year": filing_data.get("fiscal_year"),
                    "fiscal_period": filing_data.get("fiscal_period"),
                }
            })
            results["invalid_filings"] += 1
            continue
            
        # Get values from Firestore
        period_end_date = filing_data.get("period_end_date")
        filing_type = filing_data.get("filing_type")
        current_fiscal_year = filing_data.get("fiscal_year")
        current_fiscal_period = filing_data.get("fiscal_period")
        
        # Get expected values from fiscal registry (single source of truth)
        fiscal_info = fiscal_registry.determine_fiscal_period(
            ticker, period_end_date, filing_type
        )
        
        expected_fiscal_year = fiscal_info.get("fiscal_year")
        expected_fiscal_period = fiscal_info.get("fiscal_period")
        
        # Verify that current values match expected values
        if (current_fiscal_year != expected_fiscal_year or 
            current_fiscal_period != expected_fiscal_period):
            
            results["issues"].append({
                "filing_id": doc.id,
                "issue": "Fiscal period mismatch", 
                "period_end_date": period_end_date,
                "filing_type": filing_type,
                "current_values": {
                    "fiscal_year": current_fiscal_year,
                    "fiscal_period": current_fiscal_period,
                },
                "expected_values": {
                    "fiscal_year": expected_fiscal_year,
                    "fiscal_period": expected_fiscal_period,
                }
            })
            results["invalid_filings"] += 1
        else:
            results["valid_filings"] += 1
            
    return results

def main():
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(description="Verify fiscal period integrity in Firestore.")
    parser.add_argument("--ticker", required=True, help="Company ticker to verify")
    parser.add_argument("--fix", action="store_true", help="Fix invalid fiscal periods")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Setup Firestore client
    client = setup_firestore_client()
    
    # Verify period end dates
    results = verify_period_end_dates(client, args.ticker)
    
    # Print results
    print("\n=== Fiscal Period Integrity Verification Results ===")
    print(f"Company: {results['ticker']}")
    print(f"Total Filings: {results['total_filings']}")
    print(f"Valid Filings: {results['valid_filings']}")
    print(f"Invalid Filings: {results['invalid_filings']}")
    
    if results["issues"]:
        print("\nIssues Found:")
        for i, issue in enumerate(results["issues"], 1):
            print(f"\n{i}. {issue['issue']}")
            print(f"   Filing ID: {issue['filing_id']}")
            if "period_end_date" in issue:
                print(f"   Period End Date: {issue['period_end_date']}")
            if "filing_type" in issue:
                print(f"   Filing Type: {issue['filing_type']}")
            if "current_values" in issue:
                print(f"   Current Values: {issue['current_values']}")
            if "expected_values" in issue:
                print(f"   Expected Values: {issue['expected_values']}")
                
            # Fix invalid fiscal periods if requested
            if args.fix and "expected_values" in issue:
                try:
                    doc_ref = client.collection("filings").document(issue['filing_id'])
                    
                    # Update the document with expected values
                    doc_ref.update({
                        "fiscal_year": issue["expected_values"]["fiscal_year"],
                        "fiscal_period": issue["expected_values"]["fiscal_period"],
                        "fiscal_source": "company_fiscal_registry",
                        "fiscal_integrity_verified": True,
                        "display_period": f"FY{issue['expected_values']['fiscal_year']} {issue['expected_values']['fiscal_period']}"
                    })
                    
                    print(f"   ✅ Fixed issue")
                except Exception as e:
                    print(f"   ❌ Error fixing issue: {str(e)}")
    else:
        print("\nNo issues found! All filings are valid.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())