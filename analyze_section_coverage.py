#!/usr/bin/env python3
"""
SEC Filing Section Coverage Analyzer

This tool analyzes processed SEC filing documents to report on section detection
patterns and coverage metrics across multiple documents.
"""

import os
import re
import sys
import json
import argparse
from collections import defaultdict, Counter
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Required and optional sections for different filing types
SECTION_DEFINITIONS = {
    "10-K": {
        "required": [
            "ITEM_1_BUSINESS",
            "ITEM_1A_RISK_FACTORS",
            "ITEM_2_PROPERTIES",
            "ITEM_3_LEGAL_PROCEEDINGS", 
            "ITEM_7_MD_AND_A",
            "ITEM_7A_MARKET_RISK",
            "ITEM_8_FINANCIAL_STATEMENTS",
            "ITEM_9A_CONTROLS",
            "ITEM_10_DIRECTORS",
            "ITEM_11_EXECUTIVE_COMPENSATION"
        ],
        "optional": [
            "ITEM_1B_UNRESOLVED_STAFF_COMMENTS",
            "ITEM_1C_CYBERSECURITY",
            "ITEM_4_MINE_SAFETY_DISCLOSURES",
            "ITEM_5_MARKET",
            "ITEM_6_SELECTED_FINANCIAL_DATA",
            "ITEM_9_DISAGREEMENTS",
            "ITEM_9B_OTHER_INFORMATION",
            "ITEM_9C_FOREIGN_JURISDICTIONS",
            "ITEM_12_SECURITY_OWNERSHIP",
            "ITEM_13_RELATIONSHIPS",
            "ITEM_14_ACCOUNTANT_FEES",
            "ITEM_15_EXHIBITS",
            "ITEM_16_SUMMARY"
        ]
    },
    "10-Q": {
        "required": [
            "ITEM_1_FINANCIAL_STATEMENTS",
            "ITEM_2_MD_AND_A",
            "ITEM_3_MARKET_RISK",
            "ITEM_4_CONTROLS"
        ],
        "optional": [
            "ITEM_1_LEGAL_PROCEEDINGS",
            "ITEM_1A_RISK_FACTORS",
            "ITEM_2_UNREGISTERED_SALES",
            "ITEM_3_DEFAULTS",
            "ITEM_4_MINE_SAFETY",
            "ITEM_5_OTHER_INFORMATION",
            "ITEM_6_EXHIBITS"
        ]
    }
}

def parse_section_data(file_path):
    """Extract section data from a processed LLM file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Extract filing type
        filing_type_match = re.search(r'@DOCUMENT: ([A-Z]+)-(\d+[A-Z\-]+)', content)
        if filing_type_match:
            filing_type = filing_type_match.group(2)
        else:
            filing_type = "UNKNOWN"
            
        # Get all sections mentioned in the ALL_SECTIONS metadata
        all_sections_match = re.search(r'@ALL_SECTIONS:\s+(.*?)$', content, re.MULTILINE)
        all_sections = []
        if all_sections_match:
            section_text = all_sections_match.group(1)
            all_sections = [s.strip() for s in section_text.split(',')]
            
        # Also check for @SECTION markers directly
        section_markers = re.findall(r'@SECTION:\s+(\w+)', content)
        for marker in section_markers:
            marker = marker.upper()  # Normalize to uppercase
            if marker not in all_sections:
                all_sections.append(marker)
            
        # Extract found required and optional sections 
        found_required_match = re.search(r'Required sections found: (.*?)$', content, re.MULTILINE)
        found_required = []
        if found_required_match:
            found_req_text = found_required_match.group(1)
            # Convert from display format back to section ID format
            for section in found_req_text.split(','):
                section = section.strip()
                if section:
                    section_id = "ITEM_" + section.replace("Item ", "").replace(" ", "_")
                    found_required.append(section_id.upper())
                    
        found_optional_match = re.search(r'Optional sections found: (.*?)$', content, re.MULTILINE)
        found_optional = []
        if found_optional_match:
            found_opt_text = found_optional_match.group(1)
            # Convert from display format back to section ID format
            for section in found_opt_text.split(','):
                section = section.strip()
                if section:
                    section_id = "ITEM_" + section.replace("Item ", "").replace(" ", "_")
                    found_optional.append(section_id.upper())
                    
        # Extract coverage percentages
        required_coverage_match = re.search(r'10-K Required Coverage: ([\d\.]+)%', content)
        required_coverage = 0
        if required_coverage_match:
            required_coverage = float(required_coverage_match.group(1))
            
        standard_coverage_match = re.search(r'10-K Standard Coverage: ([\d\.]+)%', content)  
        standard_coverage = 0
        if standard_coverage_match:
            standard_coverage = float(standard_coverage_match.group(1))
            
        # Check for sections explicitly mentioned to be detected with alternative methods
        alternative_detection = []
        alt_detect_match = re.search(r'Note: (\d+) section\(s\) detected using alternative methods', content)
        if alt_detect_match:
            num_alt = int(alt_detect_match.group(1))
            if num_alt > 0:
                alternative_detection.append("Alternative detection used")
                
        return {
            "file": os.path.basename(file_path),
            "filing_type": filing_type,
            "all_sections": all_sections,
            "found_required": found_required,
            "found_optional": found_optional,
            "required_coverage": required_coverage,
            "standard_coverage": standard_coverage,
            "alternative_detection": alternative_detection
        }
    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")
        return None

def analyze_directory(directory_path, output_format="text"):
    """Analyze all LLM files in a directory"""
    filing_data = []
    
    if not os.path.exists(directory_path):
        logging.error(f"Directory not found: {directory_path}")
        return None
        
    # Find all LLM files
    llm_files = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith("_llm.txt"):
                llm_files.append(os.path.join(root, file))
                
    if not llm_files:
        logging.warning(f"No LLM files found in {directory_path}")
        return None
        
    logging.info(f"Found {len(llm_files)} LLM files to analyze")
    
    # Process each file
    for file_path in llm_files:
        file_data = parse_section_data(file_path)
        if file_data:
            filing_data.append(file_data)
            
    if not filing_data:
        logging.error("No valid data extracted from files")
        return None
        
    # Group by filing type for analysis
    filing_type_groups = defaultdict(list)
    for data in filing_data:
        filing_type_groups[data["filing_type"]].append(data)
        
    # Generate statistics
    stats = {
        "total_filings": len(filing_data),
        "filing_types": {},
        "section_detection_rates": defaultdict(lambda: {"count": 0, "total": 0, "percentage": 0}),
        "average_required_coverage": 0,
        "average_standard_coverage": 0,
        "files_with_alternative_detection": 0
    }
    
    total_req_coverage = 0
    total_std_coverage = 0
    
    for filing_type, filings in filing_type_groups.items():
        stats["filing_types"][filing_type] = len(filings)
        
        # Calculate section presence
        section_counts = defaultdict(int)
        for filing in filings:
            for section in filing["found_required"] + filing["found_optional"]:
                section_counts[section] += 1
                
            # Count files with alternative detection
            if filing["alternative_detection"]:
                stats["files_with_alternative_detection"] += 1
                
            # Add to coverage totals
            total_req_coverage += filing["required_coverage"]
            total_std_coverage += filing["standard_coverage"]
                
        # Calculate detection rates
        all_sections = set()
        for filing in filings:
            all_sections.update(filing["all_sections"])
            
        for section in all_sections:
            section_id = section.replace(" ", "_")
            stats["section_detection_rates"][section_id]["count"] = section_counts[section_id]
            stats["section_detection_rates"][section_id]["total"] = len(filings)
            stats["section_detection_rates"][section_id]["percentage"] = (
                section_counts[section_id] / len(filings) * 100
            )
    
    # Calculate averages            
    if filing_data:
        stats["average_required_coverage"] = total_req_coverage / len(filing_data)
        stats["average_standard_coverage"] = total_std_coverage / len(filing_data)
        
    # Determine which sections are most often missing
    required_sections = []
    optional_sections = []
    
    for filing_type, definitions in SECTION_DEFINITIONS.items():
        required_sections.extend(definitions["required"])
        optional_sections.extend(definitions["optional"])
        
    missing_required = []
    missing_optional = []
    
    for section in set(required_sections):
        if section in stats["section_detection_rates"]:
            rate = stats["section_detection_rates"][section]
            if rate["percentage"] < 90:  # Less than 90% detection
                missing_required.append({
                    "section": section, 
                    "detection_rate": rate["percentage"],
                    "count": rate["count"],
                    "total": rate["total"]
                })
                
    for section in set(optional_sections):
        if section in stats["section_detection_rates"]:
            rate = stats["section_detection_rates"][section]
            if rate["percentage"] < 50:  # Less than 50% detection
                missing_optional.append({
                    "section": section, 
                    "detection_rate": rate["percentage"],
                    "count": rate["count"],
                    "total": rate["total"]
                })
    
    # Sort by detection rate
    missing_required.sort(key=lambda x: x["detection_rate"])
    missing_optional.sort(key=lambda x: x["detection_rate"])
    
    stats["missing_required"] = missing_required
    stats["missing_optional"] = missing_optional
    
    # Filter out duplicate section IDs (normalize case)
    clean_section_rates = {}
    for section_id, rates in stats["section_detection_rates"].items():
        upper_id = section_id.upper()
        if upper_id not in clean_section_rates or rates["percentage"] > clean_section_rates[upper_id]["percentage"]:
            clean_section_rates[upper_id] = rates
            
    stats["section_detection_rates"] = clean_section_rates
    
    # Output format    
    if output_format == "json":
        return json.dumps(stats, indent=2)
    else:
        # Text report
        report = []
        report.append("=== SEC Filing Section Coverage Analysis ===")
        report.append(f"Total filings analyzed: {stats['total_filings']}")
        report.append("")
        
        report.append("Filing Types:")
        for filing_type, count in stats["filing_types"].items():
            report.append(f"  {filing_type}: {count} filings")
        report.append("")
        
        report.append("Coverage Metrics:")
        report.append(f"  Average Required Coverage: {stats['average_required_coverage']:.2f}%")
        report.append(f"  Average Standard Coverage: {stats['average_standard_coverage']:.2f}%")
        report.append(f"  Files using Alternative Detection: {stats['files_with_alternative_detection']} ({stats['files_with_alternative_detection']/stats['total_filings']*100:.1f}%)")
        report.append("")
        
        if missing_required:
            report.append("Missing Required Sections (< 90% detection):")
            for section in missing_required:
                report.append(f"  {section['section']}: {section['detection_rate']:.1f}% ({section['count']}/{section['total']})")
            report.append("")
            
        if missing_optional:
            report.append("Low Detection Optional Sections (< 50% detection):")
            for section in missing_optional:
                report.append(f"  {section['section']}: {section['detection_rate']:.1f}% ({section['count']}/{section['total']})")
            report.append("")
            
        report.append("Section Detection Rates:")
        sorted_rates = sorted(
            [(k, v) for k, v in stats["section_detection_rates"].items()],
            key=lambda x: x[1]["percentage"],
            reverse=True
        )
        
        for section_id, rate in sorted_rates:
            report.append(f"  {section_id}: {rate['percentage']:.1f}% ({rate['count']}/{rate['total']})")
            
        return "\n".join(report)

def main():
    parser = argparse.ArgumentParser(description='Analyze SEC filing section coverage')
    parser.add_argument('--dir', type=str, required=True, help='Directory containing LLM files')
    parser.add_argument('--output', type=str, choices=['text', 'json'], default='text', help='Output format')
    parser.add_argument('--outfile', type=str, help='Output file (optional, default is stdout)')
    
    args = parser.parse_args()
    
    result = analyze_directory(args.dir, args.output)
    
    if not result:
        logging.error("Analysis failed")
        sys.exit(1)
        
    if args.outfile:
        with open(args.outfile, 'w', encoding='utf-8') as f:
            f.write(result)
        logging.info(f"Results written to {args.outfile}")
    else:
        print(result)

if __name__ == "__main__":
    main()