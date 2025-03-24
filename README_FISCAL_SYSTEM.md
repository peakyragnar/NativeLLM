# Evidence-Based Fiscal Period System

This document explains the new self-learning fiscal period system implemented in this project.

## Overview

The system addresses the challenge of correctly handling fiscal periods for companies with different fiscal calendars (like Microsoft's Jul-Jun fiscal year versus standard calendar-year companies).

Key features:
- Self-learning from SEC filing evidence
- Standardized fiscal period naming
- Automatic handling of special cases
- Consistent format across all system components

## How It Works

The system collects and analyzes evidence from multiple sources:

1. **Filing Content**: Extracts quarter mentions and fiscal period information
2. **Filing Dates**: Analyzes period end dates and filing submission patterns
3. **Known Patterns**: Pre-configured with common fiscal calendars (MSFT, AAPL, etc.)
4. **Multiple Filings**: Learns patterns by analyzing sequences of filings

### Fiscal Period Standardization

All fiscal periods are standardized internally to:
- `Q1`, `Q2`, `Q3` for quarterly reports
- `annual` for annual reports (never `Q4`)

This eliminates confusion between different naming conventions (`1Q` vs `Q1`, etc.)

## Usage Examples

### Basic Usage

```python
from src.edgar.fiscal_manager import fiscal_manager

# Determine fiscal period for a company and date
fiscal_info = fiscal_manager.determine_fiscal_period(
    ticker="MSFT",
    period_end_date="2023-09-30",
    filing_type="10-Q"
)
print(fiscal_info)
# Output: {'fiscal_year': '2024', 'fiscal_period': 'Q1'}

# Converting between formats
old_format = fiscal_manager.standardize_period("Q2", "old")
print(old_format)  # Output: "2Q"

display_format = fiscal_manager.standardize_period("annual", "display")
print(display_format)  # Output: "Q4"
```

### Learning from Filings

```python
# Update model with new filing data
filing_metadata = {
    "ticker": "XYZ",
    "filing_type": "10-K",
    "period_end_date": "2023-03-31",
    "filing_date": "2023-05-15"
}

# This updates the model and returns standardized fiscal info
fiscal_info = fiscal_manager.update_model("XYZ", filing_metadata)
print(fiscal_info)
# Output: {'fiscal_year': '2023', 'fiscal_period': 'annual'}
```

## Special Company Handling

### Microsoft (MSFT)
- Fiscal year: July 1 - June 30
- Q1: Jul-Sep
- Q2: Oct-Dec
- Q3: Jan-Mar
- Annual: Apr-Jun (never "Q4")

### Apple (AAPL)
- Fiscal year: October 1 - September 30
- Q1: Oct-Dec
- Q2: Jan-Mar
- Q3: Apr-Jun
- Annual: Jul-Sep (no "Q4" 10-Q filings, only 10-K)

## Key System Components

1. **FiscalSignal**: A piece of evidence about a company's fiscal periods
2. **FiscalPattern**: A detected pattern with confidence score
3. **CompanyFiscalModel**: Company-specific model that learns from signals
4. **FiscalPeriodManager**: Central manager for all companies

## Benefits

1. **Consistency**: Eliminates format inconsistencies (`1Q` vs `Q1`, etc.)
2. **Adaptability**: Self-learns from filings without manual configuration
3. **Transparency**: Evidence-based with confidence scoring
4. **Extensibility**: Easy to add new companies without special case code
5. **Better File Organization**: Consistent folder structures and file naming