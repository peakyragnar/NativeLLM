#!/usr/bin/env python3
"""
Test fiscal-to-calendar mapping for NVIDIA
"""

import datetime

# NVIDIA fiscal year parameters
fiscal_year = 2023
fiscal_year_end_month = 1  # January
q = 1  # Q1

# Calculate expected period end month for this quarter
period_end_month = (fiscal_year_end_month + 3*q) % 12
if period_end_month == 0:
    period_end_month = 12

# Calculate calendar year based on mathematical formula
if period_end_month <= fiscal_year_end_month:
    calendar_year = fiscal_year
else:
    calendar_year = fiscal_year - 1

print(f'NVDA FY{fiscal_year} Q{q} -> Period end {calendar_year}-{period_end_month}')

# Also check the fiscal registry
from src2.sec.fiscal.company_fiscal import fiscal_registry

# Check Q1 2023
q1_info = fiscal_registry.determine_fiscal_period('NVDA', '2022-05-01', '10-Q')
print(f"Fiscal registry for 2022-05-01: {q1_info}")

# Check Q1 2023 with alternative date
q1_alt_info = fiscal_registry.determine_fiscal_period('NVDA', '2022-04-30', '10-Q')
print(f"Fiscal registry for 2022-04-30: {q1_alt_info}")

# Check if there's a mapping for April 2022
april_dates = ['2022-04-01', '2022-04-15', '2022-04-30']
for date in april_dates:
    info = fiscal_registry.determine_fiscal_period('NVDA', date, '10-Q')
    print(f"Fiscal registry for {date}: {info}")
