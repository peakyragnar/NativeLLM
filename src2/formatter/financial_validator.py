"""
Financial Statement Validator

This module provides functionality to validate and standardize financial statements
to ensure consistency across different companies and reporting structures.
"""

import re
import logging
import json
from typing import Dict, List, Any, Tuple, Optional, Set
from decimal import Decimal, InvalidOperation

class FinancialValidator:
    """
    Validates and standardizes financial statements to ensure consistency.
    """

    # Mapping of standard XBRL tags to normalized concepts
    BALANCE_SHEET_MAPPINGS = {
        # Standard tags
        "us-gaap:Assets": "Assets",
        "us-gaap:Liabilities": "Liabilities",
        "us-gaap:StockholdersEquity": "Stockholders Equity",
        "us-gaap:LiabilitiesAndStockholdersEquity": "Liabilities And Stockholders Equity",

        # Alternative tags
        "us-gaap:AssetsCurrent": "Assets Current",
        "us-gaap:AssetsNoncurrent": "Assets Noncurrent",
        "us-gaap:LiabilitiesCurrent": "Liabilities Current",
        "us-gaap:LiabilitiesNoncurrent": "Liabilities Noncurrent",

        # Minority interest variations
        "us-gaap:MinorityInterest": "Minority Interest",
        "us-gaap:RedeemableNoncontrollingInterest": "Redeemable Noncontrolling Interest",
        "us-gaap:NoncontrollingInterestEquity": "Noncontrolling Interest Equity",

        # Company-specific tags
        "tsla:DigitalAssetsNetNonCurrent": "Digital Assets Net Noncurrent",
    }

    # Mapping of statement types
    STATEMENT_TYPES = {
        "BS": "Balance Sheet",
        "IS": "Income Statement",
        "CF": "Cash Flow Statement",
        "SE": "Statement Of Equity"
    }

    def __init__(self):
        """Initialize the financial validator."""
        self.logger = logging.getLogger(__name__)

    def validate_balance_sheet(self, assets: float, liabilities: float,
                              equity: float, minority_interests: float = 0) -> Tuple[bool, str]:
        """
        Validate that the balance sheet balances.

        Args:
            assets: Total assets value
            liabilities: Total liabilities value (excluding equity and minority interests)
            equity: Total stockholders' equity value
            minority_interests: Total minority interests value (can be 0)

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Calculate total liabilities and equity
        total_liabilities_and_equity = liabilities + equity + minority_interests

        # Allow for small rounding differences (0.1% tolerance)
        tolerance = max(assets, total_liabilities_and_equity) * 0.001

        if abs(assets - total_liabilities_and_equity) <= tolerance:
            return True, ""
        else:
            # Check if assets equals liabilities (in case liabilities already includes equity)
            if abs(assets - liabilities) <= tolerance:
                self.logger.warning("Assets equals Liabilities, suggesting Liabilities may already include Equity")
                return True, ""
            else:
                return False, (f"Balance sheet doesn't balance: Assets ({assets}) != "
                              f"Liabilities ({liabilities}) + Equity ({equity}) + "
                              f"Minority Interests ({minority_interests})")

    def extract_primary_context_codes(self, xbrl_data: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Identify the primary context codes for the balance sheet.

        Args:
            xbrl_data: The raw XBRL data

        Returns:
            Mapping of period end dates to context codes
        """
        # Find the context code used for total assets
        asset_facts = [f for f in xbrl_data if f.get("name") == "us-gaap:Assets"]

        # Group by context and find the ones with the highest values
        contexts = {}
        for fact in asset_facts:
            context_ref = fact.get("contextRef")

            # Parse value, handling commas and other formatting
            value_str = fact.get("value", "0").replace(",", "")
            try:
                value = float(value_str)
            except ValueError:
                self.logger.warning(f"Could not parse value: {value_str}")
                continue

            # Extract date from context reference or label
            date = self._extract_date_from_context(context_ref, xbrl_data)
            if not date:
                continue

            if date not in contexts or value > contexts[date]["value"]:
                contexts[date] = {
                    "context_ref": context_ref,
                    "value": value
                }

        return {date: info["context_ref"] for date, info in contexts.items()}

    def _extract_date_from_context(self, context_ref: str,
                                  xbrl_data: List[Dict[str, Any]]) -> Optional[str]:
        """
        Extract the date from a context reference.

        Args:
            context_ref: The context reference
            xbrl_data: The raw XBRL data

        Returns:
            The date in YYYY-MM-DD format, or None if not found
        """
        # First, look for a context with this ID in the data
        for item in xbrl_data:
            if item.get("contextRef") == context_ref:
                # Check if there's a period end date in the item
                if "period" in item and "endDate" in item["period"]:
                    return item["period"]["endDate"]

        # If not found, try to extract from the context reference itself
        if "_I" in context_ref:
            # Instant context
            match = re.search(r'_I(\d{8})', context_ref)
            if match:
                date_str = match.group(1)
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        elif "_D" in context_ref:
            # Duration context, use end date
            match = re.search(r'_D\d{8}-(\d{8})', context_ref)
            if match:
                date_str = match.group(1)
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        # If still not found, check if the context reference is a simple date
        if re.match(r'^\d{4}-\d{2}-\d{2}$', context_ref):
            return context_ref

        return None

    def extract_fact_value(self, xbrl_data: List[Dict[str, Any]],
                          concept_name: str, context_ref: str) -> Optional[float]:
        """
        Extract a fact value from XBRL data.

        Args:
            xbrl_data: The raw XBRL data
            concept_name: The concept name (e.g., "us-gaap:Assets")
            context_ref: The context reference

        Returns:
            The fact value as a float, or None if not found
        """
        for fact in xbrl_data:
            if fact.get("name") == concept_name and fact.get("contextRef") == context_ref:
                value_str = fact.get("value", "0").replace(",", "")
                try:
                    return float(value_str)
                except ValueError:
                    self.logger.warning(f"Could not parse value: {value_str}")
                    return None

        return None

    def construct_balance_sheet(self, xbrl_data: List[Dict[str, Any]],
                               primary_contexts: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
        """
        Construct a complete balance sheet from XBRL data.

        Args:
            xbrl_data: The raw XBRL data
            primary_contexts: The primary context codes to use

        Returns:
            Complete balance sheet with all components
        """
        balance_sheet = {}

        for date, context in primary_contexts.items():
            # Extract total assets
            assets = self.extract_fact_value(xbrl_data, "us-gaap:Assets", context) or 0

            # Extract total liabilities
            liabilities = self.extract_fact_value(xbrl_data, "us-gaap:Liabilities", context) or 0

            # Extract stockholders' equity
            equity = self.extract_fact_value(xbrl_data, "us-gaap:StockholdersEquity", context) or 0

            # Extract minority interests (if any)
            minority_interests = self.extract_fact_value(xbrl_data, "us-gaap:MinorityInterest", context) or 0

            # Extract total liabilities and equity
            total_liabilities_and_equity = self.extract_fact_value(
                xbrl_data, "us-gaap:LiabilitiesAndStockholdersEquity", context
            ) or 0

            # If total liabilities and equity is not available, calculate it
            if total_liabilities_and_equity == 0:
                total_liabilities_and_equity = liabilities + equity + minority_interests

            # Validate the balance sheet
            is_valid, error_message = self.validate_balance_sheet(
                assets, liabilities, equity, minority_interests
            )

            balance_sheet[date] = {
                "context": context,
                "assets": assets,
                "liabilities": liabilities,
                "equity": equity,
                "minority_interests": minority_interests,
                "total_liabilities_and_equity": total_liabilities_and_equity,
                "is_valid": is_valid,
                "error_message": error_message
            }

        return balance_sheet

    def format_value(self, value: float) -> str:
        """
        Format a value for display.

        Args:
            value: The value to format

        Returns:
            The formatted value
        """
        if value is None:
            return "—"

        # Format with commas for thousands
        return f"${value:,.0f}"

    def create_normalized_balance_sheet(self, balance_sheet: Dict[str, Dict[str, Any]],
                                       context_mapping: Dict[str, str]) -> List[Dict[str, str]]:
        """
        Create normalized balance sheet data.

        Args:
            balance_sheet: The constructed balance sheet
            context_mapping: Mapping of context codes to compact codes

        Returns:
            Normalized balance sheet data
        """
        normalized_data = []

        for date, data in balance_sheet.items():
            context = data.get("context")
            compact_context = context_mapping.get(context, context)

            # Add total assets
            normalized_data.append({
                "statement_type": "Balance Sheet",
                "concept": "Assets",
                "value": self.format_value(data["assets"]),
                "context": compact_context,
                "context_label": f"As of {date}"
            })

            # Add total liabilities
            normalized_data.append({
                "statement_type": "Balance Sheet",
                "concept": "Liabilities",
                "value": self.format_value(data["liabilities"]),
                "context": compact_context,
                "context_label": f"As of {date}"
            })

            # Add stockholders' equity
            normalized_data.append({
                "statement_type": "Balance Sheet",
                "concept": "Stockholders Equity",
                "value": self.format_value(data["equity"]),
                "context": compact_context,
                "context_label": f"As of {date}"
            })

            # Add minority interests if non-zero
            if data["minority_interests"]:
                normalized_data.append({
                    "statement_type": "Balance Sheet",
                    "concept": "Minority Interest",
                    "value": self.format_value(data["minority_interests"]),
                    "context": compact_context,
                    "context_label": f"As of {date}"
                })

            # Add total liabilities and equity
            normalized_data.append({
                "statement_type": "Balance Sheet",
                "concept": "Liabilities And Stockholders Equity",
                "value": self.format_value(data["total_liabilities_and_equity"]),
                "context": compact_context,
                "context_label": f"As of {date}"
            })

        return normalized_data

    def extract_balance_sheet_data(self, content: str) -> Dict[str, Dict[str, float]]:
        """
        Extract balance sheet data from LLM file content.

        Args:
            content: The content of the LLM file

        Returns:
            Balance sheet data by period
        """
        balance_sheet_data = {}

        # Extract assets
        assets_matches = re.finditer(
            r'Balance Sheet\|Assets\|(\$[0-9,]+)\|(c-\d+)\|As of ([0-9-]+)',
            content
        )

        for match in assets_matches:
            value_str = match.group(1).replace('$', '').replace(',', '')
            context = match.group(2)
            date = match.group(3)

            try:
                value = float(value_str)
                if date not in balance_sheet_data:
                    balance_sheet_data[date] = {"context": context}
                balance_sheet_data[date]["assets"] = value
            except ValueError:
                self.logger.warning(f"Could not parse assets value: {value_str}")

        # Extract liabilities
        liabilities_matches = re.finditer(
            r'Balance Sheet\|Liabilities\|(\$[0-9,]+)\|(c-\d+)\|As of ([0-9-]+)',
            content
        )

        for match in liabilities_matches:
            value_str = match.group(1).replace('$', '').replace(',', '')
            context = match.group(2)
            date = match.group(3)

            try:
                value = float(value_str)
                if date not in balance_sheet_data:
                    balance_sheet_data[date] = {"context": context}
                balance_sheet_data[date]["liabilities"] = value
            except ValueError:
                self.logger.warning(f"Could not parse liabilities value: {value_str}")

        # Extract stockholders' equity
        equity_matches = re.finditer(
            r'Balance Sheet\|Stockholders Equity\|(\$[0-9,]+)\|(c-\d+)\|As of ([0-9-]+)',
            content
        )

        for match in equity_matches:
            value_str = match.group(1).replace('$', '').replace(',', '')
            context = match.group(2)
            date = match.group(3)

            try:
                value = float(value_str)
                if date not in balance_sheet_data:
                    balance_sheet_data[date] = {"context": context}
                balance_sheet_data[date]["equity"] = value
            except ValueError:
                self.logger.warning(f"Could not parse equity value: {value_str}")

        # Extract minority interests
        minority_matches = re.finditer(
            r'Balance Sheet\|Minority Interest\|(\$[0-9,]+)\|(c-\d+)\|As of ([0-9-]+)',
            content
        )

        for match in minority_matches:
            value_str = match.group(1).replace('$', '').replace(',', '')
            context = match.group(2)
            date = match.group(3)

            try:
                value = float(value_str)
                if date not in balance_sheet_data:
                    balance_sheet_data[date] = {"context": context}
                balance_sheet_data[date]["minority_interests"] = value
            except ValueError:
                self.logger.warning(f"Could not parse minority interest value: {value_str}")

        # Extract total liabilities and equity
        total_matches = re.finditer(
            r'Balance Sheet\|Liabilities And Stockholders Equity\|(\$[0-9,]+)\|(c-\d+)\|As of ([0-9-]+)',
            content
        )

        for match in total_matches:
            value_str = match.group(1).replace('$', '').replace(',', '')
            context = match.group(2)
            date = match.group(3)

            try:
                value = float(value_str)
                if date not in balance_sheet_data:
                    balance_sheet_data[date] = {"context": context}
                balance_sheet_data[date]["total_liabilities_and_equity"] = value
            except ValueError:
                self.logger.warning(f"Could not parse total liabilities and equity value: {value_str}")

        # Set default values for missing fields
        for date, data in balance_sheet_data.items():
            data.setdefault("assets", 0)
            data.setdefault("liabilities", 0)
            data.setdefault("equity", 0)
            data.setdefault("minority_interests", 0)
            data.setdefault("total_liabilities_and_equity", 0)

        return balance_sheet_data

    def verify_balance_sheet_integrity(self, llm_file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Verify the integrity of the balance sheet in the LLM file.

        Args:
            llm_file_path: Path to the LLM file

        Returns:
            Verification results
        """
        try:
            with open(llm_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            self.logger.error(f"Error reading LLM file: {str(e)}")
            return {"error": str(e)}

        # Extract balance sheet data
        balance_sheet_data = self.extract_balance_sheet_data(content)

        # Verify for each period
        results = {}
        for period, data in balance_sheet_data.items():
            assets = data.get("assets", 0)
            liabilities = data.get("liabilities", 0)
            equity = data.get("equity", 0)
            minority_interests = data.get("minority_interests", 0)

            is_valid, error_message = self.validate_balance_sheet(
                assets, liabilities, equity, minority_interests
            )

            results[period] = {
                "is_valid": is_valid,
                "error_message": error_message,
                "assets": assets,
                "liabilities": liabilities,
                "equity": equity,
                "minority_interests": minority_interests,
                "total_liabilities_and_equity": data.get("total_liabilities_and_equity", 0),
                "calculated_total": liabilities + equity + minority_interests
            }

        return results
