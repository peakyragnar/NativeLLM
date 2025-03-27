"""
Evidence-based fiscal period detection and standardization system.

This module implements a self-learning system for detecting and normalizing
fiscal periods across companies with different fiscal calendars. It works by:

1. Collecting evidence from multiple signals in SEC filings
2. Building pattern recognition models for each company
3. Automatically adapting to company-specific fiscal calendars
4. Providing consistent period naming across the entire system
"""

import os
import json
import logging
import datetime
import re
from collections import Counter, defaultdict

class FiscalSignal:
    """A piece of evidence about a company's fiscal period"""
    
    def __init__(self, source, period_type, period_value, confidence=0.0):
        """
        Initialize a fiscal signal
        
        Args:
            source (str): Where this signal came from (e.g., "period_end_date", "10-K filing")
            period_type (str): Type of period information ("fiscal_year", "fiscal_quarter", "fiscal_year_end")
            period_value: The detected value (e.g., "2023", "Q1", 6 for June)
            confidence (float): Confidence score from 0.0 to 1.0
        """
        self.source = source
        self.period_type = period_type
        self.period_value = period_value
        self.confidence = confidence
        self.timestamp = datetime.datetime.now()
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            "source": self.source,
            "period_type": self.period_type,
            "period_value": self.period_value,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create from dictionary"""
        signal = cls(
            data["source"],
            data["period_type"],
            data["period_value"],
            data.get("confidence", 0.0)
        )
        if "timestamp" in data:
            try:
                signal.timestamp = datetime.datetime.fromisoformat(data["timestamp"])
            except (ValueError, TypeError):
                signal.timestamp = datetime.datetime.now()
        return signal
    
    def __str__(self):
        return f"FiscalSignal(source={self.source}, type={self.period_type}, value={self.period_value}, confidence={self.confidence:.2f})"


class FiscalPattern:
    """A detected pattern in a company's fiscal calendar"""
    
    def __init__(self, pattern_type, value, confidence=0.0, evidence=None):
        """
        Initialize a fiscal pattern
        
        Args:
            pattern_type (str): Type of pattern ("fiscal_year_end_month", "quarter_mapping")
            value: The pattern value
            confidence (float): Confidence score from 0.0 to 1.0
            evidence (list): List of signals supporting this pattern
        """
        self.pattern_type = pattern_type
        self.value = value
        self.confidence = confidence
        self.evidence = evidence or []
        self.last_updated = datetime.datetime.now()
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            "pattern_type": self.pattern_type,
            "value": self.value,
            "confidence": self.confidence,
            "evidence": [signal.to_dict() for signal in self.evidence],
            "last_updated": self.last_updated.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create from dictionary"""
        pattern = cls(
            data["pattern_type"],
            data["value"],
            data.get("confidence", 0.0),
            [FiscalSignal.from_dict(signal) for signal in data.get("evidence", [])]
        )
        if "last_updated" in data:
            try:
                pattern.last_updated = datetime.datetime.fromisoformat(data["last_updated"])
            except (ValueError, TypeError):
                pattern.last_updated = datetime.datetime.now()
        return pattern
    
    def update_with_signal(self, signal):
        """Update pattern with a new signal"""
        self.evidence.append(signal)
        self.last_updated = datetime.datetime.now()
        
        # Directly update the value if the signal has higher confidence
        if signal.confidence > self.confidence:
            old_value = self.value
            self.value = signal.period_value
            self.confidence = signal.confidence
            if old_value != self.value:
                logging.info(f"Updated pattern value to {self.value} based on higher confidence signal")
            return
        
        # Recalculate confidence based on evidence
        if len(self.evidence) > 0:
            # Weight more recent evidence higher
            total_confidence = sum(e.confidence for e in self.evidence)
            avg_confidence = total_confidence / len(self.evidence)
            self.confidence = max(self.confidence, avg_confidence)
            
            # If we have enough evidence with consistent values, boost confidence
            value_counter = Counter(e.period_value for e in self.evidence)
            most_common_value, count = value_counter.most_common(1)[0]
            if count > len(self.evidence) * 0.7 and len(self.evidence) >= 2:
                # Strong consensus across multiple signals
                self.confidence = min(self.confidence * 1.2, 1.0)
                
                # Update value if there's a strong consensus different from current value
                if most_common_value != self.value:
                    self.value = most_common_value
                    logging.info(f"Updated pattern value to {self.value} based on strong consensus")
    
    def __str__(self):
        return f"FiscalPattern({self.pattern_type}={self.value}, confidence={self.confidence:.2f}, signals={len(self.evidence)})"


class CompanyFiscalModel:
    """
    A self-learning model of a company's fiscal calendar patterns based on evidence 
    from SEC filings. Used to correctly map period end dates to fiscal quarters and years.
    """
    
    # Standard quarters mapping for reference
    CALENDAR_YEAR_QUARTERS = {
        # Calendar fiscal year
        # Q1: Jan-Mar
        # Q2: Apr-Jun  
        # Q3: Jul-Sep
        # Q4: Oct-Dec
        1: "Q1", 2: "Q1", 3: "Q1",
        4: "Q2", 5: "Q2", 6: "Q2",
        7: "Q3", 8: "Q3", 9: "Q3",
        10: "Q4", 11: "Q4", 12: "Q4",
    }
    
    # Known fiscal patterns for popular companies
    # IMPORTANT: The fiscal period is based on when a quarter ENDS, not when it begins
    KNOWN_FISCAL_PATTERNS = {
        # Apple: Fiscal year ends in September
        # Q1: Oct-Dec (ends Dec)
        # Q2: Jan-Mar (ends Mar)
        # Q3: Apr-Jun (ends Jun)
        # Annual: Jul-Sep (ends Sep)
        "AAPL": {
            "fiscal_year_end_month": 9,
            "fiscal_year_end_day": 30,
            "quarter_mapping": {
                # Month: Quarter when a filing with period_end_date in this month is made
                10: "Q1", 11: "Q1", 12: "Q1",  # Oct-Dec = Q1
                1: "Q2", 2: "Q2", 3: "Q2",     # Jan-Mar = Q2
                4: "Q3", 5: "Q3", 6: "Q3",     # Apr-Jun = Q3
                7: "annual", 8: "annual", 9: "annual",  # Jul-Sep = Annual (10-K)
            }
        },
        # Microsoft: Fiscal year ends in June
        # Q1: Jul-Sep (ends Sep)
        # Q2: Oct-Dec (ends Dec)
        # Q3: Jan-Mar (ends Mar)
        # Annual: Apr-Jun (ends Jun)
        "MSFT": {
            "fiscal_year_end_month": 6,
            "fiscal_year_end_day": 30,
            "quarter_mapping": {
                7: "Q1", 8: "Q1", 9: "Q1",     # Jul-Sep = Q1
                10: "Q2", 11: "Q2", 12: "Q2",  # Oct-Dec = Q2
                1: "Q3", 2: "Q3", 3: "Q3",     # Jan-Mar = Q3
                4: "annual", 5: "annual", 6: "annual",  # Apr-Jun = Annual (never Q4)
            }
        },
        # Google: Calendar year (Dec)
        "GOOGL": {
            "fiscal_year_end_month": 12,
            "fiscal_year_end_day": 31,
            "quarter_mapping": {
                1: "Q1", 2: "Q1", 3: "Q1",     # Jan-Mar = Q1
                4: "Q2", 5: "Q2", 6: "Q2",     # Apr-Jun = Q2
                7: "Q3", 8: "Q3", 9: "Q3",     # Jul-Sep = Q3
                10: "Q4", 11: "Q4", 12: "annual",  # Oct-Dec = Q4/Annual
            }
        },
    }
    
    def __init__(self, ticker):
        """
        Initialize a company fiscal model
        
        Args:
            ticker (str): The company ticker symbol
        """
        self.ticker = ticker
        self.patterns = {}
        self.signals = []
        self.confidence_score = 0.0
        self.last_updated = datetime.datetime.now()
        
        # Initialize with known patterns if available
        if ticker in self.KNOWN_FISCAL_PATTERNS:
            known = self.KNOWN_FISCAL_PATTERNS[ticker]
            
            # Create fiscal year end pattern
            self._set_initial_pattern(
                "fiscal_year_end_month", 
                known["fiscal_year_end_month"],
                1.0  # High confidence for known patterns
            )
            
            # Create fiscal year end day pattern
            self._set_initial_pattern(
                "fiscal_year_end_day", 
                known["fiscal_year_end_day"],
                1.0
            )
            
            # Create quarter mapping pattern
            self._set_initial_pattern(
                "quarter_mapping", 
                known["quarter_mapping"],
                1.0
            )
            
            self.confidence_score = 1.0
            logging.info(f"Using known fiscal pattern for {ticker}: FY end {known['fiscal_year_end_month']}-{known['fiscal_year_end_day']}")
        else:
            # Default to calendar year as starting point
            self._set_initial_pattern(
                "fiscal_year_end_month", 
                12,  # December
                0.5  # Medium confidence for default
            )
            
            self._set_initial_pattern(
                "fiscal_year_end_day", 
                31,  # 31st
                0.5
            )
            
            # Default to standard calendar quarter mapping
            quarter_mapping = {}
            for month, quarter in self.CALENDAR_YEAR_QUARTERS.items():
                quarter_mapping[month] = quarter
            
            # December 31st should be "annual" for 10-K filings
            quarter_mapping[12] = "Q4"  # Will be overridden to "annual" for 10-K
            
            self._set_initial_pattern(
                "quarter_mapping", 
                quarter_mapping,
                0.5
            )
            
            self.confidence_score = 0.5
            logging.info(f"Using default calendar year pattern for {ticker}, will learn from filings")
    
    def _set_initial_pattern(self, pattern_type, value, confidence):
        """Set an initial pattern with an initial signal"""
        initial_signal = FiscalSignal(
            source="initial_configuration",
            period_type=pattern_type,
            period_value=value,
            confidence=confidence
        )
        
        pattern = FiscalPattern(
            pattern_type=pattern_type,
            value=value,
            confidence=confidence,
            evidence=[initial_signal]
        )
        
        self.patterns[pattern_type] = pattern
        self.signals.append(initial_signal)
    
    def to_dict(self):
        """Convert to dictionary for storage"""
        return {
            "ticker": self.ticker,
            "patterns": {k: p.to_dict() for k, p in self.patterns.items()},
            "signals": [s.to_dict() for s in self.signals],
            "confidence_score": self.confidence_score,
            "last_updated": self.last_updated.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create from dictionary"""
        # Initialize with plain ticker only, don't use known patterns
        model = cls.__new__(cls)  # Create instance without calling __init__
        model.ticker = data["ticker"]
        model.patterns = {}
        model.signals = []
        model.confidence_score = 0.0
        model.last_updated = datetime.datetime.now()
        
        # Load patterns
        model.patterns = {
            k: FiscalPattern.from_dict(p) 
            for k, p in data.get("patterns", {}).items()
        }
        
        # Load signals
        model.signals = [
            FiscalSignal.from_dict(s) 
            for s in data.get("signals", [])
        ]
        
        # Load metadata
        model.confidence_score = data.get("confidence_score", 0.0)
        
        if "last_updated" in data:
            try:
                model.last_updated = datetime.datetime.fromisoformat(data["last_updated"])
            except (ValueError, TypeError):
                model.last_updated = datetime.datetime.now()
        
        return model
    
    def get_fiscal_year_end_month(self):
        """Get the fiscal year end month"""
        pattern = self.patterns.get("fiscal_year_end_month")
        return pattern.value if pattern else 12  # Default to December
    
    def get_fiscal_year_end_day(self):
        """Get the fiscal year end day"""
        pattern = self.patterns.get("fiscal_year_end_day")
        return pattern.value if pattern else 31  # Default to 31st
    
    def get_quarter_mapping(self):
        """Get the quarter mapping"""
        pattern = self.patterns.get("quarter_mapping")
        if pattern:
            return pattern.value
        
        # Fallback to standard calendar quarters
        quarter_mapping = {}
        for month, quarter in self.CALENDAR_YEAR_QUARTERS.items():
            quarter_mapping[month] = quarter
        return quarter_mapping
    
    def add_signal(self, signal):
        """Add a new signal and update patterns"""
        self.signals.append(signal)
        
        # Update relevant pattern
        if signal.period_type in self.patterns:
            pattern = self.patterns[signal.period_type]
            pattern.update_with_signal(signal)
        else:
            # Create new pattern
            pattern = FiscalPattern(
                pattern_type=signal.period_type,
                value=signal.period_value,
                confidence=signal.confidence,
                evidence=[signal]
            )
            self.patterns[signal.period_type] = pattern
        
        # Update pattern value with the signal value if it has higher confidence
        if pattern.value != signal.period_value and signal.confidence > pattern.confidence:
            pattern.value = signal.period_value
            pattern.confidence = max(pattern.confidence, signal.confidence)
            logging.info(f"Updated {signal.period_type} to {signal.period_value} based on higher confidence signal")
        
        # Update overall confidence score
        pattern_confidences = [p.confidence for p in self.patterns.values()]
        if pattern_confidences:
            self.confidence_score = sum(pattern_confidences) / len(pattern_confidences)
        
        self.last_updated = datetime.datetime.now()
        return pattern
    
    def extract_signals_from_filing(self, filing_metadata):
        """
        Extract fiscal signals from a filing
        
        Args:
            filing_metadata (dict): Filing metadata including period_end_date, filing_type, etc.
            
        Returns:
            list: List of extracted signals
        """
        signals = []
        
        # Extract from period_end_date
        period_end_date = filing_metadata.get("period_end_date")
        filing_type = filing_metadata.get("filing_type")
        
        if period_end_date:
            try:
                end_date = datetime.datetime.strptime(period_end_date, '%Y-%m-%d')
                
                # If it's a 10-K, it likely indicates fiscal year end
                if filing_type == "10-K":
                    # Signal for fiscal year end month
                    signals.append(FiscalSignal(
                        source="10-K_period_end_date",
                        period_type="fiscal_year_end_month",
                        period_value=end_date.month,
                        confidence=0.8  # High confidence from 10-K filings
                    ))
                    
                    # Signal for fiscal year end day
                    signals.append(FiscalSignal(
                        source="10-K_period_end_date",
                        period_type="fiscal_year_end_day",
                        period_value=end_date.day,
                        confidence=0.8
                    ))
                
                # For quarterly filings (10-Q), they tell us which quarter corresponds to which months
                if filing_type == "10-Q":
                    # Analyze the filing text if available
                    html_url = filing_metadata.get("html_url") or filing_metadata.get("document_url")
                    if html_url and "html_content" in filing_metadata:
                        content = filing_metadata["html_content"]
                        quarter_match = re.search(r'(?:for the|for our|quarterly|quarter ended) ([a-zA-Z]+)\s+(\d{1,2})(?:st|nd|rd|th)?(?:,)?\s+(\d{4})', content, re.IGNORECASE)
                        if quarter_match:
                            # Extract month name, day, year
                            month_name, day, year = quarter_match.groups()
                            
                            # Convert month name to number
                            try:
                                month_number = datetime.datetime.strptime(month_name, '%B').month
                            except ValueError:
                                try:
                                    month_number = datetime.datetime.strptime(month_name[:3], '%b').month
                                except ValueError:
                                    month_number = end_date.month
                            
                            # Signal for quarter mapping
                            quarter_mapping = self.get_quarter_mapping().copy()
                            
                            # Determine quarter from filing
                            quarter_text_match = re.search(r'(?:quarterly report|QUARTERLY REPORT|Form 10-Q).*?(?:quarterly|quarter|three months).*?(?:first|second|third|fourth|1st|2nd|3rd|4th|\sQ1|\sQ2|\sQ3|\sQ4)', content, re.IGNORECASE)
                            if quarter_text_match:
                                quarter_text = quarter_text_match.group(0).lower()
                                if "first" in quarter_text or "1st" in quarter_text or "q1" in quarter_text:
                                    quarter = "Q1"
                                elif "second" in quarter_text or "2nd" in quarter_text or "q2" in quarter_text:
                                    quarter = "Q2"
                                elif "third" in quarter_text or "3rd" in quarter_text or "q3" in quarter_text:
                                    quarter = "Q3" 
                                elif "fourth" in quarter_text or "4th" in quarter_text or "q4" in quarter_text:
                                    quarter = "Q4"
                                else:
                                    # Default to calendar quarters
                                    quarter = self.CALENDAR_YEAR_QUARTERS.get(month_number, "Q")
                                
                                # Update quarter mapping for this month
                                quarter_mapping[month_number] = quarter
                                
                                signals.append(FiscalSignal(
                                    source="10-Q_quarter_mention",
                                    period_type="quarter_mapping",
                                    period_value=quarter_mapping,
                                    confidence=0.7
                                ))
            except (ValueError, TypeError) as e:
                logging.warning(f"Error extracting signals from period_end_date: {str(e)}")
        
        # Extract signals from filing date patterns
        filing_date = filing_metadata.get("filing_date")
        if filing_date and period_end_date:
            try:
                filing_dt = datetime.datetime.strptime(filing_date, '%Y-%m-%d')
                period_dt = datetime.datetime.strptime(period_end_date, '%Y-%m-%d')
                
                # Most companies file within 30-60 days after the period end
                days_diff = (filing_dt - period_dt).days
                
                if 20 <= days_diff <= 90:
                    # This is a typical filing pattern
                    if filing_type == "10-K":
                        # For 10-K, the period end is likely fiscal year end
                        signals.append(FiscalSignal(
                            source="10-K_filing_date_pattern",
                            period_type="fiscal_year_end_month",
                            period_value=period_dt.month,
                            confidence=0.6
                        ))
            except (ValueError, TypeError):
                pass
        
        return signals
    
    def update_from_filing(self, filing_metadata):
        """
        Update model from filing metadata
        
        Args:
            filing_metadata (dict): Filing metadata
            
        Returns:
            dict: Fiscal information including fiscal_year and fiscal_period
        """
        # Extract signals from filing
        new_signals = self.extract_signals_from_filing(filing_metadata)
        
        # Add all signals and update patterns
        for signal in new_signals:
            self.add_signal(signal)
        
        # Calculate fiscal information for this filing
        fiscal_info = self.determine_fiscal_period(
            filing_metadata.get("period_end_date"),
            filing_metadata.get("filing_type", "10-Q")
        )
        
        return fiscal_info
    
    def determine_fiscal_period(self, period_end_date_str, filing_type="10-Q"):
        """
        Determine the fiscal quarter and year for a given period end date
        
        Args:
            period_end_date_str: Period end date as string (YYYY-MM-DD)
            filing_type: Type of filing (10-Q or 10-K)
            
        Returns:
            Dict with fiscal_year and fiscal_period
        """
        # Standard format is "Q1", "Q2", "Q3", "annual"
        
        if not period_end_date_str:
            return {"fiscal_year": None, "fiscal_period": None}
        
        try:
            period_end = datetime.datetime.strptime(period_end_date_str, '%Y-%m-%d')
        except (ValueError, TypeError):
            logging.warning(f"Invalid period end date: {period_end_date_str}")
            return {"fiscal_year": None, "fiscal_period": None}
        
        # Get the company's fiscal patterns
        fiscal_year_end_month = self.get_fiscal_year_end_month()
        fiscal_year_end_day = self.get_fiscal_year_end_day()
        quarter_mapping = self.get_quarter_mapping()
        
        # Determine which fiscal year this date belongs to
        is_after_fiscal_year_end = (
            period_end.month > fiscal_year_end_month or 
            (period_end.month == fiscal_year_end_month and period_end.day >= fiscal_year_end_day)
        )
        
        if is_after_fiscal_year_end:
            # Date is in the first part of the next fiscal year
            fiscal_year = period_end.year + 1
        else:
            # Date is in the latter part of the current fiscal year
            fiscal_year = period_end.year
        
        # For special cases like Microsoft's 10-K in June, adjust fiscal year
        if filing_type == "10-K" and period_end.month == fiscal_year_end_month and period_end.day == fiscal_year_end_day:
            fiscal_year = period_end.year
        
        # Special case for Microsoft: Apr-Jun is always annual for both 10-K and 10-Q
        if self.ticker == "MSFT" and period_end.month in [4, 5, 6]:
            fiscal_period = "annual"
        # Get fiscal period from quarter mapping or calculate based on months from fiscal year end
        elif filing_type == "10-K":
            fiscal_period = "annual"
        else:
            # Use quarter mapping if available for this month
            if period_end.month in quarter_mapping:
                fiscal_period = quarter_mapping.get(period_end.month, "Q")
            else:
                # Calculate which quarter based on months from fiscal year end
                months_from_fy_end = (period_end.month - fiscal_year_end_month) % 12
                
                if 1 <= months_from_fy_end <= 3:
                    fiscal_period = "Q1"
                elif 4 <= months_from_fy_end <= 6:
                    fiscal_period = "Q2"
                elif 7 <= months_from_fy_end <= 9:
                    fiscal_period = "Q3"
                else:
                    fiscal_period = "Q4"
        
        return {
            "fiscal_year": str(fiscal_year),
            "fiscal_period": fiscal_period
        }
    
    def get_standardized_period(self, period):
        """
        Standardize a fiscal period to the consistent format
        
        Args:
            period (str): The fiscal period in any format (1Q, Q1, QTR1, etc.)
            
        Returns:
            str: Standardized period format (Q1, Q2, Q3, annual)
        """
        if not period:
            return None
            
        period = str(period).strip().upper()
        
        # Handle annual/FY format
        if period in ["FY", "ANNUAL", "10-K", "YEAR", "YR", "FINAL", "A"]:
            return "annual"
        
        # Handle various quarter formats
        if period in ["1", "1Q", "Q1", "QTR1", "FIRST", "1ST"]:
            return "Q1"
        elif period in ["2", "2Q", "Q2", "QTR2", "SECOND", "2ND"]:
            return "Q2"
        elif period in ["3", "3Q", "Q3", "QTR3", "THIRD", "3RD"]:
            return "Q3"
        elif period in ["4", "4Q", "Q4", "QTR4", "FOURTH", "4TH"]:
            return "Q4"
        
        # Handle formats like "Q1-2023"
        quarter_match = re.match(r'[QTR]*\s*(\d)[QTR\s-]*', period)
        if quarter_match:
            quarter_num = int(quarter_match.group(1))
            if 1 <= quarter_num <= 4:
                return f"Q{quarter_num}"
        
        # Default to returning the input if we can't standardize it
        return period


class FiscalPeriodManager:
    """
    Manager to store, retrieve, and update company fiscal models.
    Provides a standardized interface for all fiscal period operations.
    """
    
    def __init__(self, storage_path="data/fiscal_models.json"):
        """
        Initialize the manager
        
        Args:
            storage_path (str): Path to store the fiscal models
        """
        self.storage_path = storage_path
        self.models = {}
        self.last_updated = datetime.datetime.now()
        self._load_registry()
    
    def _load_registry(self):
        """Load models from storage"""
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'r') as f:
                    data = json.load(f)
                    
                for ticker, model_data in data.items():
                    self.models[ticker] = CompanyFiscalModel.from_dict(model_data)
                    
                logging.info(f"Loaded fiscal models for {len(self.models)} companies")
            except Exception as e:
                logging.error(f"Error loading fiscal models: {str(e)}")
                self.models = {}
        else:
            os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
            self.models = {}
    
    def save_registry(self):
        """Save models to storage"""
        try:
            data = {ticker: model.to_dict() for ticker, model in self.models.items()}
            
            with open(self.storage_path, 'w') as f:
                json.dump(data, f, indent=2)
                
            self.last_updated = datetime.datetime.now()
            logging.info(f"Saved fiscal models for {len(self.models)} companies")
            return True
        except Exception as e:
            logging.error(f"Error saving fiscal models: {str(e)}")
            return False
    
    def get_model(self, ticker):
        """Get a company's fiscal model"""
        ticker = ticker.upper()
        if ticker not in self.models:
            self.models[ticker] = CompanyFiscalModel(ticker)
        return self.models[ticker]
    
    def update_model(self, ticker, filing_metadata):
        """
        Update a company's fiscal model with new filing data
        
        Args:
            ticker (str): Company ticker symbol
            filing_metadata (dict): Filing metadata
            
        Returns:
            dict: Updated fiscal information
        """
        ticker = ticker.upper()
        model = self.get_model(ticker)
        fiscal_info = model.update_from_filing(filing_metadata)
        self.save_registry()
        return fiscal_info
    
    def determine_fiscal_period(self, ticker, period_end_date, filing_type="10-Q"):
        """
        Determine fiscal period for a company and date
        
        Args:
            ticker (str): Company ticker symbol
            period_end_date (str): Filing period end date (YYYY-MM-DD)
            filing_type (str): Filing type (10-Q or 10-K)
            
        Returns:
            dict: Fiscal information including fiscal_year and fiscal_period
        """
        ticker = ticker.upper()
        model = self.get_model(ticker)
        return model.determine_fiscal_period(period_end_date, filing_type)
    
    def standardize_period(self, period, output_format="internal"):
        """
        Standardize a fiscal period to a consistent format
        
        Args:
            period (str): The fiscal period in any format (1Q, Q1, QTR1, etc.)
            output_format (str): Format to use: 
                - "internal": standard Q1,Q2,Q3,annual format used internally
                - "display": standard Q1,Q2,Q3,Q4 format for user display
                - "folder": format for folder naming (Q1,Q2,Q3,annual)
                - "old": old 1Q,2Q,3Q,4Q format
                
        Returns:
            str: Standardized period format
        """
        if not period:
            return None
        
        # First convert to our standard internal format
        internal_format = None
        
        # Handle standard quarter formats
        if period in ["Q1", "1Q", "QTR1", "FIRST"]:
            internal_format = "Q1"
        elif period in ["Q2", "2Q", "QTR2", "SECOND"]:
            internal_format = "Q2"
        elif period in ["Q3", "3Q", "QTR3", "THIRD"]:
            internal_format = "Q3"
        elif period in ["Q4", "4Q", "QTR4", "FOURTH"]:
            internal_format = "Q4"
        # Handle annual format
        elif period in ["ANNUAL", "A", "FY", "F", "YEAR", "Y"]:
            internal_format = "annual"
        else:
            # For anything else, use a simple model to standardize
            # Looking for a digit followed by Q or a Q followed by a digit
            match_1q = re.search(r'(?:^|\s)(\d)[qQ]', period)
            match_q1 = re.search(r'[qQ](\d)(?:\s|$)', period)
            
            if match_1q:
                qtr = match_1q.group(1)
                if 1 <= int(qtr) <= 4:
                    internal_format = f"Q{qtr}"
            elif match_q1:
                qtr = match_q1.group(1)
                if 1 <= int(qtr) <= 4:
                    internal_format = f"Q{qtr}"
            elif period.upper() in ["FY", "ANNUAL", "10-K", "K"]:
                internal_format = "annual"
            else:
                # Default to the input if we can't recognize it
                internal_format = period
        
        # Check if Q4 should be converted to annual based on context
        # This would require additional information we don't have here
        
        # Convert to requested output format
        if output_format == "internal":
            return internal_format
        elif output_format == "display":
            if internal_format == "annual":
                return "Q4"  # Convert annual to Q4 for display
            return internal_format
        elif output_format == "folder":
            return internal_format  # Same as internal for folders
        elif output_format == "old":
            # Convert to old format (1Q, 2Q, 3Q, 4Q)
            if internal_format == "Q1":
                return "1Q"
            elif internal_format == "Q2":
                return "2Q"
            elif internal_format == "Q3":
                return "3Q"
            elif internal_format == "Q4" or internal_format == "annual":
                return "4Q"
            else:
                return internal_format
        
        # Default to internal format
        return internal_format
    
    def get_quarter_from_month(self, ticker, month, filing_type="10-Q"):
        """
        Get fiscal quarter for a given month for a company
        
        Args:
            ticker (str): Company ticker
            month (int): Month (1-12)
            filing_type (str): Filing type
            
        Returns:
            str: Fiscal period (Q1, Q2, Q3, annual)
        """
        ticker = ticker.upper()
        model = self.get_model(ticker)
        quarter_mapping = model.get_quarter_mapping()
        
        if filing_type == "10-K":
            # Special case for Microsoft: Apr-Jun is always annual for both 10-K and 10-Q
            if ticker == "MSFT" and month in [4, 5, 6]:
                return "annual"
            return "annual"
        
        return quarter_mapping.get(month, "Q")
    
    def convert_period_format(self, period, source_format, target_format):
        """
        Convert between different fiscal period formats
        
        Args:
            period (str): The fiscal period to convert
            source_format (str): The current format ("Q1", "1Q", etc.)
            target_format (str): The desired format ("Q1", "1Q", etc.)
            
        Returns:
            str: The converted fiscal period
        """
        # First standardize to internal format
        internal = self.standardize_period(period, "internal")
        
        # Then convert to target format
        if target_format == "Q1":  # Q1, Q2, Q3, annual (internal)
            return internal
        elif target_format == "1Q":  # 1Q, 2Q, 3Q, 4Q (old)
            return self.standardize_period(internal, "old")
        elif target_format == "display":  # Q1, Q2, Q3, Q4 (display)
            return self.standardize_period(internal, "display")
        elif target_format == "folder":  # Q1, Q2, Q3, annual (folder)
            return self.standardize_period(internal, "folder")
        
        # Default to internal format
        return internal


# Initialize global manager
fiscal_manager = FiscalPeriodManager()