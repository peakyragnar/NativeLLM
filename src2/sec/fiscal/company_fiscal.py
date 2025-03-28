# src2/sec/fiscal/company_fiscal.py
import os
import json
import datetime
import logging
from collections import Counter

class CompanyFiscalCalendar:
    """
    Maintains and learns a company's fiscal calendar patterns based on SEC filings.
    Used to correctly map period end dates to fiscal quarters and years.
    """
    
    # Known fiscal patterns for popular companies
    # IMPORTANT: The fiscal period is based on when a quarter ENDS, not when it begins
    # For example, a filing with period_end_date of April 1 is reporting on Q2 (Jan-Mar)
    KNOWN_FISCAL_PATTERNS = {
        # Apple: Fiscal year ends in September
        # Q1: Oct-Dec (ends Dec)
        # Q2: Jan-Mar (ends Mar)
        # Q3: Apr-Jun (ends Jun)
        # Q4: Jul-Sep (ends Sep)
        "AAPL": {"month": 9, "day": 30, "confidence": 1.0},
        # Microsoft: Fiscal year ends in June
        "MSFT": {"month": 6, "day": 30, "confidence": 1.0},
        # NVIDIA: Fiscal year ends in January
        # Q1: Feb-Apr (ends Apr)
        # Q2: May-Jul (ends Jul)
        # Q3: Aug-Oct (ends Oct)
        # Annual: Nov-Jan (ends Jan)
        "NVDA": {"month": 1, "day": 26, "confidence": 1.0},
        # Google: Calendar year (Dec)
        "GOOGL": {"month": 12, "day": 31, "confidence": 1.0},
        # Amazon: Calendar year (Dec)
        "AMZN": {"month": 12, "day": 31, "confidence": 1.0}
    }
    
    def __init__(self, ticker):
        self.ticker = ticker
        
        # Use known pattern if available
        if ticker in self.KNOWN_FISCAL_PATTERNS:
            known = self.KNOWN_FISCAL_PATTERNS[ticker]
            self.fiscal_year_end_month = known["month"]
            self.fiscal_year_end_day = known["day"]
            self.confidence_score = known["confidence"]
            logging.info(f"Using known fiscal pattern for {ticker}: FY end {self.fiscal_year_end_month}-{self.fiscal_year_end_day}")
        else:
            self.fiscal_year_end_month = None
            self.fiscal_year_end_day = None
            self.confidence_score = 0
            
        self.historical_filings = {}
        self.last_updated = datetime.datetime.now().isoformat()
    
    def analyze_10k_filings(self, filings):
        """
        Analyze 10-K filings to determine the company's fiscal year end pattern
        
        Args:
            filings: List of filing metadata containing period_end_date for 10-K filings
        """
        if not filings:
            return
            
        # Extract all period end dates
        end_dates = []
        for filing in filings:
            period_end = filing.get('period_end_date')
            if period_end:
                try:
                    end_date = datetime.datetime.strptime(period_end, '%Y-%m-%d')
                    end_dates.append(end_date)
                    self.historical_filings[period_end] = filing.get('filing_type')
                except (ValueError, TypeError):
                    logging.warning(f"Invalid period end date format: {period_end}")
        
        if not end_dates:
            return
            
        # Count frequency of end months to determine fiscal year end
        month_counter = Counter([date.month for date in end_dates])
        most_common_month = month_counter.most_common(1)[0][0]
        
        # Count frequency of end days
        day_counter = Counter([date.day for date in end_dates])
        most_common_day = day_counter.most_common(1)[0][0]
        
        self.fiscal_year_end_month = most_common_month
        self.fiscal_year_end_day = most_common_day
        self.confidence_score = month_counter[most_common_month] / len(end_dates)
        self.last_updated = datetime.datetime.now().isoformat()
        
        logging.info(f"Fiscal calendar for {self.ticker}: Year ends on month {self.fiscal_year_end_month}, day {self.fiscal_year_end_day}")
    
    def determine_fiscal_period(self, period_end_date_str, filing_type="10-Q"):
        """
        Determine the fiscal quarter and year for a given period end date
        
        Args:
            period_end_date_str: Period end date as string (YYYY-MM-DD)
            filing_type: Type of filing (10-Q or 10-K)
            
        Returns:
            Dict with fiscal_year and fiscal_period
        """
        if not self.fiscal_year_end_month:
            # If we don't know the fiscal year end, use calendar quarters as fallback
            return self._fallback_fiscal_period(period_end_date_str)
        
        try:
            period_end = datetime.datetime.strptime(period_end_date_str, '%Y-%m-%d')
        except (ValueError, TypeError):
            logging.warning(f"Invalid period end date: {period_end_date_str}")
            return {"fiscal_year": None, "fiscal_period": None}
        
        # Special handling for Apple
        if self.ticker == "AAPL":
            # Apple's fiscal year ends in September
            # Q1: Oct-Dec, Q2: Jan-Mar, Q3: Apr-Jun, Q4: Jul-Sep
            month = period_end.month
            
            if month in [10, 11, 12]:  # Oct-Dec = Q1 of next calendar year
                fiscal_year = str(period_end.year + 1)
                fiscal_period = "Q1" if filing_type != "10-K" else "annual"
            elif month in [1, 2, 3]:  # Jan-Mar = Q2
                fiscal_year = str(period_end.year)
                fiscal_period = "Q2" if filing_type != "10-K" else "annual"
            elif month in [4, 5, 6]:  # Apr-Jun = Q3
                fiscal_year = str(period_end.year)
                fiscal_period = "Q3" if filing_type != "10-K" else "annual"
            else:  # Jul-Sep
                fiscal_year = str(period_end.year)
                if filing_type == "10-K":
                    fiscal_period = "annual"
                else:
                    # Apple has no Q4 10-Q filings, as they're covered by 10-K
                    # If we find a 10-Q in this period, it must be Q3
                    fiscal_period = "Q3"
            
            return {
                "fiscal_year": fiscal_year,
                "fiscal_period": fiscal_period
            }
        
        # Special handling for Microsoft
        if self.ticker == "MSFT":
            # Microsoft's fiscal year ends in June
            # Fiscal year 20XX runs from July 1, 20XX-1 to June 30, 20XX
            # Q1: Jul-Sep, Q2: Oct-Dec, Q3: Jan-Mar, annual (never Q4): Apr-Jun
            month = period_end.month
            
            # For Microsoft, determine the fiscal year based on the month
            # If the date is between Jul-Dec, it's in the next fiscal year
            # If the date is between Jan-Jun, it's in the current fiscal year
            if month >= 7:  # Jul-Dec
                base_fiscal_year = period_end.year + 1
            else:  # Jan-Jun
                base_fiscal_year = period_end.year
                
            # Determine quarter
            if month in [7, 8, 9]:  # Jul-Sep = Q1
                fiscal_period = "Q1" if filing_type != "10-K" else "annual"
            elif month in [10, 11, 12]:  # Oct-Dec = Q2
                fiscal_period = "Q2" if filing_type != "10-K" else "annual"
            elif month in [1, 2, 3]:  # Jan-Mar = Q3
                fiscal_period = "Q3" if filing_type != "10-K" else "annual"
            else:  # Apr-Jun = ALWAYS annual for both 10-K and 10-Q
                fiscal_period = "annual"
                
            # For annual reports (10-K), if it's at fiscal year end (June 30),
            # it should represent the fiscal year that's ending
            if filing_type == "10-K" and month == 6 and period_end.day == 30:
                fiscal_year = str(period_end.year)
            else:
                fiscal_year = str(base_fiscal_year)
            
            return {
                "fiscal_year": fiscal_year,
                "fiscal_period": fiscal_period
            }
            
        # Special handling for NVIDIA
        if self.ticker == "NVDA":
            # NVIDIA's fiscal year ends in January
            # Q1: Feb-Apr (ends Apr)
            # Q2: May-Jul (ends Jul)
            # Q3: Aug-Oct (ends Oct)
            # Annual: Nov-Jan (ends Jan)
            month = period_end.month
            
            # Determine fiscal year based on month
            if month == 1:  # January (fiscal year end month)
                if period_end.day >= self.fiscal_year_end_day:
                    # After fiscal year end day in January
                    fiscal_year = str(period_end.year)
                else:
                    # Before fiscal year end day in January
                    fiscal_year = str(period_end.year)
            elif 2 <= month <= 12:  # Feb-Dec
                # For NVIDIA, Feb-Dec dates are part of the NEXT fiscal year
                fiscal_year = str(period_end.year + 1)
            
            # Determine fiscal period based on month
            if filing_type == "10-K":
                fiscal_period = "annual"
            elif month in [2, 3, 4]:  # Feb-Apr = Q1
                fiscal_period = "Q1"
            elif month in [5, 6, 7]:  # May-Jul = Q2
                fiscal_period = "Q2"
            elif month in [8, 9, 10]:  # Aug-Oct = Q3
                fiscal_period = "Q3"
            else:  # Nov-Jan = Annual period, covered by 10-K
                fiscal_period = "annual"
            
            return {
                "fiscal_year": fiscal_year,
                "fiscal_period": fiscal_period
            }
            
        # Standard handling for other companies
        # Calculate months from fiscal year end
        month_diff = (period_end.month - self.fiscal_year_end_month) % 12
        
        # Determine fiscal year
        # If the date is after fiscal year end month, it's in the next fiscal year
        if month_diff == 0:
            # Same month as fiscal year end
            if period_end.day >= self.fiscal_year_end_day:
                fiscal_year = period_end.year
                fiscal_period = "Q4" if filing_type != "10-K" else "annual"
            else:
                fiscal_year = period_end.year
                fiscal_period = self._get_quarter_by_month_diff((month_diff - 1) % 12)
                if filing_type == "10-K":
                    fiscal_period = "annual"
        elif month_diff > 0:
            # Month is after fiscal year end month in the same calendar year
            fiscal_year = period_end.year
            fiscal_period = self._get_quarter_by_month_diff(month_diff)
            if filing_type == "10-K":
                fiscal_period = "annual"
        else:
            # Month is before fiscal year end month in the next calendar year
            fiscal_year = period_end.year - 1
            fiscal_period = self._get_quarter_by_month_diff(month_diff)
            if filing_type == "10-K":
                fiscal_period = "annual"
        
        return {
            "fiscal_year": str(fiscal_year),
            "fiscal_period": fiscal_period
        }
    
    def _get_quarter_by_month_diff(self, month_diff):
        """Map months from fiscal year end to quarters"""
        if 0 <= month_diff < 3:
            return "Q1"
        elif 3 <= month_diff < 6:
            return "Q2"
        elif 6 <= month_diff < 9:
            return "Q3"
        else:
            return "Q4"
    
    def _fallback_fiscal_period(self, period_end_date_str):
        """Fallback to calendar quarters if fiscal year end is unknown"""
        try:
            period_end = datetime.datetime.strptime(period_end_date_str, '%Y-%m-%d')
            month = period_end.month
            
            # Use calendar quarters
            if 1 <= month <= 3:
                quarter = "Q1"
            elif 4 <= month <= 6:
                quarter = "Q2"
            elif 7 <= month <= 9:
                quarter = "Q3"
            else:
                quarter = "Q4"
                
            return {
                "fiscal_year": str(period_end.year),
                "fiscal_period": quarter
            }
        except (ValueError, TypeError):
            logging.warning(f"Invalid period end date in fallback: {period_end_date_str}")
            return {"fiscal_year": None, "fiscal_period": None}
    
    def to_dict(self):
        """Convert to dictionary for storage"""
        return {
            "ticker": self.ticker,
            "fiscal_year_end_month": self.fiscal_year_end_month,
            "fiscal_year_end_day": self.fiscal_year_end_day,
            "confidence_score": self.confidence_score,
            "historical_filings": self.historical_filings,
            "last_updated": self.last_updated
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create from dictionary"""
        calendar = cls(data["ticker"])
        calendar.fiscal_year_end_month = data.get("fiscal_year_end_month")
        calendar.fiscal_year_end_day = data.get("fiscal_year_end_day")
        calendar.confidence_score = data.get("confidence_score", 0)
        calendar.historical_filings = data.get("historical_filings", {})
        calendar.last_updated = data.get("last_updated")
        return calendar


class FiscalCalendarRegistry:
    """
    Registry to store and retrieve company fiscal calendars
    """
    
    def __init__(self, storage_path="data/fiscal_calendars.json"):
        self.storage_path = storage_path
        self.calendars = {}
        self._load_registry()
    
    def _load_registry(self):
        """Load registry from storage"""
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'r') as f:
                    data = json.load(f)
                    
                for ticker, calendar_data in data.items():
                    self.calendars[ticker] = CompanyFiscalCalendar.from_dict(calendar_data)
                    
                logging.info(f"Loaded fiscal calendars for {len(self.calendars)} companies")
            except Exception as e:
                logging.error(f"Error loading fiscal registry: {str(e)}")
                self.calendars = {}
        else:
            os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
            self.calendars = {}
    
    def save_registry(self):
        """Save registry to storage"""
        try:
            data = {ticker: calendar.to_dict() for ticker, calendar in self.calendars.items()}
            
            with open(self.storage_path, 'w') as f:
                json.dump(data, f, indent=2)
                
            logging.info(f"Saved fiscal calendars for {len(self.calendars)} companies")
        except Exception as e:
            logging.error(f"Error saving fiscal registry: {str(e)}")
    
    def get_calendar(self, ticker):
        """Get a company's fiscal calendar"""
        if ticker not in self.calendars:
            self.calendars[ticker] = CompanyFiscalCalendar(ticker)
        return self.calendars[ticker]
    
    def update_calendar(self, ticker, filings):
        """Update a company's fiscal calendar with new filings"""
        calendar = self.get_calendar(ticker)
        calendar.analyze_10k_filings(filings)
        self.save_registry()
        return calendar
    
    def determine_fiscal_period(self, ticker, period_end_date, filing_type="10-Q"):
        """Determine fiscal period for a company and date"""
        calendar = self.get_calendar(ticker)
        return calendar.determine_fiscal_period(period_end_date, filing_type)


# Initialize global registry
fiscal_registry = FiscalCalendarRegistry()