import os
import sys
import unittest
import datetime
from unittest.mock import patch, MagicMock

# Add parent directory to path to import from src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.edgar.fiscal_manager import fiscal_manager, FiscalSignal, CompanyFiscalModel

class TestFiscalPeriodManager(unittest.TestCase):
    """Tests for the evidence-based fiscal period system"""
    
    def setUp(self):
        """Setup test cases"""
        # Create a fresh manager for testing
        self.manager = fiscal_manager
        
        # Test data
        self.test_tickers = ["AAPL", "MSFT", "GOOGL", "XYZ"]  # XYZ is unknown
    
    def test_known_fiscal_patterns(self):
        """Test that known fiscal patterns are correctly initialized"""
        # Apple (Sep 30 fiscal year end)
        apple_model = self.manager.get_model("AAPL")
        self.assertEqual(apple_model.get_fiscal_year_end_month(), 9)
        self.assertEqual(apple_model.get_fiscal_year_end_day(), 30)
        
        # Microsoft (Jun 30 fiscal year end)
        msft_model = self.manager.get_model("MSFT")
        self.assertEqual(msft_model.get_fiscal_year_end_month(), 6)
        self.assertEqual(msft_model.get_fiscal_year_end_day(), 30)
        
        # Google (Dec 31 fiscal year end - calendar year)
        googl_model = self.manager.get_model("GOOGL")
        self.assertEqual(googl_model.get_fiscal_year_end_month(), 12)
        self.assertEqual(googl_model.get_fiscal_year_end_day(), 31)
    
    def test_unknown_company_defaults(self):
        """Test that unknown companies get reasonable defaults"""
        xyz_model = self.manager.get_model("XYZ")
        self.assertEqual(xyz_model.get_fiscal_year_end_month(), 12)  # Default Dec
        self.assertEqual(xyz_model.get_fiscal_year_end_day(), 31)    # Default 31st
        self.assertLess(xyz_model.confidence_score, 1.0)  # Lower confidence for default
    
    def test_fiscal_period_standardization(self):
        """Test that fiscal periods are standardized correctly"""
        # Test various input formats
        self.assertEqual(self.manager.standardize_period("Q1"), "Q1")
        self.assertEqual(self.manager.standardize_period("1Q"), "Q1")
        self.assertEqual(self.manager.standardize_period("QTR1"), "Q1")
        self.assertEqual(self.manager.standardize_period("FIRST"), "Q1")
        
        self.assertEqual(self.manager.standardize_period("annual"), "annual")
        self.assertEqual(self.manager.standardize_period("ANNUAL"), "annual")
        self.assertEqual(self.manager.standardize_period("FY"), "annual")
        self.assertEqual(self.manager.standardize_period("A"), "annual")
        
        # Test output format options
        self.assertEqual(self.manager.standardize_period("Q4", "internal"), "Q4")
        self.assertEqual(self.manager.standardize_period("annual", "display"), "Q4")
        self.assertEqual(self.manager.standardize_period("Q1", "old"), "1Q")
        self.assertEqual(self.manager.standardize_period("Q2", "folder"), "Q2")
    
    def test_apple_fiscal_periods(self):
        """Test fiscal period determination for Apple"""
        # Apple: FY ends Sep 30
        # Q1: Oct-Dec, Q2: Jan-Mar, Q3: Apr-Jun, Annual: Jul-Sep
        apple_model = self.manager.get_model("AAPL")
        
        # Q1 (Oct-Dec)
        q1_period = apple_model.determine_fiscal_period("2023-12-31", "10-Q")
        self.assertEqual(q1_period["fiscal_year"], "2024")  # FY2024 Q1
        self.assertEqual(q1_period["fiscal_period"], "Q1")
        
        # Q2 (Jan-Mar)
        q2_period = apple_model.determine_fiscal_period("2023-03-31", "10-Q")
        self.assertEqual(q2_period["fiscal_year"], "2023")  # FY2023 Q2
        self.assertEqual(q2_period["fiscal_period"], "Q2")
        
        # Q3 (Apr-Jun)
        q3_period = apple_model.determine_fiscal_period("2023-06-30", "10-Q")
        self.assertEqual(q3_period["fiscal_year"], "2023")  # FY2023 Q3
        self.assertEqual(q3_period["fiscal_period"], "Q3")
        
        # Annual (Jul-Sep)
        annual_period = apple_model.determine_fiscal_period("2023-09-30", "10-K")
        self.assertEqual(annual_period["fiscal_year"], "2023")  # FY2023 Annual
        self.assertEqual(annual_period["fiscal_period"], "annual")
    
    def test_microsoft_fiscal_periods(self):
        """Test fiscal period determination for Microsoft"""
        # Microsoft: FY ends Jun 30
        # Q1: Jul-Sep, Q2: Oct-Dec, Q3: Jan-Mar, Annual: Apr-Jun
        msft_model = self.manager.get_model("MSFT")
        
        # Q1 (Jul-Sep)
        q1_period = msft_model.determine_fiscal_period("2023-09-30", "10-Q")
        self.assertEqual(q1_period["fiscal_year"], "2024")  # FY2024 Q1
        self.assertEqual(q1_period["fiscal_period"], "Q1")
        
        # Q2 (Oct-Dec)
        q2_period = msft_model.determine_fiscal_period("2023-12-31", "10-Q")
        self.assertEqual(q2_period["fiscal_year"], "2024")  # FY2024 Q2
        self.assertEqual(q2_period["fiscal_period"], "Q2")
        
        # Q3 (Jan-Mar)
        q3_period = msft_model.determine_fiscal_period("2023-03-31", "10-Q")
        self.assertEqual(q3_period["fiscal_year"], "2023")  # FY2023 Q3
        self.assertEqual(q3_period["fiscal_period"], "Q3")
        
        # IMPORTANT: Microsoft NEVER has Q4, Apr-Jun is always annual
        april_period = msft_model.determine_fiscal_period("2023-04-30", "10-Q")
        self.assertEqual(april_period["fiscal_period"], "annual")
        
        may_period = msft_model.determine_fiscal_period("2023-05-31", "10-Q")
        self.assertEqual(may_period["fiscal_period"], "annual")
        
        june_period = msft_model.determine_fiscal_period("2023-06-30", "10-Q")
        self.assertEqual(june_period["fiscal_period"], "annual")
        
        # Annual (Apr-Jun)
        annual_period = msft_model.determine_fiscal_period("2023-06-30", "10-K")
        self.assertEqual(annual_period["fiscal_year"], "2023")  # FY2023 Annual
        self.assertEqual(annual_period["fiscal_period"], "annual")
    
    def test_evidence_collection(self):
        """Test that the system collects and processes evidence correctly"""
        # Create a test model
        test_model = CompanyFiscalModel("TEST")
        
        # Add some signals
        signal1 = FiscalSignal("10-K_filing", "fiscal_year_end_month", 3, 0.8)  # March
        test_model.add_signal(signal1)
        
        signal2 = FiscalSignal("10-K_filing", "fiscal_year_end_month", 3, 0.7)  # Another March
        test_model.add_signal(signal2)
        
        # The model should now believe the fiscal year ends in March with higher confidence
        self.assertEqual(test_model.get_fiscal_year_end_month(), 3)
        self.assertGreater(test_model.patterns["fiscal_year_end_month"].confidence, 0.7)
        
        # Now add a contradicting signal with lower confidence
        signal3 = FiscalSignal("10-Q_filing", "fiscal_year_end_month", 6, 0.4)  # June
        test_model.add_signal(signal3)
        
        # The model should still believe March due to stronger evidence
        self.assertEqual(test_model.get_fiscal_year_end_month(), 3)
        
        # Add more June signals to shift the consensus
        signal4 = FiscalSignal("10-K_filing", "fiscal_year_end_month", 6, 0.9)
        signal5 = FiscalSignal("10-K_filing", "fiscal_year_end_month", 6, 0.8)
        test_model.add_signal(signal4)
        test_model.add_signal(signal5)
        
        # Now the model should update to June
        self.assertEqual(test_model.get_fiscal_year_end_month(), 6)
    
    def test_learning_from_filings(self):
        """Test that the system learns from filings"""
        # Create a new unknown company
        test_ticker = "NEWCO"
        model = self.manager.get_model(test_ticker)
        
        # Store the initial fiscal year end month
        initial_fy_end = model.get_fiscal_year_end_month()
        self.assertIn(initial_fy_end, [3, 12])  # Either default or could be set by test order
        
        # Create a mock 10-K filing that suggests Mar fiscal year end
        filing_metadata = {
            "ticker": test_ticker,
            "filing_type": "10-K",
            "period_end_date": "2023-03-31",
            "filing_date": "2023-05-15",  # Filed 45 days after period end (typical)
        }
        
        # Update the model
        fiscal_info = self.manager.update_model(test_ticker, filing_metadata)
        
        # The model should now have learned that fiscal year likely ends in March
        model = self.manager.get_model(test_ticker)
        self.assertIn("fiscal_year_end_month", model.patterns)
        
        # Test that the fiscal period determination works with the learned pattern
        q1_period = model.determine_fiscal_period("2023-06-30", "10-Q")
        self.assertEqual(q1_period["fiscal_period"], "Q1")  # Apr-Jun = Q1 for Mar FYE
    
    def test_serialization(self):
        """Test that models can be serialized and deserialized"""
        test_ticker = "SERIAL"
        model = self.manager.get_model(test_ticker)
        
        # Add a signal
        signal = FiscalSignal("test", "fiscal_year_end_month", 9, 0.8)
        model.add_signal(signal)
        
        # Serialize
        data = model.to_dict()
        
        # Deserialize
        new_model = CompanyFiscalModel.from_dict(data)
        
        # Check equality
        self.assertEqual(new_model.ticker, test_ticker)
        self.assertEqual(new_model.get_fiscal_year_end_month(), 9)
        self.assertGreaterEqual(len(new_model.signals), 1)

if __name__ == "__main__":
    unittest.main()