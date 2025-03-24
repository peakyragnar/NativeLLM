# Tests Directory

This directory contains all tests for the NativeLLM project, organized into different categories:

## Test Categories

- **Unit Tests** - Basic functionality tests that don't require external services
- **Integration Tests** - Tests that involve integration with external services like GCS or Firestore
- **Validation Tests** - Tests that validate data integrity and correctness

## Running Tests

### Run All Tests

```bash
# From project root
./run_tests.sh
```

### Run Specific Test Categories

```bash
# Run only unit tests
./run_tests.sh --unit

# Run only integration tests
./run_tests.sh --integration

# Run only validation tests
./run_tests.sh --validation
```

### Verbose Output

```bash
# Run with verbose output
./run_tests.sh --verbose
# or
./run_tests.sh -v
```

## Adding New Tests

- **Unit tests** - Add new test files in the `tests/` directory with naming convention `test_*.py`
- **Validation tests** - Add new validation modules in the `tests/validation/` directory with either:
  - Naming convention `validate_*.py` or `check_*.py`
  - Functions inside modules should follow naming convention `check_*()` or `validate_*()`

## Test Details

### Unit Tests
- `test_pipeline.py` - Tests the main data processing pipeline
- `test_llm_integration.py` - Tests LLM integration functionality
- `test_html_extraction.py` - Tests HTML extraction from SEC filings
- `test_company_formats.py` - Tests company-specific formatting
- `test_adaptive_xbrl.py` - Tests adaptive XBRL parsing
- `test_fiscal_handling.py` - Tests fiscal period determination

### Integration Tests
- `test_gcs_upload.py` - Tests Google Cloud Storage uploads
- `test_firestore_setup.py` - Tests Firestore database setup
- `test_gcs_firestore_consistency.py` - Tests consistency between GCS and Firestore

### Validation Tests
- `validate_data_integrity.py` - Validates overall data integrity
- `check_duplicates.py` - Checks for duplicate filings
- `check_fiscal_calendar.py` - Validates fiscal calendar mappings
- `check_filings.py` - Checks individual filing details