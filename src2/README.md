# NativeLLM - Modernized Financial Data Pipeline

This directory contains the modernized version of the NativeLLM financial data processing system, following a modular architecture for better maintainability, testability, and extensibility.

## Project Structure

The codebase is organized into specialized modules:

```
src2/
├── config.py                 # Central configuration settings
├── downloader/               # SEC filing download modules
│   └── direct_edgar_downloader.py
├── edgar/                    # SEC EDGAR specific utilities
│   └── edgar_utils.py
├── formatter/                # Text and data formatting modules
│   ├── llm_formatter.py
│   └── normalize_value.py
├── processor/                # Data processing modules
│   ├── enhanced_processor.py  # Combined XBRL/iXBRL processor
│   ├── html_optimizer.py
│   ├── html_processor.py
│   ├── ixbrl_extractor.py
│   ├── parallel_processor.py
│   └── xbrl_processor.py
├── sec/                      # SEC filing specific modules
│   ├── batch_pipeline.py
│   ├── company_list.py
│   ├── downloader.py
│   ├── extractor.py
│   ├── finder.py
│   ├── fiscal/               # Fiscal period handling
│   │   ├── company_fiscal.py
│   │   └── fiscal_manager.py
│   ├── pipeline.py
│   └── renderer.py
├── storage/                  # Cloud storage handling
│   └── gcp_storage.py
└── xbrl/                     # XBRL file utilities
    ├── company_formats.py
    ├── html_text_extractor.py
    └── xbrl_parser.py
```

## Module Descriptions

### 1. Configuration

- `config.py`: Central configuration settings for the entire system

### 2. Downloader Modules

- `downloader/direct_edgar_downloader.py`: Direct HTTP-based SEC EDGAR downloader

### 3. Processor Modules

- `processor/enhanced_processor.py`: Unified processor for both XBRL and iXBRL formats
- `processor/html_optimizer.py`: HTML optimization with data integrity preservation
- `processor/html_processor.py`: HTML processing with section and content extraction
- `processor/ixbrl_extractor.py`: Extract data from inline XBRL (iXBRL) documents
- `processor/parallel_processor.py`: Parallel processing of multiple companies
- `processor/xbrl_processor.py`: Process traditional XBRL files

### 4. SEC-Specific Modules

- `sec/batch_pipeline.py`: Batch processing of multiple SEC filings
- `sec/company_list.py`: Company listings and selection
- `sec/downloader.py`: SEC EDGAR downloader with compliance
- `sec/extractor.py`: Extract facts from SEC documents
- `sec/finder.py`: SEC filing finder with URL construction
- `sec/fiscal/`: Fiscal period handling for different companies
- `sec/pipeline.py`: Main SEC processing pipeline
- `sec/renderer.py`: Rendering engine for SEC documents

### 5. Formatter Modules

- `formatter/llm_formatter.py`: Format data for LLM consumption
- `formatter/normalize_value.py`: Value normalization utilities

### 6. Storage Modules

- `storage/gcp_storage.py`: Google Cloud Storage integration

### 7. XBRL Utilities

- `xbrl/company_formats.py`: Company-specific XBRL formats
- `xbrl/html_text_extractor.py`: Extract text from HTML documents
- `xbrl/xbrl_parser.py`: Parse XBRL documents

## Key Improvements

1. **Modular Design**: Clear separation of concerns with specialized modules
2. **Consistent Interfaces**: Standard patterns for function and method signatures
3. **Error Handling**: Comprehensive error handling throughout the codebase
4. **Logging**: Improved logging with proper log levels
5. **Configurability**: Centralized configuration management
6. **Documentation**: Docstrings and module-level documentation
7. **Enhanced Fiscal Support**: Better handling of company-specific fiscal calendars
8. **Format Flexibility**: Support for both XBRL and inline XBRL (iXBRL)
9. **Cloud Integration**: Better GCP storage and Firestore integration

## Usage Examples

See `run_pipeline.py` for examples of how to use the updated modules.