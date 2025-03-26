# SEC XBRL Pipeline Streamlining Plan

## Goals
1. Create a single, reliable pipeline that works for all filing years (2022-2025+)
2. Combine the strengths of both approaches into one optimal solution
3. Ensure consistent handling of URLs, XBRL data, and HTML processing

## Hybrid Approach

### Keep from secedgar_pipeline.py:
- **URL Construction Approach**: Leverage the secedgar library for reliable filing URL discovery
- **Filing Handling**: Use the secedgar library's systematic approach to document retrieval
- **Simpler Core**: Adopt cleaner code organization for the core filing retrieval

### Keep from enhanced_pipeline.py:
- **Calendar-Based Filtering**: Maintain ability to process filings by calendar year ranges
- **Full HTML Processing**: Preserve complex HTML extraction logic
- **GCP Integration**: Maintain comprehensive cloud storage integration (GCS + Firestore)
- **Rich Feature Set**: Keep the extensive functionality and options
- **Command Interface**: Maintain the flexible command-line interface

## Implementation Plan

### Phase 1: Create the Hybrid Pipeline
1. Start with the structure of enhanced_pipeline.py
2. Replace the SEC URL construction and filing discovery with secedgar-based approach
3. Retain the HTML processing and GCP integration from enhanced_pipeline.py
4. Create adapter layer between secedgar and the existing HTML/XBRL processors

### Phase 2: Test and Verify
1. Test with 2022-2023 filings to ensure HTML extraction works
2. Test with 2024-2025 filings to ensure compatibility is maintained
3. Verify cloud storage integration works properly

### Phase 3: Clean Up
1. Remove duplicate code and unused functions
2. Improve error handling and logging
3. Document the new hybrid approach

## Technical Details

### Key Integration Points
1. Replace the URL construction and document discovery in enhanced_pipeline.py with secedgar-based approach
2. Adapt secedgar's filing objects to work with existing HTML processing pipeline
3. Ensure the metadata format from secedgar is compatible with HTML extraction
4. Maintain all functionality from enhanced_pipeline.py while using secedgar for SEC communication

## Next Steps
1. Create new branch "streamline"
2. Create a hybrid version that integrates secedgar for URL handling
3. Test extensively with both 2022-2023 and 2024-2025 filings
4. Push the streamlined solution once verified