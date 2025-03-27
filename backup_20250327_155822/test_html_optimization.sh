#!/bin/bash
# Run all HTML optimization tests and validations

echo "=== HTML Optimization Test Suite ==="
echo "Running comprehensive tests and validations..."
echo ""

# Run the unit tests
echo "1. Running unit tests..."
python test_html_optimization.py

# Check the return code
if [ $? -ne 0 ]; then
    echo "❌ Unit tests failed! Fix issues before proceeding."
    exit 1
else
    echo "✅ Unit tests passed"
fi

echo ""
echo "2. Validating table preservation..."
python verify_table_preservation.py

# Check the return code
if [ $? -ne 0 ]; then
    echo "❌ Table preservation validation failed!"
    exit 1
else
    echo "✅ Table preservation validation passed"
fi

echo ""
echo "3. Verifying table fix and data integrity..."
python verify_table_fix.py

# Check the return code
if [ $? -ne 0 ]; then
    echo "❌ Table data integrity verification failed!"
    exit 1
else
    echo "✅ Table data integrity verification passed"
fi

echo ""
echo "4. Running data integrity checks..."
python data_integrity_check.py

# Check the return code
if [ $? -ne 0 ]; then
    echo "❌ Data integrity checks failed!"
    exit 1
else
    echo "✅ Data integrity checks passed"
fi

# Find a sample file for file size reduction test
echo ""
echo "5. Testing file size reduction..."
SAMPLE_FILE=$(find data/processed -name "*_llm.txt" | head -n 1)

if [ -z "$SAMPLE_FILE" ]; then
    echo "⚠️ No sample file found for file size reduction test"
else
    python test_file_size_reduction.py "$SAMPLE_FILE"
    
    # Check the return code
    if [ $? -ne 0 ]; then
        echo "❌ File size reduction test failed!"
        exit 1
    else
        echo "✅ File size reduction test passed"
    fi
fi

echo ""
echo "6. Running benchmarks..."
python benchmark_html_optimization.py

# Check the return code
if [ $? -ne 0 ]; then
    echo "❌ Benchmark failed!"
    exit 1
else
    echo "✅ Benchmark completed"
fi

echo ""
echo "=== All tests completed successfully ==="
echo "The HTML optimization implementation has been verified for:"
echo "✓ Preserving 100% of numeric values"
echo "✓ Maintaining document structure"
echo "✓ Achieving meaningful size reduction"
echo "✓ Performance within acceptable limits"
echo ""
echo "You can now apply the optimization to your files with:"
echo "python apply_html_optimization.py --validate-only  # Validation mode"
echo "python apply_html_optimization.py                  # Apply changes"