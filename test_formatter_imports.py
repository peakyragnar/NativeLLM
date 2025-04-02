#!/usr/bin/env python3
"""
Test LLM formatter imports
"""

print("Testing imports...")

try:
    # Try importing the direct way
    from src2.formatter.llm_formatter import LLMFormatter
    print("✅ Import from src2.formatter.llm_formatter succeeded")
except Exception as e:
    print(f"❌ Import from src2.formatter.llm_formatter failed: {e}")

try:
    # Try importing through the package
    from src2.formatter import LLMFormatter
    print("✅ Import from src2.formatter succeeded")
except Exception as e:
    print(f"❌ Import from src2.formatter failed: {e}")

print("\nLet's check for indentation issues:")
try:
    import tokenize
    with open("src2/formatter/llm_formatter.py", "rb") as f:
        tokens = list(tokenize.tokenize(f.readline))
    print("✅ No syntax errors detected")
except tokenize.TokenError as e:
    print(f"❌ Tokenize error: {e}")
except IndentationError as e:
    print(f"❌ Indentation error: {e}")
except Exception as e:
    print(f"❌ Other error: {e}")

# Check the LLMFormatter class definition
with open("src2/formatter/llm_formatter.py", "r") as f:
    content = f.read()
    if "class LLMFormatter" in content:
        print("✅ LLMFormatter class found in the file")
    else:
        print("❌ LLMFormatter class not found in the file")