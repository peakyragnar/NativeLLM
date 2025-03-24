#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import time
from typing import List, Dict, Tuple, Optional, Any
import importlib.util

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

# Set PYTHONPATH environment variable for subprocess calls
os.environ["PYTHONPATH"] = os.pathsep.join([PROJECT_ROOT, os.environ.get("PYTHONPATH", "")])

def load_module_from_path(module_path: str, module_name: str) -> Any:
    """Dynamically load a module from a file path."""
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if not spec or not spec.loader:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def find_test_files() -> Dict[str, List[str]]:
    """Find all test files in the tests directory."""
    test_files = {
        "unit": [],
        "integration": [],
        "validation": []
    }
    
    # Unit tests
    for file in os.listdir(os.path.dirname(os.path.abspath(__file__))):
        if file.startswith("test_") and file.endswith(".py") and file != "test_all.py":
            test_files["unit"].append(os.path.join(os.path.dirname(os.path.abspath(__file__)), file))
    
    # Validation tests
    validation_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation")
    if os.path.exists(validation_dir):
        for file in os.listdir(validation_dir):
            if (file.startswith("validate_") or file.startswith("check_")) and file.endswith(".py"):
                test_files["validation"].append(os.path.join(validation_dir, file))
    
    # Integration tests
    # These typically involve external services like GCS/Firestore
    for file in test_files["unit"]:
        if "integration" in file or "gcs" in file or "firestore" in file:
            test_files["integration"].append(file)
            test_files["unit"].remove(file)
            
    return test_files

def run_test_file(test_file: str, verbose: bool = False) -> Tuple[bool, str]:
    """Run a single test file."""
    try:
        print(f"Running test: {os.path.basename(test_file)}")
        
        # Ensure PYTHONPATH includes project root
        env = os.environ.copy()
        
        if verbose:
            result = subprocess.run(
                ["python", test_file], 
                check=False, 
                capture_output=True, 
                text=True, 
                env=env
            )
        else:
            result = subprocess.run(
                ["python", test_file], 
                check=False, 
                capture_output=True, 
                text=True, 
                env=env
            )
        
        if result.returncode == 0:
            if verbose:
                print(f"✅ PASSED: {os.path.basename(test_file)}")
                if result.stdout.strip():
                    print(result.stdout)
            return True, "PASSED"
        else:
            print(f"❌ FAILED: {os.path.basename(test_file)}")
            print(result.stderr)
            if verbose and result.stdout.strip():
                print(result.stdout)
            return False, result.stderr
    except Exception as e:
        print(f"❌ ERROR: {os.path.basename(test_file)}: {str(e)}")
        return False, str(e)

def run_validation_module(validation_file: str, verbose: bool = False) -> Tuple[bool, Dict[str, Any]]:
    """Run a validation module that has check_* functions."""
    try:
        module_name = os.path.basename(validation_file).replace(".py", "")
        module = load_module_from_path(validation_file, module_name)
        
        if not module:
            return False, {"error": f"Could not load module {module_name}"}
        
        results = {}
        success = True
        
        # Find and run all check_* and validate_* functions
        for attr_name in dir(module):
            if attr_name.startswith("check_") or attr_name.startswith("validate_"):
                func = getattr(module, attr_name)
                if callable(func):
                    print(f"Running validation: {attr_name}")
                    try:
                        result = func()
                        if isinstance(result, dict):
                            results[attr_name] = result
                            if result.get("success", True) is False:
                                success = False
                                print(f"❌ FAILED: {attr_name}")
                                if verbose:
                                    print(f"  Details: {result}")
                            else:
                                print(f"✅ PASSED: {attr_name}")
                                if verbose and 'details' in result:
                                    print(f"  Details: {result['details']}")
                        else:
                            results[attr_name] = {"success": bool(result)}
                            if not result:
                                success = False
                                print(f"❌ FAILED: {attr_name}")
                            else:
                                print(f"✅ PASSED: {attr_name}")
                    except Exception as e:
                        success = False
                        results[attr_name] = {"success": False, "error": str(e)}
                        print(f"❌ ERROR: {attr_name}: {str(e)}")
        
        return success, results
    except Exception as e:
        print(f"❌ ERROR in validation file {os.path.basename(validation_file)}: {str(e)}")
        return False, {"error": str(e)}

def main():
    parser = argparse.ArgumentParser(description="Run all tests in the tests directory")
    parser.add_argument("--unit", action="store_true", help="Run only unit tests")
    parser.add_argument("--integration", action="store_true", help="Run only integration tests")
    parser.add_argument("--validation", action="store_true", help="Run only validation tests")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    test_files = find_test_files()
    
    start_time = time.time()
    results = {
        "unit": {},
        "integration": {},
        "validation": {}
    }
    
    # Determine which test types to run
    run_all = not (args.unit or args.integration or args.validation)
    
    # Run unit tests
    if run_all or args.unit:
        print("\n=== Running Unit Tests ===\n")
        for test_file in test_files["unit"]:
            success, message = run_test_file(test_file, args.verbose)
            results["unit"][os.path.basename(test_file)] = {"success": success, "message": message}
    
    # Run integration tests
    if run_all or args.integration:
        print("\n=== Running Integration Tests ===\n")
        for test_file in test_files["integration"]:
            success, message = run_test_file(test_file, args.verbose)
            results["integration"][os.path.basename(test_file)] = {"success": success, "message": message}
    
    # Run validation tests
    if run_all or args.validation:
        print("\n=== Running Validation Tests ===\n")
        for validation_file in test_files["validation"]:
            success, validation_results = run_validation_module(validation_file, args.verbose)
            results["validation"][os.path.basename(validation_file)] = {
                "success": success, 
                "results": validation_results
            }
    
    # Print summary
    print("\n=== Test Summary ===\n")
    total_tests = 0
    passed_tests = 0
    
    if run_all or args.unit:
        unit_total = len(results["unit"])
        unit_passed = sum(1 for result in results["unit"].values() if result["success"])
        print(f"Unit Tests: {unit_passed}/{unit_total} passed")
        total_tests += unit_total
        passed_tests += unit_passed
    
    if run_all or args.integration:
        integration_total = len(results["integration"])
        integration_passed = sum(1 for result in results["integration"].values() if result["success"])
        print(f"Integration Tests: {integration_passed}/{integration_total} passed")
        total_tests += integration_total
        passed_tests += integration_passed
    
    if run_all or args.validation:
        validation_total = len(results["validation"])
        validation_passed = sum(1 for result in results["validation"].values() if result["success"])
        print(f"Validation Tests: {validation_passed}/{validation_total} passed")
        total_tests += validation_total
        passed_tests += validation_passed
    
    print(f"\nTotal: {passed_tests}/{total_tests} passed")
    print(f"Time taken: {time.time() - start_time:.2f} seconds")
    
    # Return exit code based on test results
    return 0 if passed_tests == total_tests else 1

if __name__ == "__main__":
    sys.exit(main())