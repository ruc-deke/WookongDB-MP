#!/usr/bin/env python3
import os
import subprocess
import sys

def main():
    # Determine the absolute path of this script
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)
    
    # Assuming the directory structure is project_root/tests/scripts/
    # We need to run tests from project_root
    project_root = os.path.dirname(os.path.dirname(script_dir))
    
    # Change working directory to project root
    os.chdir(project_root)
    print(f"Running tests from project root: {project_root}")
    
    # List of test scripts to run
    # We can either hardcode them or discover them. 
    # Hardcoding ensures order if needed, but discovery is more flexible.
    # Based on the user request, we'll list the specific ones found earlier.
    test_scripts = [
        "basic_query_test.py",
        "cache_consistency_test.py",
        "concurrency_test.py",
        "join_test.py",
        "load_table_test.py"
    ]
    
    results = []
    
    print("=" * 60)
    print("Starting Test Suite")
    print("=" * 60)
    
    all_passed = True
    
    for test_script in test_scripts:
        full_path = os.path.join("tests", "scripts", test_script)
        
        if not os.path.exists(full_path):
            print(f"Error: Test script not found: {full_path}")
            results.append((test_script, "SKIPPED (Not Found)"))
            all_passed = False
            continue
            
        print(f"\nRunning {test_script}...")
        print("-" * 30)
        
        start_time = time.time()
        try:
            # flush stdout to ensure order
            sys.stdout.flush()
            
            # Run the test script
            # We assume python3 is available in path
            process = subprocess.run(
                [sys.executable, full_path],
                text=True
            )
            
            duration = time.time() - start_time
            
            if process.returncode == 0:
                print(f"\n[PASS] {test_script} (Time: {duration:.2f}s)")
                results.append((test_script, "PASS"))
            else:
                print(f"\n[FAIL] {test_script} (Exit Code: {process.returncode})")
                results.append((test_script, "FAIL"))
                all_passed = False
                
        except Exception as e:
            print(f"\n[ERROR] Failed to execute {test_script}: {e}")
            results.append((test_script, f"ERROR ({e})"))
            all_passed = False
            
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    for name, status in results:
        # Pad name for alignment
        print(f"{name:<30} : {status}")
        
    print("-" * 60)
    if all_passed:
        print("ALL TESTS PASSED")
        sys.exit(0)
    else:
        print("SOME TESTS FAILED")
        sys.exit(1)

import time

if __name__ == "__main__":
    main()
