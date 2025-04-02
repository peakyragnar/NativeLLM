#!/usr/bin/env python3
import os
import sys

def check_file(path):
    print(f"Checking file: {path}")
    print(f"Absolute path: {os.path.abspath(path)}")
    print(f"File exists: {os.path.exists(path)}")
    print(f"Is file: {os.path.isfile(path)}")
    print(f"Parent dir exists: {os.path.exists(os.path.dirname(path))}")
    print(f"Current working directory: {os.getcwd()}")

    # List files in the directory
    parent_dir = os.path.dirname(path)
    if os.path.exists(parent_dir):
        print(f"Files in {parent_dir}:")
        for f in os.listdir(parent_dir):
            print(f"  - {f}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        check_file(sys.argv[1])
    else:
        print("Please provide a file path to check") 