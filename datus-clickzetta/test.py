#!/usr/bin/env python3
"""
Simple test runner that delegates to the tests directory.
This provides a convenient way to run tests from the project root.
"""

import sys
import subprocess
from pathlib import Path

def main():
    # Change to tests directory and run the main test runner
    tests_dir = Path(__file__).parent / "tests"

    # Forward all arguments to the main test runner
    cmd = [sys.executable, "run_tests.py"] + sys.argv[1:]

    # Run in tests directory
    result = subprocess.run(cmd, cwd=tests_dir)
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()