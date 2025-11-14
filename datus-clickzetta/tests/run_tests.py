#!/usr/bin/env python3
# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""
Unified test runner for ClickZetta adapter tests.

This script provides different test execution modes:
- Unit tests only
- Integration tests only
- All tests
- Quick tests (unit + fast integration)
- Coverage report
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path


def run_command(cmd, description=""):
    """Run a command and return the exit code."""
    print(f"\n🔧 {description}")
    print(f"Running: {' '.join(cmd)}")
    print("=" * 60)

    result = subprocess.run(cmd, capture_output=False)

    if result.returncode == 0:
        print(f"✅ {description} - PASSED")
    else:
        print(f"❌ {description} - FAILED")

    return result.returncode


def main():
    parser = argparse.ArgumentParser(description='Run ClickZetta adapter tests')
    parser.add_argument('--mode', choices=['unit', 'integration', 'all', 'quick', 'coverage'],
                       default='all', help='Test mode to run')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--markers', '-m', help='Pytest markers to run (e.g., "not slow")')
    parser.add_argument('--pattern', '-k', help='Run tests matching pattern')

    args = parser.parse_args()

    # Change to the script directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir)

    print("🚀 ClickZetta Adapter Test Runner")
    print("=" * 60)

    # Check if pytest is available
    try:
        subprocess.run([sys.executable, '-m', 'pytest', '--version'],
                      capture_output=True, check=True)
    except subprocess.CalledProcessError:
        print("❌ pytest not found. Trying to install...")
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install', 'pytest'], check=True)
        except subprocess.CalledProcessError:
            try:
                subprocess.run([sys.executable, '-m', 'pip', 'install', '--break-system-packages', 'pytest'], check=True)
            except subprocess.CalledProcessError:
                print("⚠️  Could not install pytest automatically.")
                print("Please install pytest manually:")
                print("  pip install pytest")
                print("  or")
                print("  pip install --break-system-packages pytest")
                return 1

    # Base pytest command
    base_cmd = [sys.executable, '-m', 'pytest']

    if args.verbose:
        base_cmd.append('-v')
    else:
        base_cmd.extend(['-q', '--tb=short'])

    # Add pattern if specified
    if args.pattern:
        base_cmd.extend(['-k', args.pattern])

    exit_codes = []

    if args.mode == 'unit':
        cmd = base_cmd + ['./unit/']
        if args.markers:
            cmd.extend(['-m', args.markers])
        exit_codes.append(run_command(cmd, "Unit Tests"))

    elif args.mode == 'integration':
        cmd = base_cmd + ['./integration/']
        if args.markers:
            cmd.extend(['-m', args.markers])
        exit_codes.append(run_command(cmd, "Integration Tests"))

    elif args.mode == 'quick':
        # Run unit tests + fast integration tests
        cmd = base_cmd + ['./unit/']
        exit_codes.append(run_command(cmd, "Unit Tests"))

        marker_expr = f'not slow and ({args.markers})' if args.markers else 'not slow'
        cmd = base_cmd + ['./integration/', '-m', marker_expr]
        exit_codes.append(run_command(cmd, "Fast Integration Tests"))

    elif args.mode == 'coverage':
        # Install coverage if needed
        try:
            subprocess.run([sys.executable, '-m', 'coverage', '--version'],
                          capture_output=True, check=True)
        except subprocess.CalledProcessError:
            print("Installing coverage...")
            subprocess.run([sys.executable, '-m', 'pip', 'install', 'coverage'], check=True)

        # Run tests with coverage
        cmd = [sys.executable, '-m', 'coverage', 'run', '-m', 'pytest'] + base_cmd[3:]
        if args.markers:
            cmd.extend(['-m', args.markers])
        exit_codes.append(run_command(cmd, "Tests with Coverage"))

        # Generate coverage report
        cmd = [sys.executable, '-m', 'coverage', 'report', '--show-missing']
        exit_codes.append(run_command(cmd, "Coverage Report"))

        # Generate HTML report
        cmd = [sys.executable, '-m', 'coverage', 'html']
        exit_codes.append(run_command(cmd, "HTML Coverage Report"))
        print("\n📊 HTML coverage report generated in: htmlcov/index.html")

    else:  # all
        # Run all tests
        cmd = base_cmd + ['./']
        if args.markers:
            cmd.extend(['-m', args.markers])
        exit_codes.append(run_command(cmd, "All Tests"))

    # Summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)

    failed_tests = sum(1 for code in exit_codes if code != 0)
    total_tests = len(exit_codes)

    if failed_tests == 0:
        print(f"🎉 All {total_tests} test suite(s) PASSED!")
        return 0
    else:
        print(f"❌ {failed_tests} out of {total_tests} test suite(s) FAILED!")
        return 1


if __name__ == "__main__":
    sys.exit(main())