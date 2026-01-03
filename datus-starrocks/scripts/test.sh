#!/bin/bash
# Test runner script for datus-starrocks
# Usage: ./scripts/test.sh [unit|integration|acceptance|all]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Force set test environment variables (override production values)
export STARROCKS_HOST="localhost"
export STARROCKS_PORT="9030"
export STARROCKS_USER="root"
export STARROCKS_PASSWORD=""
export STARROCKS_CATALOG="default_catalog"
export STARROCKS_DATABASE="test"

# Function to run tests
run_unit_tests() {
    echo -e "${GREEN}Running unit tests (no database required)...${NC}"
    uv run pytest tests/ -m "not integration" -v
}

run_integration_tests() {
    echo -e "${GREEN}Running integration tests (requires StarRocks)...${NC}"
    echo -e "${YELLOW}Using: ${STARROCKS_USER}@${STARROCKS_HOST}:${STARROCKS_PORT}/${STARROCKS_DATABASE}${NC}"
    uv run pytest tests/integration -v
}

run_acceptance_tests() {
    echo -e "${GREEN}Running acceptance tests...${NC}"
    echo -e "${YELLOW}Unit tests:${NC}"
    uv run pytest tests/ -m "acceptance and not integration" -v
    echo -e "\n${YELLOW}Integration tests:${NC}"
    uv run pytest tests/ -m "acceptance and integration" -v
}

run_all_tests() {
    run_unit_tests
    echo ""
    run_integration_tests
}

# Parse command
case "${1:-all}" in
    unit)
        run_unit_tests
        ;;
    integration)
        run_integration_tests
        ;;
    acceptance)
        run_acceptance_tests
        ;;
    all)
        run_all_tests
        ;;
    *)
        echo -e "${RED}Usage: $0 [unit|integration|acceptance|all]${NC}"
        exit 1
        ;;
esac
