# ClickZetta Adapter Tests

This directory contains comprehensive tests for the ClickZetta database adapter.

## Test Structure

```
tests/
├── conftest.py                    # Shared test configuration and fixtures
├── unit/                          # Unit tests for individual functions
│   ├── test_config.py            # Configuration class tests
│   └── test_utils.py             # Utility function tests
└── integration/                   # Integration tests with mocked dependencies
    └── test_connector_integration.py  # Full connector functionality tests
```

## Test Categories

### Unit Tests (`tests/unit/`)
- **Purpose**: Test individual functions and utilities in isolation
- **Dependencies**: Minimal, mostly pure Python functions
- **Speed**: Very fast (< 1 second)
- **Coverage**: Configuration validation, utility functions, data processing

### Integration Tests (`tests/integration/`)
- **Purpose**: Test component interactions with mocked dependencies
- **Dependencies**: Mock ClickZetta SDK and Datus framework
- **Speed**: Fast to medium (1-10 seconds)
- **Coverage**: Connection management, SQL operations, metadata discovery

## Running Tests

### Quick Start

```bash
# Run all tests
./run_tests.py

# Or with python
python run_tests.py
```

### Test Modes

```bash
# Unit tests only (fastest)
./run_tests.py --mode unit

# Integration tests only
./run_tests.py --mode integration

# Quick tests (unit + fast integration)
./run_tests.py --mode quick

# All tests including slow ones
./run_tests.py --mode all

# With coverage report
./run_tests.py --mode coverage
```

### Advanced Options

```bash
# Verbose output
./run_tests.py -v

# Run specific test patterns
./run_tests.py -k "test_config"

# Run tests with specific markers
./run_tests.py -m "unit and not slow"

# Combine options
./run_tests.py --mode unit -v -k "validation"
```

### Using pytest directly

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_config.py

# Run with coverage
pytest --cov=datus_clickzetta tests/

# Run with markers
pytest -m unit
pytest -m "integration and not slow"
```

## Test Markers

Tests are automatically marked based on their location and content:

- `unit`: Tests in `tests/unit/` directory
- `integration`: Tests in `tests/integration/` directory
- `requires_clickzetta`: Tests that need actual ClickZetta credentials
- `slow`: Long-running tests

## Mock Environment

The test suite uses comprehensive mocking to avoid dependencies on:
- Datus framework packages
- ClickZetta SDK
- External databases
- Network connections

All tests run in isolation with predictable mocked responses.

## Configuration

### Environment Variables
For tests that require actual ClickZetta connection (marked with `requires_clickzetta`):

```bash
export CLICKZETTA_SERVICE="your-service.clickzetta.com"
export CLICKZETTA_USERNAME="your-username"
export CLICKZETTA_PASSWORD="your-password"
export CLICKZETTA_INSTANCE="your-instance"
export CLICKZETTA_WORKSPACE="your-workspace"
export CLICKZETTA_SCHEMA="PUBLIC"
export CLICKZETTA_VCLUSTER="DEFAULT_AP"
```

### pytest Configuration
Settings are defined in `pytest.ini`:
- Test discovery patterns
- Output formatting
- Marker definitions
- Warning filters

## Writing New Tests

### Unit Test Example

```python
# tests/unit/test_new_feature.py
import pytest
from datus_clickzetta.connector import some_utility_function

class TestNewFeature:
    def test_utility_function(self):
        result = some_utility_function("input")
        assert result == "expected_output"
```

### Integration Test Example

```python
# tests/integration/test_new_integration.py
import pytest
from unittest.mock import patch

class TestNewIntegration:
    @patch('datus_clickzetta.connector.Session')
    def test_connector_feature(self, mock_session, mock_datus_modules):
        from datus_clickzetta.connector import ClickZettaConnector

        # Setup mocks
        mock_session.builder.configs.return_value.create.return_value = mock_session

        # Test
        connector = ClickZettaConnector(...)
        result = connector.some_method()

        # Assert
        assert result.success
```

## Continuous Integration

For CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run tests
  run: |
    python run_tests.py --mode quick -v

- name: Run coverage
  run: |
    python run_tests.py --mode coverage
```

## Test Data

Test fixtures and sample data are defined in `conftest.py`:
- Standard test configurations
- Mock session objects
- Common test utilities

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all dependencies are mocked in `conftest.py`
2. **Slow Tests**: Use `--mode quick` or `-m "not slow"` to skip slow tests
3. **Coverage Issues**: Make sure all code paths are tested

### Debug Mode

```bash
# Run with Python debugger
pytest --pdb tests/unit/test_config.py

# Run single test with verbose output
pytest -vvv -s tests/unit/test_config.py::TestClickZettaConfig::test_valid_configuration
```

## Contributing

When adding new features:

1. Write unit tests for new utility functions
2. Write integration tests for new connector methods
3. Use appropriate markers for test categorization
4. Update this README if adding new test categories or requirements