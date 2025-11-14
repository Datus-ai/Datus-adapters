# Datus ClickZetta Adapter

This package provides a [ClickZetta](https://www.singdata.com/) Lakehouse adapter for [Datus](https://github.com/datusai/datus-agent), enabling seamless integration with ClickZetta analytics platform.

[ClickZetta](https://www.singdata.com/) is developed by [Singdata](https://www.singdata.com/) and [Yunqi](https://www.yunqi.tech/).

## Installation

```bash
pip install datus-clickzetta
```

## Dependencies

This adapter requires the following ClickZetta Python packages:
- `clickzetta-connector-python`
- `clickzetta-zettapark-python`

## Configuration

Configure ClickZetta connection in your Datus configuration:

```yaml
namespaces:
  - name: "clickzetta_prod"
    connector: "clickzetta"
    config:
      service: "your-service-endpoint.clickzetta.com"
      username: "your-username"
      password: "your-password"
      instance: "your-instance-id"
      workspace: "your-workspace"
      schema: "PUBLIC"  # optional, defaults to PUBLIC
      vcluster: "DEFAULT_AP"  # optional, defaults to DEFAULT_AP
      secure: true  # optional
```

### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `service` | string | Yes | - | ClickZetta service endpoint |
| `username` | string | Yes | - | ClickZetta username |
| `password` | string | Yes | - | ClickZetta password |
| `instance` | string | Yes | - | ClickZetta instance identifier |
| `workspace` | string | Yes | - | ClickZetta workspace name |
| `schema` | string | No | "PUBLIC" | Default schema name |
| `vcluster` | string | No | "DEFAULT_AP" | Virtual cluster name |
| `secure` | boolean | No | null | Enable secure connection |
| `hints` | object | No | {} | Additional connection hints |
| `extra` | object | No | {} | Extra connection parameters |

## Features

- **Full SQL Support**: Execute queries, DDL, DML operations
- **Metadata Discovery**: Automatic discovery of databases, schemas, tables, and views
- **Volume Integration**: Read files from ClickZetta volumes
- **Sample Data**: Extract sample rows for data profiling
- **Connection Management**: Automatic connection pooling and session management

## Usage

Once installed and configured, use the ClickZetta adapter with Datus:

```python
# Execute queries
result = agent.query("SELECT * FROM my_table LIMIT 10")

# Get table information
tables = agent.get_tables("my_schema")
```

## Volume Operations

The adapter supports reading files from ClickZetta volumes:

```python
# Read a file from a volume
content = connector.read_volume_file("volume:user://my_volume", "path/to/file.yaml")

# List files in a volume directory
files = connector.list_volume_files("volume:user://my_volume", "config/", suffixes=(".yaml", ".yml"))
```

## Connection Hints

You can customize ClickZetta connection behavior using hints:

```yaml
config:
  # ... other config
  hints:
    sdk.job.timeout: 600
    query_tag: "Datus Analytics Query"
    cz.storage.parquet.vector.index.read.memory.cache: "true"
```

## Error Handling

The adapter provides comprehensive error handling with detailed error messages for common issues:

- Connection failures
- Authentication errors
- Query execution errors
- Schema/workspace switching limitations

## Development

To contribute to this adapter:

1. Clone the repository
2. Install development dependencies: `pip install -e .[dev]`
3. Run tests:
   - `python test.py` (from project root)
   - Or: `cd tests && python run_tests.py`
4. Follow the existing code style and patterns

## Testing

This adapter includes comprehensive test coverage with multiple test types and execution modes.

### Test Structure

```text
tests/
├── unit/                     # Unit tests for individual components
├── integration/              # Integration tests with mocked dependencies
├── run_tests.py             # Main test runner with multiple modes
├── comprehensive_test.py     # Real connection testing script
└── conftest.py              # Shared test fixtures and configuration
```

### Running Tests

**Quick Start (from project root):**
```bash
# Run all tests
python test.py

# Run specific test types
python test.py --mode unit          # Unit tests only (fastest)
python test.py --mode integration   # Integration tests only
python test.py --mode all          # All tests
python test.py --mode coverage     # Tests with coverage report
```

**Advanced Usage (from tests/ directory):**
```bash
cd tests

# Basic test execution
python run_tests.py --mode unit
python run_tests.py --mode integration -v

# Real connection testing (requires credentials)
python comprehensive_test.py

# Direct pytest usage
pytest unit/                    # Unit tests
pytest integration/             # Integration tests
pytest -k "test_config"        # Specific test patterns
```

### Test Requirements

- **Unit Tests**: No external dependencies, run with mocked components
- **Integration Tests**: Mocked ClickZetta SDK, test connector logic
- **Real Connection Tests**: Require actual ClickZetta credentials

Set environment variables for real connection testing:
```bash
export CLICKZETTA_SERVICE="your-service.clickzetta.com"
export CLICKZETTA_USERNAME="your-username"
export CLICKZETTA_PASSWORD="your-password"
export CLICKZETTA_INSTANCE="your-instance"
export CLICKZETTA_WORKSPACE="your-workspace"
export CLICKZETTA_SCHEMA="your-schema"
export CLICKZETTA_VCLUSTER="your-vcluster"
```

### Test Coverage

- ✅ Configuration validation and error handling
- ✅ SQL query execution and result processing
- ✅ Metadata discovery (tables, views, schemas)
- ✅ Connection management and lifecycle
- ✅ Volume operations and file listing
- ✅ Error handling and exception cases

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](../../LICENSE) file for details.

## Support

For issues and questions:
- [GitHub Issues](https://github.com/datusai/Datus-adapters/issues)
- [Datus Documentation](https://docs.datus.ai/)
- [ClickZetta Documentation](https://www.yunqi.tech/documents)