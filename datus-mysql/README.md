# datus-mysql

MySQL database adapter for Datus.

## Installation

```bash
pip install datus-mysql
```

This will automatically install the required dependencies:
- `datus-agent`
- `datus-sqlalchemy`
- `pymysql`

## Usage

The adapter is automatically registered with Datus when installed. Configure your database connection in your Datus configuration:

```yaml
database:
  type: mysql
  host: localhost
  port: 3306
  username: root
  password: your_password
  database: your_database
```

Or use programmatically:

```python
from datus_mysql import MySQLConnector, MySQLConfig

# Using config object
config = MySQLConfig(
    host="localhost",
    port=3306,
    username="root",
    password="your_password",
    database="mydb"
)
connector = MySQLConnector(config)

# Or using dict
connector = MySQLConnector({
    "host": "localhost",
    "port": 3306,
    "username": "root",
    "password": "your_password",
    "database": "mydb"
})

# Test connection
connector.test_connection()

# Execute query
result = connector.execute({"sql_query": "SELECT * FROM users LIMIT 10"})
print(result.sql_return)

# Get table list
tables = connector.get_tables(database_name="mydb")
print(f"Tables: {tables}")

# Get table schema
schema = connector.get_schema(database_name="mydb", table_name="users")
for column in schema:
    print(f"{column['name']}: {column['type']}")
```

## Features

- Full CRUD operations (SELECT, INSERT, UPDATE, DELETE)
- DDL execution (CREATE, ALTER, DROP)
- Metadata retrieval (tables, views, schemas)
- Sample data extraction
- Multiple result formats (pandas, arrow, csv, list)
- Connection pooling and management
- Comprehensive error handling

## Testing

### Quick Start

```bash
# Unit tests (no database required, < 0.1s)
uv run pytest tests/ -m "not integration" -v

# Acceptance tests (core functionality, < 1s)
uv run pytest tests/ -m "acceptance and not integration" -v

# All tests with coverage
uv run pytest tests/ -v --cov=datus_mysql --cov-report=term-missing
```

### Integration Tests (Requires MySQL)

```bash
# Start MySQL container
docker-compose up -d

# Run integration tests
uv run pytest tests/integration/ -m integration -v

# Run all acceptance tests (unit + integration)
uv run pytest tests/ -m acceptance -v

# Stop MySQL
docker-compose down
```

### Test Statistics

- **Unit Tests**: 50 tests (config, connector, identifiers)
- **Integration Tests**: 20 tests (connection, CRUD, DDL, metadata)
- **Acceptance Tests**: 21 tests (15 unit + 6 integration)
- **Total**: 70 tests

For more details, see [tests/integration/README.md](tests/integration/README.md).

## Development

### Setup

```bash
# Install dependencies
uv sync

# Install in editable mode
uv pip install -e .
```

### Running Tests

```bash
# Fast unit tests
uv run pytest tests/ -m "not integration" -v

# With coverage
uv run pytest tests/ --cov=datus_mysql --cov-report=html
open htmlcov/index.html
```

### Code Quality

```bash
# Format code
black datus_mysql tests
isort datus_mysql tests

# Lint
ruff check datus_mysql tests
flake8 datus_mysql tests
```

## Requirements

- Python >= 3.12
- MySQL >= 5.7 or MariaDB >= 10.2
- datus-agent >= 0.3.0
- datus-sqlalchemy >= 0.1.0
- pymysql >= 1.0.0

## License

Apache License 2.0
