# datus-clickhouse

ClickHouse database adapter for Datus.

## Installation

```bash
pip install datus-clickhouse
```

This will automatically install the required dependencies:
- `datus-agent`
- `datus-sqlalchemy`
- `clickhouse-sqlalchemy`

## Usage

The adapter is automatically registered with Datus when installed. Configure your database connection in your Datus configuration:

```yaml
database:
  type: clickhouse
  host: localhost
  port: 9000
  username: root
  password: your_password
  database: your_database
```

Or use programmatically:

```python
from datus_clickhouse import ClickHouseConfig,ClickHouseConnector

# Create connector
config = ClickHouseConfig(
    host="localhost",
    port=9000,
    username="root",
    password="your_password",
    database="mydb",
)

connector = ClickHouseConnector(config)

# Test connection
connector.test_connection()

# Execute query
result = connector.execute_query("SELECT * FROM users LIMIT 10")
print(result.sql_return)

# Get table list
tables = connector.get_tables()
print(f"Tables: {tables}")

# Get table schema
schema = connector.get_schema(table_name="users")
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

## Requirements

- Python >= 3.10
- ClickHouse >= 20.1
- datus-agent >= 0.3.0
- datus-sqlalchemy >= 0.1.0
- clickhouse-sqlalchemy >= 0.3.2

## License

Apache License 2.0
