# Datus Redshift Adapter

Amazon Redshift adapter for [Datus Agent](https://github.com/Datus-ai/Datus-agent), enabling seamless integration with Redshift data warehouses.

## Features

- ✅ Full support for Redshift databases, schemas, tables, views, and materialized views
- ✅ Efficient metadata retrieval using PostgreSQL system catalogs
- ✅ Multiple authentication methods (username/password and IAM)
- ✅ SSL/TLS secure connections
- ✅ Arrow-based query execution for high performance
- ✅ Comprehensive error handling and logging

## Installation

```bash
pip install datus-redshift
```

Or install from source:

```bash
cd datus-redshift
pip install -e .
```

## Usage

### Basic Connection (Username/Password)

```python
from datus_redshift import RedshiftConnector, RedshiftConfig

# Create configuration
config = RedshiftConfig(
    host="my-cluster.abc123.us-west-2.redshift.amazonaws.com",
    username="admin",
    password="MySecurePassword123",
    database="my_database",
    schema="public",
    port=5439,
    ssl=True
)

# Create connector instance
connector = RedshiftConnector(config)

# Test connection
result = connector.test_connection()
print(result)

# Execute a query
result = connector.execute_query("SELECT * FROM my_table LIMIT 10")
print(result.sql_return)

# Close connection when done
connector.close()
```

### IAM Authentication

```python
config = RedshiftConfig(
    host="my-cluster.abc123.us-west-2.redshift.amazonaws.com",
    username="iam_user",
    database="my_database",
    iam=True,
    cluster_identifier="my-cluster",
    region="us-west-2",
    access_key_id="YOUR_ACCESS_KEY",
    secret_access_key="YOUR_SECRET_KEY"
)

connector = RedshiftConnector(config)
```

### Using with Datus Agent

Once installed, the Redshift adapter is automatically available in Datus Agent:

```python
from datus.tools.db_tools import get_connector

# Datus will automatically use RedshiftConnector for redshift connections
connector = get_connector(
    dialect="redshift",
    config={
        "host": "my-cluster.abc123.us-west-2.redshift.amazonaws.com",
        "username": "admin",
        "password": "MySecurePassword123",
        "database": "my_database"
    }
)
```

## Common Operations

### List Databases

```python
databases = connector.get_databases()
print(f"Available databases: {databases}")
```

### List Schemas

```python
schemas = connector.get_schemas()
print(f"Available schemas: {schemas}")
```

### List Tables

```python
tables = connector.get_tables(schema_name="public")
print(f"Tables in public schema: {tables}")
```

### Get Table Schema

```python
schema_info = connector.get_schema(
    schema_name="public",
    table_name="my_table"
)
print(f"Table columns: {schema_info}")
```

### Execute Queries with Different Formats

```python
# Get results as CSV
result = connector.execute_query("SELECT * FROM my_table", result_format="csv")
print(result.sql_return)

# Get results as pandas DataFrame
result = connector.execute_query("SELECT * FROM my_table", result_format="pandas")
df = result.sql_return

# Get results as Arrow table (best for large datasets)
result = connector.execute_query("SELECT * FROM my_table", result_format="arrow")
arrow_table = result.sql_return

# Get results as list of dictionaries
result = connector.execute_query("SELECT * FROM my_table", result_format="list")
rows = result.sql_return
```

### Get Sample Data

```python
# Get sample rows from all tables in a schema
samples = connector.get_sample_rows(
    schema_name="public",
    top_n=5
)

# Get sample rows from specific tables
samples = connector.get_sample_rows(
    schema_name="public",
    tables=["table1", "table2"],
    top_n=10
)
```

### Working with Views and Materialized Views

```python
# List views
views = connector.get_views(schema_name="public")

# List materialized views
mvs = connector.get_materialized_views(schema_name="public")

# Get view definitions with DDL
views_with_ddl = connector.get_views_with_ddl(schema_name="public")
for view in views_with_ddl:
    print(f"View: {view['table_name']}")
    print(f"DDL: {view['definition']}")
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | str | Required | Redshift cluster endpoint |
| `username` | str | Required | Username for authentication |
| `password` | str | Required | Password for authentication |
| `port` | int | 5439 | Redshift port |
| `database` | str | None | Default database to connect to |
| `schema` | str | None | Default schema (uses 'public' if not specified) |
| `timeout_seconds` | int | 30 | Connection timeout in seconds |
| `ssl` | bool | True | Enable SSL/TLS connection |
| `iam` | bool | False | Use IAM authentication |
| `cluster_identifier` | str | None | Cluster ID for IAM auth |
| `region` | str | None | AWS region for IAM auth |
| `access_key_id` | str | None | AWS access key for IAM auth |
| `secret_access_key` | str | None | AWS secret key for IAM auth |

## Requirements

- Python >= 3.8
- datus-agent >= 0.2.1
- redshift_connector >= 2.0.0
- pyarrow (installed with datus-agent)
- pandas (installed with datus-agent)

## Development

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
```

### Code Structure

```
datus-redshift/
├── datus_redshift/
│   ├── __init__.py       # Module initialization and registration
│   ├── config.py         # Configuration class (RedshiftConfig)
│   └── connector.py      # Main connector implementation (RedshiftConnector)
├── tests/
│   ├── __init__.py
│   └── test_connector.py # Unit tests
├── pyproject.toml        # Package configuration
└── README.md            # This file
```

## Troubleshooting

### Connection Issues

1. **Timeout errors**: Increase `timeout_seconds` in the configuration
2. **SSL errors**: Try setting `ssl=False` if your cluster doesn't require SSL
3. **IAM auth fails**: Verify your AWS credentials and cluster identifier are correct

### Query Performance

1. Use `result_format="arrow"` for large result sets (most efficient)
2. Always specify schema names to avoid scanning all schemas
3. Use LIMIT clauses for exploratory queries

## Contributing

Contributions are welcome! Please see the main [Datus-adapters repository](https://github.com/Datus-ai/Datus-adapters) for contribution guidelines.

## License

Apache License 2.0 - See [LICENSE](../LICENSE) file for details.

## Support

- GitHub Issues: [Report bugs or request features](https://github.com/Datus-ai/Datus-adapters/issues)
- Documentation: [Datus Agent Docs](https://docs.datus.ai/)
- Slack Community: [Join Datus Slack](https://join.slack.com/t/datus-ai/shared_invite/...)

## Related Projects

- [datus-snowflake](../datus-snowflake) - Snowflake adapter
- [datus-mysql](../datus-mysql) - MySQL adapter
- [datus-starrocks](../datus-starrocks) - StarRocks adapter
- [Datus Agent](https://github.com/Datus-ai/Datus-agent) - Main Datus framework

