# datus-postgresql

PostgreSQL database adapter for Datus.

## Installation

```bash
pip install datus-postgresql
```

## Usage

```python
from datus_postgresql import PostgreSQLConnector, PostgreSQLConfig

# Using config object
config = PostgreSQLConfig(
    host="localhost",
    port=5432,
    username="postgres",
    password="password",
    database="mydb",
    schema_name="public",
)

connector = PostgreSQLConnector(config)

# Or using dict
connector = PostgreSQLConnector({
    "host": "localhost",
    "port": 5432,
    "username": "postgres",
    "password": "password",
    "database": "mydb",
})

# Test connection
connector.test_connection()

# Execute queries
result = connector.execute({"sql_query": "SELECT * FROM users"})
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| host | str | "127.0.0.1" | PostgreSQL server host |
| port | int | 5432 | PostgreSQL server port |
| username | str | required | PostgreSQL username |
| password | str | "" | PostgreSQL password |
| database | str | None | Default database name |
| schema | str | "public" | Default schema name |
| sslmode | str | "prefer" | SSL mode |
| timeout_seconds | int | 30 | Connection timeout |

## License

Apache-2.0
