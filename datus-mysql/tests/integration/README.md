# Integration Tests

Integration tests require a real MySQL database to validate end-to-end functionality.

## Quick Start

```bash
# Start MySQL container
docker-compose up -d

# Run integration tests
uv run pytest tests/integration/ -m integration -v

# Stop MySQL
docker-compose down
```

## Setup

### Docker (Recommended)

MySQL container is pre-configured in `docker-compose.yml`:
- Image: `mysql:8.0`
- Database: `test`
- User: `test_user` / `test_password`
- Port: `3306`

```bash
# Start and wait for health check
docker-compose up -d
docker-compose ps  # Should show "healthy"

# View logs if needed
docker-compose logs mysql
```

### Manual Setup (Alternative)

```sql
CREATE DATABASE test;
CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'test_password';
GRANT ALL PRIVILEGES ON test.* TO 'test_user'@'localhost';
```

Set environment variables:
```bash
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=test_user
export MYSQL_PASSWORD=test_password
export MYSQL_DATABASE=test
```

## Test Coverage (20 tests)

| Category | Tests | What's Tested |
|----------|-------|---------------|
| Connection | 2 | Config object, dict config |
| Database Ops | 2 | List databases, filter system DBs |
| Table Metadata | 4 | Tables, views, DDL retrieval |
| Schema | 1 | Column info, types, constraints |
| SQL Execution | 5 | SELECT, INSERT, UPDATE, DELETE, DDL |
| Sample Data | 1 | Row sampling |
| Edge Cases | 3 | Special chars, empty results, errors |
| Utilities | 2 | Error handling, connection mgmt |

## Test Pattern

All tests use **dynamic table creation** to avoid conflicts:

```python
@pytest.mark.integration
def test_example(connector, config):
    # Unique table name per test run
    suffix = uuid.uuid4().hex[:8]
    table_name = f"test_xxx_{suffix}"

    # Create → Test → Cleanup
    connector.execute_ddl(f"CREATE TABLE {table_name} ...")
    try:
        # Run test
        result = connector.some_operation(table_name)
        assert result.success
    finally:
        # Always cleanup
        connector.execute_ddl(f"DROP TABLE IF EXISTS {table_name}")
```

## Acceptance Tests

A subset of 6 integration tests are marked as `acceptance` tests for quick validation:
- Connection test
- Database/table operations
- Schema retrieval
- SELECT and DDL execution

Run only acceptance integration tests:
```bash
uv run pytest tests/ -m "acceptance and integration" -v
```

## Troubleshooting

**Tests skipped?**
- Check MySQL status: `docker-compose ps`
- Wait for "healthy" status (up to 30s)
- View logs: `docker-compose logs mysql`

**Port 3306 in use?**
```bash
# Option 1: Stop local MySQL
brew services stop mysql  # macOS
sudo systemctl stop mysql # Linux

# Option 2: Change port in docker-compose.yml
ports:
  - "3307:3306"
# Then: export MYSQL_PORT=3307
```

**Clean slate?**
```bash
# Remove all data
docker-compose down -v
```

## CI/CD

Integration tests run automatically in GitHub Actions with MySQL service container.
