# Contributing to Datus Database Adapters

Thank you for your interest in contributing to Datus Database Adapters! This document provides guidelines and requirements for contributing to this project.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Environment Setup](#development-environment-setup)
- [Code Standards](#code-standards)
- [Testing Requirements](#testing-requirements)
- [Submitting Changes](#submitting-changes)
- [Creating a New Adapter](#creating-a-new-adapter)

## Getting Started

Before contributing, please:

1. Check existing [issues](https://github.com/Datus-ai/datus-db-adapters/issues) and [pull requests](https://github.com/Datus-ai/datus-db-adapters/pulls)
2. For major changes, open an issue first to discuss your proposed changes
3. Fork the repository and create a feature branch from `main`

## Development Environment Setup

### Prerequisites

- Python >= 3.12
- `uv` (recommended) or `pip`
- `git`
- `pre-commit`

### Setting up the Development Environment

#### Using uv (Recommended)

```bash
# Clone the repository
git clone https://github.com/Datus-ai/datus-db-adapters.git
cd datus-db-adapters

# Install all adapters in development mode
uv sync

# Install pre-commit hooks
uv run pre-commit install
```

#### Using pip

```bash
# Clone the repository
git clone https://github.com/Datus-ai/datus-db-adapters.git
cd datus-db-adapters

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install development dependencies
pip install -r requirements-dev.txt

# Install adapters in editable mode
pip install -e datus-sqlalchemy
pip install -e datus-mysql
pip install -e datus-starrocks
pip install -e datus-snowflake
pip install -e datus-clickzetta
pip install -e datus-redshift

# Install pre-commit hooks
pre-commit install
```

## Code Standards

### Code Quality Tools

This project uses the following tools to ensure code quality:

- **Black**: Code formatting (line length: 120)
- **Flake8**: Linting and style checking
- **isort**: Import sorting
- **pytest**: Testing framework

### Running Code Quality Checks

#### Pre-commit Hooks (Automatic)

Pre-commit hooks will automatically run when you commit changes:

```bash
git commit -m "Your commit message"
```

#### Manual Checks

```bash
# Format code with Black
black --line-length=120 datus-<adapter>/

# Check with Flake8
flake8 --max-line-length=120 --extend-ignore=E203,W503 datus-<adapter>/

# Sort imports with isort
isort --profile=black --line-length=120 datus-<adapter>/

# Run all pre-commit checks
pre-commit run --all-files
```

### Code Style Guidelines

1. **Line Length**: Maximum 120 characters
2. **Imports**: Sorted using isort with Black profile
3. **Docstrings**: Use Google-style docstrings for all public functions and classes
4. **Type Hints**: Use type hints for function signatures
5. **Comments**: Write clear, concise comments in English
6. **Naming Conventions**:
   - Classes: `PascalCase`
   - Functions/methods: `snake_case`
   - Constants: `UPPER_CASE`
   - Private methods: `_leading_underscore`

### Example Code Style

```python
from typing import Optional, Dict, Any

from datus.tools.db_tools.base import BaseSqlConnector
from datus.tools.db_tools.result import ExecuteSQLResult


class MyDatabaseConnector(BaseSqlConnector):
    """Connector for MyDatabase.

    Args:
        host: Database host address
        port: Database port number
        username: Database username
        password: Database password
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: Optional[str] = None,
    ) -> None:
        super().__init__(dialect="mydatabase")
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._connection = None

    def execute_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> ExecuteSQLResult:
        """Execute a SQL query and return results.

        Args:
            sql: SQL query string to execute
            params: Optional query parameters

        Returns:
            ExecuteSQLResult containing query results

        Raises:
            ConnectionError: If database connection fails
            QueryError: If query execution fails
        """
        # Implementation here
        pass
```

## Testing Requirements

### Test Structure

Each adapter should have tests in the following structure:

```
datus-<adapter>/
└── tests/
    ├── __init__.py
    ├── test_connector.py    # Unit tests for core connector functionality
    ├── test_metadata.py     # Unit tests for metadata operations
    ├── test_operations.py   # Unit tests for CRUD operations
    └── integration/         # Integration tests
        ├── __init__.py
        ├── test_integration.py
        └── README.md        # Environment setup instructions
```

**Note**: Both unit tests and integration tests will run in CI. Integration tests should use Docker containers or similar to provide test databases.

### Running Tests

```bash
# Run all tests for a specific adapter (unit + integration, CI runs this)
pytest datus-<adapter>/tests/

# Run tests with coverage
pytest --cov=datus_<adapter> datus-<adapter>/tests/

# Run unit tests only
pytest datus-<adapter>/tests/ --ignore=datus-<adapter>/tests/integration/
# Or using markers:
pytest datus-<adapter>/tests/ -m "not integration"

# Run integration tests only
pytest datus-<adapter>/tests/integration/
# Or using markers:
pytest datus-<adapter>/tests/ -m integration
```

**Note**: Integration tests use the `@pytest.mark.integration` decorator and should be configured in `pytest.ini` or `pyproject.toml`.

### Test Requirements

1. **Unit Tests** (Required, runs in CI)
   - Test individual components and methods without external dependencies
   - Must pass in CI before merging
   - Should use mocks/stubs for database connections
   - Aim for at least 80% code coverage
2. **Integration Tests** (Required, runs in CI)
   - Test actual database connections and end-to-end workflows
   - Must pass in CI before merging (or skip gracefully if database unavailable)
   - Use Docker containers or similar to provide test databases in CI
   - Mark tests with `@pytest.mark.integration` decorator
   - Tests should auto-skip if database connection fails (use pytest.skip)
   - Document required database versions and configurations
   - Include steps to reproduce the test environment locally (e.g., Docker Compose, SQL scripts)
   - Create a `tests/integration/README.md` with setup instructions
   - Ensure tests can be run independently and are reproducible
3. **Test Data**: Use fixtures for test data, never hard-code credentials

### Example Tests

#### Unit Test Example

```python
import pytest
from unittest.mock import Mock, patch
from datus_mydatabase import MyDatabaseConnector


@pytest.fixture
def connector_config():
    return {
        "host": "localhost",
        "port": 3306,
        "username": "test_user",
        "password": "test_pass",
        "database": "test_db",
    }


def test_connector_initialization(connector_config):
    """Test that connector initializes correctly."""
    connector = MyDatabaseConnector(**connector_config)
    assert connector.host == "localhost"
    assert connector.port == 3306


@patch('datus_mydatabase.connector.create_engine')
def test_execute_query(mock_engine, connector_config):
    """Test query execution with mocked database."""
    mock_result = Mock()
    mock_engine.return_value.execute.return_value = mock_result

    connector = MyDatabaseConnector(**connector_config)
    result = connector.execute_query("SELECT 1")
    assert result is not None
```

#### Integration Test Example

```python
import pytest
import os
from datus_mydatabase import MyDatabaseConnector


@pytest.fixture
def integration_connector_config():
    """Get config from environment variables."""
    return {
        "host": os.getenv("TEST_DB_HOST", "localhost"),
        "port": int(os.getenv("TEST_DB_PORT", "3306")),
        "username": os.getenv("TEST_DB_USER", "test_user"),
        "password": os.getenv("TEST_DB_PASSWORD", "test_pass"),
        "database": os.getenv("TEST_DB_NAME", "test_db"),
    }


@pytest.mark.integration
def test_real_database_connection(integration_connector_config):
    """Test actual database connection."""
    try:
        connector = MyDatabaseConnector(**integration_connector_config)
        result = connector.execute_query("SELECT 1")
        assert result is not None
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


@pytest.mark.integration
def test_get_tables(integration_connector_config):
    """Test retrieving tables from real database."""
    try:
        connector = MyDatabaseConnector(**integration_connector_config)
        tables = connector.get_tables("test_db")
        assert isinstance(tables, list)
    except Exception as e:
        pytest.skip(f"Database not available: {e}")
```

## Submitting Changes

### Pull Request Process

1. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Your Changes**
   - Write clean, well-documented code
   - Add or update tests as needed
   - Update documentation if applicable

3. **Run Quality Checks**
   ```bash
   # Format code
   black --line-length=120 .
   isort --profile=black --line-length=120 .

   # Run tests
   pytest

   # Run pre-commit checks
   pre-commit run --all-files
   ```

4. **Commit Your Changes**
   ```bash
   git add .
   git commit -m "Add support for XYZ feature"
   ```

5. **Push and Create Pull Request**
   ```bash
   git push origin feature/your-feature-name
   ```

   Then create a pull request on GitHub with:
   - Clear title describing the change
   - Detailed description of what changed and why
   - References to related issues (if any)
   - Screenshots or examples (if applicable)

### Pull Request Requirements

All pull requests must:

- ✅ Pass all CI checks (format, lint, tests)
- ✅ Include appropriate tests
- ✅ Update documentation if needed
- ✅ Follow the code style guidelines
- ✅ Have a clear, descriptive title
- ✅ Include a detailed description

### CI Checks

Your pull request will automatically run the following checks:

1. **Title Check**: Ensures PR title follows conventions
2. **Format Check**: Verifies code formatting with Black
3. **Lint Check**: Checks code quality with Flake8
4. **Import Check**: Verifies import sorting with isort
5. **Tests**: Runs all unit and integration test suites to ensure functionality

## Creating a New Adapter

### Step 1: Choose Base Layer

Decide which base to inherit from:

- **SQLAlchemy-based databases**: Inherit from `datus-sqlalchemy`
- **Native SDK databases**: Inherit from `BaseSqlConnector`

### Step 2: Create Package Structure

```bash
# Create new adapter directory
mkdir datus-<database>
cd datus-<database>

# Create package structure
mkdir -p datus_<database>
mkdir -p tests/integration
touch datus_<database>/__init__.py
touch datus_<database>/config.py
touch datus_<database>/connector.py
touch tests/__init__.py
touch tests/test_connector.py
touch tests/test_metadata.py
touch tests/test_operations.py
touch tests/integration/__init__.py
touch tests/integration/test_integration.py
touch tests/integration/README.md
touch README.md
touch pyproject.toml
touch docker-compose.yml
```

### Step 3: Implement Core Components

#### 3.1 Configuration (`config.py`)

```python
from pydantic import BaseModel, Field


class MyDatabaseConfig(BaseModel):
    """Configuration for MyDatabase adapter."""

    host: str = Field(..., description="Database host")
    port: int = Field(default=5432, description="Database port")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    database: str = Field(..., description="Database name")
```

#### 3.2 Connector (`connector.py`)

```python
from typing import Optional, Dict, Any
from datus.tools.db_tools.base import BaseSqlConnector
from datus.tools.db_tools.result import ExecuteSQLResult


class MyDatabaseConnector(BaseSqlConnector):
    """Connector for MyDatabase."""

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(dialect="mydatabase")
        # Initialize connection

    def execute_query(self, sql: str) -> ExecuteSQLResult:
        """Execute SQL query."""
        # Implementation
        pass

    def get_databases(self) -> list[str]:
        """Get list of databases."""
        # Implementation
        pass

    def get_tables(self, database: str, schema: Optional[str] = None) -> list[str]:
        """Get list of tables."""
        # Implementation
        pass
```

#### 3.3 Registration (`__init__.py`)

```python
from datus.tools.db_tools import connector_registry
from .connector import MyDatabaseConnector


def register():
    """Register MyDatabase adapter with Datus."""
    connector_registry.register("mydatabase", MyDatabaseConnector)


# Auto-register when package is imported
register()
```

### Step 4: Configure `pyproject.toml`

```toml
[project]
name = "datus-mydatabase"
version = "0.1.0"
description = "MyDatabase adapter for Datus"
readme = "README.md"
requires-python = ">=3.12"
license = {text = "Apache-2.0"}
authors = [
    {name = "DatusAI", email = "support@datus.ai"}
]
keywords = ["datus", "database", "mydatabase", "adapter"]

dependencies = [
    "datus-agent>=0.2.0",
    # Add database-specific dependencies
    "mydatabase-driver>=1.0.0",
]

[project.urls]
Homepage = "https://github.com/Datus-ai/datus-db-adapters"
Repository = "https://github.com/Datus-ai/datus-db-adapters"
Issues = "https://github.com/Datus-ai/datus-db-adapters/issues"

[project.entry-points."datus.adapters"]
mydatabase = "datus_mydatabase:register"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["datus_mydatabase"]

[tool.pytest.ini_options]
markers = [
    "integration: marks tests as integration tests (deselect with '-m \"not integration\"')",
]
testpaths = ["tests"]
```

### Step 5: Write Documentation

Create a comprehensive `README.md` with:

- Overview and features
- Installation instructions
- Configuration examples
- Usage examples
- Limitations (if any)

### Step 6: Add Tests

Write comprehensive unit tests covering:

- Connection initialization (mocked)
- Query execution (mocked)
- Metadata retrieval (mocked)
- Error handling
- Edge cases

Add integration tests with:

- Real database connection tests (using Docker containers)
- End-to-end workflow tests
- A `tests/integration/README.md` with environment setup instructions
- Docker Compose configuration for local testing
- Proper test skipping when database is unavailable

### Step 7: Update Workspace

Add your adapter to the workspace in the root `pyproject.toml`:

```toml
[tool.uv.workspace]
members = [
    # ... existing adapters
    "datus-mydatabase"
]
```

### Step 8: Submit for Review

1. Ensure all tests pass
2. Ensure code quality checks pass
3. Update root README.md if needed
4. Submit a pull request with detailed description
5. Address Code Rabbit review comments
   - Code Rabbit will automatically review your PR
   - All review comments must be resolved before merging
   - Respond to feedback and make necessary changes

## Getting Help

If you have questions or need help:

- Open an [issue](https://github.com/Datus-ai/datus-db-adapters/issues)
- Check existing documentation
- Review example adapters in this repository

## Code of Conduct

Please note that this project follows a Code of Conduct. By participating in this project, you agree to abide by its terms.

## License

By contributing to this project, you agree that your contributions will be licensed under the Apache License 2.0.

---

Thank you for contributing to Datus Database Adapters!
