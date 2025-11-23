# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""
Unit tests for Redshift connector.

These tests verify the basic functionality of the RedshiftConnector class.
Note: These tests require a running Redshift cluster with proper credentials.
"""

import os
import pytest
from datus_redshift import RedshiftConnector, RedshiftConfig


# Skip tests if Redshift credentials are not available
# Set these environment variables to run the tests:
# - REDSHIFT_HOST
# - REDSHIFT_USERNAME
# - REDSHIFT_PASSWORD
# - REDSHIFT_DATABASE (optional, defaults to 'dev')
pytestmark = pytest.mark.skipif(
    not os.getenv("REDSHIFT_HOST"),
    reason="Redshift credentials not available in environment variables"
)


@pytest.fixture
def redshift_config():
    """
    Create a Redshift configuration from environment variables.
    
    This fixture reads connection details from environment variables
    and creates a RedshiftConfig object for testing.
    """
    return RedshiftConfig(
        host=os.getenv("REDSHIFT_HOST"),
        username=os.getenv("REDSHIFT_USERNAME"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        database=os.getenv("REDSHIFT_DATABASE", "dev"),
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        schema=os.getenv("REDSHIFT_SCHEMA", "public"),
        ssl=True,
        timeout_seconds=30
    )


@pytest.fixture
def connector(redshift_config):
    """
    Create a RedshiftConnector instance for testing.
    
    This fixture creates a connector and ensures it's properly closed
    after the test completes.
    """
    # Create connector instance
    conn = RedshiftConnector(redshift_config)
    
    # Provide the connector to the test
    yield conn
    
    # Clean up: close the connection after test
    conn.close()


class TestRedshiftConfig:
    """Test cases for RedshiftConfig class."""
    
    def test_config_creation_with_required_fields(self):
        """Test that config can be created with only required fields."""
        config = RedshiftConfig(
            host="my-cluster.us-west-2.redshift.amazonaws.com",
            username="testuser",
            password="testpass"
        )
        
        # Verify required fields
        assert config.host == "my-cluster.us-west-2.redshift.amazonaws.com"
        assert config.username == "testuser"
        assert config.password == "testpass"
        
        # Verify default values
        assert config.port == 5439
        assert config.database is None
        assert config.schema_name is None
        assert config.timeout_seconds == 30
        assert config.ssl is True
        assert config.iam is False
    
    def test_config_creation_with_all_fields(self):
        """Test that config can be created with all fields."""
        config = RedshiftConfig(
            host="my-cluster.us-west-2.redshift.amazonaws.com",
            username="testuser",
            password="testpass",
            port=5440,
            database="testdb",
            schema="testschema",
            timeout_seconds=60,
            ssl=False,
            iam=True,
            cluster_identifier="my-cluster",
            region="us-west-2",
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        )
        
        # Verify all fields are set correctly
        assert config.host == "my-cluster.us-west-2.redshift.amazonaws.com"
        assert config.username == "testuser"
        assert config.password == "testpass"
        assert config.port == 5440
        assert config.database == "testdb"
        assert config.schema_name == "testschema"
        assert config.timeout_seconds == 60
        assert config.ssl is False
        assert config.iam is True
        assert config.cluster_identifier == "my-cluster"
        assert config.region == "us-west-2"
    
    def test_config_with_dict(self):
        """Test that RedshiftConfig can be created from dict."""
        config_dict = {
            "host": "my-cluster.us-west-2.redshift.amazonaws.com",
            "username": "testuser",
            "password": "testpass",
            "database": "testdb"
        }
        
        # Test that config can be created from dict (no connection attempted)
        config = RedshiftConfig(**config_dict)
        assert config is not None
        assert config.host == "my-cluster.us-west-2.redshift.amazonaws.com"
        assert config.username == "testuser"
        assert config.password == "testpass"
        assert config.database == "testdb"


class TestRedshiftConnector:
    """Test cases for RedshiftConnector class."""
    
    def test_connector_creation(self, redshift_config):
        """Test that connector can be created successfully."""
        connector = RedshiftConnector(redshift_config)
        assert connector is not None
        assert connector.get_type() == "redshift"
        connector.close()
    
    def test_connection(self, connector):
        """Test that connection works."""
        result = connector.test_connection()
        
        # Verify connection result
        assert result["success"] is True
        assert "message" in result
        assert result["message"] == "Connection successful"
    
    def test_simple_query(self, connector):
        """Test executing a simple query."""
        result = connector.execute_query("SELECT 1 as test_column")
        
        # Verify query result
        assert result.success is True
        assert result.row_count == 1
        assert "test_column" in result.sql_return
    
    def test_query_with_parameters(self, connector):
        """Test executing a query with parameters."""
        result = connector.execute_arrow("SELECT %s as value", [42])
        
        # Verify query result
        assert result.success is True
        assert result.row_count == 1
    
    def test_get_databases(self, connector):
        """Test getting list of databases."""
        databases = connector.get_databases(include_sys=False)
        
        # Should return at least one database (the one we're connected to)
        assert isinstance(databases, list)
        assert len(databases) > 0
        assert connector.database_name in databases
    
    def test_get_schemas(self, connector):
        """Test getting list of schemas."""
        schemas = connector.get_schemas(include_sys=False)
        
        # Should return at least the public schema
        assert isinstance(schemas, list)
        assert len(schemas) > 0
        assert "public" in schemas
    
    def test_get_tables(self, connector):
        """Test getting list of tables."""
        tables = connector.get_tables(schema_name="public")
        
        # Should return a list (may be empty if no tables in public schema)
        assert isinstance(tables, list)
    
    def test_execute_different_formats(self, connector):
        """Test executing query with different output formats."""
        sql = "SELECT 1 as num, 'test' as str"
        
        # Test CSV format
        result_csv = connector.execute_query(sql, result_format="csv")
        assert result_csv.success is True
        assert result_csv.result_format == "csv"
        assert isinstance(result_csv.sql_return, str)
        
        # Test pandas format
        result_pandas = connector.execute_query(sql, result_format="pandas")
        assert result_pandas.success is True
        assert result_pandas.result_format == "pandas"
        
        # Test arrow format
        result_arrow = connector.execute_query(sql, result_format="arrow")
        assert result_arrow.success is True
        assert result_arrow.result_format == "arrow"
        
        # Test list format
        result_list = connector.execute_query(sql, result_format="list")
        assert result_list.success is True
        assert result_list.result_format == "list"
        assert isinstance(result_list.sql_return, list)
    
    def test_error_handling(self, connector):
        """Test that errors are handled properly."""
        # Execute invalid SQL
        result = connector.execute_query("SELECT * FROM nonexistent_table_xyz")
        
        # Should return error result (not raise exception)
        assert result.success is False
        assert result.error is not None
        assert len(result.error) > 0


class TestRedshiftMetadata:
    """Test cases for metadata retrieval."""
    
    def test_full_name_generation(self, connector):
        """Test generating fully qualified table names."""
        # With database and schema (three-part qualification for cross-database queries)
        full_name = connector.full_name(
            database_name="mydb",
            schema_name="myschema",
            table_name="mytable"
        )
        assert full_name == '"mydb"."myschema"."mytable"'
        
        # With schema only (two-part qualification)
        full_name = connector.full_name(
            schema_name="myschema",
            table_name="mytable"
        )
        assert full_name == '"myschema"."mytable"'
        
        # Without schema (single table name)
        full_name = connector.full_name(
            table_name="mytable"
        )
        assert full_name == '"mytable"'
    
    def test_identifier_generation(self, connector):
        """Test generating table identifiers."""
        identifier = connector.identifier(
            database_name="mydb",
            schema_name="myschema",
            table_name="mytable"
        )
        assert "mydb" in identifier
        assert "myschema" in identifier
        assert "mytable" in identifier


class TestSQLInjectionPrevention:
    """Test cases for SQL injection prevention."""
    
    def test_valid_schema_names(self, connector):
        """Test that valid schema names are accepted."""
        # These should all work without raising errors
        valid_names = [
            "public",
            "my_schema",
            "schema123",
            "Schema_With_Underscores",
            "_private_schema",
            "schema$with$dollar",
        ]
        
        for schema_name in valid_names:
            # Should not raise ValueError
            try:
                # We're just testing validation, the actual switch may fail if schema doesn't exist
                # But it should not fail with ValueError for valid names
                from datus_redshift.connector import _validate_sql_identifier
                _validate_sql_identifier(schema_name, "schema")
            except ValueError as e:
                pytest.fail(f"Valid schema name '{schema_name}' was rejected: {e}")
    
    def test_invalid_schema_names(self):
        """Test that invalid schema names are rejected."""
        from datus_redshift.connector import _validate_sql_identifier
        
        # These should all raise ValueError
        invalid_names = [
            "schema; DROP TABLE users--",  # SQL injection attempt
            "schema' OR '1'='1",           # SQL injection attempt
            "schema with spaces",          # Spaces not allowed
            "123schema",                   # Cannot start with digit
            "schema-name",                 # Hyphens not allowed (only underscore)
            "schema.name",                 # Dots not allowed
            "schema;name",                 # Semicolons not allowed
            "schema'name",                 # Quotes not allowed
            "a" * 128,                     # Too long (>127 chars)
        ]
        
        for invalid_name in invalid_names:
            with pytest.raises(ValueError, match="Invalid"):
                _validate_sql_identifier(invalid_name, "schema")
    
    def test_empty_identifier_allowed(self):
        """Test that empty identifiers are allowed (means 'not specified')."""
        from datus_redshift.connector import _validate_sql_identifier
        
        # Empty string should not raise error
        _validate_sql_identifier("", "schema")
        _validate_sql_identifier(None, "schema") if None else None  # Handle None gracefully


def test_manual_connection():
    """
    Manual test function that can be run independently.
    
    This test demonstrates how to use the connector manually
    without pytest fixtures. Uncomment and run if you want to
    test with your own credentials.
    """
    # Uncomment and fill in your credentials to test manually:
    """
    config = RedshiftConfig(
        host="your-cluster.region.redshift.amazonaws.com",
        username="your_username",
        password="your_password",
        database="your_database",
        schema="public",
        port=5439,
        ssl=True
    )
    
    connector = RedshiftConnector(config)
    
    try:
        # Test connection
        print("Testing connection...")
        result = connector.test_connection()
        print(f"Connection test: {result}")
        
        # Get databases
        print("\nGetting databases...")
        databases = connector.get_databases()
        print(f"Databases: {databases}")
        
        # Get schemas
        print("\nGetting schemas...")
        schemas = connector.get_schemas()
        print(f"Schemas: {schemas}")
        
        # Get tables
        print("\nGetting tables in public schema...")
        tables = connector.get_tables(schema_name="public")
        print(f"Tables: {tables}")
        
        # Execute query
        print("\nExecuting simple query...")
        result = connector.execute_query("SELECT current_database(), current_schema()")
        print(f"Query result: {result.sql_return}")
        
    finally:
        connector.close()
        print("\nConnection closed.")
    """
    pass


if __name__ == "__main__":
    """
    Run tests from command line.
    
    Usage:
        # Run all tests with pytest
        pytest test_connector.py -v
        
        # Run specific test
        pytest test_connector.py::TestRedshiftConfig::test_config_creation_with_required_fields -v
        
        # Run with coverage
        pytest test_connector.py --cov=datus_redshift --cov-report=html
    """
    # Uncomment to run manual test
    # test_manual_connection()
    
    # Run pytest
    pytest.main([__file__, "-v"])

