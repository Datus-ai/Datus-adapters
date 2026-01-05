# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from unittest.mock import patch

import pytest
from datus.utils.exceptions import DatusException
from datus_postgresql import PostgreSQLConfig, PostgreSQLConnector


@pytest.mark.acceptance
def test_connector_initialization_with_config_object():
    """Test connector initialization with PostgreSQLConfig object."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_pass",
        database="testdb",
        schema_name="myschema",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)

        assert connector.config == config
        assert connector.host == "localhost"
        assert connector.port == 5432
        assert connector.username == "test_user"
        assert connector.password == "test_pass"
        assert connector.database_name == "testdb"
        assert connector.schema_name == "myschema"


@pytest.mark.acceptance
def test_connector_initialization_with_dict():
    """Test connector initialization with dict config."""
    config_dict = {
        "host": "192.168.1.100",
        "port": 5433,
        "username": "admin",
        "password": "secret",
        "database": "mydb",
        "schema_name": "custom_schema",
    }

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config_dict)

        assert connector.host == "192.168.1.100"
        assert connector.port == 5433
        assert connector.username == "admin"
        assert connector.password == "secret"
        assert connector.database_name == "mydb"
        assert connector.schema_name == "custom_schema"


def test_connector_initialization_invalid_type():
    """Test that connector raises TypeError for invalid config type."""
    with pytest.raises(TypeError, match="config must be PostgreSQLConfig or dict"):
        PostgreSQLConnector("invalid_config")


@pytest.mark.acceptance
def test_connector_connection_string_basic():
    """Test connection string generation with basic config."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="user",
        password="pass",
        database="db",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        PostgreSQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        assert "postgresql+psycopg2://user:pass@localhost:5432/db" in connection_string
        assert "sslmode=prefer" in connection_string


@pytest.mark.acceptance
def test_connector_connection_string_special_password():
    """Test connection string generation with special characters in password."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="user",
        password="p@ss!w0rd#$%",
        database="db",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        PostgreSQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        # Password should be URL encoded
        assert "p%40ss%21w0rd%23%24%25" in connection_string


def test_connector_connection_string_no_password():
    """Test connection string generation with empty password."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="user",
        password="",
        database="db",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        PostgreSQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        assert "postgresql+psycopg2://user:@localhost:5432/db" in connection_string


def test_connector_connection_string_no_database():
    """Test connection string generation without database uses 'postgres'."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="user",
        password="pass",
        database=None,
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        PostgreSQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        # Default database should be 'postgres'
        assert "postgresql+psycopg2://user:pass@localhost:5432/postgres" in connection_string


def test_connector_connection_string_custom_sslmode():
    """Test connection string with custom sslmode."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="user",
        password="pass",
        database="db",
        sslmode="require",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        PostgreSQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        assert "sslmode=require" in connection_string


@pytest.mark.acceptance
def test_sys_databases():
    """Test _sys_databases returns correct system databases."""
    config = PostgreSQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)
        sys_dbs = connector._sys_databases()

        assert sys_dbs == {"template0", "template1"}
        assert isinstance(sys_dbs, set)


def test_sys_schemas():
    """Test _sys_schemas returns correct system schemas."""
    config = PostgreSQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)
        sys_schemas = connector._sys_schemas()

        assert "pg_catalog" in sys_schemas
        assert "information_schema" in sys_schemas
        assert "pg_toast" in sys_schemas


@pytest.mark.acceptance
def test_quote_identifier_basic():
    """Test _quote_identifier with basic identifier."""
    assert PostgreSQLConnector._quote_identifier("table_name") == '"table_name"'


@pytest.mark.acceptance
def test_quote_identifier_with_double_quotes():
    """Test _quote_identifier escapes double quotes."""
    assert PostgreSQLConnector._quote_identifier('table"name') == '"table""name"'


def test_quote_identifier_with_multiple_double_quotes():
    """Test _quote_identifier escapes multiple double quotes."""
    assert PostgreSQLConnector._quote_identifier('ta"ble"name') == '"ta""ble""name"'


def test_quote_identifier_empty_string():
    """Test _quote_identifier with empty string."""
    assert PostgreSQLConnector._quote_identifier("") == '""'


def test_quote_identifier_special_characters():
    """Test _quote_identifier with special characters."""
    assert PostgreSQLConnector._quote_identifier("table-name_123") == '"table-name_123"'


@pytest.mark.acceptance
def test_full_name_with_schema():
    """Test full_name method with schema."""
    config = PostgreSQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)
        connector.schema_name = "public"
        full_name = connector.full_name(schema_name="myschema", table_name="mytable")

        assert full_name == '"myschema"."mytable"'


def test_full_name_with_default_schema():
    """Test full_name method uses default schema."""
    config = PostgreSQLConfig(username="user", schema_name="public")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)
        connector.schema_name = "public"
        full_name = connector.full_name(table_name="mytable")

        assert full_name == '"public"."mytable"'


def test_full_name_with_special_characters():
    """Test full_name with special characters (double quotes are escaped)."""
    config = PostgreSQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)
        connector.schema_name = "public"
        full_name = connector.full_name(schema_name='my"schema', table_name='my"table')

        assert full_name == '"my""schema"."my""table"'


def test_identifier_with_schema():
    """Test identifier method with schema."""
    config = PostgreSQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)
        connector.schema_name = "public"
        identifier = connector.identifier(schema_name="myschema", table_name="mytable")

        assert identifier == "myschema.mytable"


def test_identifier_with_default_schema():
    """Test identifier method uses default schema."""
    config = PostgreSQLConfig(username="user", schema_name="public")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)
        connector.schema_name = "public"
        identifier = connector.identifier(table_name="mytable")

        assert identifier == "public.mytable"


@pytest.mark.acceptance
def test_get_metadata_config_valid_table_type():
    """Test _get_metadata_config with valid table type."""
    from datus_postgresql.connector import _get_metadata_config

    config = _get_metadata_config("table")
    assert config.info_table == "tables"
    assert config.table_types == ["BASE TABLE"]


def test_get_metadata_config_view_type():
    """Test _get_metadata_config with view type."""
    from datus_postgresql.connector import _get_metadata_config

    config = _get_metadata_config("view")
    assert config.info_table == "views"


def test_get_metadata_config_mv_type():
    """Test _get_metadata_config with materialized view type."""
    from datus_postgresql.connector import _get_metadata_config

    config = _get_metadata_config("mv")
    assert config.info_table == "pg_matviews"


@pytest.mark.acceptance
def test_get_metadata_config_invalid_type():
    """Test _get_metadata_config with invalid table type."""
    from datus_postgresql.connector import _get_metadata_config

    with pytest.raises(DatusException, match="Invalid table type"):
        _get_metadata_config("invalid_type")


def test_connector_stores_config():
    """Test that connector stores the config object."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="user",
        password="pass",
        database="db",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)

        assert connector.config == config
        assert isinstance(connector.config, PostgreSQLConfig)


def test_connector_database_name_attribute():
    """Test that connector sets database_name attribute."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="user",
        password="pass",
        database="testdb",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)

        assert connector.database_name == "testdb"


def test_connector_database_name_default_when_none():
    """Test that database_name defaults to 'postgres' when config.database is None."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="user",
        password="pass",
        database=None,
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)

        assert connector.database_name == "postgres"


def test_connector_schema_name_attribute():
    """Test that connector sets schema_name attribute."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="user",
        password="pass",
        schema_name="custom_schema",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)

        assert connector.schema_name == "custom_schema"


def test_connector_schema_name_default():
    """Test that schema_name defaults to 'public'."""
    config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        username="user",
        password="pass",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = PostgreSQLConnector(config)

        assert connector.schema_name == "public"
