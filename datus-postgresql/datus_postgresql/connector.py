# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from typing import Any, Dict, List, Optional, Set, Union, override
from urllib.parse import quote_plus

from datus.schemas.base import TABLE_TYPE
from datus.tools.db_tools.base import list_to_in_str
from datus.utils.constants import DBType
from datus.utils.exceptions import DatusException, ErrorCode
from datus.utils.loggings import get_logger
from datus_sqlalchemy import SQLAlchemyConnector
from pydantic import BaseModel, Field
from sqlalchemy import text

from .config import PostgreSQLConfig

logger = get_logger(__name__)


class TableMetadataNames(BaseModel):
    """Metadata configuration for different PostgreSQL object types."""

    info_table: str = Field(..., description="INFORMATION_SCHEMA table name or pg_catalog view")
    table_types: Optional[List[str]] = Field(default=None, description="TABLE_TYPE values in INFORMATION_SCHEMA")


# Metadata configuration for PostgreSQL objects
METADATA_DICT: Dict[TABLE_TYPE, TableMetadataNames] = {
    "table": TableMetadataNames(
        info_table="tables",
        table_types=["BASE TABLE"],
    ),
    "view": TableMetadataNames(
        info_table="views",
    ),
    "mv": TableMetadataNames(
        info_table="pg_matviews",
    ),
}


def _get_metadata_config(table_type: TABLE_TYPE) -> TableMetadataNames:
    """Get metadata configuration for given table type."""
    if table_type not in METADATA_DICT:
        raise DatusException(ErrorCode.COMMON_FIELD_INVALID, f"Invalid table type '{table_type}'")
    return METADATA_DICT[table_type]


class PostgreSQLConnector(SQLAlchemyConnector):
    """PostgreSQL database connector."""

    def __init__(self, config: Union[PostgreSQLConfig, dict]):
        """
        Initialize PostgreSQL connector.

        Args:
            config: PostgreSQLConfig object or dict with configuration
        """
        # Handle config object or dict
        if isinstance(config, dict):
            config = PostgreSQLConfig(**config)
        elif not isinstance(config, PostgreSQLConfig):
            raise TypeError(f"config must be PostgreSQLConfig or dict, got {type(config)}")

        self.config = config
        self.host = config.host
        self.port = config.port
        self.username = config.username
        self.password = config.password
        database = config.database or "postgres"

        # URL encode password to handle special characters
        encoded_password = quote_plus(self.password) if self.password else ""

        # Build connection string
        connection_string = (
            f"postgresql+psycopg2://{self.username}:{encoded_password}@{self.host}:{self.port}/"
            f"{database}?sslmode={config.sslmode}"
        )

        super().__init__(connection_string, dialect=DBType.POSTGRESQL)
        self.database_name = database
        self.schema_name = config.schema_name or "public"

    # ==================== System Resources ====================

    @override
    def _sys_databases(self) -> Set[str]:
        """System databases to filter out."""
        return {"template0", "template1"}

    @override
    def _sys_schemas(self) -> Set[str]:
        """System schemas to filter out."""
        return {"pg_catalog", "information_schema", "pg_toast", "pg_temp_1", "pg_toast_temp_1"}

    # ==================== Utility Methods ====================

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Safely wrap identifiers with double quotes for PostgreSQL."""
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    # ==================== Metadata Retrieval ====================

    def _get_metadata(
        self,
        table_type: TABLE_TYPE = "table",
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
    ) -> List[Dict[str, str]]:
        """
        Get metadata for tables/views from INFORMATION_SCHEMA or pg_catalog.

        Args:
            table_type: Type of object (table, view, mv)
            catalog_name: Catalog name (unused in PostgreSQL)
            database_name: Database name (unused, uses current database)
            schema_name: Schema name to query

        Returns:
            List of metadata dictionaries
        """
        self.connect()
        schema_name = schema_name or self.schema_name

        # Get metadata configuration
        metadata_config = _get_metadata_config(table_type)

        if table_type == "mv":
            # Materialized views use pg_matviews
            if schema_name:
                where = f"schemaname = '{schema_name}'"
            else:
                where = f"{list_to_in_str('schemaname not in', list(self._sys_schemas()))}"

            query = f"""
                SELECT schemaname as table_schema, matviewname as table_name
                FROM pg_matviews
                WHERE {where}
            """
        else:
            # Tables and views use information_schema
            if schema_name:
                where = f"table_schema = '{schema_name}'"
            else:
                where = f"{list_to_in_str('table_schema not in', list(self._sys_schemas()))}"

            if table_type == "table":
                type_filter = list_to_in_str("and table_type in", metadata_config.table_types)
            else:
                type_filter = ""

            query = f"""
                SELECT table_schema, table_name
                FROM information_schema.{metadata_config.info_table}
                WHERE {where} {type_filter}
            """

        query_result = self._execute_pandas(query)

        # Format results
        result = []
        for i in range(len(query_result)):
            schema = query_result["table_schema"][i]
            tb_name = query_result["table_name"][i]
            result.append(
                {
                    "identifier": self.identifier(schema_name=schema, table_name=tb_name),
                    "catalog_name": "",
                    "database_name": self.database_name,
                    "schema_name": schema,
                    "table_name": tb_name,
                    "table_type": table_type,
                }
            )
        return result

    def _get_ddl(self, schema_name: str, table_name: str, object_type: str = "TABLE") -> str:
        """
        Get DDL for a table/view using pg_get_tabledef or reconstructing from metadata.

        Args:
            schema_name: Schema name
            table_name: Table name
            object_type: Object type (TABLE, VIEW, MATERIALIZED VIEW)

        Returns:
            DDL statement as string
        """
        full_name = self.full_name(schema_name=schema_name, table_name=table_name)

        if object_type == "VIEW":
            # Get view definition
            sql = f"""
                SELECT pg_get_viewdef('{schema_name}.{table_name}'::regclass, true) as definition
            """
            result = self._execute_pandas(sql)
            if not result.empty and result["definition"][0]:
                return f"CREATE VIEW {full_name} AS\n{result['definition'][0]}"
            return f"-- DDL not available for {full_name}"

        elif object_type == "MATERIALIZED VIEW":
            # Get materialized view definition
            sql = f"""
                SELECT definition
                FROM pg_matviews
                WHERE schemaname = '{schema_name}' AND matviewname = '{table_name}'
            """
            result = self._execute_pandas(sql)
            if not result.empty and result["definition"][0]:
                return f"CREATE MATERIALIZED VIEW {full_name} AS\n{result['definition'][0]}"
            return f"-- DDL not available for {full_name}"

        else:
            # For tables, reconstruct DDL from column info
            columns = self.get_schema(schema_name=schema_name, table_name=table_name)
            if not columns:
                return f"-- DDL not available for {full_name}"

            col_defs = []
            pk_cols = []
            for col in columns:
                col_def = f"    {self._quote_identifier(col['name'])} {col['type']}"
                if not col.get("nullable", True):
                    col_def += " NOT NULL"
                if col.get("default_value"):
                    col_def += f" DEFAULT {col['default_value']}"
                col_defs.append(col_def)
                if col.get("pk"):
                    pk_cols.append(col["name"])

            ddl = f"CREATE TABLE {full_name} (\n"
            ddl += ",\n".join(col_defs)
            if pk_cols:
                pk_names = ", ".join(self._quote_identifier(c) for c in pk_cols)
                ddl += f",\n    PRIMARY KEY ({pk_names})"
            ddl += "\n);"
            return ddl

    def _get_objects_with_ddl(
        self,
        table_type: TABLE_TYPE = "table",
        tables: Optional[List[str]] = None,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
    ) -> List[Dict[str, str]]:
        """
        Get metadata with DDL statements.

        Args:
            table_type: Type of object
            tables: Optional list of specific tables to retrieve
            catalog_name: Catalog name (unused)
            database_name: Database name (unused)
            schema_name: Schema name

        Returns:
            List of metadata dictionaries with DDL
        """
        result = []
        filter_tables = self._reset_filter_tables(tables, catalog_name, database_name, schema_name)

        object_type_map = {
            "table": "TABLE",
            "view": "VIEW",
            "mv": "MATERIALIZED VIEW",
        }
        object_type = object_type_map.get(table_type, "TABLE")

        for meta in self._get_metadata(table_type, catalog_name, database_name, schema_name):
            full_name = self.full_name(schema_name=meta["schema_name"], table_name=meta["table_name"])

            # Skip if not in filter list
            if filter_tables and full_name not in filter_tables:
                continue

            # Get DDL
            try:
                ddl = self._get_ddl(meta["schema_name"], meta["table_name"], object_type)
            except Exception as e:
                logger.warning(f"Could not get DDL for {full_name}: {e}")
                ddl = f"-- DDL not available for {meta['table_name']}"

            meta["definition"] = ddl
            result.append(meta)

        return result

    @override
    def get_tables(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        """Get list of table names."""
        return [meta["table_name"] for meta in self._get_metadata("table", catalog_name, database_name, schema_name)]

    @override
    def get_views(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        """Get list of view names."""
        return [meta["table_name"] for meta in self._get_metadata("view", catalog_name, database_name, schema_name)]

    @override
    def get_materialized_views(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[str]:
        """Get list of materialized view names."""
        return [meta["table_name"] for meta in self._get_metadata("mv", catalog_name, database_name, schema_name)]

    @override
    def get_tables_with_ddl(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", tables: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        """Get tables with DDL statements."""
        return self._get_objects_with_ddl("table", tables, catalog_name, database_name, schema_name)

    @override
    def get_views_with_ddl(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[Dict[str, str]]:
        """Get views with DDL statements."""
        return self._get_objects_with_ddl("view", None, catalog_name, database_name, schema_name)

    @override
    def get_schema(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Get table schema using INFORMATION_SCHEMA.

        Args:
            catalog_name: Catalog name (unused)
            database_name: Database name (unused)
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of column information dictionaries
        """
        if not table_name:
            return []

        schema_name = schema_name or self.schema_name

        # Use INFORMATION_SCHEMA to get schema with comments
        sql = f"""
            SELECT
                c.column_name as field,
                c.data_type as type,
                c.is_nullable as nullable,
                c.column_default as default_value,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk,
                pgd.description as comment
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = '{schema_name}'
                    AND tc.table_name = '{table_name}'
            ) pk ON c.column_name = pk.column_name
            LEFT JOIN pg_catalog.pg_statio_all_tables st
                ON st.schemaname = c.table_schema AND st.relname = c.table_name
            LEFT JOIN pg_catalog.pg_description pgd
                ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
            WHERE c.table_schema = '{schema_name}'
              AND c.table_name = '{table_name}'
            ORDER BY c.ordinal_position
        """
        query_result = self._execute_pandas(sql)

        result = []
        for i in range(len(query_result)):
            result.append(
                {
                    "cid": i,
                    "name": query_result["field"][i],
                    "type": query_result["type"][i],
                    "nullable": query_result["nullable"][i] == "YES",
                    "default_value": query_result["default_value"][i],
                    "pk": bool(query_result["is_pk"][i]),
                    "comment": query_result["comment"][i] if query_result["comment"][i] else None,
                }
            )
        return result

    # ==================== Database/Schema Management ====================

    @override
    def get_databases(self, catalog_name: str = "", include_sys: bool = False) -> List[str]:
        """Get list of databases."""
        sql = "SELECT datname FROM pg_database WHERE datistemplate = false"
        result = self._execute_pandas(sql)
        databases = result["datname"].tolist()

        if not include_sys:
            sys_dbs = self._sys_databases()
            databases = [db for db in databases if db not in sys_dbs]

        return databases

    @override
    def get_schemas(self, catalog_name: str = "", database_name: str = "", include_sys: bool = False) -> List[str]:
        """Get list of schemas in the current database."""
        sql = "SELECT schema_name FROM information_schema.schemata"
        result = self._execute_pandas(sql)
        schemas = result["schema_name"].tolist()

        if not include_sys:
            sys_schemas = self._sys_schemas()
            schemas = [s for s in schemas if s not in sys_schemas]

        return schemas

    @override
    def _sqlalchemy_schema(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> Optional[str]:
        """Get schema name for SQLAlchemy Inspector."""
        return schema_name or self.schema_name

    @override
    def do_switch_context(self, catalog_name: str = "", database_name: str = "", schema_name: str = ""):
        """Switch schema context using SET search_path."""
        if schema_name:
            with self.engine.connect() as conn:
                conn.execute(text(f"SET search_path TO {self._quote_identifier(schema_name)}"))
                conn.commit()
            self.schema_name = schema_name

    # ==================== Sample Data ====================

    def get_sample_rows(
        self,
        tables: Optional[List[str]] = None,
        top_n: int = 5,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        table_type: TABLE_TYPE = "table",
    ) -> List[Dict[str, str]]:
        """Get sample rows from tables."""
        # Delegate to base class for unsupported table types (e.g., "full")
        if table_type == "full" or table_type not in METADATA_DICT:
            return super().get_sample_rows(
                tables=tables,
                top_n=top_n,
                catalog_name=catalog_name,
                database_name=database_name,
                schema_name=schema_name,
                table_type=table_type,
            )

        self.connect()
        schema_name = schema_name or self.schema_name
        result = []

        # If specific tables provided, query those
        if tables:
            for table_name in tables:
                full_name = self.full_name(schema_name=schema_name, table_name=table_name)
                sql = f"SELECT * FROM {full_name} LIMIT {top_n}"
                df = self._execute_pandas(sql)
                if not df.empty:
                    result.append(
                        {
                            "identifier": self.identifier(schema_name=schema_name, table_name=table_name),
                            "catalog_name": "",
                            "database_name": self.database_name,
                            "schema_name": schema_name,
                            "table_name": table_name,
                            "sample_rows": df.to_csv(index=False),
                        }
                    )
            return result

        # Otherwise get metadata and query all tables
        metadata = self._get_metadata(table_type, "", "", schema_name)
        for meta in metadata:
            full_name = self.full_name(schema_name=meta["schema_name"], table_name=meta["table_name"])
            sql = f"SELECT * FROM {full_name} LIMIT {top_n}"
            df = self._execute_pandas(sql)
            if not df.empty:
                result.append(
                    {
                        "identifier": meta["identifier"],
                        "catalog_name": "",
                        "database_name": self.database_name,
                        "schema_name": meta["schema_name"],
                        "table_name": meta["table_name"],
                        "sample_rows": df.to_csv(index=False),
                    }
                )
        return result

    # ==================== Utility Methods ====================

    @override
    def identifier(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> str:
        """Generate a unique identifier for a table."""
        schema_name = schema_name or self.schema_name
        if schema_name:
            return f"{schema_name}.{table_name}"
        return table_name

    @override
    def full_name(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> str:
        """Build fully-qualified table name."""
        schema_name = schema_name or self.schema_name
        if schema_name:
            return f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"
        return self._quote_identifier(table_name)

    @override
    def _reset_filter_tables(
        self, tables: Optional[List[str]] = None, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[str]:
        """Reset filter tables with full names."""
        schema_name = schema_name or self.schema_name
        return super()._reset_filter_tables(tables, "", "", schema_name)
