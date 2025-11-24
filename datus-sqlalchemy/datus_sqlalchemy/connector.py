# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from typing import Any, Dict, Iterator, List, Literal, Optional, Tuple, override

from datus.schemas.base import TABLE_TYPE
from datus.schemas.node_models import ExecuteSQLResult
from datus.tools.db_tools.base import BaseSqlConnector
from datus.tools.db_tools.config import ConnectionConfig
from datus.utils.constants import DBType, SQLType
from datus.utils.exceptions import DatusException, ErrorCode
from datus.utils.loggings import get_logger
from datus.utils.sql_utils import parse_context_switch, parse_sql_type
from pandas import DataFrame
from pyarrow import Table
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Inspector
from sqlalchemy.exc import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NoSuchTableError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    SQLAlchemyError,
    TimeoutError,
)

logger = get_logger(__name__)


class SQLAlchemyConnector(BaseSqlConnector):
    """
    Base SQLAlchemy connector for database adapters.
    Provides common SQLAlchemy functionality with Arrow support.
    """

    def __init__(self, connection_string: str, dialect: str = "", timeout_seconds: int = 30):
        """
        Initialize SQLAlchemyConnector.

        Args:
            connection_string: SQLAlchemy connection string
            dialect: Database dialect (mysql, postgresql, etc.)
            timeout_seconds: Connection timeout in seconds
        """
        # Auto-detect dialect from connection string if not provided
        if not dialect:
            prefix = connection_string.split(":")[0] if isinstance(connection_string, str) else "unknown"
            dialect = DBType.MYSQL if prefix == "mysql+pymysql" else prefix

        config = ConnectionConfig(timeout_seconds=timeout_seconds)
        super().__init__(config, dialect)
        self.connection_string = connection_string
        self.engine = None
        self.connection = None
        self._owns_engine = False

    def __del__(self):
        """Destructor to ensure connections are properly closed."""
        try:
            self.close()
        except Exception:
            pass

    # ==================== Connection Management ====================

    @override
    def connect(self):
        """Initialize the connection pool (engine only, no persistent connection)."""
        if self.engine and self._owns_engine:
            return

        try:
            self._safe_close()

            # Create engine with connection pool
            if self.dialect not in (DBType.DUCKDB, DBType.SQLITE):
                self.engine = create_engine(
                    self.connection_string,
                    pool_size=10,  # Increased for parallel execution
                    max_overflow=20,  # Allow more overflow connections
                    pool_timeout=self.timeout_seconds,
                    pool_recycle=3600,
                    pool_pre_ping=True,  # Verify connections before use
                )
            else:
                self.engine = create_engine(self.connection_string)

            self._owns_engine = True

        except Exception as e:
            self._force_reset()
            raise self._handle_exception(e, "", "connection") from e

        if not self.engine:
            self._force_reset()
            raise DatusException(
                ErrorCode.DB_CONNECTION_FAILED, message_args={"error_message": "Failed to establish connection"}
            )

    @override
    def close(self):
        """Dispose the connection pool."""
        try:
            if self.engine:
                self.engine.dispose()
                self.engine = None
            self._owns_engine = False
        except Exception as e:
            logger.warning(f"Error disposing engine: {str(e)}")

    def _safe_close(self):
        """Safely close connection, ignoring errors."""
        try:
            self.close()
        except Exception:
            pass

    def _force_reset(self):
        """Force reset engine on error."""
        try:
            if self.engine:
                try:
                    self.engine.dispose()
                except Exception:
                    pass
                self.engine = None
            self._owns_engine = False
        except Exception:
            self.engine = None
            self._owns_engine = False

    # ==================== Error Handling ====================

    def _handle_exception(self, e: Exception, sql: str = "", operation: str = "SQL execution") -> DatusException:
        """Map SQLAlchemy exceptions to Datus exceptions."""
        if isinstance(e, DatusException):
            return e

        # Extract error message
        if hasattr(e, "detail") and e.detail:
            error_message = str(e.detail) if not isinstance(e.detail, list) else "\n".join(e.detail)
        elif hasattr(e, "orig") and e.orig is not None:
            error_message = str(e.orig)
        else:
            error_message = str(e)

        message_args = {"error_message": error_message, "sql": sql}
        error_msg_lower = error_message.lower()

        # Syntax errors
        if any(kw in error_msg_lower for kw in ["syntax", "parse error", "sql error"]):
            return DatusException(ErrorCode.DB_EXECUTION_SYNTAX_ERROR, message_args=message_args)

        # Table not found
        if isinstance(e, NoSuchTableError):
            return DatusException(ErrorCode.DB_TABLE_NOT_EXISTS, message_args={"table_name": str(e)})

        # Connection and operational errors
        if isinstance(e, (OperationalError, InterfaceError)):
            # Transaction rollback errors
            if any(kw in error_msg_lower for kw in ["invalid transaction", "can't reconnect"]):
                logger.warning("Invalid transaction state detected, resetting connection")
                self._force_reset()
                return DatusException(ErrorCode.DB_TRANSACTION_FAILED, message_args=message_args)

            # Timeout errors
            if any(kw in error_msg_lower for kw in ["timeout", "timed out"]):
                return DatusException(ErrorCode.DB_CONNECTION_TIMEOUT, message_args=message_args)

            # Authentication errors
            if any(kw in error_msg_lower for kw in ["authentication", "access denied", "login failed"]):
                return DatusException(ErrorCode.DB_AUTHENTICATION_FAILED, message_args=message_args)

            # Permission errors
            if any(kw in error_msg_lower for kw in ["permission denied", "insufficient privilege"]):
                message_args["operation"] = operation
                return DatusException(ErrorCode.DB_PERMISSION_DENIED, message_args=message_args)

            # Connection errors
            if any(kw in error_msg_lower for kw in ["connection refused", "connection failed", "unable to open"]):
                return DatusException(ErrorCode.DB_CONNECTION_FAILED, message_args=message_args)

            return DatusException(ErrorCode.DB_EXECUTION_ERROR, message_args=message_args)

        # Programming errors
        if isinstance(e, ProgrammingError):
            if any(kw in error_msg_lower for kw in ["syntax", "parse error", "sql error"]):
                return DatusException(ErrorCode.DB_EXECUTION_SYNTAX_ERROR, message_args=message_args)
            return DatusException(ErrorCode.DB_EXECUTION_ERROR, message_args=message_args)

        # Constraint violations
        if isinstance(e, IntegrityError):
            return DatusException(ErrorCode.DB_CONSTRAINT_VIOLATION, message_args=message_args)

        # Timeout errors
        if isinstance(e, TimeoutError):
            return DatusException(ErrorCode.DB_EXECUTION_TIMEOUT, message_args=message_args)

        # Other database errors
        if isinstance(e, (DatabaseError, DataError, InternalError, NotSupportedError)):
            return DatusException(ErrorCode.DB_EXECUTION_ERROR, message_args=message_args)

        # Fallback
        return DatusException(ErrorCode.DB_EXECUTION_ERROR, message_args=message_args)

    # ==================== Core Execute Methods ====================

    @override
    def execute_query(
        self, sql: str, result_format: Literal["csv", "arrow", "pandas", "list"] = "csv"
    ) -> ExecuteSQLResult:
        """Execute SELECT query."""
        try:
            self.connect()
            result = self._execute_query(sql)
            row_count = len(result)

            # Format result based on requested format
            if result_format == "csv":
                df = DataFrame(result)
                result = df.to_csv(index=False)
            elif result_format == "arrow":
                result = Table.from_pylist(result)
            elif result_format == "pandas":
                result = DataFrame(result)

            return ExecuteSQLResult(
                success=True, sql_query=sql, sql_return=result, row_count=row_count, result_format=result_format
            )
        except Exception as e:
            ex = e if isinstance(e, DatusException) else self._handle_exception(e, sql)
            return ExecuteSQLResult(success=False, error=str(ex), sql_query=sql)

    def _execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """Internal query execution returning list of dicts."""
        if parse_sql_type(sql, self.dialect) in (
            SQLType.INSERT,
            SQLType.UPDATE,
            SQLType.DELETE,
            SQLType.MERGE,
            SQLType.CONTENT_SET,
            SQLType.UNKNOWN,
        ):
            raise DatusException(ErrorCode.DB_EXECUTION_ERROR, message="Only SELECT and metadata queries are supported")

        self.connect()
        try:
            # Get connection from pool for this query
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                rows = result.fetchall()
                return [row._asdict() for row in rows]
        except DatusException:
            raise
        except Exception as e:
            raise self._handle_exception(e, sql, "query") from e

    @override
    def execute_insert(self, sql: str) -> ExecuteSQLResult:
        """Execute INSERT statement."""
        try:
            self.connect()
            with self.engine.connect() as conn:
                res = conn.execute(text(sql))
                conn.commit()

                # Get inserted primary key or row count
                inserted_pk = None
                try:
                    if hasattr(res, "inserted_primary_key") and res.inserted_primary_key:
                        inserted_pk = res.inserted_primary_key
                except Exception:
                    pass

                lastrowid = getattr(res, "lastrowid", None)
                return_value = inserted_pk if inserted_pk else (lastrowid if lastrowid else res.rowcount)

                return ExecuteSQLResult(
                    success=True, sql_query=sql, sql_return=str(return_value), row_count=res.rowcount
                )
        except Exception as e:
            ex = e if isinstance(e, DatusException) else self._handle_exception(e, sql)
            return ExecuteSQLResult(success=False, error=str(ex), sql_query=sql, sql_return="", row_count=0)

    @override
    def execute_update(self, sql: str) -> ExecuteSQLResult:
        """Execute UPDATE statement."""
        try:
            self.connect()
            with self.engine.connect() as conn:
                res = conn.execute(text(sql))
                conn.commit()
                return ExecuteSQLResult(
                    success=True, sql_query=sql, sql_return=str(res.rowcount), row_count=res.rowcount
                )
        except Exception as e:
            ex = e if isinstance(e, DatusException) else self._handle_exception(e, sql)
            return ExecuteSQLResult(success=False, error=str(ex), sql_query=sql, sql_return="", row_count=0)

    @override
    def execute_delete(self, sql: str) -> ExecuteSQLResult:
        """Execute DELETE statement."""
        try:
            self.connect()
            with self.engine.connect() as conn:
                res = conn.execute(text(sql))
                conn.commit()
                return ExecuteSQLResult(
                    success=True, sql_query=sql, sql_return=str(res.rowcount), row_count=res.rowcount
                )
        except Exception as e:
            ex = e if isinstance(e, DatusException) else self._handle_exception(e, sql)
            return ExecuteSQLResult(success=False, error=str(ex), sql_query=sql, sql_return="", row_count=0)

    @override
    def execute_ddl(self, sql: str) -> ExecuteSQLResult:
        """Execute DDL statement (CREATE, ALTER, DROP, etc.)."""
        try:
            self.connect()
            with self.engine.connect() as conn:
                res = conn.execute(text(sql))
                conn.commit()
                return ExecuteSQLResult(
                    success=True, sql_query=sql, sql_return=str(res.rowcount), row_count=res.rowcount
                )
        except Exception as e:
            ex = e if isinstance(e, DatusException) else self._handle_exception(e, sql)
            return ExecuteSQLResult(success=False, sql_query=sql, error=str(ex))

    def execute_pandas(self, sql: str) -> ExecuteSQLResult:
        """Execute query and return pandas DataFrame."""
        try:
            df = self._execute_pandas(sql)
            return ExecuteSQLResult(
                success=True, sql_query=sql, sql_return=df, row_count=len(df), result_format="pandas"
            )
        except Exception as e:
            ex = e if isinstance(e, DatusException) else self._handle_exception(e, sql)
            return ExecuteSQLResult(success=False, error=str(ex), sql_query=sql)

    def _execute_pandas(self, sql: str) -> DataFrame:
        """Internal pandas execution."""
        return DataFrame(self._execute_query(sql))

    def execute_csv(self, sql: str) -> ExecuteSQLResult:
        """Execute query and return CSV format."""
        try:
            self.connect()
            df = self._execute_pandas(sql)
            return ExecuteSQLResult(
                success=True, sql_query=sql, sql_return=df.to_csv(index=False), row_count=len(df), result_format="csv"
            )
        except Exception as e:
            ex = e if isinstance(e, DatusException) else self._handle_exception(e, sql)
            return ExecuteSQLResult(
                success=False, sql_query=sql, sql_return="", row_count=0, error=str(ex), result_format="csv"
            )

    def execute_arrow(self, sql: str) -> ExecuteSQLResult:
        """Execute query and return Arrow table."""
        try:
            self.connect()
            result = self.connection.execute(text(sql))
            if result.returns_rows:
                df = DataFrame(result.fetchall(), columns=result.keys())
                table = Table.from_pandas(df)
                return ExecuteSQLResult(
                    success=True, sql_query=sql, sql_return=table, row_count=len(df), result_format="arrow"
                )
            return ExecuteSQLResult(
                success=True, sql_query=sql, sql_return=result.rowcount, row_count=0, result_format="arrow"
            )
        except Exception as e:
            ex = e if isinstance(e, DatusException) else self._handle_exception(e, sql)
            return ExecuteSQLResult(
                success=False, error=str(ex), sql_query=sql, sql_return="", row_count=0, result_format="arrow"
            )

    @override
    def execute_content_set(self, sql: str) -> ExecuteSQLResult:
        """Execute USE/SET commands."""
        self.connect()
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()

            # Update context if applicable
            if self.dialect != DBType.SQLITE.value:
                context = parse_context_switch(sql=sql, dialect=self.dialect)
                if context:
                    if catalog := context.get("catalog_name"):
                        self.catalog_name = catalog
                    if database := context.get("database_name"):
                        self.database_name = database
                    if schema := context.get("schema_name"):
                        self.schema_name = schema

            return ExecuteSQLResult(success=True, sql_query=sql, sql_return="Successful", row_count=0)
        except Exception as e:
            ex = e if isinstance(e, DatusException) else self._handle_exception(e, sql)
            return ExecuteSQLResult(success=False, error=str(ex), sql_query=sql)

    def execute_queries(self, queries: List[str]) -> List[Any]:
        """Execute multiple queries."""
        results = []
        self.connect()
        try:
            with self.engine.connect() as conn:
                for query in queries:
                    result = conn.execute(text(query))
                    if result.returns_rows:
                        df = DataFrame(result.fetchall(), columns=list(result.keys()))
                        results.append(df.to_dict(orient="records"))
                    else:
                        query_lower = query.strip().lower()
                        if query_lower.startswith("insert"):
                            inserted_pk = None
                            try:
                                if hasattr(result, "inserted_primary_key") and result.inserted_primary_key:
                                    inserted_pk = result.inserted_primary_key
                            except Exception:
                                pass
                            lastrowid = getattr(result, "lastrowid", None)
                            results.append(
                                inserted_pk if inserted_pk else (lastrowid if lastrowid else result.rowcount)
                            )
                        elif query_lower.startswith(("update", "delete")):
                            results.append(result.rowcount)
                        else:
                            results.append(None)
                conn.commit()
        except SQLAlchemyError as e:
            raise self._handle_exception(e, "\n".join(queries), "batch query") from e
        return results

    def test_connection(self) -> bool:
        """Test database connection."""
        self.connect()
        try:
            self._execute_query("SELECT 1")
            return True
        except Exception as e:
            if isinstance(e, DatusException):
                raise
            raise DatusException(
                ErrorCode.DB_CONNECTION_FAILED, message_args={"error_message": "Connection test failed"}
            ) from e

    # ==================== Metadata Methods ====================

    def _inspector(self) -> Inspector:
        """Get SQLAlchemy inspector."""
        self.connect()
        try:
            return inspect(self.engine)
        except Exception as e:
            raise self._handle_exception(e, operation="inspector creation") from e

    def get_tables(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        """Get list of tables."""
        self.connect()
        sqlalchemy_schema = self._sqlalchemy_schema(catalog_name, database_name, schema_name)
        inspector = self._inspector()
        return inspector.get_table_names(schema=sqlalchemy_schema)

    def get_views(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        """Get list of views."""
        self.connect()
        sqlalchemy_schema = self._sqlalchemy_schema(catalog_name, database_name, schema_name)
        inspector = self._inspector()
        try:
            return inspector.get_view_names(schema=sqlalchemy_schema)
        except Exception as e:
            raise DatusException(
                ErrorCode.DB_FAILED, message_args={"operation": "get_views", "error_message": str(e)}
            ) from e

    @override
    def get_schemas(self, catalog_name: str = "", database_name: str = "", include_sys: bool = False) -> List[str]:
        """Get list of schemas."""
        schemas = self._inspector().get_schema_names()
        if not include_sys:
            system_schemas = self._sys_schemas()
            schemas = [s for s in schemas if s.lower() not in system_schemas]
        return schemas

    def get_schema(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> List[Dict[str, Any]]:
        """Get table schema information."""
        sqlalchemy_schema = self._sqlalchemy_schema(
            catalog_name or self.catalog_name, database_name or self.database_name, schema_name or self.schema_name
        )
        inspector = self._inspector()
        try:
            schemas: List[Dict[str, Any]] = []
            pk_columns = set(
                inspector.get_pk_constraint(table_name=table_name, schema=sqlalchemy_schema)["constrained_columns"]
            )
            columns = inspector.get_columns(table_name=table_name, schema=sqlalchemy_schema)
            for i, col in enumerate(columns):
                schemas.append(
                    {
                        "cid": i,
                        "name": col["name"],
                        "type": str(col["type"]),
                        "comment": str(col["comment"]) if "comment" in col else None,
                        "nullable": col["nullable"],
                        "pk": col["name"] in pk_columns,
                        "default_value": col["default"],
                    }
                )
            return schemas
        except Exception as e:
            raise self._handle_exception(e, sql="", operation="get schema") from e

    def get_materialized_views(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[str]:
        """Get list of materialized views."""
        inspector = self._inspector()
        try:
            if hasattr(inspector, "get_materialized_view_names"):
                return inspector.get_materialized_view_names(schema=schema_name if schema_name else None)
            return []
        except Exception as e:
            logger.debug(f"Materialized views not supported: {str(e)}")
            return []

    def get_sample_rows(
        self,
        tables: Optional[List[str]] = None,
        top_n: int = 5,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        table_type: TABLE_TYPE = "table",
    ) -> List[Dict[str, str]]:
        """Get sample data from tables."""
        self._inspector()
        try:
            samples = []
            if not tables:
                tables = []
                if table_type in ("table", "full"):
                    tables.extend(self.get_tables(catalog_name, database_name, schema_name))
                if table_type in ("view", "full"):
                    tables.extend(self.get_views(catalog_name, database_name, schema_name))
                if table_type in ("mv", "full"):
                    try:
                        tables.extend(self.get_materialized_views(catalog_name, database_name, schema_name))
                    except Exception as e:
                        logger.debug(f"Materialized views not supported: {e}")

            logger.info(f"Getting sample data from {len(tables)} tables, limit {top_n}")
            for table_name in tables:
                full_name = self.full_name(catalog_name, database_name, schema_name, table_name)
                query = f"SELECT * FROM {full_name} LIMIT {top_n}"
                result = self._execute_pandas(query)
                if not result.empty:
                    samples.append(
                        {
                            "identifier": self.identifier(catalog_name, database_name, schema_name, table_name),
                            "catalog_name": catalog_name,
                            "database_name": database_name,
                            "schema_name": schema_name,
                            "table_name": table_name,
                            "table_type": table_type,
                            "sample_rows": result.to_csv(index=False),
                        }
                    )
            return samples
        except DatusException:
            raise
        except Exception as e:
            raise self._handle_exception(e) from e

    def _sqlalchemy_schema(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> Optional[str]:
        """Get schema name for SQLAlchemy Inspector."""
        return database_name or schema_name

    def full_name(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> str:
        """Build fully-qualified table name."""
        return self.identifier(catalog_name, database_name, schema_name, table_name)

    # ==================== Streaming Methods ====================

    def execute_csv_iterator(self, sql: str, max_rows: int = 100, with_header: bool = True) -> Iterator[Tuple]:
        """Execute query and return CSV rows in batches."""
        self.connect()
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql).execution_options(stream_results=True, max_row_buffer=max_rows))
                if result.returns_rows:
                    if with_header:
                        yield result.keys()
                    while True:
                        batch_rows = result.fetchmany(max_rows)
                        if not batch_rows:
                            break
                        for row in batch_rows:
                            yield row
                else:
                    if with_header:
                        yield ()
                    yield from []
        except Exception as e:
            raise self._handle_exception(e) from e
