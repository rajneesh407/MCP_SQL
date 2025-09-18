import os
from datetime import datetime, date

from fastmcp import FastMCP
from fastmcp.utilities.logging import get_logger

from sqlalchemy import create_engine, inspect, text

import os

os.environ["DB_URL"] = "sqlite:///latest_db.db"


### Helpers ###
def tests_set_global(k, v):
    globals()[k] = v


### Database ###
logger = get_logger(__name__)
ENGINE = None
import os


def create_new_engine():
    """Create engine with MCP-optimized settings to handle long-running connections"""
    import json

    db_engine_options = os.environ.get("DB_ENGINE_OPTIONS")
    user_options = json.loads(db_engine_options) if db_engine_options else {}

    # MCP-optimized defaults that can be overridden by user
    options = {
        "isolation_level": "AUTOCOMMIT",
        # Test connections before use (handles MySQL 8hr timeout, network drops)
        "pool_pre_ping": True,
        # Keep minimal connections (MCP typically handles one request at a time)
        "pool_size": 1,
        # Allow temporary burst capacity for edge cases
        "max_overflow": 2,
        # Force refresh connections older than 1hr (well under MySQL's 8hr default)
        "pool_recycle": 3600,
        # User can override any of the above
        **user_options,
    }

    # Ensure DB_URL is set
    db_url = os.environ.get("DB_URL")
    if not db_url:
        raise ValueError(
            "DB_URL environment variable is not set. Please set it to your database connection string."
        )
    return create_engine(db_url, **options)


def get_connection():
    global ENGINE

    try:
        try:
            if ENGINE is None:
                ENGINE = create_new_engine()

            connection = ENGINE.connect()

            # Set version variable for databases that support it
            try:
                # Assuming VERSION is defined globally later in the script
                connection.execute(text(f"SET @mcp_alchemy_version = '{VERSION}'"))
            except Exception:
                # Some databases don't support session variables
                pass

            return connection

        except Exception as e:
            logger.warning(f"First connection attempt failed: {e}")

            # Database might have restarted or network dropped - start fresh
            if ENGINE is not None:
                try:
                    ENGINE.dispose()
                except Exception:
                    pass

            # One retry with fresh engine handles most transient failures
            ENGINE = create_new_engine()
            connection = ENGINE.connect()

            return connection

    except Exception as e:
        logger.exception("Failed to get database connection after retry")
        raise


def get_db_info():
    with get_connection() as conn:
        engine = conn.engine
        url = engine.url
        result = [
            f"Connected to {engine.dialect.name}",
            f"version {'.'.join(str(x) for x in engine.dialect.server_version_info)}",
            f"database {url.database}",
        ]

        if url.host:
            result.append(f"on {url.host}")

        if url.username:
            result.append(f"as user {url.username}")

        return " ".join(result) + "."


### Constants ###

VERSION = "2025.8.15.91819"
DB_INFO = get_db_info()
EXECUTE_QUERY_MAX_CHARS = int(os.environ.get("EXECUTE_QUERY_MAX_CHARS", 4000))
READ_ONLY_MODE = os.environ.get("READ_ONLY_MODE", "true").lower() == "true"

### MCP ###

mcp = FastMCP("MCP Alchemy")
get_logger(__name__).info(f"Starting MCP Alchemy version {VERSION}")


@mcp.tool(
    description=f"Return all table names in the database separated by comma. {DB_INFO}"
)
def all_table_names() -> str:
    with get_connection() as conn:
        inspector = inspect(conn)
        return ", ".join(inspector.get_table_names())


@mcp.tool(
    description=f"Return all table names in the database containing the substring 'q' separated by comma. {DB_INFO}"
)
def filter_table_names(q: str) -> str:
    with get_connection() as conn:
        inspector = inspect(conn)
        return ", ".join(x for x in inspector.get_table_names() if q in x)


@mcp.tool(
    description=f"Returns schema and relation information for the given tables. {DB_INFO}"
)
def schema_definitions(table_names: list[str]) -> str:
    def format_schema(inspector, table_name):
        columns = inspector.get_columns(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)
        primary_keys = set(
            inspector.get_pk_constraint(table_name)["constrained_columns"]
        )
        result = [f"{table_name}:"]

        # Process columns
        show_key_only = {"nullable", "autoincrement"}
        for column in columns:
            if "comment" in column:
                del column["comment"]
            name = column.pop("name")
            column_parts = (
                (["primary key"] if name in primary_keys else [])
                + [str(column.pop("type"))]
                + [
                    k if k in show_key_only else f"{k}={v}"
                    for k, v in column.items()
                    if v
                ]
            )
            result.append(f"    {name}: " + ", ".join(column_parts))

        # Process relationships
        if foreign_keys:
            result.extend(["", "    Relationships:"])
            for fk in foreign_keys:
                constrained_columns = ", ".join(fk["constrained_columns"])
                referred_table = fk["referred_table"]
                referred_columns = ", ".join(fk["referred_columns"])
                result.append(
                    f"        {constrained_columns} -> {referred_table}.{referred_columns}"
                )

        return "\n".join(result)

    with get_connection() as conn:
        inspector = inspect(conn)
        return "\n\n".join(
            format_schema(inspector, table_name) for table_name in table_names
        )


def is_cud_operation(query):
    """Check if the query is a Create, Update, or Delete operation"""
    query_upper = query.strip().upper()

    # Check for common CUD operation keywords
    cud_keywords = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "TRUNCATE",
        "REPLACE",
        "MERGE",
        "UPSERT",
    ]

    # Split by whitespace and check first meaningful word
    words = query_upper.split()
    if not words:
        return False

    first_keyword = words[0]
    return first_keyword in cud_keywords


def execute_query_description():
    parts = [
        f"Execute a SQL query and return results in a readable format. Results will be truncated after {EXECUTE_QUERY_MAX_CHARS} characters."
    ]
    if READ_ONLY_MODE:
        parts.append(
            "READ-ONLY MODE: Only SELECT queries are allowed. CUD operations (CREATE, UPDATE, DELETE) are blocked."
        )
    parts.append(
        "IMPORTANT: You MUST use the params parameter for query parameter substitution (e.g. 'WHERE id = :id' with "
        "params={'id': 123}) to prevent SQL injection. Direct string concatenation is a serious security risk."
    )
    parts.append(DB_INFO)
    return " ".join(parts)


@mcp.tool(description=execute_query_description())
def execute_query(query: str, params: dict = None) -> str:
    # Check if read-only mode is enabled and query is a CUD operation
    if READ_ONLY_MODE and is_cud_operation(query):
        return "Error: READ-ONLY MODE is enabled. Only SELECT queries are allowed. CUD operations (CREATE, UPDATE, DELETE) are blocked."

    # Ensure params is a dictionary for the text() clause
    if params is None:
        params = {}

    def format_value(val):
        """Format a value for display, handling None and datetime types"""
        if val is None:
            return "NULL"
        if isinstance(val, (datetime, date)):
            return val.isoformat()
        return str(val)

    def format_result(cursor_result):
        """Format rows in a clean vertical format"""
        result = []
        size, row_count, did_truncate = 0, 0, False

        # Make the cursor result iterable
        rows = list(cursor_result)
        keys = cursor_result.keys()

        for row in rows:
            row_count += 1
            if did_truncate:
                continue

            sub_result = [f"{row_count}. row"]
            for col, val in zip(keys, row):
                sub_result.append(f"{col}: {format_value(val)}")
            sub_result.append("")

            size += sum(len(x) + 1 for x in sub_result)  # +1 is for line endings

            if size > EXECUTE_QUERY_MAX_CHARS:
                did_truncate = True
                break
            else:
                result.extend(sub_result)

        if row_count == 0:
            return ["No rows returned"]
        elif did_truncate:
            result.append(
                f"Result: showing first {row_count-1} rows (output truncated)"
            )
            return result
        else:
            result.append(f"Result: {row_count} rows")
            return result

    try:
        with get_connection() as connection:
            # Execute query directly since AUTOCOMMIT is enabled
            cursor_result = connection.execute(text(query), params)

            if not cursor_result.returns_rows:
                # For statements like INSERT, UPDATE, DELETE, rowcount gives the number of affected rows.
                # AUTOCOMMIT ensures changes are committed automatically.
                return f"Success: {cursor_result.rowcount} rows affected"

            output = format_result(cursor_result)

            return "\n".join(output)
    except Exception as e:
        return f"Error: {str(e)}"


def main():
    """
    Main function to run the MCP server.
    """
    # Run the MCP server over HTTP on localhost port 8080
    mcp.run(transport="http", host="0.0.0.0", port=8080)


if __name__ == "__main__":
    # Ensure you have the required environment variables set, for example:
    # In your terminal, run: export DB_URL="sqlite:///mydatabase.db"
    main()
