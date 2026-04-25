"""
Fabric SQL Endpoint + DuckDB Local Processing Pipeline
========================================================
Connect to Microsoft Fabric SQL endpoints, fetch data into DuckDB,
and perform local transformations without consuming Fabric compute.

Usage:
    1. Copy .env.example to .env and fill in your values.
    2. Set FABRIC_AUTH_MODE to "interactive" (dev) or "spn" (automated).
    3. Run: python fabric_duckdb_processor.py
"""

import os
import logging
from typing import Optional

import pyodbc
import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ODBC driver auto-detection
# ---------------------------------------------------------------------------
def _get_best_driver() -> str:
    """Return the best available SQL Server ODBC driver."""
    drivers = pyodbc.drivers()
    for preferred in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]:
        if preferred in drivers:
            return preferred
    raise RuntimeError(f"No suitable SQL Server ODBC driver found. Installed: {drivers}")


# ---------------------------------------------------------------------------
# Fabric SQL connection
# ---------------------------------------------------------------------------
def get_fabric_connection() -> pyodbc.Connection:
    """
    Create a pyodbc connection to a Fabric SQL analytics endpoint.
    Reads config from environment variables.
    Supports both interactive (Azure AD browser) and service principal auth.
    """
    server = os.environ["FABRIC_SERVER"]
    database = os.environ["FABRIC_DATABASE"]
    auth_mode = os.getenv("FABRIC_AUTH_MODE", "interactive").lower()
    driver = _get_best_driver()

    base = (
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={database};"
        f"Encrypt=Yes;"
    )

    if auth_mode == "spn":
        client_id = os.environ["FABRIC_CLIENT_ID"]
        tenant_id = os.environ["FABRIC_TENANT_ID"]
        client_secret = os.environ["FABRIC_CLIENT_SECRET"]
        base += (
            "Authentication=ActiveDirectoryServicePrincipal;"
            f"UID={client_id}@{tenant_id};"
            f"PWD={client_secret};"
        )
        logger.info("Connecting to Fabric via Service Principal...")
    else:
        base += "Authentication=ActiveDirectoryInteractive;"
        logger.info("Connecting to Fabric via Interactive auth (browser prompt)...")

    conn = pyodbc.connect(base)
    logger.info("Fabric connection established.")
    return conn


# ---------------------------------------------------------------------------
# DuckDB session
# ---------------------------------------------------------------------------
def get_duckdb_connection(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """
    Return a DuckDB connection.
    Use ':memory:' for ephemeral sessions or a file path to persist data
    across runs (e.g. 'local_cache.duckdb').
    """
    con = duckdb.connect(db_path)
    logger.info("DuckDB connection ready (%s).", db_path)
    return con


# ---------------------------------------------------------------------------
# Option 1 – Execute arbitrary SQL and load results into DuckDB
# ---------------------------------------------------------------------------
def execute_sql_to_duckdb(
    fabric_conn: pyodbc.Connection,
    duck_con: duckdb.DuckDBPyConnection,
    sql: str,
    target_table: str,
    if_exists: str = "replace",
) -> int:
    """
    Run *sql* against the Fabric SQL endpoint, fetch the result set,
    and store it as *target_table* inside DuckDB.

    Parameters
    ----------
    fabric_conn : active pyodbc connection to Fabric
    duck_con    : DuckDB connection
    sql         : any SELECT statement valid on the Fabric endpoint
    target_table: name of the destination table in DuckDB
    if_exists   : 'replace' (default) drops the table first;
                  'append' inserts into an existing table

    Returns
    -------
    int – row count loaded into DuckDB
    """
    logger.info("Executing SQL against Fabric:\n  %s", sql.strip()[:200])
    df = pd.read_sql(sql, fabric_conn)
    row_count = len(df)
    logger.info("Fetched %d rows from Fabric.", row_count)

    if if_exists == "replace":
        duck_con.execute(f"DROP TABLE IF EXISTS {target_table}")

    duck_con.execute(f"CREATE TABLE IF NOT EXISTS {target_table} AS SELECT * FROM df")
    logger.info("Loaded into DuckDB table '%s'.", target_table)
    return row_count


# ---------------------------------------------------------------------------
# Option 2 – Fetch a full table into DuckDB
# ---------------------------------------------------------------------------
def fetch_table_to_duckdb(
    fabric_conn: pyodbc.Connection,
    duck_con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    target_table: Optional[str] = None,
    if_exists: str = "replace",
) -> int:
    """
    Pull an entire Fabric table into DuckDB.

    Parameters
    ----------
    schema       : Fabric schema name (e.g. 'dbo')
    table        : Fabric table name
    target_table : DuckDB destination name (defaults to *table*)
    if_exists    : 'replace' or 'append'

    Returns
    -------
    int – row count
    """
    target_table = target_table or table
    sql = f"SELECT * FROM [{schema}].[{table}]"
    return execute_sql_to_duckdb(fabric_conn, duck_con, sql, target_table, if_exists)


# ---------------------------------------------------------------------------
# Transformation layer (placeholder – replace with your logic)
# ---------------------------------------------------------------------------
def transform(duck_con: duckdb.DuckDBPyConnection) -> None:
    """
    Run DuckDB-local transformations.

    This is a placeholder showing the pattern.  Replace the body with your
    real transformation logic — joins, aggregations, window functions, etc.
    All processing happens locally in DuckDB; no Fabric compute is consumed.
    """
    logger.info("Running transformations in DuckDB...")

    # --- Example: create a summary table from a source table ---------------
    # Assumes 'sales' was previously loaded via fetch_table_to_duckdb().
    duck_con.execute("""
        CREATE OR REPLACE TABLE sales_summary AS
        SELECT
            product_category,
            COUNT(*)            AS order_count,
            SUM(quantity)       AS total_quantity,
            ROUND(SUM(amount), 2) AS total_revenue,
            ROUND(AVG(amount), 2) AS avg_order_value
        FROM sales
        GROUP BY product_category
        ORDER BY total_revenue DESC
    """)

    logger.info("Transformation complete – 'sales_summary' table created.")


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def preview_table(duck_con: duckdb.DuckDBPyConnection, table: str, limit: int = 10) -> pd.DataFrame:
    """Quick preview of a DuckDB table. Handy during development."""
    df = duck_con.execute(f"SELECT * FROM {table} LIMIT {limit}").df()
    print(f"\n--- Preview: {table} ({limit} rows) ---")
    print(df.to_string(index=False))
    return df


def list_tables(duck_con: duckdb.DuckDBPyConnection) -> list[str]:
    """List all tables currently in the DuckDB session."""
    tables = duck_con.execute("SHOW TABLES").fetchall()
    names = [t[0] for t in tables]
    logger.info("DuckDB tables: %s", names)
    return names


def export_to_parquet(duck_con: duckdb.DuckDBPyConnection, table: str, path: str) -> None:
    """Export a DuckDB table to a local Parquet file."""
    duck_con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")
    logger.info("Exported '%s' → %s", table, path)


# ---------------------------------------------------------------------------
# Main – wire it all together
# ---------------------------------------------------------------------------
def main() -> None:
    # 1. Establish connections
    fabric_conn = get_fabric_connection()
    duck_con = get_duckdb_connection(db_path="local_cache.duckdb")

    # 2a. Fetch an entire table
    fetch_table_to_duckdb(
        fabric_conn, duck_con,
        schema="dbo",
        table="sales",
    )

    # 2b. Execute a custom SQL query and store results
    execute_sql_to_duckdb(
        fabric_conn, duck_con,
        sql="""
            SELECT customer_id, order_date, amount
            FROM dbo.orders
            WHERE order_date >= '2025-01-01'
        """,
        target_table="recent_orders",
    )

    # 3. Run local transformations (no Fabric CU cost)
    transform(duck_con)

    # 4. Inspect results
    list_tables(duck_con)
    preview_table(duck_con, "sales_summary")

    # 5. Optionally export for sharing or upstream loading
    export_to_parquet(duck_con, "sales_summary", "output/sales_summary.parquet")

    # 6. Clean up
    fabric_conn.close()
    duck_con.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
