import logging
from datetime import date
from typing import List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from config import RedshiftConfig, SourceDBConfig
from redshift_loader import RedshiftLoader

logger = logging.getLogger(__name__)

DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"


def _build_connection_string(cfg: SourceDBConfig) -> str:
    driver = cfg.driver or DEFAULT_DRIVER
    conn_str = (
        f"mssql+pyodbc://{cfg.user}:{cfg.password}@{cfg.host},{cfg.port}"
        f"/{cfg.database}?driver={driver.replace(' ', '+')}"
        f"&TrustServerCertificate=yes"
        f"&connect_timeout={cfg.connect_timeout}"
    )
    for key, val in cfg.extra_params.items():
        conn_str += f"&{key}={val}"
    return conn_str


def ingest_sqlserver(
    source_config: SourceDBConfig,
    redshift_config: RedshiftConfig,
    target_table: str,
    target_schema: Optional[str] = None,
    # Source selection — provide either a table or a custom query
    source_table: Optional[str] = None,
    source_schema: Optional[str] = None,
    custom_query: Optional[str] = None,
    # Query window (incremental load)
    date_column: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    # Column controls
    columns: Optional[List[str]] = None,
    # Row limit (useful for testing)
    row_limit: Optional[int] = None,
    # Chunked extraction
    read_chunksize: Optional[int] = None,
    # Redshift target options
    if_exists: str = "append",
    use_azure_staging: bool = False,
    load_chunksize: int = 10_000,
) -> None:
    """Ingest data from SQL Server into Redshift as-is.

    Args:
        source_config: SQL Server connection configuration.
        redshift_config: Redshift connection configuration.
        target_table: Destination table name in Redshift.
        target_schema: Destination schema; falls back to redshift_config.schema.
        source_table: Table to read from (used when custom_query is None).
        source_schema: Schema of the source table; defaults to source_config.schema.
        custom_query: Fully custom SQL query overriding source_table selection.
        date_column: Column name used for the query window filter.
        start_date: Lower bound of the query window (inclusive).
        end_date: Upper bound of the query window (exclusive).
        columns: Subset of columns to select (ignored when custom_query is set).
        row_limit: Cap on rows returned; appends TOP N to generated queries.
        read_chunksize: Rows per chunk streamed from SQL Server.
        if_exists: 'append' or 'replace'.
        use_azure_staging: Use S3 staging + COPY instead of direct INSERT.
        load_chunksize: Rows per batch for direct INSERT.
    """
    engine = create_engine(_build_connection_string(source_config), fast_executemany=True)
    loader = RedshiftLoader(redshift_config)

    query = _build_query(
        custom_query=custom_query,
        source_table=source_table,
        source_schema=source_schema or source_config.schema,
        columns=columns,
        date_column=date_column,
        start_date=start_date,
        end_date=end_date,
        row_limit=row_limit,
        dialect="sqlserver",
    )
    logger.info(f"SQL Server query: {query}")

    first = True
    with engine.connect() as conn:
        iterator = pd.read_sql(
            text(query),
            conn,
            chunksize=read_chunksize,
        )
        if read_chunksize:
            for chunk in iterator:
                loader.load_dataframe(
                    chunk,
                    target_table,
                    target_schema,
                    if_exists if first else "append",
                    load_chunksize,
                    use_azure_staging,
                )
                first = False
        else:
            df = pd.read_sql(text(query), conn)
            logger.info(f"Read {len(df):,} rows from SQL Server")
            loader.load_dataframe(df, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)


def _build_query(
    custom_query: Optional[str],
    source_table: Optional[str],
    source_schema: Optional[str],
    columns: Optional[List[str]],
    date_column: Optional[str],
    start_date: Optional[date],
    end_date: Optional[date],
    row_limit: Optional[int],
    dialect: str,
) -> str:
    if custom_query:
        return custom_query

    col_expr = ", ".join(columns) if columns else "*"
    top_clause = f"TOP {row_limit} " if row_limit and dialect == "sqlserver" else ""
    schema_prefix = f"{source_schema}." if source_schema else ""
    base = f"SELECT {top_clause}{col_expr} FROM {schema_prefix}{source_table}"

    filters = []
    if date_column and start_date:
        filters.append(f"{date_column} >= '{start_date}'")
    if date_column and end_date:
        filters.append(f"{date_column} < '{end_date}'")

    if filters:
        base += " WHERE " + " AND ".join(filters)
    if row_limit and dialect != "sqlserver":
        base += f" LIMIT {row_limit}"
    return base


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from datetime import date

    src = SourceDBConfig(
        host=r"localhost\SQLEXPRESS",
        port=1433,
        database="AdventureWorks",
        schema="Sales",
        user="sa",
        password="password",
        driver="ODBC Driver 18 for SQL Server",
    )
    tgt = RedshiftConfig.from_env()

    ingest_sqlserver(
        source_config=src,
        redshift_config=tgt,
        target_table="raw_sales_orders",
        target_schema="bronze",
        source_table="SalesOrderHeader",
        source_schema="Sales",
        date_column="OrderDate",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        read_chunksize=50_000,
        if_exists="append",
    )