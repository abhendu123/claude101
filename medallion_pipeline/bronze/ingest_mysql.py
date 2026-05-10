import logging
from datetime import date
from typing import List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from config import RedshiftConfig, SourceDBConfig
from redshift_loader import RedshiftLoader
from ingest_sqlserver import _build_query

logger = logging.getLogger(__name__)


def _build_mysql_dsn(cfg: SourceDBConfig) -> str:
    """Build a MySQL / MariaDB SQLAlchemy connection URL using PyMySQL."""
    port = cfg.port or 3306
    extras = "&".join(f"{k}={v}" for k, v in cfg.extra_params.items())
    base = (
        f"mysql+pymysql://{cfg.user}:{cfg.password}@{cfg.host}:{port}/{cfg.database}"
        f"?charset=utf8mb4&connect_timeout={cfg.connect_timeout}"
    )
    return f"{base}&{extras}" if extras else base


def ingest_mysql(
    source_config: SourceDBConfig,
    redshift_config: RedshiftConfig,
    target_table: str,
    target_schema: Optional[str] = None,
    # Source selection
    source_table: Optional[str] = None,
    source_schema: Optional[str] = None,
    custom_query: Optional[str] = None,
    # Query window
    date_column: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    # Column controls
    columns: Optional[List[str]] = None,
    # Row limit
    row_limit: Optional[int] = None,
    # Chunked extraction
    read_chunksize: Optional[int] = None,
    # Redshift target options
    if_exists: str = "append",
    use_azure_staging: bool = False,
    load_chunksize: int = 10_000,
) -> None:
    """Ingest data from MySQL or MariaDB into Redshift as-is.

    Args:
        source_config: MySQL connection configuration (host, port defaults to
            3306, database, schema, user, password).
        redshift_config: Redshift connection configuration.
        target_table: Destination table name in Redshift.
        target_schema: Destination schema; falls back to redshift_config.schema.
        source_table: Table to read from (used when custom_query is None).
        source_schema: Schema / database of the source table.
        custom_query: Fully custom SQL query overriding source_table selection.
        date_column: Column used for the query window filter.
        start_date: Lower bound of the query window (inclusive).
        end_date: Upper bound of the query window (exclusive).
        columns: Subset of columns to select.
        row_limit: Cap on rows returned (LIMIT N).
        read_chunksize: Rows per chunk streamed from MySQL.
        if_exists: 'append' or 'replace'.
        use_azure_staging: Use S3 staging + COPY instead of direct INSERT.
        load_chunksize: Rows per batch for direct INSERT.
    """
    engine = create_engine(_build_mysql_dsn(source_config))
    loader = RedshiftLoader(redshift_config)

    query = _build_query(
        custom_query=custom_query,
        source_table=source_table,
        source_schema=source_schema or source_config.schema or source_config.database,
        columns=columns,
        date_column=date_column,
        start_date=start_date,
        end_date=end_date,
        row_limit=row_limit,
        dialect="mysql",
    )
    logger.info(f"MySQL query: {query}")

    first = True
    with engine.connect() as conn:
        if read_chunksize:
            for chunk in pd.read_sql(text(query), conn, chunksize=read_chunksize):
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
            logger.info(f"Read {len(df):,} rows from MySQL")
            loader.load_dataframe(df, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from datetime import date

    src = SourceDBConfig(
        host="mysql-host.example.com",
        port=3306,
        database="ecommerce",
        schema="ecommerce",
        user="reader",
        password="password",
    )
    tgt = RedshiftConfig.from_env()

    ingest_mysql(
        source_config=src,
        redshift_config=tgt,
        target_table="raw_orders",
        target_schema="bronze",
        source_table="orders",
        date_column="created_at",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 1, 1),
        read_chunksize=50_000,
        if_exists="append",
    )