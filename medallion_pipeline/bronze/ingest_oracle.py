import logging
from datetime import date
from typing import List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from config import RedshiftConfig, SourceDBConfig
from redshift_loader import RedshiftLoader
from ingest_sqlserver import _build_query

logger = logging.getLogger(__name__)


def _build_oracle_dsn(cfg: SourceDBConfig) -> str:
    """Build an Oracle SQLAlchemy connection URL.

    Supports service_name (preferred) and SID-based connections.
    Requires oracledb (thin mode) or cx_Oracle installed.
    """
    if cfg.service_name:
        dsn = f"oracle+oracledb://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/?service_name={cfg.service_name}"
    elif cfg.sid:
        dsn = f"oracle+oracledb://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.sid}"
    else:
        raise ValueError("SourceDBConfig must supply either service_name or sid for Oracle")
    return dsn


def ingest_oracle(
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
    # Fetch size (controls network round-trips)
    fetch_size: int = 10_000,
    # Chunked extraction
    read_chunksize: Optional[int] = None,
    # Redshift target options
    if_exists: str = "append",
    use_azure_staging: bool = False,
    load_chunksize: int = 10_000,
) -> None:
    """Ingest data from Oracle into Redshift as-is.

    Args:
        source_config: Oracle connection configuration.  Requires service_name
            or sid plus host, port, user, password.
        redshift_config: Redshift connection configuration.
        target_table: Destination table name in Redshift.
        target_schema: Destination schema; falls back to redshift_config.schema.
        source_table: Table to read from (used when custom_query is None).
        source_schema: Schema / owner of the source table.
        custom_query: Fully custom SQL query overriding source_table selection.
        date_column: Column used for the query window filter.
        start_date: Lower bound of the query window (inclusive).
        end_date: Upper bound of the query window (exclusive).
        columns: Subset of columns to select.
        row_limit: Cap on rows returned (uses FETCH FIRST N ROWS ONLY).
        fetch_size: Rows fetched per network round-trip from Oracle.
        read_chunksize: Rows per chunk streamed from Oracle.
        if_exists: 'append' or 'replace'.
        use_azure_staging: Use S3 staging + COPY instead of direct INSERT.
        load_chunksize: Rows per batch for direct INSERT.
    """
    dsn = _build_oracle_dsn(source_config)
    engine = create_engine(
        dsn,
        connect_args={"tcp_connect_timeout": source_config.connect_timeout},
        thick_mode=False,   # thin mode — no Oracle Client install required
    )
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
        dialect="oracle",
    )

    if row_limit and not custom_query:
        query += f" FETCH FIRST {row_limit} ROWS ONLY"

    logger.info(f"Oracle query: {query}")

    first = True
    with engine.connect().execution_options(stream_results=True, max_row_buffer=fetch_size) as conn:
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
            logger.info(f"Read {len(df):,} rows from Oracle")
            loader.load_dataframe(df, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from datetime import date

    src = SourceDBConfig(
        host="oracle-host.example.com",
        port=1521,
        database="",
        schema="HR",
        user="hr_user",
        password="password",
        service_name="ORCL",
    )
    tgt = RedshiftConfig.from_env()

    ingest_oracle(
        source_config=src,
        redshift_config=tgt,
        target_table="raw_hr_employees",
        target_schema="bronze",
        source_table="EMPLOYEES",
        source_schema="HR",
        date_column="HIRE_DATE",
        start_date=date(2020, 1, 1),
        end_date=date(2025, 1, 1),
        fetch_size=5_000,
        read_chunksize=50_000,
        if_exists="append",
    )