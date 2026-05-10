import json
import logging
from typing import List, Optional, Union

import pandas as pd

from config import RedshiftConfig
from redshift_loader import RedshiftLoader

logger = logging.getLogger(__name__)


def ingest_json(
    file_path: str,
    redshift_config: RedshiftConfig,
    target_table: str,
    target_schema: Optional[str] = None,
    # JSON structure
    orient: Optional[str] = None,  # 'records','split','index','columns','values','table'
    lines: bool = False,           # True for newline-delimited JSON (NDJSON)
    record_path: Optional[Union[str, List]] = None,   # path to nested records
    meta: Optional[List[str]] = None,                 # parent fields to include
    # Row controls
    nrows: Optional[int] = None,
    # Type handling
    dtype: Optional[dict] = None,
    convert_dates: Union[bool, List[str]] = True,
    precise_float: bool = False,
    # Encoding
    encoding: str = "utf-8",
    # Chunked loading (NDJSON only)
    read_chunksize: Optional[int] = None,
    # Redshift target options
    if_exists: str = "append",
    use_azure_staging: bool = False,
    load_chunksize: int = 10_000,
) -> None:
    """Ingest a JSON or NDJSON file into Redshift.

    Args:
        file_path: Absolute path to the JSON file.
        redshift_config: Redshift connection configuration.
        target_table: Destination table name in Redshift.
        target_schema: Destination schema; falls back to redshift_config.schema.
        orient: JSON format orientation for pandas read_json.
        lines: Set True for newline-delimited (NDJSON) format.
        record_path: Key path to the list of records in nested JSON.
        meta: Parent-level keys to keep as columns when flattening nested JSON.
        nrows: Maximum rows to read.
        dtype: Dict mapping column names to data types.
        convert_dates: Columns to parse as dates, or True for auto-detection.
        precise_float: Use higher-precision float conversion.
        encoding: File encoding.
        read_chunksize: Rows per chunk (NDJSON with lines=True only).
        if_exists: 'append' or 'replace'.
        use_azure_staging: Use S3 staging + COPY instead of direct INSERT.
        load_chunksize: Rows per batch for direct INSERT.
    """
    loader = RedshiftLoader(redshift_config)

    if record_path is not None:
        # Nested JSON — load raw then normalise
        with open(file_path, encoding=encoding) as fh:
            raw = json.load(fh)
        df = pd.json_normalize(raw, record_path=record_path, meta=meta or [])
        logger.info(f"Normalised {len(df):,} records from {file_path}")
        loader.load_dataframe(df, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)
        return

    read_kwargs = {
        "encoding": encoding,
        "convert_dates": convert_dates,
        "precise_float": precise_float,
        "lines": lines,
    }
    if orient is not None:
        read_kwargs["orient"] = orient
    if nrows is not None:
        read_kwargs["nrows"] = nrows
    if dtype is not None:
        read_kwargs["dtype"] = dtype
    if read_chunksize is not None:
        read_kwargs["chunksize"] = read_chunksize

    if lines and read_chunksize:
        first = True
        for chunk in pd.read_json(file_path, **read_kwargs):
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
        df = pd.read_json(file_path, **read_kwargs)
        logger.info(f"Read {len(df):,} rows from {file_path}")
        loader.load_dataframe(df, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cfg = RedshiftConfig.from_env()

    # Flat JSON
    ingest_json(
        file_path=r"C:\data\sample.json",
        redshift_config=cfg,
        target_table="raw_json_data",
        target_schema="bronze",
        orient="records",
        convert_dates=["created_at"],
        if_exists="append",
    )

    # Nested JSON with normalisation
    ingest_json(
        file_path=r"C:\data\nested.json",
        redshift_config=cfg,
        target_table="raw_nested_json",
        target_schema="bronze",
        record_path=["items"],
        meta=["order_id", "customer_id"],
        if_exists="append",
    )