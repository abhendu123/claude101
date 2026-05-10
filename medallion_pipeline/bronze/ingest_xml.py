import logging
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional

import pandas as pd

from config import RedshiftConfig
from redshift_loader import RedshiftLoader

logger = logging.getLogger(__name__)


def ingest_xml(
    file_path: str,
    redshift_config: RedshiftConfig,
    target_table: str,
    target_schema: Optional[str] = None,
    # XPath / namespace
    xpath: str = "./*",
    namespaces: Optional[Dict[str, str]] = None,
    # Field extraction
    elems_only: bool = False,
    attrs_only: bool = False,
    names: Optional[List[str]] = None,
    # Encoding & parser
    encoding: Optional[str] = None,
    parser: str = "lxml",         # 'lxml' or 'etree'
    # Type handling
    dtype: Optional[dict] = None,
    parse_dates: Optional[List[str]] = None,
    # Redshift target options
    if_exists: str = "append",
    use_azure_staging: bool = False,
    load_chunksize: int = 10_000,
) -> None:
    """Ingest an XML file into Redshift.

    Supports flat and moderately nested XML.  For deeply nested structures the
    raw ElementTree parse path is used to build a list of dicts before
    constructing the DataFrame.

    Args:
        file_path: Absolute path to the XML file.
        redshift_config: Redshift connection configuration.
        target_table: Destination table name in Redshift.
        target_schema: Destination schema; falls back to redshift_config.schema.
        xpath: XPath expression selecting the repeating record elements.
        namespaces: Dict of XML namespace prefixes to URIs.
        elems_only: Only extract child element text (ignore attributes).
        attrs_only: Only extract element attributes (ignore child text).
        names: Override column names; must match the number of fields extracted.
        encoding: XML file encoding; None lets lxml auto-detect.
        parser: 'lxml' (preferred, faster) or 'etree' (stdlib fallback).
        dtype: Dict mapping column names to data types.
        parse_dates: Columns to parse as dates.
        if_exists: 'append' or 'replace'.
        use_azure_staging: Use S3 staging + COPY instead of direct INSERT.
        load_chunksize: Rows per batch for direct INSERT.
    """
    loader = RedshiftLoader(redshift_config)

    try:
        read_kwargs = {
            "xpath": xpath,
            "elems_only": elems_only,
            "attrs_only": attrs_only,
            "parser": parser,
        }
        if namespaces is not None:
            read_kwargs["namespaces"] = namespaces
        if names is not None:
            read_kwargs["names"] = names
        if encoding is not None:
            read_kwargs["encoding"] = encoding
        if dtype is not None:
            read_kwargs["dtype"] = dtype
        if parse_dates is not None:
            read_kwargs["parse_dates"] = parse_dates

        df = pd.read_xml(file_path, **read_kwargs)

    except Exception as pd_err:
        logger.warning(f"pd.read_xml failed ({pd_err}); falling back to ElementTree parser")
        df = _parse_with_etree(file_path, xpath, namespaces, encoding)

    logger.info(f"Read {len(df):,} rows from {file_path}")
    loader.load_dataframe(df, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)


def _parse_with_etree(
    file_path: str,
    xpath: str,
    namespaces: Optional[Dict[str, str]],
    encoding: Optional[str],
) -> pd.DataFrame:
    """Fallback XML parser using the stdlib ElementTree."""
    tree = ET.parse(file_path)
    root = tree.getroot()
    records = []
    for elem in root.findall(xpath, namespaces or {}):
        row = {**elem.attrib}
        for child in elem:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            row[tag] = child.text
        records.append(row)
    return pd.DataFrame(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cfg = RedshiftConfig.from_env()
    ingest_xml(
        file_path=r"C:\data\sample.xml",
        redshift_config=cfg,
        target_table="raw_xml_data",
        target_schema="bronze",
        xpath=".//record",
        namespaces={"ns": "http://example.com/schema"},
        parse_dates=["date_field"],
        if_exists="append",
    )