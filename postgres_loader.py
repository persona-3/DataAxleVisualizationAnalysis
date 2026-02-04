#!/usr/bin/env python3
"""
Load Data Axle match data from PostgreSQL matched_emails table and flatten
to the same column structure expected by user_analysis_dashboard.py (CSV-style).
"""

import json
import os
import pandas as pd

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import sql
except ImportError:
    psycopg2 = None
    sql = None


PREFIX = "data.document.attributes"


def _flatten_value(prefix: str, value, out: dict) -> None:
    """Recursively flatten a value into out with keys like data.document.attributes.*"""
    if value is None:
        out[prefix] = None
        return
    if isinstance(value, dict):
        for k, v in value.items():
            key = k if k.isidentifier() or k.replace(" ", "_").replace("-", "_").replace(".", "_").isidentifier() else k
            _flatten_value(f"{prefix}.{key}", v, out)
        return
    if isinstance(value, list):
        for i, item in enumerate(value):
            _flatten_value(f"{prefix}[{i}]", item, out)
        return
    out[prefix] = value


def _flatten_document(doc: dict) -> dict:
    """Flatten a Data Axle document (with 'attributes') into a single dict."""
    out = {}
    if not isinstance(doc, dict):
        return out
    attrs = doc.get("attributes") if "attributes" in doc else doc
    if not isinstance(attrs, dict):
        return out
    for key, value in attrs.items():
        _flatten_value(f"{PREFIX}.{key}", value, out)
    return out


def _row_to_flat(email: str, raw: dict) -> dict:
    """Convert one matched_emails row to a flat dict for the dashboard."""
    doc = raw.get("document") if isinstance(raw, dict) and "document" in raw else raw
    if not doc:
        return {"email": email}
    flat = _flatten_document(doc)
    flat["email"] = email
    return flat


def load_from_postgres(
    connection_string: str = None,
    table: str = "matched_emails",
    email_column: str = "email",
    data_column: str = "response_json",
    store_column: str = "external_store_id",
) -> pd.DataFrame:
    """
    Load Data Axle match data from PostgreSQL and return a DataFrame with the same
    column names as the dashboard expects.

    Expected table columns: email, response_json (JSON/JSONB with match result),
    and optionally external_store_id (or store_column) for per-store dashboards.
    """
    if psycopg2 is None:
        raise ImportError("psycopg2 is required for PostgreSQL. Install with: pip install psycopg2-binary")

    conn_str = (
        connection_string
        or os.environ.get("DATABASE_URL")
        or os.environ.get("POSTGRES_URI")
    )
    if not conn_str:
        raise ValueError(
            "PostgreSQL connection string required. Set DATABASE_URL or POSTGRES_URI, or pass connection_string=..."
        )

    columns = [email_column, data_column]
    if store_column:
        columns.append(store_column)

    print(f"Connecting to PostgreSQL and reading from {table}...")
    conn = psycopg2.connect(conn_str)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql.SQL("SELECT {} FROM {}").format(
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.Identifier(table),
                ),
                (),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print("No rows found in matched_emails.")
        return pd.DataFrame()

    records = []
    for row in rows:
        email = row.get(email_column)
        raw = row.get(data_column)
        if raw is None:
            flat = {"email": email}
        else:
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError:
                    flat = {"email": email}
                else:
                    flat = _row_to_flat(email, raw)
            else:
                flat = _row_to_flat(email, raw)
        if store_column and store_column in row:
            flat["external_store_id"] = row.get(store_column)
        records.append(flat)

    df = pd.DataFrame(records)
    print(f"Loaded {len(df):,} records from PostgreSQL ({len(df.columns)} columns)")
    return df
