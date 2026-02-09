#!/usr/bin/env python3
"""
Load FullContact match data from PostgreSQL fullcontact_matches table and flatten
the response JSON into dotted column names (e.g. fullcontact.gender, fullcontact.details.locations[0].city).
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


def _flatten_value(prefix: str, value, out: dict) -> None:
    """Recursively flatten a value into out with dotted keys and [i] for arrays."""
    if value is None:
        out[prefix] = None
        return
    if isinstance(value, dict):
        for k, v in value.items():
            _flatten_value(f"{prefix}.{k}", v, out)
        return
    if isinstance(value, list):
        for i, item in enumerate(value):
            _flatten_value(f"{prefix}[{i}]", item, out)
        return
    out[prefix] = value


def _flatten_payload(obj: dict) -> dict:
    """Flatten a FullContact payload (e.g. {email, fullcontact}) from root."""
    out = {}
    if not isinstance(obj, dict):
        return out
    for key, value in obj.items():
        _flatten_value(key, value, out)
    return out


def _row_to_flat(email: str, raw) -> dict:
    """Convert one fullcontact_matches row to a flat dict. Preserves email and flattens fullcontact data."""
    if raw is None:
        return {"email": email}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return {"email": email}
    if not isinstance(raw, dict):
        return {"email": email}
    flat = _flatten_payload(raw)
    flat["email"] = email
    return flat


def load_from_postgres(
    connection_string: str = None,
    table: str = "fullcontact_matches",
    email_column: str = "email",
    data_column: str = "response_json",
    store_column: str = "external_store_id",
) -> pd.DataFrame:
    """
    Load FullContact match data from PostgreSQL and return a DataFrame with flattened
    column names (e.g. fullcontact.gender, fullcontact.details.name.full).

    Expected table columns: email, response_json (JSON/JSONB with FullContact result),
    and optionally external_store_id for per-store dashboards.
    """
    if psycopg2 is None:
        raise ImportError("psycopg2 is required. Install with: pip install psycopg2-binary")

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
        print("No rows found in fullcontact_matches.")
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
