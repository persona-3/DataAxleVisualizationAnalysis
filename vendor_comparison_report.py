#!/usr/bin/env python3
"""
Vendor comparison report: Data Axle vs FullContact for the same store (e.g. smarty_swimoutlet_412).
Loads both datasets, compares metrics side-by-side, and highlights anomalies.
Output: vendor_comparison_<store_suffix>.html
"""

import argparse
import os
import re
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import pandas as pd
import numpy as np

try:
    from postgres_loader import load_from_postgres as load_data_axle
except ImportError:
    load_data_axle = None
try:
    from fullcontact_loader import load_from_postgres as load_fullcontact
except ImportError:
    load_fullcontact = None

# Column names
DA = {
    "email": "email",
    "gender": "data.document.attributes.gender",
    "state": "data.document.attributes.state",
    "city": "data.document.attributes.city",
    "income": "data.document.attributes.family.estimated_income",
}
FC = {
    "email": "email",
    "gender": "fullcontact.gender",
    "gender2": "fullcontact.details.gender",
    "state": "fullcontact.details.locations[0].region",
    "state_code": "fullcontact.details.locations[0].regionCode",
    "city": "fullcontact.details.locations[0].city",
    "income": "fullcontact.details.household.finance.income",
    "age": "fullcontact.details.age.value",
}


def _col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _safe_value_counts(series, top_n=15):
    if series is None or series.empty:
        return pd.Series(dtype=object)
    s = series.dropna().astype(str)
    s = s[s.str.strip() != ""]
    return s.value_counts().head(top_n)


def _pct(part, whole):
    if whole == 0:
        return 0.0
    return 100.0 * part / whole


def run_comparison(store_id: str, connection_string: str, out_path: str = None):
    store_suffix = "".join(c for c in str(store_id) if c.isalnum() or c in " _-").replace(" ", "").replace("-", "_") or "store"
    out_path = out_path or f"vendor_comparison_{store_suffix}.html"

    if load_data_axle is None or load_fullcontact is None:
        raise SystemExit("Need postgres_loader and fullcontact_loader (pip install psycopg2-binary)")

    print(f"Loading Data Axle (matched_emails, store={store_id})...")
    df_da = load_data_axle(
        connection_string=connection_string,
        table="matched_emails",
        email_column="email",
        data_column="response_json",
        store_column="external_store_id",
    )
    df_da = df_da[df_da["external_store_id"] == store_id].copy()
    n_da = len(df_da)

    print(f"Loading FullContact (fullcontact_matches, store={store_id})...")
    df_fc = load_fullcontact(
        connection_string=connection_string,
        table="fullcontact_matches",
        email_column="email",
        data_column="response_json",
        store_column="external_store_id",
    )
    df_fc = df_fc[df_fc["external_store_id"] == store_id].copy()
    n_fc = len(df_fc)

    emails_da = set(df_da["email"].dropna().astype(str).str.strip().tolist())
    emails_fc = set(df_fc["email"].dropna().astype(str).str.strip().tolist())
    emails_da.discard("")
    emails_fc.discard("")
    overlap = emails_da & emails_fc
    only_da = emails_da - emails_fc
    only_fc = emails_fc - emails_da
    union = emails_da | emails_fc

    # Metrics
    da_gender_col = _col(df_da, [DA["gender"]])
    fc_gender_col = _col(df_fc, [FC["gender"], FC["gender2"]])
    da_state_col = _col(df_da, [DA["state"]])
    fc_state_col = _col(df_fc, [FC["state_code"], FC["state"]])
    da_city_col = _col(df_da, [DA["city"]])
    fc_city_col = _col(df_fc, [FC["city"]])
    da_income_col = _col(df_da, [DA["income"]])
    fc_income_col = _col(df_fc, [FC["income"]]) or next((c for c in df_fc.columns if "household.finance.income" in c), None)

    # Distributions (normalize gender: M/Male, F/Female for comparison)
    def _normalize_gender(s):
        if pd.isna(s) or not str(s).strip():
            return None
        x = str(s).strip().lower()
        if x in ("m", "male"):
            return "Male"
        if x in ("f", "female"):
            return "Female"
        return s

    da_gender_series = df_da[da_gender_col].map(_normalize_gender) if da_gender_col and da_gender_col in df_da.columns else None
    fc_gender_series = df_fc[fc_gender_col].map(_normalize_gender) if fc_gender_col and fc_gender_col in df_fc.columns else None
    da_gender = _safe_value_counts(da_gender_series)
    fc_gender = _safe_value_counts(fc_gender_series)
    da_state = _safe_value_counts(df_da[da_state_col] if da_state_col else None, 10)
    fc_state = _safe_value_counts(df_fc[fc_state_col] if fc_state_col else None, 10)
    da_city = _safe_value_counts(df_da[da_city_col] if da_city_col else None, 10)
    fc_city = _safe_value_counts(df_fc[fc_city_col] if fc_city_col else None, 10)

    # Income: Data Axle numeric; FullContact string buckets
    da_income_vals = None
    if da_income_col and da_income_col in df_da.columns:
        da_income_vals = pd.to_numeric(df_da[da_income_col], errors="coerce").dropna()
    fc_income_vals = _safe_value_counts(df_fc[fc_income_col] if fc_income_col else None, 10)

    # Anomalies
    anomalies = []

    if n_da != n_fc:
        anomalies.append({
            "metric": "Record count",
            "detail": f"Data Axle: {n_da:,} | FullContact: {n_fc:,} | Difference: {abs(n_da - n_fc):,}",
            "severity": "high" if abs(n_da - n_fc) > max(n_da, n_fc) * 0.1 else "medium",
        })
    if len(only_fc) > 0:
        anomalies.append({
            "metric": "Emails only in FullContact",
            "detail": f"{len(only_fc):,} emails have FullContact data but no Data Axle match (possible enrichment gap or different feed)",
            "severity": "medium",
        })
    if len(only_da) > 0:
        anomalies.append({
            "metric": "Emails only in Data Axle",
            "detail": f"{len(only_da):,} emails have Data Axle data but no FullContact match",
            "severity": "medium",
        })

    # Gender comparison
    if not da_gender.empty and not fc_gender.empty:
        for g in set(da_gender.index) | set(fc_gender.index):
            pct_da = _pct(da_gender.get(g, 0), n_da)
            pct_fc = _pct(fc_gender.get(g, 0), n_fc)
            diff = abs(pct_da - pct_fc)
            if diff >= 5.0:
                anomalies.append({
                    "metric": f"Gender ({g})",
                    "detail": f"Data Axle: {pct_da:.1f}% | FullContact: {pct_fc:.1f}% | Δ {diff:.1f}pp",
                    "severity": "high" if diff >= 10 else "medium",
                })

    # Top state
    if not da_state.empty and not fc_state.empty:
        top_da = da_state.index[0]
        top_fc = fc_state.index[0]
        if top_da != top_fc:
            pct_da = _pct(da_state.iloc[0], n_da)
            pct_fc = _pct(fc_state.iloc[0], n_fc)
            anomalies.append({
                "metric": "Top state/region",
                "detail": f"Data Axle top: {top_da} ({pct_da:.1f}%) | FullContact top: {top_fc} ({pct_fc:.1f}%)",
                "severity": "medium",
            })

    # Build HTML
    def row(label, da_val, fc_val, anomaly=False):
        return f"""<tr class="{'anomaly' if anomaly else ''}">
            <td><strong>{label}</strong></td>
            <td>{da_val}</td>
            <td>{fc_val}</td>
        </tr>"""

    gender_rows = ""
    for g in sorted(set(da_gender.index.tolist() + fc_gender.index.tolist())):
        c_da = da_gender.get(g, 0)
        c_fc = fc_gender.get(g, 0)
        p_da = _pct(c_da, n_da)
        p_fc = _pct(c_fc, n_fc)
        gender_rows += row(g, f"{c_da:,} ({p_da:.1f}%)", f"{c_fc:,} ({p_fc:.1f}%)", abs(p_da - p_fc) >= 5)

    all_states = sorted(set(list(da_state.index)[:15]) | set(list(fc_state.index)[:15]))
    state_rows = ""
    for s in all_states:
        c_da = da_state.get(s, 0)
        c_fc = fc_state.get(s, 0)
        state_rows += row(s, f"{c_da:,}" if c_da else "—", f"{c_fc:,}" if c_fc else "—", c_da != c_fc)

    all_cities = sorted(set(list(da_city.index)[:15]) | set(list(fc_city.index)[:15]))
    city_rows = ""
    for c in all_cities:
        ct_da = da_city.get(c, 0)
        ct_fc = fc_city.get(c, 0)
        city_rows += row(c, f"{ct_da:,}" if ct_da else "—", f"{ct_fc:,}" if ct_fc else "—", ct_da != ct_fc)

    income_da_str = "—"
    if da_income_vals is not None and len(da_income_vals) > 0:
        income_da_str = f"Mean ${da_income_vals.mean():,.0f} | Median ${da_income_vals.median():,.0f} | N={len(da_income_vals):,}"
    income_fc_str = "—"
    if not fc_income_vals.empty:
        income_fc_str = " | ".join([f"{idx}: {v:,}" for idx, v in fc_income_vals.head(5).items()])

    anomaly_rows = "".join(
        f'<div class="anomaly {a["severity"]}"><strong>{a["metric"]}</strong>: {a["detail"]}</div>'
        for a in anomalies
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Vendor Comparison: Data Axle vs FullContact — {store_id}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f5f5f5; color: #222; padding: 24px; }}
        .container {{ max-width: 1000px; margin: 0 auto; }}
        h1 {{ margin-bottom: 8px; color: #1a1a2e; }}
        .subtitle {{ color: #666; margin-bottom: 24px; }}
        section {{ background: #fff; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); padding: 24px; margin-bottom: 24px; }}
        section h2 {{ margin-bottom: 16px; color: #16213e; font-size: 1.25rem; border-bottom: 2px solid #e0e0e0; padding-bottom: 8px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid #eee; }}
        th {{ background: #f8f9fa; font-weight: 600; }}
        td:first-child {{ width: 28%; }}
        tr.anomaly {{ background: #fff8e6; }}
        .anomaly-box {{ background: #fff3cd; border: 1px solid #ffc107; border-radius: 8px; padding: 16px; margin-bottom: 16px; }}
        .anomaly-box h3 {{ margin-bottom: 12px; color: #856404; }}
        .anomaly.high {{ color: #b71c1c; }}
        .anomaly.medium {{ color: #e65100; }}
        .metric {{ font-size: 1.1rem; margin-bottom: 8px; }}
        .metric strong {{ color: #0d47a1; }}
        .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
        @media (max-width: 700px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Vendor Comparison: Data Axle vs FullContact</h1>
        <p class="subtitle">Store: <strong>{store_id}</strong> (Smarty SwimOutlet conversion) · Generated {datetime.now().strftime("%B %d, %Y %H:%M")}</p>
        <p class="subtitle">Compare: <code>user_dashboard_smarty_swimoutlet_412.html</code> (Data Axle) vs <code>fullcontact_user_dashboard.html</code> / <code>fullcontact_user_dashboard_smarty_swimoutlet_412.html</code> (FullContact)</p>

        <section>
            <h2>Record counts & email overlap</h2>
            <div class="two-col">
                <div>
                    <p class="metric"><strong>Data Axle</strong> (matched_emails): {n_da:,} records</p>
                    <p class="metric"><strong>FullContact</strong> (fullcontact_matches): {n_fc:,} records</p>
                </div>
                <div>
                    <p class="metric"><strong>Emails in both</strong>: {len(overlap):,}</p>
                    <p class="metric"><strong>Only in Data Axle</strong>: {len(only_da):,}</p>
                    <p class="metric"><strong>Only in FullContact</strong>: {len(only_fc):,}</p>
                    <p class="metric"><strong>Union (unique emails)</strong>: {len(union):,}</p>
                </div>
            </div>
        </section>

        <section>
            <h2>Anomalies & differences</h2>
            <p style="margin-bottom: 12px;">These metrics differ meaningfully between vendors (coverage, schema, or enrichment timing).</p>
            <div class="anomaly-box">
                {anomaly_rows if anomalies else '<p>No major anomalies detected; distributions are in line.</p>'}
            </div>
        </section>

        <section>
            <h2>Gender distribution</h2>
            <table>
                <thead><tr><th>Gender</th><th>Data Axle</th><th>FullContact</th></tr></thead>
                <tbody>{gender_rows}</tbody>
            </table>
        </section>

        <section>
            <h2>Top states / regions</h2>
            <table>
                <thead><tr><th>State/Region</th><th>Data Axle</th><th>FullContact</th></tr></thead>
                <tbody>{state_rows or '<tr><td colspan="3">No state data</td></tr>'}</tbody>
            </table>
        </section>

        <section>
            <h2>Top cities</h2>
            <table>
                <thead><tr><th>City</th><th>Data Axle</th><th>FullContact</th></tr></thead>
                <tbody>{city_rows or '<tr><td colspan="3">No city data</td></tr>'}</tbody>
            </table>
        </section>

        <section>
            <h2>Income / financial</h2>
            <table>
                <thead><tr><th>Metric</th><th>Data Axle</th><th>FullContact</th></tr></thead>
                <tbody>
                    <tr><td>Income</td><td>{income_da_str}</td><td>{income_fc_str}</td></tr>
                </tbody>
            </table>
            <p style="margin-top: 12px; font-size: 0.9rem; color: #666;">Data Axle: numeric estimated_income. FullContact: bucket labels (e.g. $75,000 - $99,999). Direct comparison is approximate.</p>
        </section>

        <section>
            <h2>How to use this report</h2>
            <ul style="line-height: 1.8;">
                <li><strong>Record count mismatch</strong>: Different tables or enrichment pipelines may include/exclude users (e.g. no match in one vendor).</li>
                <li><strong>Gender/geo differences</strong>: Different sources and refresh dates; FullContact uses household/location data, Data Axle uses its own attributes.</li>
                <li><strong>Income</strong>: Data Axle = modeled income; FullContact = self-reported or household finance buckets — not directly comparable.</li>
                <li>For a single source of truth, pick one vendor per use case or reconcile on email overlap and flag conflicts.</li>
            </ul>
        </section>
    </div>
</body>
</html>"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ Report written: {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Compare Data Axle vs FullContact for a store; output HTML report.")
    parser.add_argument("--store", default="smarty_swimoutlet_412", help="external_store_id to filter (default: smarty_swimoutlet_412)")
    parser.add_argument("--postgres", default=os.environ.get("FULLCONTACT_DATABASE_URL") or os.environ.get("DATABASE_URL"), help="PostgreSQL URL")
    parser.add_argument("--out", help="Output HTML path (default: vendor_comparison_<store>.html)")
    args = parser.parse_args()
    if not args.postgres:
        raise SystemExit("Set DATABASE_URL or FULLCONTACT_DATABASE_URL, or pass --postgres")
    run_comparison(args.store, args.postgres, args.out)


if __name__ == "__main__":
    main()
