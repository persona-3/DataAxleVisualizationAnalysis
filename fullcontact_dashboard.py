#!/usr/bin/env python3
"""
FullContact User Demographics and Behavior Analysis Dashboard
Generates the same chart types as the Data Axle dashboard using fullcontact_matches data.
"""

import argparse
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# FullContact config: env vars (from .env or environment) override defaults
def _fullcontact_env(key: str, default: str) -> str:
    return os.environ.get(f"FULLCONTACT_{key}", default)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

try:
    from fullcontact_loader import load_from_postgres
except ImportError:
    load_from_postgres = None

plt.style.use("seaborn-v0_8")
sns.set_palette("husl")

# FullContact flattened column names (from fullcontact_loader)
COL = {
    "gender": "fullcontact.gender",
    "details_gender": "fullcontact.details.gender",
    "full_name": "fullcontact.fullName",
    "name_full": "fullcontact.details.name.full",
    "location": "fullcontact.location",
    "age": "fullcontact.details.age",
    "age_value": "fullcontact.details.age.value",
    "age_range": "fullcontact.ageRange",
    "city": "fullcontact.details.locations[0].city",
    "region": "fullcontact.details.locations[0].region",
    "country": "fullcontact.details.locations[0].country",
    "income": "fullcontact.details.household.finance.income",
    "net_worth": "fullcontact.details.household.finance.netWorth",
    "owner_renter": "fullcontact.details.household.homeInfo.ownerOrRenter",
    "marital_status": "fullcontact.details.household.homeInfo.maritalStatus",
}
OUTPUT_PREFIX = "fullcontact_"


def _col(df, *candidates):
    """Return first column that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _gender_col(df):
    return _col(df, COL["gender"], COL["details_gender"])


def _state_col(df):
    return _col(df, COL["region"], "fullcontact.details.locations[0].regionCode", COL["location"])


def _city_col(df):
    return _col(df, COL["city"])


def normalize_numeric_columns(df):
    """Coerce FullContact numeric columns."""
    for col in list(df.columns):
        if col in (COL["age"], COL["age_value"], "fullcontact.details.age", "fullcontact.details.age.value"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def create_geographic_analysis(df, suffix=""):
    """Create geographic distribution visualizations from FullContact location data."""
    print(f"Creating geographic analysis{suffix}...")
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle("Geographic Distribution of Users (FullContact)", fontsize=16, fontweight="bold")

    state_col = _state_col(df)
    city_col = _city_col(df)

    if state_col:
        state_counts = df[state_col].dropna().astype(str).value_counts().head(15)
        if len(state_counts) > 0:
            axes[0, 0].bar(range(len(state_counts)), state_counts.values)
            axes[0, 0].set_xticks(range(len(state_counts)))
            axes[0, 0].set_xticklabels(state_counts.index, rotation=45)
            axes[0, 0].set_title("Top 15 Regions / States by User Count")
            axes[0, 0].set_ylabel("Number of Users")
    else:
        axes[0, 0].text(0.5, 0.5, "No region/state data", ha="center", va="center", transform=axes[0, 0].transAxes)
        axes[0, 0].set_title("Top 15 Regions by User Count")

    if city_col:
        city_counts = df[city_col].dropna().astype(str).value_counts().head(15)
        if len(city_counts) > 0:
            axes[0, 1].barh(range(len(city_counts)), city_counts.values)
            axes[0, 1].set_yticks(range(len(city_counts)))
            axes[0, 1].set_yticklabels(city_counts.index)
            axes[0, 1].set_title("Top 15 Cities by User Count")
            axes[0, 1].set_xlabel("Number of Users")
    else:
        axes[0, 1].text(0.5, 0.5, "No city data", ha="center", va="center", transform=axes[0, 1].transAxes)
        axes[0, 1].set_title("Top 15 Cities")

    if state_col:
        state_data = df[state_col].dropna().astype(str).value_counts()
        top_states = state_data.head(20)
        if len(top_states) > 0:
            bars = axes[1, 0].bar(
                range(len(top_states)), top_states.values, color=plt.cm.viridis(np.linspace(0, 1, len(top_states)))
            )
            axes[1, 0].set_xticks(range(len(top_states)))
            axes[1, 0].set_xticklabels(top_states.index, rotation=45)
            axes[1, 0].set_title("User Concentration by Region (Top 20)")
            axes[1, 0].set_ylabel("Number of Users")
    else:
        axes[1, 0].axis("off")

    axes[1, 1].axis("off")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PREFIX}geographic_analysis{suffix}.png", dpi=300, bbox_inches="tight")
    plt.close()


def create_demographic_analysis(df, suffix=""):
    """Create demographic visualizations (gender, age, name) from FullContact."""
    print(f"Creating demographic analysis{suffix}...")
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle("User Demographics Analysis (FullContact)", fontsize=16, fontweight="bold")

    gender_col = _gender_col(df)
    if gender_col:
        gender_counts = df[gender_col].dropna().astype(str).value_counts()
        if len(gender_counts) > 0:
            axes[0, 0].pie(gender_counts.values, labels=gender_counts.index, autopct="%1.1f%%")
        axes[0, 0].set_title("Gender Distribution")
    else:
        axes[0, 0].text(0.5, 0.5, "No gender data", ha="center", va="center", transform=axes[0, 0].transAxes)
        axes[0, 0].set_title("Gender Distribution")

    age_col = _col(df, COL["age_value"], COL["age"], COL["age_range"])
    if age_col:
        age_vals = pd.to_numeric(df[age_col], errors="coerce").dropna()
        if len(age_vals) > 0:
            bins = min(30, max(5, int(age_vals.max() - age_vals.min()) or 10))
            axes[0, 1].hist(age_vals, bins=bins, edgecolor="black", alpha=0.7)
            axes[0, 1].set_title("Age Distribution")
            axes[0, 1].set_xlabel("Age")
            axes[0, 1].set_ylabel("Number of Users")
    else:
        axes[0, 1].text(0.5, 0.5, "No age data", ha="center", va="center", transform=axes[0, 1].transAxes)
        axes[0, 1].set_title("Age Distribution")

    name_col = _col(df, COL["full_name"], COL["name_full"])
    if name_col:
        has_name = df[name_col].notna() & (df[name_col].astype(str).str.strip() != "")
        axes[1, 0].bar(["With name", "Missing name"], [has_name.sum(), (~has_name).sum()])
        axes[1, 0].set_title("Name Coverage")
        axes[1, 0].set_ylabel("Number of Users")
    else:
        axes[1, 0].text(0.5, 0.5, "No name data", ha="center", va="center", transform=axes[1, 0].transAxes)
        axes[1, 0].set_title("Name Coverage")

    axes[1, 1].axis("off")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PREFIX}demographic_analysis{suffix}.png", dpi=300, bbox_inches="tight")
    plt.close()


def _parse_income_midpoint(s):
    """Parse '$75,000 - $99,999' to midpoint (87500). Return None if unparseable."""
    if pd.isna(s) or not isinstance(s, str):
        return None
    import re
    nums = re.findall(r"\$?([\d,]+)", s)
    if len(nums) >= 2:
        try:
            lo = int(nums[0].replace(",", ""))
            hi = int(nums[1].replace(",", ""))
            return (lo + hi) / 2
        except ValueError:
            pass
    if len(nums) == 1:
        try:
            return int(nums[0].replace(",", ""))
        except ValueError:
            pass
    return None


def create_financial_analysis(df, suffix=""):
    """Financial profile from fullcontact.details.household.finance and homeInfo."""
    print(f"Creating financial analysis{suffix}...")
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle("Financial Profile of Users (FullContact)", fontsize=16, fontweight="bold")

    income_col = _col(df, COL["income"]) or next((c for c in df.columns if "household.finance.income" in c), None)
    networth_col = _col(df, COL["net_worth"]) or next((c for c in df.columns if "household.finance.netWorth" in c or "household.finance.netWorthRange" in c), None)
    owner_col = _col(df, COL["owner_renter"]) or next((c for c in df.columns if "household.homeInfo.ownerOrRenter" in c), None)
    marital_col = _col(df, COL["marital_status"]) or next((c for c in df.columns if "household.homeInfo.maritalStatus" in c), None)

    # Income distribution (by bucket string)
    if income_col:
        inc_counts = df[income_col].dropna().astype(str).value_counts()
        inc_counts = inc_counts[inc_counts.index.str.strip() != ""]
        if len(inc_counts) > 0:
            top_inc = inc_counts.head(12)
            axes[0, 0].barh(range(len(top_inc)), top_inc.values, color="steelblue")
            axes[0, 0].set_yticks(range(len(top_inc)))
            axes[0, 0].set_yticklabels(top_inc.index, fontsize=8)
            axes[0, 0].set_title("Income (Household Finance)")
            axes[0, 0].set_xlabel("Number of Users")
        else:
            axes[0, 0].text(0.5, 0.5, "No income data", ha="center", va="center", transform=axes[0, 0].transAxes)
            axes[0, 0].set_title("Income")
    else:
        axes[0, 0].text(0.5, 0.5, "No income column", ha="center", va="center", transform=axes[0, 0].transAxes)
        axes[0, 0].set_title("Income")

    # Net worth distribution
    if networth_col:
        nw_counts = df[networth_col].dropna().astype(str).value_counts()
        nw_counts = nw_counts[nw_counts.index.str.strip() != ""]
        if len(nw_counts) > 0:
            top_nw = nw_counts.head(12)
            axes[0, 1].barh(range(len(top_nw)), top_nw.values, color="seagreen")
            axes[0, 1].set_yticks(range(len(top_nw)))
            axes[0, 1].set_yticklabels(top_nw.index, fontsize=8)
            axes[0, 1].set_title("Net Worth (Household Finance)")
            axes[0, 1].set_xlabel("Number of Users")
        else:
            axes[0, 1].text(0.5, 0.5, "No net worth data", ha="center", va="center", transform=axes[0, 1].transAxes)
            axes[0, 1].set_title("Net Worth")
    else:
        axes[0, 1].text(0.5, 0.5, "No net worth column", ha="center", va="center", transform=axes[0, 1].transAxes)
        axes[0, 1].set_title("Net Worth")

    # Home: owner vs renter
    if owner_col:
        owner_counts = df[owner_col].dropna().astype(str).value_counts()
        owner_counts = owner_counts[owner_counts.index.str.strip() != ""]
        if len(owner_counts) > 0:
            labels = [str(x) if x != "H" else "Owner (H)" for x in owner_counts.index]
            axes[1, 0].pie(owner_counts.values, labels=labels, autopct="%1.1f%%")
            axes[1, 0].set_title("Owner vs Renter (Home Info)")
        else:
            axes[1, 0].text(0.5, 0.5, "No owner/renter data", ha="center", va="center", transform=axes[1, 0].transAxes)
            axes[1, 0].set_title("Owner vs Renter")
    else:
        axes[1, 0].text(0.5, 0.5, "No owner/renter column", ha="center", va="center", transform=axes[1, 0].transAxes)
        axes[1, 0].set_title("Owner vs Renter")

    # Marital status (homeInfo)
    if marital_col:
        mar_counts = df[marital_col].dropna().astype(str).value_counts()
        mar_counts = mar_counts[mar_counts.index.str.strip() != ""]
        if len(mar_counts) > 0:
            axes[1, 1].pie(mar_counts.values, labels=mar_counts.index, autopct="%1.1f%%")
            axes[1, 1].set_title("Marital Status (Home Info)")
        else:
            axes[1, 1].text(0.5, 0.5, "No marital data", ha="center", va="center", transform=axes[1, 1].transAxes)
            axes[1, 1].set_title("Marital Status")
    else:
        axes[1, 1].text(0.5, 0.5, "No marital column", ha="center", va="center", transform=axes[1, 1].transAxes)
        axes[1, 1].set_title("Marital Status")

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PREFIX}financial_analysis{suffix}.png", dpi=300, bbox_inches="tight")
    plt.close()


def _interest_label(col_name):
    """Derive short label from column path, e.g. details.marketTrends.enthusiasts.football -> football."""
    parts = col_name.replace("fullcontact.details.", "").split(".")
    return parts[-1] if parts else col_name


def create_interests_analysis(df, suffix=""):
    """Interests from surveys (Y), marketTrends (Likely/Highly Likely), and details.interests[*]."""
    print(f"Creating interests analysis{suffix}...")
    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    fig.suptitle("User Interests (FullContact: Surveys & Market Trends)", fontsize=16, fontweight="bold")

    interest_counts = {}

    # 1) Surveys: value "Y" = has interest (e.g. hobby.baking, mailOrder.apparel)
    survey_cols = [c for c in df.columns if "fullcontact.details.surveys" in c]
    for col in survey_cols:
        count = (df[col].astype(str).str.strip().str.upper() == "Y").sum()
        if count > 0:
            label = _interest_label(col)
            interest_counts[label] = interest_counts.get(label, 0) + count

    # 2) Market trends: "Likely" or "Highly Likely" = interest (e.g. enthusiasts.football)
    market_cols = [c for c in df.columns if "fullcontact.details.marketTrends" in c]
    for col in market_cols:
        count = df[col].astype(str).str.strip().isin(["Likely", "Highly Likely"]).sum()
        if count > 0:
            label = _interest_label(col)
            interest_counts[label] = interest_counts.get(label, 0) + count

    # 3) details.interests[*] array (string values)
    interest_array_cols = [c for c in df.columns if "fullcontact.details.interests" in c and "[" in c and "]" in c]
    for col in interest_array_cols:
        vals = df[col].dropna().astype(str)
        vals = vals[vals.str.strip() != ""]
        for v in vals:
            v = v.strip()
            if v:
                interest_counts[v] = interest_counts.get(v, 0) + 1

    # 4) demographics.enthusiasts.niches (e.g. "IRA Spenders")
    niche_cols = [c for c in df.columns if "demographics.enthusiasts" in c or "enthusiasts.niches" in c]
    for col in niche_cols:
        vals = df[col].dropna().astype(str)
        vals = vals[vals.str.strip() != ""]
        for v in vals:
            v = v.strip()
            if v:
                interest_counts[v] = interest_counts.get(v, 0) + 1

    if not interest_counts:
        for ax in axes.flat:
            ax.text(0.5, 0.5, "No interest data in this dataset", ha="center", va="center", transform=ax.transAxes)
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_PREFIX}interests_analysis{suffix}.png", dpi=300, bbox_inches="tight")
        plt.close()
        return

    from collections import Counter
    top_all = Counter(interest_counts).most_common(15)
    labels, values = zip(*top_all) if top_all else ([], [])

    axes[0, 0].barh(range(len(labels)), values, color="skyblue")
    axes[0, 0].set_yticks(range(len(labels)))
    axes[0, 0].set_yticklabels(labels, fontsize=9)
    axes[0, 0].set_title("Top 15 Interests (Surveys + Market Trends + Niches)")
    axes[0, 0].set_xlabel("Number of Users")

    # Top market trends only (Likely/Highly Likely) – from market_cols only
    market_only = {}
    for col in market_cols:
        label = _interest_label(col)
        count = df[col].astype(str).str.strip().isin(["Likely", "Highly Likely"]).sum()
        if count > 0:
            market_only[label] = market_only.get(label, 0) + count
    if market_only:
        top_market = sorted(market_only.items(), key=lambda x: -x[1])[:15]
        mk_labels, mk_vals = zip(*top_market)
        axes[0, 1].barh(range(len(mk_labels)), mk_vals, color="lightcoral")
        axes[0, 1].set_yticks(range(len(mk_labels)))
        axes[0, 1].set_yticklabels(mk_labels, fontsize=9)
        axes[0, 1].set_title("Top 15 Market Trends (Likely / Highly Likely)")
        axes[0, 1].set_xlabel("Number of Users")
    else:
        axes[0, 1].text(0.5, 0.5, "No market trends data", ha="center", va="center", transform=axes[0, 1].transAxes)
        axes[0, 1].set_title("Market Trends")

    # Surveys only (Y)
    survey_only = {}
    for col in survey_cols:
        count = (df[col].astype(str).str.strip().str.upper() == "Y").sum()
        if count > 0:
            survey_only[_interest_label(col)] = count
    if survey_only:
        top_survey = sorted(survey_only.items(), key=lambda x: -x[1])[:15]
        sv_labels, sv_vals = zip(*top_survey)
        axes[1, 0].barh(range(len(sv_labels)), sv_vals, color="lightgreen")
        axes[1, 0].set_yticks(range(len(sv_labels)))
        axes[1, 0].set_yticklabels(sv_labels, fontsize=9)
        axes[1, 0].set_title("Top 15 Survey Interests (Y)")
        axes[1, 0].set_xlabel("Number of Users")
    else:
        axes[1, 0].text(0.5, 0.5, "No survey data", ha="center", va="center", transform=axes[1, 0].transAxes)
        axes[1, 0].set_title("Survey Interests")

    axes[1, 1].axis("off")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PREFIX}interests_analysis{suffix}.png", dpi=300, bbox_inches="tight")
    plt.close()


def create_summary_dashboard(df, suffix=""):
    """Create high-level summary dashboard for FullContact."""
    print(f"Creating summary dashboard{suffix}...")
    fig, axes = plt.subplots(2, 2, figsize=(20, 12))
    fig.suptitle("User Base Summary (FullContact)", fontsize=18, fontweight="bold")

    total_users = len(df)
    state_col = _state_col(df)
    city_col = _city_col(df)
    gender_col = _gender_col(df)

    if state_col:
        state_counts = df[state_col].dropna().astype(str).value_counts().head(10)
        if len(state_counts) > 0:
            axes[0, 0].bar(range(len(state_counts)), state_counts.values, color="skyblue")
            axes[0, 0].set_xticks(range(len(state_counts)))
            axes[0, 0].set_xticklabels(state_counts.index, rotation=45)
            axes[0, 0].set_title("Top 10 Regions by User Volume")
            axes[0, 0].set_ylabel("Number of Users")
    else:
        axes[0, 0].text(0.5, 0.5, "No region data", ha="center", va="center", transform=axes[0, 0].transAxes)
        axes[0, 0].set_title("Top 10 Regions")

    if gender_col:
        gender_counts = df[gender_col].dropna().astype(str).value_counts()
        if len(gender_counts) > 0:
            axes[0, 1].pie(gender_counts.values, labels=gender_counts.index, autopct="%1.1f%%")
        axes[0, 1].set_title("User Demographics (Gender)")
    else:
        axes[0, 1].text(0.5, 0.5, "No gender data", ha="center", va="center", transform=axes[0, 1].transAxes)
        axes[0, 1].set_title("User Demographics")

    stats_text = f"Total users: {total_users:,}\n"
    if state_col:
        stats_text += f"Regions: {df[state_col].dropna().nunique()}\n"
    if city_col:
        stats_text += f"Cities: {df[city_col].dropna().nunique()}\n"
    axes[1, 0].text(0.1, 0.5, stats_text, transform=axes[1, 0].transAxes, fontsize=12, verticalalignment="center")
    axes[1, 0].axis("off")
    axes[1, 0].set_title("Overview")

    if city_col:
        city_counts = df[city_col].dropna().astype(str).value_counts().head(10)
        if len(city_counts) > 0:
            axes[1, 1].barh(range(len(city_counts)), city_counts.values, color="orange")
            axes[1, 1].set_yticks(range(len(city_counts)))
            axes[1, 1].set_yticklabels(city_counts.index)
            axes[1, 1].set_title("Top 10 Cities")
            axes[1, 1].set_xlabel("Number of Users")
    else:
        axes[1, 1].axis("off")

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PREFIX}summary_dashboard{suffix}.png", dpi=300, bbox_inches="tight")
    plt.close()


def generate_html_dashboard(df, suffix=""):
    """Generate HTML dashboard that embeds FullContact charts."""
    print(f"Generating FullContact HTML dashboard{suffix}...")
    total_users = len(df)
    state_col = _state_col(df)
    city_col = _city_col(df)
    unique_states = df[state_col].dropna().nunique() if state_col else 0
    unique_cities = df[city_col].dropna().nunique() if city_col else 0
    from datetime import datetime
    current_time = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FullContact User Analysis Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; background: #f8f9fa; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px 0; margin-bottom: 30px; border-radius: 10px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        header h1 {{ font-size: 2.5em; margin-bottom: 10px; }}
        header p {{ font-size: 1.2em; opacity: 0.9; }}
        .section {{ background: white; margin-bottom: 40px; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .section-header {{ background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); color: white; padding: 25px; }}
        .section-content {{ padding: 30px; }}
        .chart-container {{ text-align: center; }}
        .chart-container img {{ max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }}
        nav {{ background: white; padding: 20px; border-radius: 10px; margin-bottom: 30px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        nav a {{ color: #667eea; text-decoration: none; padding: 10px 20px; border-radius: 25px; }}
        nav a:hover {{ background: #667eea; color: white; }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>FullContact User Analysis</h1>
            <p>Demographics & geographic insights from fullcontact_matches</p>
            <p style="font-size: 0.95em; margin-top: 8px;">{total_users:,} records | Generated {current_time}</p>
        </header>
        <nav>
            <a href="#summary">Summary</a>
            <a href="#geographic">Geographic</a>
            <a href="#demographics">Demographics</a>
            <a href="#financial">Financial</a>
            <a href="#interests">Interests</a>
        </nav>
        <section id="summary" class="section">
            <div class="section-header"><h2>Executive Summary</h2></div>
            <div class="section-content">
                <div class="chart-container"><img src="{OUTPUT_PREFIX}summary_dashboard{suffix}.png" alt="Summary"></div>
            </div>
        </section>
        <section id="geographic" class="section">
            <div class="section-header"><h2>Geographic Distribution</h2></div>
            <div class="section-content">
                <div class="chart-container"><img src="{OUTPUT_PREFIX}geographic_analysis{suffix}.png" alt="Geographic"></div>
            </div>
        </section>
        <section id="demographics" class="section">
            <div class="section-header"><h2>Demographics</h2></div>
            <div class="section-content">
                <div class="chart-container"><img src="{OUTPUT_PREFIX}demographic_analysis{suffix}.png" alt="Demographics"></div>
            </div>
        </section>
        <section id="financial" class="section">
            <div class="section-header"><h2>Financial Profile</h2></div>
            <div class="section-content">
                <div class="chart-container"><img src="{OUTPUT_PREFIX}financial_analysis{suffix}.png" alt="Financial"></div>
            </div>
        </section>
        <section id="interests" class="section">
            <div class="section-header"><h2>Interests</h2></div>
            <div class="section-content">
                <div class="chart-container"><img src="{OUTPUT_PREFIX}interests_analysis{suffix}.png" alt="Interests"></div>
            </div>
        </section>
    </div>
</body>
</html>"""

    filename = f"{OUTPUT_PREFIX}user_dashboard{suffix}.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"✅ FullContact HTML dashboard written: {filename}")


def _store_id_to_suffix(store_id):
    if store_id is None or (isinstance(store_id, float) and np.isnan(store_id)):
        return ""
    s = str(store_id).strip()
    return "".join(c for c in s if c.isalnum() or c in " _-").replace(" ", "").replace("-", "_").replace("__", "_") or "Store"


def main():
    # Defaults from env: FULLCONTACT_DATABASE_URL, FULLCONTACT_TABLE, FULLCONTACT_STORE_COLUMN, etc.
    db_url_default = _fullcontact_env("DATABASE_URL", "") or os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URI")
    parser = argparse.ArgumentParser(
        description="FullContact user analysis. Reads from PostgreSQL. Config via .env (FULLCONTACT_*) or CLI.",
        epilog="Env: FULLCONTACT_DATABASE_URL, FULLCONTACT_TABLE, FULLCONTACT_STORE_COLUMN, FULLCONTACT_EMAIL_COLUMN, FULLCONTACT_DATA_COLUMN",
    )
    parser.add_argument(
        "--postgres",
        metavar="URL",
        default=db_url_default,
        help="PostgreSQL URL (default: FULLCONTACT_DATABASE_URL or DATABASE_URL)",
    )
    parser.add_argument(
        "--table",
        default=_fullcontact_env("TABLE", "fullcontact_matches"),
        help="Table name (default: FULLCONTACT_TABLE or fullcontact_matches)",
    )
    parser.add_argument(
        "--email-col",
        default=_fullcontact_env("EMAIL_COLUMN", "email"),
        dest="email_col",
        help="Email column name (default: FULLCONTACT_EMAIL_COLUMN or email)",
    )
    parser.add_argument(
        "--data-col",
        default=_fullcontact_env("DATA_COLUMN", "response_json"),
        dest="data_col",
        help="JSON/JSONB column name (default: FULLCONTACT_DATA_COLUMN or response_json)",
    )
    parser.add_argument(
        "--store-col",
        default=_fullcontact_env("STORE_COLUMN", "external_store_id"),
        dest="store_col",
        help="Store ID column in DB for per-store dashboards (default: FULLCONTACT_STORE_COLUMN or external_store_id)",
    )
    args = parser.parse_args()

    if not args.postgres:
        raise SystemExit(
            "PostgreSQL required. Set FULLCONTACT_DATABASE_URL or DATABASE_URL in .env, or pass --postgres."
        )
    if load_from_postgres is None:
        raise SystemExit("fullcontact_loader required.")

    print("=== FullContact User Analysis ===")
    print("Config: table=%s, email_col=%s, data_col=%s, store_col=%s" % (
        args.table, args.email_col, args.data_col, args.store_col,
    ))
    print("Loading from PostgreSQL...\n")
    df = load_from_postgres(
        connection_string=args.postgres,
        table=args.table,
        email_column=args.email_col,
        data_column=args.data_col,
        store_column=args.store_col,
    )
    df = normalize_numeric_columns(df)
    print(f"Records: {len(df):,} | Columns: {len(df.columns)}")

    has_store = "external_store_id" in df.columns and df["external_store_id"].notna().any()
    stores = df["external_store_id"].dropna().unique().tolist() if has_store else []

    def run(data, suf, label):
        create_summary_dashboard(data, suffix=suf)
        create_geographic_analysis(data, suffix=suf)
        create_demographic_analysis(data, suffix=suf)
        create_financial_analysis(data, suffix=suf)
        create_interests_analysis(data, suffix=suf)
        generate_html_dashboard(data, suffix=suf)

    run(df, "", "all")
    if has_store and stores:
        for store_id in stores:
            file_suffix = _store_id_to_suffix(store_id)
            if not file_suffix:
                continue
            suf = "_" + file_suffix
            df_store = df[df["external_store_id"] == store_id]
            if len(df_store) == 0:
                continue
            print(f"\n--- Store: {store_id} ({len(df_store):,} records) ---")
            run(df_store, suf, str(store_id))

    print("\n✅ FullContact analysis complete. Outputs:")
    print(f"  {OUTPUT_PREFIX}summary_dashboard.png, {OUTPUT_PREFIX}geographic_analysis.png,")
    print(f"  {OUTPUT_PREFIX}demographic_analysis.png, {OUTPUT_PREFIX}financial_analysis.png,")
    print(f"  {OUTPUT_PREFIX}interests_analysis.png, {OUTPUT_PREFIX}user_dashboard.html")
    if stores:
        print("  (+ per-store files with suffix)")


if __name__ == "__main__":
    main()
