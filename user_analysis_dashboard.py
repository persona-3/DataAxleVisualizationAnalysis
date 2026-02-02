#!/usr/bin/env python3
"""
User Demographics and Behavior Analysis Dashboard
Analyzes Data Axle enriched user data to provide insights for business teams.
Loads data only from PostgreSQL matched_emails table (email, response_json). No CSV.
"""

import argparse
import os

# Load .env so DATABASE_URL (and POSTGRES_URI) are available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

try:
    from postgres_loader import load_from_postgres
except ImportError:
    load_from_postgres = None

# Set style for better looking plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")


def normalize_numeric_columns(df):
    """
    Coerce columns that should be numeric from string/mixed types (e.g. from Postgres JSON).
    Fixes errors like '<' not supported between instances of 'int' and 'str'.
    """
    numeric_cols = [
        'data.document.attributes.family.member_count',
        'data.document.attributes.family.adult_count',
        'data.document.attributes.family.estimated_income',
        'data.document.attributes.family.estimated_wealth[0]',
        'data.document.attributes.family.estimated_wealth[1]',
        'data.document.attributes.lifestyle_segment',
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    # Interest columns (scores 1-9)
    for col in df.columns:
        if 'interests.' in col and col not in ('data.document.attributes.interests.id', 'data.document.attributes.interests.created_at'):
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def load_and_clean_data(filename='data_axle_results.csv'):
    """Load and perform basic cleaning of the data"""
    print(f"Loading data from {filename}...")
    
    # Read with specific columns we need for analysis
    columns_to_read = [
        'email', 'data.document.attributes.first_name', 'data.document.attributes.last_name',
        'data.document.attributes.gender', 'data.document.attributes.city', 
        'data.document.attributes.state', 'data.document.attributes.postal_code',
        'data.document.attributes.family.estimated_income', 'data.document.attributes.family.estimated_wealth[0]',
        'data.document.attributes.family.estimated_wealth[1]', 'data.document.attributes.family.estimated_education_level', 
        'data.document.attributes.family.home_owner', 'data.document.attributes.family.adult_count', 
        'data.document.attributes.family.member_count', 'data.document.attributes.estimated_married', 
        'data.document.attributes.lifestyle_segment', 'data.document.attributes.political_party_affiliation'
    ]
    
    # Interest columns (sample - there are many more)
    exclude_cols = [
        'data.document.attributes.interests.internet', 'data.document.attributes.interests.credit_cards',
        'data.document.attributes.interests.catalogs'
    ]

    all_columns = pd.read_csv(filename, nrows=0).columns.tolist()
    use_cols = [col for col in all_columns if col not in exclude_cols]
    
    try:
        # Try to read specific columns first
        df = pd.read_csv(filename, usecols=use_cols, low_memory=False)
    except:
        # If that fails, read all columns
        print("Reading all columns...")
        df = pd.read_csv(filename, low_memory=False)
    
    print(f"Loaded {len(df)} records")
    return df

def create_geographic_analysis(df):
    """Create geographic distribution visualizations"""
    print("Creating geographic analysis...")
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Geographic Distribution of Users', fontsize=16, fontweight='bold')
    
    # State distribution
    if 'data.document.attributes.state' in df.columns:
        state_counts = df['data.document.attributes.state'].value_counts().head(15)
        axes[0,0].bar(range(len(state_counts)), state_counts.values)
        axes[0,0].set_xticks(range(len(state_counts)))
        axes[0,0].set_xticklabels(state_counts.index, rotation=45)
        axes[0,0].set_title('Top 15 States by User Count')
        axes[0,0].set_ylabel('Number of Users')
    
    # City distribution (top cities)
    if 'data.document.attributes.city' in df.columns:
        city_counts = df['data.document.attributes.city'].value_counts().head(15)
        axes[0,1].barh(range(len(city_counts)), city_counts.values)
        axes[0,1].set_yticks(range(len(city_counts)))
        axes[0,1].set_yticklabels(city_counts.index)
        axes[0,1].set_title('Top 15 Cities by User Count')
        axes[0,1].set_xlabel('Number of Users')
    
    # Geographic heat map by state (if we have enough data)
    if 'data.document.attributes.state' in df.columns:
        state_data = df['data.document.attributes.state'].value_counts()
        # Create a simple visualization
        top_states = state_data.head(20)
        bars = axes[1,0].bar(range(len(top_states)), top_states.values, 
                            color=plt.cm.viridis(np.linspace(0, 1, len(top_states))))
        axes[1,0].set_xticks(range(len(top_states)))
        axes[1,0].set_xticklabels(top_states.index, rotation=45)
        axes[1,0].set_title('User Concentration by State (Top 20)')
        axes[1,0].set_ylabel('Number of Users')
    
    # Geographic diversity metrics
    if 'data.document.attributes.state' in df.columns:
        total_states = df['data.document.attributes.state'].nunique()
        total_cities = df['data.document.attributes.city'].nunique() if 'data.document.attributes.city' in df.columns else 0
        
        metrics_text = f"""Geographic Coverage:
        
Total States: {total_states}
Total Cities: {total_cities}
Most Common State: {df['data.document.attributes.state'].mode().iloc[0] if len(df['data.document.attributes.state'].mode()) > 0 else 'N/A'}
        
Top 3 States:
"""
        if len(state_counts) >= 3:
            for i, (state, count) in enumerate(state_counts.head(3).items()):
                pct = (count / len(df)) * 100
                metrics_text += f"{i+1}. {state}: {count} ({pct:.1f}%)\n"
        
        # axes[1,1].text(0.1, 0.9, metrics_text, transform=axes[1,1].transAxes, 
        #               fontsize=10, verticalalignment='top', 
        #               bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
        axes[1,1].set_xlim(0, 1)
        axes[1,1].set_ylim(0, 1)
        axes[1,1].axis('off')
        # axes[1,1].set_title('Geographic Summary')
    
    plt.tight_layout()
    plt.savefig('geographic_analysis.png', dpi=300, bbox_inches='tight')
    # plt.show()

def create_demographic_analysis(df):
    """Create demographic distribution visualizations"""
    print("Creating demographic analysis...")
    
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('User Demographics Analysis', fontsize=16, fontweight='bold')
    
    # Gender distribution
    if 'data.document.attributes.gender' in df.columns:
        gender_counts = df['data.document.attributes.gender'].value_counts()
        axes[0,0].pie(gender_counts.values, labels=gender_counts.index, autopct='%1.1f%%')
        axes[0,0].set_title('Gender Distribution')
    
    # Marital status
    if 'data.document.attributes.estimated_married' in df.columns:
        married_counts = df['data.document.attributes.estimated_married'].value_counts()
        axes[0,1].pie(married_counts.values, labels=['Married' if x else 'Single' for x in married_counts.index], 
                     autopct='%1.1f%%')
        axes[0,1].set_title('Marital Status Distribution')
    
    # Home ownership
    # if 'data.document.attributes.family.home_owner' in df.columns:
    #     home_counts = df['data.document.attributes.family.home_owner'].value_counts()
    #     axes[0,2].pie(home_counts.values, labels=['Owner' if x else 'Renter' for x in home_counts.index], 
    #                  autopct='%1.1f%%')
    #     axes[0,2].set_title('Home Ownership')
    
    # Education level
    if 'data.document.attributes.family.estimated_education_level' in df.columns:
        edu_counts = df['data.document.attributes.family.estimated_education_level'].value_counts()
        axes[1,0].bar(range(len(edu_counts)), edu_counts.values)
        axes[1,0].set_xticks(range(len(edu_counts)))
        axes[1,0].set_xticklabels(edu_counts.index, rotation=45)
        axes[1,0].set_title('Education Level Distribution')
        axes[1,0].set_ylabel('Number of Users')
    
    # Family size
    if 'data.document.attributes.family.member_count' in df.columns:
        family_size = df['data.document.attributes.family.member_count'].value_counts().sort_index()
        axes[1,1].bar(family_size.index, family_size.values)
        axes[1,1].set_title('Family Size Distribution')
        axes[1,1].set_xlabel('Family Members')
        axes[1,1].set_ylabel('Number of Users')
    
    # Political affiliation
    # if 'data.document.attributes.political_party_affiliation' in df.columns:
    #     pol_counts = df['data.document.attributes.political_party_affiliation'].value_counts()
    #     if len(pol_counts) > 0:
    #         axes[1,2].pie(pol_counts.values, labels=pol_counts.index, autopct='%1.1f%%')
    #         axes[1,2].set_title('Political Affiliation')
    #     else:
    #         axes[1,2].text(0.5, 0.5, 'No Political Data Available', 
    #                       transform=axes[1,2].transAxes, ha='center', va='center')
    #         axes[1,2].set_title('Political Affiliation')
    
    plt.tight_layout()
    plt.savefig('demographic_analysis.png', dpi=300, bbox_inches='tight')
    # plt.show()

def create_financial_analysis(df):
    """Create financial/income distribution visualizations"""
    print("Creating financial analysis...")
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Financial Profile of Users', fontsize=16, fontweight='bold')
    
    # Income distribution
    if 'data.document.attributes.family.estimated_income' in df.columns:
        income_data = df['data.document.attributes.family.estimated_income'].dropna()
        if len(income_data) > 0:
            axes[0,0].hist(income_data, bins=30, edgecolor='black', alpha=0.7)
            axes[0,0].set_title('Estimated Income Distribution')
            axes[0,0].set_xlabel('Income ($)')
            axes[0,0].set_ylabel('Number of Users')
            
            # Format x-axis to show proper dollar amounts
            def format_dollars_income(x, pos):
                if x >= 1000000:
                    return f'${x/1000000:.1f}M'
                elif x >= 1000:
                    return f'${x/1000:.0f}K'
                else:
                    return f'${x:.0f}'
            
            from matplotlib.ticker import FuncFormatter
            axes[0,0].xaxis.set_major_formatter(FuncFormatter(format_dollars_income))
            
            # Add income statistics
            income_stats = f"""Income Statistics:
Mean: ${income_data.mean():,.0f}
Median: ${income_data.median():,.0f}
Q1: ${income_data.quantile(0.25):,.0f}
Q3: ${income_data.quantile(0.75):,.0f}"""
            # axes[0,0].text(0.7, 0.95, income_stats, transform=axes[0,0].transAxes, 
            #               fontsize=9, verticalalignment='top',
            #               bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))
    
    # Wealth distribution (using range midpoint)
    if ('data.document.attributes.family.estimated_wealth[0]' in df.columns and 
        'data.document.attributes.family.estimated_wealth[1]' in df.columns):
        wealth_min = pd.to_numeric(df['data.document.attributes.family.estimated_wealth[0]'], errors='coerce')
        wealth_max = pd.to_numeric(df['data.document.attributes.family.estimated_wealth[1]'], errors='coerce')
        
        # Calculate midpoint of wealth ranges for analysis
        wealth_midpoint = (wealth_min + wealth_max) / 2
        wealth_midpoint = wealth_midpoint.dropna()
        
        if len(wealth_midpoint) > 0:
            axes[0,1].hist(wealth_midpoint, bins=30, edgecolor='black', alpha=0.7, color='green')
            axes[0,1].set_title('Estimated Wealth Distribution\n(Range Midpoints)')
            axes[0,1].set_xlabel('Wealth ($)')
            axes[0,1].set_ylabel('Number of Users')
            
            # Format x-axis to show proper dollar amounts
            def format_dollars(x, pos):
                if x >= 1000000:
                    return f'${x/1000000:.1f}M'
                elif x >= 1000:
                    return f'${x/1000:.0f}K'
                else:
                    return f'${x:.0f}'
            
            from matplotlib.ticker import FuncFormatter
            axes[0,1].xaxis.set_major_formatter(FuncFormatter(format_dollars))
            
            # Add wealth statistics
            wealth_stats = f"""Wealth Statistics:
Mean: ${wealth_midpoint.mean():,.0f}
Median: ${wealth_midpoint.median():,.0f}
Q1: ${wealth_midpoint.quantile(0.25):,.0f}
Q3: ${wealth_midpoint.quantile(0.75):,.0f}
Range Data: {len(wealth_midpoint):,} users"""
            # axes[0,1].text(0.7, 0.95, wealth_stats, transform=axes[0,1].transAxes, 
            #               fontsize=9, verticalalignment='top',
            #               bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.8))
    
    # Income vs Home Ownership
    # if 'data.document.attributes.family.estimated_income' in df.columns and 'data.document.attributes.family.home_owner' in df.columns:
    #     income_by_ownership = df.groupby('data.document.attributes.family.home_owner')['data.document.attributes.family.estimated_income'].mean()
        # if len(income_by_ownership) > 0:
        #     bars = axes[1,0].bar(['Renter', 'Owner'], income_by_ownership.values)
        #     axes[1,0].set_title('Average Income by Home Ownership')
        #     axes[1,0].set_ylabel('Average Income ($)')
            
        #     # Add value labels on bars
        #     for bar, value in zip(bars, income_by_ownership.values):
        #         axes[1,0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1000, 
        #                       f'${value:,.0f}', ha='center', va='bottom')
    
    # Wealth brackets (using range midpoints)
    if ('data.document.attributes.family.estimated_wealth[0]' in df.columns and 
        'data.document.attributes.family.estimated_wealth[1]' in df.columns):
        wealth_min = pd.to_numeric(df['data.document.attributes.family.estimated_wealth[0]'], errors='coerce')
        wealth_max = pd.to_numeric(df['data.document.attributes.family.estimated_wealth[1]'], errors='coerce')
        wealth_midpoint = (wealth_min + wealth_max) / 2
        wealth_midpoint = wealth_midpoint.dropna()
        
        if len(wealth_midpoint) > 0:
            # Create wealth brackets
            wealth_brackets = pd.cut(wealth_midpoint, 
                                   bins=[0, 50000, 100000, 250000, 500000, 1000000, float('inf')],
                                   labels=['<$50K', '$50K-$100K', '$100K-$250K', '$250K-$500K', '$500K-$1M', '>$1M'])
            bracket_counts = wealth_brackets.value_counts()
            
            axes[1,0].pie(bracket_counts.values, labels=bracket_counts.index, autopct='%1.1f%%')
            axes[1,0].set_title('Wealth Bracket Distribution\n(Range Midpoints)')
    else:
        # Fallback to income brackets if wealth data not available
        if 'data.document.attributes.family.estimated_income' in df.columns:
            income_data = df['data.document.attributes.family.estimated_income'].dropna()
            if len(income_data) > 0:
                # Create income brackets
                income_brackets = pd.cut(income_data, 
                                       bins=[0, 25000, 50000, 75000, 100000, 150000, float('inf')],
                                       labels=['<$25K', '$25K-$50K', '$50K-$75K', '$75K-$100K', '$100K-$150K', '>$150K'])
                bracket_counts = income_brackets.value_counts()
                
                axes[1,0].pie(bracket_counts.values, labels=bracket_counts.index, autopct='%1.1f%%')
                axes[1,0].set_title('Income Bracket Distribution')

    axes[1,1].axis('off')
    
    plt.tight_layout()
    plt.savefig('financial_analysis.png', dpi=300, bbox_inches='tight')
    # plt.show()

def create_interests_analysis(df):
    """Create interests and lifestyle analysis"""
    print("Creating interests analysis...")

    # Find interest columns
    interest_cols = [col for col in df.columns if 'interests.' in col and col not in ['data.document.attributes.interests.id', 'data.document.attributes.interests.created_at']]
    
    if len(interest_cols) == 0:
        print("No interest data found in the dataset")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    fig.suptitle('User Interests and Lifestyle Analysis\n(Scores: 1=Low Interest, 9=High Interest)', fontsize=16, fontweight='bold')
    
    # Analyze interest scores properly (1-9 scale)
    interest_analysis = {}
    for col in interest_cols[:100]:
        interest_name = col.split('.')[-1].replace('_', ' ').title()
        interest_data = df[col].dropna()
        
        if len(interest_data) > 0:
            # Convert to numeric, handling any string values
            try:
                interest_data = pd.to_numeric(interest_data, errors='coerce').dropna()
                if len(interest_data) > 0:
                    interest_analysis[interest_name] = {
                        'user_count': len(interest_data),
                        'avg_score': interest_data.mean(),
                        'high_interest_users': (interest_data >= 7).sum(),  # Users with strong interest (7-9)
                        'weighted_score': len(interest_data) * interest_data.mean()  # Volume * Intensity
                    }
            except:
                continue
    
    if interest_analysis:
        # Top interests by user volume
        top_by_volume = sorted(interest_analysis.items(), key=lambda x: x[1]['user_count'], reverse=True)[:15]
        interests, data = zip(*top_by_volume)
        user_counts = [d['user_count'] for d in data]
        
        axes[0,0].barh(range(len(interests)), user_counts, color='skyblue')
        axes[0,0].set_yticks(range(len(interests)))
        axes[0,0].set_yticklabels(interests)
        axes[0,0].set_title('Top 15 Interests by User Volume')
        axes[0,0].set_xlabel('Number of Users with This Interest')
        
        # Top interests by average score (intensity)
        top_by_intensity = sorted(interest_analysis.items(), key=lambda x: x[1]['avg_score'], reverse=True)[:15]
        interests_int, data_int = zip(*top_by_intensity)
        avg_scores = [d['avg_score'] for d in data_int]
        
        bars = axes[0,1].barh(range(len(interests_int)), avg_scores, color='lightcoral')
        axes[0,1].set_yticks(range(len(interests_int)))
        axes[0,1].set_yticklabels(interests_int)
        axes[0,1].set_title('Top 15 Interests by Average Score (Intensity)')
        axes[0,1].set_xlabel('Average Interest Score (1-9)')
        axes[0,1].set_xlim(0, 9)
        
        # Add score labels on bars
        for i, (bar, score) in enumerate(zip(bars, avg_scores)):
            axes[0,1].text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2, 
                          f'{score:.1f}', ha='left', va='center', fontsize=8)
        
        # High engagement interests (users with scores 7-9)
        high_engagement = sorted(interest_analysis.items(), key=lambda x: x[1]['high_interest_users'], reverse=True)[:15]
        interests_he, data_he = zip(*high_engagement)
        high_users = [d['high_interest_users'] for d in data_he]
        
        axes[1,0].barh(range(len(interests_he)), high_users, color='lightgreen')
        axes[1,0].set_yticks(range(len(interests_he)))
        axes[1,0].set_yticklabels(interests_he)
        axes[1,0].set_title('Top 15 Interests by High Engagement\n(Users with Scores 7-9)')
        axes[1,0].set_xlabel('Number of Highly Engaged Users')
    
    # Lifestyle segments
    if 'data.document.attributes.lifestyle_segment' in df.columns:
        lifestyle_counts = df['data.document.attributes.lifestyle_segment'].value_counts().head(10)
        if len(lifestyle_counts) > 0:
            axes[1,1].pie(lifestyle_counts.values, labels=lifestyle_counts.index, autopct='%1.1f%%')
            axes[1,1].set_title('Top Lifestyle Segments')
    
    # Interest score distribution
    if interest_analysis:
        all_scores = []
        for col in interest_cols[:50]:  # Sample from interest columns
            scores = pd.to_numeric(df[col], errors='coerce').dropna()
            all_scores.extend(scores.tolist())
        
        # if all_scores:
        #     axes[1,1].hist(all_scores, bins=range(1, 11), edgecolor='black', alpha=0.7, color='orange')
        #     axes[1,1].set_title('Distribution of Interest Scores\n(All Interests Combined)')
        #     axes[1,1].set_xlabel('Interest Score (1-9)')
        #     axes[1,1].set_ylabel('Frequency')
        #     axes[1,1].set_xticks(range(1, 10))
            
        #     # Add statistics
        #     mean_score = np.mean(all_scores)
        #     axes[1,1].axvline(mean_score, color='red', linestyle='--', linewidth=2, 
        #                      label=f'Mean: {mean_score:.1f}')
        #     axes[1,1].legend()
    
    # Interest insights and statistics
    total_users = len(df)
    total_interest_categories = len(interest_analysis)
    
    if interest_analysis:
        # Calculate average scores across all interests
        all_avg_scores = [data['avg_score'] for data in interest_analysis.values()]
        overall_avg_score = np.mean(all_avg_scores)
        
        # Find most engaging interest
        most_engaging = max(interest_analysis.items(), key=lambda x: x[1]['avg_score'])
        most_popular = max(interest_analysis.items(), key=lambda x: x[1]['user_count'])
        
        summary_text = f"""Interest Insights:

📊 OVERVIEW:
• Total Users: {total_users:,}
• Interest Categories: {total_interest_categories}
• Overall Avg Score: {overall_avg_score:.1f}/9

🔥 HIGHEST ENGAGEMENT:
• {most_engaging[0]}: {most_engaging[1]['avg_score']:.1f}/9
• {most_engaging[1]['user_count']:,} users

👥 MOST POPULAR:
• {most_popular[0]}: {most_popular[1]['user_count']:,} users
• Avg Score: {most_popular[1]['avg_score']:.1f}/9

📈 HIGH INTEREST USERS (7-9):"""
        
        for i, (interest, data) in enumerate(high_engagement[:3]):
            pct = (data['high_interest_users'] / total_users) * 100
            summary_text += f"\n{i+1}. {interest}: {data['high_interest_users']:,} ({pct:.1f}%)"
        
        # axes[1,2].text(0.05, 0.95, summary_text, transform=axes[1,2].transAxes, 
        #               fontsize=10, verticalalignment='top',
        #               bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        # axes[1,2].set_xlim(0, 1)
        # axes[1,2].set_ylim(0, 1)
        # axes[1,2].axis('off')
        # axes[1,2].set_title('Key Interest Insights')
    
    plt.tight_layout()
    plt.savefig('interests_analysis.png', dpi=300, bbox_inches='tight')
    # plt.show()

def create_summary_dashboard(df):
    """Create a high-level summary dashboard"""
    print("Creating summary dashboard...")
    
    fig, axes = plt.subplots(2, 2, figsize=(20, 12))
    fig.suptitle('User Base Summary Dashboard', fontsize=18, fontweight='bold')
    
    # Key metrics
    total_users = len(df)
    unique_states = df['data.document.attributes.state'].nunique() if 'data.document.attributes.state' in df.columns else 0
    unique_cities = df['data.document.attributes.city'].nunique() if 'data.document.attributes.city' in df.columns else 0
    avg_income = df['data.document.attributes.family.estimated_income'].mean() if 'data.document.attributes.family.estimated_income' in df.columns else 0
    
    # User volume by state (top 10)
    if 'data.document.attributes.state' in df.columns:
        state_counts = df['data.document.attributes.state'].value_counts().head(10)
        axes[0,0].bar(range(len(state_counts)), state_counts.values, color='skyblue')
        axes[0,0].set_xticks(range(len(state_counts)))
        axes[0,0].set_xticklabels(state_counts.index, rotation=45)
        axes[0,0].set_title('Top 10 States by User Volume')
        axes[0,0].set_ylabel('Number of Users')
    
    # Gender & Marital Status
    demo_data = []
    demo_labels = []
    
    if 'data.document.attributes.gender' in df.columns:
        gender_counts = df['data.document.attributes.gender'].value_counts()
        demo_data.extend(gender_counts.values)
        demo_labels.extend([f"{k} Gender" for k in gender_counts.index])
    
    if demo_data:
        axes[0,1].pie(demo_data, labels=demo_labels, autopct='%1.1f%%')
        axes[0,1].set_title('User Demographics')
    
    # Income distribution summary
    if 'data.document.attributes.family.estimated_income' in df.columns:
        income_data = df['data.document.attributes.family.estimated_income'].dropna()
        if len(income_data) > 0:
            income_brackets = pd.cut(income_data, 
                                   bins=[0, 30000, 60000, 100000, 150000, float('inf')],
                                   labels=['<$30K', '$30K-$60K', '$60K-$100K', '$100K-$150K', '>$150K'])
            bracket_counts = income_brackets.value_counts()
            
            axes[1,0].bar(range(len(bracket_counts)), bracket_counts.values, color='lightgreen')
            axes[1,0].set_xticks(range(len(bracket_counts)))
            axes[1,0].set_xticklabels(bracket_counts.index, rotation=45)
            axes[1,0].set_title('Income Distribution')
            axes[1,0].set_ylabel('Number of Users')
    
    # Key statistics text
    stats_text = f"""USER BASE OVERVIEW
    
📊 TOTAL USERS: {total_users:,}
    
🗺️ GEOGRAPHIC REACH:
• States: {unique_states}
• Cities: {unique_cities}
    
💰 FINANCIAL PROFILE:
• Avg Income: ${avg_income:,.0f}
    
👥 DEMOGRAPHICS:"""
    
    if 'data.document.attributes.gender' in df.columns:
        gender_dist = df['data.document.attributes.gender'].value_counts()
        for gender, count in gender_dist.items():
            pct = (count / total_users) * 100
            stats_text += f"\n• {gender}: {pct:.1f}%"
    
    if 'data.document.attributes.family.home_owner' in df.columns:
        homeowner_pct = (df['data.document.attributes.family.home_owner'].sum() / total_users) * 100
        stats_text += f"\n• Homeowners: {homeowner_pct:.1f}%"
    
    # axes[1,0].text(0.05, 0.95, stats_text, transform=axes[1,0].transAxes, 
    #               fontsize=12, verticalalignment='top', fontweight='bold',
    #               bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
    # axes[1,0].set_xlim(0, 1)
    # axes[1,0].set_ylim(0, 1)
    # axes[1,0].axis('off')
    
    # Top cities
    if 'data.document.attributes.city' in df.columns:
        city_counts = df['data.document.attributes.city'].value_counts().head(10)
        axes[1,1].barh(range(len(city_counts)), city_counts.values, color='orange')
        axes[1,1].set_yticks(range(len(city_counts)))
        axes[1,1].set_yticklabels(city_counts.index)
        axes[1,1].set_title('Top 10 Cities')
        axes[1,1].set_xlabel('Number of Users')
    
    # Business insights
    insights_text = """KEY BUSINESS INSIGHTS
    
🎯 TARGET SEGMENTS:"""
    
    if 'data.document.attributes.state' in df.columns:
        top_state = df['data.document.attributes.state'].value_counts().index[0]
        top_state_pct = (df['data.document.attributes.state'].value_counts().iloc[0] / total_users) * 100
        insights_text += f"\n• {top_state}: {top_state_pct:.1f}% of users"
    
    if 'data.document.attributes.family.estimated_income' in df.columns:
        high_income = (df['data.document.attributes.family.estimated_income'] > 75000).sum()
        high_income_pct = (high_income / total_users) * 100
        insights_text += f"\n• High Income (>$75K): {high_income_pct:.1f}%"
    
    insights_text += f"""
    
📈 GROWTH OPPORTUNITIES:
• Geographic expansion potential
• Interest-based targeting
• Income-based product tiers
    
🔍 RECOMMENDATIONS:
• Focus marketing in top states
• Develop premium offerings
• Target high-income segments"""
    
    # axes[1,2].text(0.05, 0.95, insights_text, transform=axes[1,2].transAxes, 
    #               fontsize=10, verticalalignment='top',
    #               bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))
    # axes[1,2].set_xlim(0, 1)
    # axes[1,2].set_ylim(0, 1)
    # axes[1,2].axis('off')
    
    plt.tight_layout()
    plt.savefig('summary_dashboard.png', dpi=300, bbox_inches='tight')
    # plt.show()

def create_business_insights_report(df):
    """Generate key business insights about user interests"""
    print("Generating business insights report...")
    
    # Find interest columns
    interest_cols = [col for col in df.columns if 'interests.' in col and col not in ['data.document.attributes.interests.id', 'data.document.attributes.interests.created_at']]
    
    insights = {}
    for col in interest_cols:
        interest_name = col.split('.')[-1].replace('_', ' ').title()
        interest_data = pd.to_numeric(df[col], errors='coerce').dropna()
        
        if len(interest_data) > 0:
            insights[interest_name] = {
                'users': len(interest_data),
                'avg_score': interest_data.mean(),
                'high_interest': (interest_data >= 7).sum(),
                'moderate_interest': ((interest_data >= 4) & (interest_data < 7)).sum(),
                'low_interest': (interest_data < 4).sum()
            }
    
    # Business segments based on interests
    print("\n🎯 KEY BUSINESS INSIGHTS:")
    print("=" * 50)
    
    if insights:
        # High-value segments (high engagement + volume)
        high_value = sorted(insights.items(), 
                           key=lambda x: x[1]['high_interest'] * x[1]['avg_score'], 
                           reverse=True)[:5]
        
        print("\n📈 HIGH-VALUE INTEREST SEGMENTS:")
        for i, (interest, data) in enumerate(high_value):
            engagement_rate = (data['high_interest'] / data['users']) * 100
            print(f"{i+1}. {interest}:")
            print(f"   • {data['high_interest']:,} highly engaged users ({engagement_rate:.1f}%)")
            print(f"   • Average score: {data['avg_score']:.1f}/9")
            print(f"   • Total interested users: {data['users']:,}")
        
        # Emerging opportunities (moderate volume, high intensity)
        emerging = sorted([(k, v) for k, v in insights.items() if v['users'] >= 100 and v['avg_score'] >= 6], 
                         key=lambda x: x[1]['avg_score'], reverse=True)[:3]
        
        print("\n🚀 EMERGING OPPORTUNITIES:")
        for i, (interest, data) in enumerate(emerging):
            print(f"{i+1}. {interest}: {data['avg_score']:.1f}/9 avg score, {data['users']:,} users")
        
        # Mass market interests (high volume)
        mass_market = sorted(insights.items(), key=lambda x: x[1]['users'], reverse=True)[:5]
        
        print("\n👥 MASS MARKET INTERESTS:")
        for i, (interest, data) in enumerate(mass_market):
            print(f"{i+1}. {interest}: {data['users']:,} users (avg: {data['avg_score']:.1f}/9)")

def generate_html_dashboard(df):
    """Generate HTML dashboard that combines all visualizations with dynamic content"""
    print("Generating dynamic HTML dashboard...")
    
    # Calculate key metrics from the data
    total_users = len(df)
    unique_states = df['data.document.attributes.state'].nunique() if 'data.document.attributes.state' in df.columns else 0
    unique_cities = df['data.document.attributes.city'].nunique() if 'data.document.attributes.city' in df.columns else 0
    
    # Calculate interest insights
    interest_cols = [col for col in df.columns if 'interests.' in col and col not in ['data.document.attributes.interests.id', 'data.document.attributes.interests.created_at']]
    total_interests = 0
    high_value_interests = []
    emerging_opportunities = []
    mass_market_interests = []
    
    interest_analysis = {}
    for col in interest_cols:
        interest_name = col.split('.')[-1].replace('_', ' ').title()
        interest_data = pd.to_numeric(df[col], errors='coerce').dropna()
        
        if len(interest_data) > 0:
            interest_analysis[interest_name] = {
                'users': len(interest_data),
                'avg_score': interest_data.mean(),
                'high_interest': (interest_data >= 7).sum(),
                'moderate_interest': ((interest_data >= 4) & (interest_data < 7)).sum(),
                'low_interest': (interest_data < 4).sum()
            }
    
    total_interests = len(interest_analysis)
    
    if interest_analysis:
        # High-value segments (high engagement + volume)
        high_value_temp = sorted(interest_analysis.items(), 
                                key=lambda x: x[1]['high_interest'] * x[1]['avg_score'], 
                                reverse=True)[:3]
        high_value_interests = high_value_temp
        
        # Emerging opportunities
        emerging_temp = sorted([(k, v) for k, v in interest_analysis.items() if v['users'] >= 50 and v['avg_score'] >= 6], 
                              key=lambda x: x[1]['avg_score'], reverse=True)[:3]
        emerging_opportunities = emerging_temp
        
        # Mass market interests
        mass_market_temp = sorted(interest_analysis.items(), key=lambda x: x[1]['users'], reverse=True)[:3]
        mass_market_interests = mass_market_temp
    
    # Geographic insights
    top_states = []
    if 'data.document.attributes.state' in df.columns:
        state_counts = df['data.document.attributes.state'].value_counts().head(3)
        for state, count in state_counts.items():
            pct = (count / total_users) * 100
            top_states.append((state, count, pct))
    
    # Income insights
    avg_income = 0
    if 'data.document.attributes.family.estimated_income' in df.columns:
        avg_income = df['data.document.attributes.family.estimated_income'].mean()
    
    # Generate current timestamp
    from datetime import datetime
    current_time = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    
    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>User Demographics & Behavior Analysis Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f8f9fa;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px 0;
            margin-bottom: 30px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        
        header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 300;
        }}
        
        header p {{
            font-size: 1.2em;
            opacity: 0.9;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        
        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }}
        
        .stat-card:hover {{
            transform: translateY(-5px);
        }}
        
        .stat-number {{
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 10px;
        }}
        
        .stat-label {{
            color: #666;
            font-size: 1.1em;
        }}
        
        .nav-menu {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .nav-menu ul {{
            list-style: none;
            display: flex;
            flex-wrap: wrap;
            justify-content: center;
            gap: 30px;
        }}
        
        .nav-menu a {{
            color: #667eea;
            text-decoration: none;
            font-weight: 500;
            padding: 10px 20px;
            border-radius: 25px;
            transition: all 0.3s ease;
        }}
        
        .nav-menu a:hover {{
            background: #667eea;
            color: white;
        }}
        
        .section {{
            background: white;
            margin-bottom: 40px;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .section-header {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            color: white;
            padding: 25px;
        }}
        
        .section-header h2 {{
            font-size: 1.8em;
            margin-bottom: 10px;
        }}
        
        .section-header p {{
            opacity: 0.9;
            font-size: 1.1em;
        }}
        
        .section-content {{
            padding: 30px;
        }}
        
        .chart-container {{
            text-align: center;
            margin-bottom: 30px;
        }}
        
        .chart-container img {{
            max-width: 100%;
            height: auto;
            border-radius: 8px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        }}
        
        .insights-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 25px;
            margin-top: 30px;
        }}
        
        .insight-card {{
            background: #f8f9fa;
            padding: 25px;
            border-radius: 10px;
            border-left: 5px solid #667eea;
        }}
        
        .insight-card h4 {{
            color: #667eea;
            margin-bottom: 15px;
            font-size: 1.2em;
        }}
        
        .insight-card ul {{
            list-style: none;
        }}
        
        .insight-card li {{
            margin-bottom: 8px;
            padding-left: 20px;
            position: relative;
        }}
        
        .insight-card li:before {{
            content: "▶";
            color: #667eea;
            position: absolute;
            left: 0;
        }}
        
        .business-insights {{
            background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
            padding: 30px;
            border-radius: 10px;
            margin: 30px 0;
        }}
        
        .business-insights h3 {{
            color: #2c3e50;
            margin-bottom: 20px;
            font-size: 1.5em;
        }}
        
        .insights-columns {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 25px;
        }}
        
        .insight-column h4 {{
            color: #34495e;
            margin-bottom: 15px;
            font-size: 1.2em;
        }}
        
        .insight-item {{
            background: rgba(255,255,255,0.7);
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 10px;
        }}
        
        .insight-item strong {{
            color: #2c3e50;
        }}
        
        footer {{
            background: #2c3e50;
            color: white;
            text-align: center;
            padding: 30px;
            border-radius: 10px;
            margin-top: 40px;
        }}
        
        .methodology {{
            background: #e8f4f8;
            padding: 25px;
            border-radius: 10px;
            margin: 30px 0;
            border-left: 5px solid #17a2b8;
        }}
        
        .methodology h4 {{
            color: #17a2b8;
            margin-bottom: 15px;
        }}
        
        .timestamp {{
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            text-align: center;
            color: #856404;
        }}
        
        @media (max-width: 768px) {{
            .container {{
                padding: 10px;
            }}
            
            header h1 {{
                font-size: 2em;
            }}
            
            .stats-grid {{
                grid-template-columns: repeat(2, 1fr);
            }}
            
            .nav-menu ul {{
                flex-direction: column;
                align-items: center;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>User Demographics & Behavior Analysis</h1>
            <p>Data-Driven Insights for Strategic Business Decisions</p>
            <p style="font-size: 1em; margin-top: 10px; opacity: 0.8;">Based on {total_users:,} Data Axle enriched user records</p>
            <p style="font-size: 0.9em; margin-top: 6px; opacity: 0.85;">Source: PostgreSQL table <code>matched_emails</code> (email, response_json) | Generated {current_time}</p>
        </header>

        <!-- <div class="timestamp">
            📅 Dashboard generated on {current_time} | Data is current as of latest analysis
        </div> -->

        <!-- <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-number">{total_users:,}</div>
                <div class="stat-label">Total Users Analyzed</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{unique_states}</div>
                <div class="stat-label">States Covered</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{unique_cities}</div>
                <div class="stat-label">Cities Represented</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{total_interests}</div>
                <div class="stat-label">Interest Categories</div>
            </div>
        </div> -->

        <nav class="nav-menu">
            <ul>
                <li><a href="#summary">Executive Summary</a></li>
                <li><a href="#geographic">Geographic Analysis</a></li>
                <li><a href="#demographics">Demographics</a></li>
                <li><a href="#financial">Financial Profile</a></li>
                <li><a href="#interests">Interest Intelligence</a></li>
                <!-- <li><a href="#business-insights">Business Insights</a></li> -->
            </ul>
        </nav>

        <section id="summary" class="section">
            <div class="section-header">
                <h2>📊 Executive Summary Dashboard</h2>
                <p>High-level overview of user base with key metrics and strategic insights</p>
            </div>
            <div class="section-content">
                <div class="chart-container">
                    <img src="summary_dashboard.png" alt="Executive Summary Dashboard">
                </div>
                <!-- <div class="insights-grid">
                    <div class="insight-card">
                        <h4>🎯 Key Takeaways</h4>
                        <ul>
                            <li>Geographic presence across {unique_states} states and {unique_cities} cities</li>'''
    
    if avg_income > 0:
        html_content += f'''
                            <li>Average income: ${avg_income:,.0f} creates diverse segments</li>'''
    
    html_content += f'''
                            <li>Rich interest data with {total_interests} categories analyzed</li>
                            <li>Strong engagement patterns across multiple interests</li>
                        </ul>
                    </div>
                    <div class="insight-card">
                        <h4>📈 Growth Opportunities</h4>
                        <ul>'''
    
    if top_states:
        html_content += f'''
                            <li>Geographic expansion beyond top states: {', '.join([state for state, _, _ in top_states[:3]])}</li>'''
    
    html_content += f'''
                            <li>Premium product offerings for high-income segments</li>
                            <li>Interest-based targeted marketing campaigns</li>
                            <li>Cross-selling opportunities between related interests</li>
                        </ul>
                    </div>
                </div> -->
            </div>
        </section>

        <section id="geographic" class="section">
            <div class="section-header">
                <h2>🗺️ Geographic Distribution</h2>
                <p>Understanding where users are located for regional strategy and expansion planning</p>
            </div>
            <div class="section-content">
                <div class="chart-container">
                    <img src="geographic_analysis.png" alt="Geographic Analysis">
                </div>
                <!-- <div class="insights-grid">
                    <div class="insight-card">
                        <h4>🏙️ Market Concentration</h4>
                        <ul>'''
    
    for i, (state, count, pct) in enumerate(top_states):
        html_content += f'''
                            <li>{state}: {count:,} users ({pct:.1f}% of total)</li>'''
    
    html_content += f'''
                        </ul>
                    </div>
                    <div class="insight-card">
                        <h4>🎯 Strategic Implications</h4>
                        <ul>
                            <li>Focus marketing spend in high-concentration areas</li>
                            <li>Develop region-specific campaigns</li>
                            <li>Consider local partnerships in key markets</li>
                            <li>Tailor product offerings to regional preferences</li>
                        </ul>
                    </div>
                </div> -->
            </div>
        </section>

        <section id="demographics" class="section">
            <div class="section-header">
                <h2>👥 User Demographics</h2>
                <p>Comprehensive profile of user base including gender, education, and family characteristics</p>
            </div>
            <div class="section-content">
                <div class="chart-container">
                    <img src="demographic_analysis.png" alt="Demographic Analysis">
                </div>
                <!-- <div class="insights-grid">
                    <div class="insight-card">
                        <h4>👨‍👩‍👧‍👦 Family Insights</h4>
                        <ul>
                            <li>Diverse family sizes and structures</li>
                            <li>High home ownership rates indicate stability</li>
                            <li>Education levels span from high school to post-graduate</li>
                            <li>Mixed marital status creates varied targeting opportunities</li>
                        </ul>
                    </div>
                    <div class="insight-card">
                        <h4>🎯 Targeting Strategy</h4>
                        <ul>
                            <li>Family-oriented products and services</li>
                            <li>Education-based content and offerings</li>
                            <li>Home ownership benefits and services</li>
                            <li>Life stage-appropriate marketing messages</li>
                        </ul>
                    </div>
                </div> -->
            </div>
        </section>

        <section id="financial" class="section">
            <div class="section-header">
                <h2>💰 Financial Profile</h2>
                <p>Income and wealth distribution insights for pricing strategy and product positioning</p>
            </div>
            <div class="section-content">
                <div class="chart-container">
                    <img src="financial_analysis.png" alt="Financial Analysis">
                </div>
                <!-- <div class="insights-grid">
                    <div class="insight-card">
                        <h4>💵 Income Segments</h4>
                        <ul>'''
    
    if avg_income > 0:
        html_content += f'''
                            <li>Average income: ${avg_income:,.0f}</li>'''
    
    html_content += f'''
                            <li>Wide income distribution creates multiple segments</li>
                            <li>Homeowners show higher average income</li>
                            <li>Premium offerings viable for high-income users</li>
                        </ul>
                    </div>
                    <div class="insight-card">
                        <h4>🛍️ Pricing Strategy</h4>
                        <ul>
                            <li>Tiered pricing to match income levels</li>
                            <li>Premium features for high-income segments</li>
                            <li>Value offerings for price-sensitive users</li>
                            <li>Financial products and services opportunities</li>
                        </ul>
                    </div>
                </div> -->
            </div>
        </section>

        <section id="interests" class="section">
            <div class="section-header">
                <h2>🎯 Interest Intelligence</h2>
                <p>Behavioral insights based on purchase patterns, subscriptions, and engagement intensity (1-9 scale)</p>
            </div>
            <div class="section-content">
                <div class="chart-container">
                    <img src="interests_analysis.png" alt="Interest Analysis">
                </div>
                
                <!-- <div class="methodology">
                    <h4>📋 Interest Scoring Methodology</h4>
                    <p><strong>Data Sources:</strong> Purchases, memberships, magazine subscriptions, survey responses</p>
                    <p><strong>Scoring Scale:</strong> 1 (low interest) to 9 (high interest)</p>
                    <p><strong>Factors:</strong> Recency, frequency, monetary value, number of sources</p>
                    <p><strong>High Engagement:</strong> Scores 7-9 indicate strong purchasing patterns and multiple engagement sources</p>
                </div> -->

                <!-- <div class="business-insights">
                    <h3>🎯 Key Interest Insights</h3>
                    <div class="insights-columns">
                        <div class="insight-column">
                            <h4>🔥 High-Value Segments</h4>'''
    
    for interest, data in high_value_interests:
        engagement_rate = (data['high_interest'] / data['users']) * 100
        html_content += f'''
                            <div class="insight-item">
                                <strong>{interest}:</strong> {data['high_interest']:,} highly engaged users ({engagement_rate:.1f}%)<br>
                                Average score: {data['avg_score']:.1f}/9 | Total: {data['users']:,} users
                            </div>'''
    
    html_content += f'''
                        </div>
                        <div class="insight-column">
                            <h4>🚀 Emerging Opportunities</h4>'''
    
    for interest, data in emerging_opportunities:
        html_content += f'''
                            <div class="insight-item">
                                <strong>{interest}:</strong> {data['avg_score']:.1f}/9 average score<br>
                                {data['users']:,} users with high engagement potential
                            </div>'''
    
    html_content += f'''
                        </div>
                        <div class="insight-column">
                            <h4>👥 Mass Market Appeal</h4>'''
    
    for interest, data in mass_market_interests:
        html_content += f'''
                            <div class="insight-item">
                                <strong>{interest}:</strong> {data['users']:,} total users<br>
                                Average engagement: {data['avg_score']:.1f}/9
                            </div>'''
    
    html_content += f'''
                        </div>
                    </div>
                </div> -->

                <!-- <div class="insights-grid">
                    <div class="insight-card">
                        <h4>🎯 Targeting Recommendations</h4>
                        <ul>'''
    
    if high_value_interests:
        top_interest = high_value_interests[0][0]
        html_content += f'''
                            <li>Focus on {top_interest} interest users for core products</li>'''
    
    html_content += f'''
                            <li>Develop premium offerings for high-intensity users (scores 7-9)</li>'''
    
    if emerging_opportunities:
        seasonal_interest = emerging_opportunities[0][0]
        html_content += f'''
                            <li>Create targeted campaigns around {seasonal_interest} interest</li>'''
    
    html_content += f'''
                            <li>Build loyalty programs for top engagement categories</li>
                        </ul>
                    </div>
                    <div class="insight-card">
                        <h4>📊 Segmentation Strategy</h4>
                        <ul>
                            <li>High-intensity users (7-9): Premium products, exclusive access</li>
                            <li>Moderate interest (4-6): Standard offerings, nurture campaigns</li>
                            <li>Low interest (1-3): Awareness campaigns, introductory offers</li>
                            <li>Cross-sell between related interests for increased revenue</li>
                        </ul>
                    </div>
                </div> -->
            </div>
        </section>

        <!-- <section id="business-insights" class="section">
            <div class="section-header">
                <h2>💡 Strategic Business Recommendations</h2>
                <p>Actionable insights for marketing, product development, and business strategy</p>
            </div>
            <div class="section-content">
                <div class="insights-grid">
                    <div class="insight-card">
                        <h4>📈 Marketing Strategy</h4>
                        <ul>
                            <li>Implement intensity-based email segmentation</li>
                            <li>Create interest-specific landing pages</li>
                            <li>Develop lookalike audiences based on high-intensity users</li>'''
    
    if top_states:
        html_content += f'''
                            <li>Launch geo-targeted campaigns in {top_states[0][0]} and other top states</li>'''
    
    if emerging_opportunities:
        html_content += f'''
                            <li>Build seasonal campaigns around {emerging_opportunities[0][0]} and other high-intensity interests</li>'''
    
    html_content += f'''
                        </ul>
                    </div>
                    <div class="insight-card">
                        <h4>🛍️ Product Development</h4>
                        <ul>'''
    
    if high_value_interests:
        html_content += f'''
                            <li>Expand {high_value_interests[0][0].lower()}-oriented product lines</li>'''
        if len(high_value_interests) > 1:
            html_content += f'''
                            <li>Develop premium {high_value_interests[1][0].lower()} collections</li>'''
    
    html_content += f'''
                            <li>Create products targeting emerging high-intensity segments</li>
                            <li>Bundle related interest categories for increased value</li>
                            <li>Design offerings with broad demographic appeal</li>
                        </ul>
                    </div>
                    <div class="insight-card">
                        <h4>💰 Revenue Optimization</h4>
                        <ul>'''
    
    if avg_income > 0:
        html_content += f'''
                            <li>Implement tiered pricing (avg income: ${avg_income:,.0f})</li>'''
    
    html_content += f'''
                            <li>Offer premium subscriptions for high-intensity users</li>
                            <li>Create loyalty programs for top {len(high_value_interests)} interest categories</li>
                            <li>Develop cross-selling strategies between related interests</li>'''
    
    if top_states:
        html_content += f'''
                            <li>Focus ad spend on {len(top_states)} high-engagement geographic regions</li>'''
    
    html_content += f'''
                        </ul>
                    </div>
                    <div class="insight-card">
                        <h4>🎯 Customer Experience</h4>
                        <ul>
                            <li>Personalize content based on interest intensity scores</li>
                            <li>Create user journey maps for different engagement levels</li>
                            <li>Implement recommendation engines using interest data</li>
                            <li>Develop interest-based customer service specializations</li>
                            <li>Build community features around top interests</li>
                        </ul>
                    </div>
                </div>
            </div>
        </section> -->

        <!-- <footer>
            <p><strong>User Demographics & Behavior Analysis Dashboard</strong></p>
            <p>Generated using Data Axle enriched customer data | Analysis covers {total_users:,} user records</p>
            <p style="margin-top: 10px; opacity: 0.8;">Interest scores based on purchases, memberships, subscriptions, and survey responses</p>
            <p style="margin-top: 5px; opacity: 0.8;">Dashboard generated on {current_time}</p>
        </footer> -->
    </div>
</body>
</html>'''
    
    # Write the HTML file
    with open('user_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print("✅ Dynamic HTML dashboard generated: user_dashboard.html")
    print("📱 Open this file in any web browser to view the complete dashboard")
    print("🔄 Dashboard content reflects current data and can be regenerated anytime")

def main():
    """Main function to run all analyses. Data is loaded only from PostgreSQL matched_emails (no CSV)."""
    parser = argparse.ArgumentParser(
        description="User Demographics & Behavior Analysis (Data Axle). Loads only from PostgreSQL matched_emails."
    )
    parser.add_argument(
        "--postgres",
        metavar="URL",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL connection URL (or set DATABASE_URL)",
    )
    parser.add_argument(
        "--table",
        default="matched_emails",
        help="PostgreSQL table name (default: matched_emails)",
    )
    parser.add_argument(
        "--email-col",
        default="email",
        dest="email_col",
        help="Email column name (default: email)",
    )
    parser.add_argument(
        "--data-col",
        default="response_json",
        dest="data_col",
        help="JSON/JSONB column name (default: response_json)",
    )
    args = parser.parse_args()

    if not args.postgres:
        raise SystemExit(
            "PostgreSQL required. Set DATABASE_URL or pass --postgres with your connection URL. CSV is not used."
        )
    if load_from_postgres is None:
        raise SystemExit("PostgreSQL support requires psycopg2. Install with: pip install psycopg2-binary")

    print("=== USER DEMOGRAPHICS AND BEHAVIOR ANALYSIS ===")
    print("Loading Data Axle data from PostgreSQL (matched_emails)...\n")

    df = load_from_postgres(
        connection_string=args.postgres,
        table=args.table,
        email_column=args.email_col,
        data_column=args.data_col,
    )
    df = normalize_numeric_columns(df)

    print(f"\nDataset Overview:")
    print(f"- Total records: {len(df):,}")
    print(f"- Total columns: {len(df.columns)}")
    print(f"- Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    # Create visualizations
    try:
        create_summary_dashboard(df)
        create_geographic_analysis(df)
        create_demographic_analysis(df)
        create_financial_analysis(df)
        create_interests_analysis(df)
        create_business_insights_report(df)
        generate_html_dashboard(df)
        
        print("\n✅ Analysis complete! Generated files:")
        print("📊 Individual Visualizations:")
        print("  - summary_dashboard.png: High-level business overview")
        print("  - geographic_analysis.png: User geographic distribution")  
        print("  - demographic_analysis.png: Age, gender, education demographics")
        print("  - financial_analysis.png: Income and wealth analysis")
        print("  - interests_analysis.png: User interests with intensity scores (1-9)")
        print("\n🌐 Comprehensive Dashboard:")
        print("  - user_dashboard.html: Single-page dashboard with all insights")
        
        print("\n🎯 Business insights show high-value segments and opportunities!")
        print("📱 Share 'user_dashboard.html' with your business team for easy viewing!")
        print("\nThese insights will help your business team understand user engagement intensity,")
        print("not just participation, for more targeted marketing and product development!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        print("Please check your data file and column names.")

if __name__ == "__main__":
    main() 