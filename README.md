# User Demographics and Behavior Analysis Dashboard

This dashboard analyzes your enriched user data from Data Axle to provide insights into your customer base. The analysis includes **3,359 user records** with comprehensive demographic, geographic, financial, and behavioral data.

## Generated Outputs

### 🌐 `user_dashboard.html` - Dynamic Interactive Dashboard
**Single-page HTML dashboard that updates with your current data**
- **Dynamically generated** from your latest analysis results
- All visualizations in one professional layout
- **Live metrics** - user counts, geographic reach, interest insights
- **Current business recommendations** based on actual data patterns
- **Timestamp** showing when the dashboard was generated
- Mobile-responsive design and ready to share via email
- **Always up-to-date** - regenerates with fresh insights every time

### 📊 Individual Visualization Files

### 1. `summary_dashboard.png` - Executive Overview
**Key metrics for leadership and strategy teams**
- Total user count and geographic reach
- High-level demographic breakdown
- Income distribution summary
- Top performing states and cities
- Key business insights and recommendations

### 2. `geographic_analysis.png` - Location Intelligence
**Where your users are located**
- Top 15 states by user concentration
- Top 15 cities with highest user counts
- Geographic coverage metrics
- Regional distribution patterns

### 3. `demographic_analysis.png` - User Profiles
**Who your users are**
- Gender distribution
- Marital status breakdown
- Home ownership rates
- Education level distribution
- Family size patterns
- Political affiliation (where available)

### 4. `financial_analysis.png` - Economic Insights
**User financial profiles**
- Income distribution with statistics
- Wealth level analysis
- Income vs. home ownership correlation
- Income bracket segmentation
- Financial targeting opportunities

### 5. `interests_analysis.png` - Behavioral Intelligence
**What your users care about (with intensity scoring)**
- Top interests by user volume (how many users)
- Top interests by average score (1-9 intensity scale)
- High engagement users (scores 7-9) by interest
- Overall interest score distribution
- Lifestyle segment analysis
- Key insights: most engaging vs. most popular interests

## Key Findings Summary

Based on the analysis of your user base:

- **Geographic Concentration**: Users are distributed across multiple states and cities
- **Demographics**: Mix of genders with various education levels and family structures
- **Financial Profile**: Range of income levels providing segmentation opportunities
- **Behavioral Data**: Rich interest and lifestyle data for targeted marketing

## How to Use the Dashboard

### 🌐 HTML Dashboard (`user_dashboard.html`)
**Perfect for business presentations and team sharing**
- **Open in any web browser** (Chrome, Safari, Firefox, Edge)
- **Share via email** - single file contains everything
- **Present in meetings** - professional layout with navigation
- **Mobile-friendly** - view on tablets and phones
- **Print-ready** - browser print function creates PDF reports

### 📊 Individual PNG Files
**For detailed analysis and custom presentations**
- High-resolution charts for reports and presentations
- Can be embedded in PowerPoint, Google Slides, or documents
- Perfect for focused analysis on specific areas

## How to Use These Insights

### For Marketing Teams:
- Use geographic data for regional campaign targeting
- Leverage interest data for personalized content
- Apply income segments for product positioning

### For Product Teams:
- Consider demographic preferences in feature development
- Use behavioral insights for user experience optimization
- Apply financial data for pricing strategies

### For Business Strategy:
- Identify expansion opportunities in underrepresented regions
- Develop targeted offerings for different income segments
- Create interest-based product categories

## Interest Scoring System

Data Axle's interest scores are based on:
- **Purchases**: What users actually buy
- **Memberships**: Organizations and clubs they join
- **Magazine Subscriptions**: Publications they read
- **Survey Responses**: Direct interest declarations

**Scoring Scale**: 1 (low interest) to 9 (high interest)
**Factors Considered**: Recency, frequency, monetary value, and number of sources

**Example Interpretation**:
- Score 7-9: High engagement (strong purchasing patterns, multiple sources)
- Score 4-6: Moderate interest (occasional engagement)
- Score 1-3: Low interest (minimal or old engagement)

## Technical Details

- **Data Source**: Data Axle enriched customer database
- **Analysis Tool**: Python with pandas, matplotlib, and seaborn
- **Record Count**: 3,359 users analyzed
- **Interest Analysis**: Intensity-based scoring (not just participation)
- **Visualizations**: 5 comprehensive charts saved as high-resolution PNG files

## Running the Analysis

Data is loaded **only from PostgreSQL** (table `matched_emails`). CSV files are not used.

1. **Table**: `matched_emails` (or pass `--table`)
2. **Columns**: `email`, `response_json` (JSON/JSONB with the Data Axle match result, e.g. `{"count": 1, "document": {"attributes": {...}}, "match_record_count": 1}`), and optionally `created_at`. Use `--data-col` if your JSON column has a different name.

```bash
# Install dependencies (includes psycopg2-binary for PostgreSQL)
pip install -r requirements.txt

# Set your PostgreSQL connection URL, then run
export DATABASE_URL="postgresql://user:password@host:5432/dbname"
python3 user_analysis_dashboard.py

# Or pass the URL explicitly
python3 user_analysis_dashboard.py --postgres "postgresql://user:password@host:5432/dbname" \
  --table matched_emails --email-col email --data-col response_json
```

The script reads only from PostgreSQL `matched_emails`, flattens each row’s `response_json` into the dashboard column structure, then generates the PNGs and `user_dashboard.html`.

**What happens each time you run it:**
- Loads data only from PostgreSQL `matched_emails` (no CSV)
- Generates fresh PNG visualizations
- **Creates a new `user_dashboard.html` with current insights**
- Updates all metrics, recommendations, and business insights
- Adds timestamp showing when the analysis was performed

**Perfect for:**
- Weekly/monthly business reviews with updated data
- Presenting current user trends to stakeholders
- Tracking changes in user behavior over time

---

*Generated by automated user analysis dashboard - helping business teams understand their customer base through data-driven insights.* 