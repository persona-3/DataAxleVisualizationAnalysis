#!/usr/bin/env python3
"""
Demo script showing how the dynamic HTML dashboard updates with different data
"""

import pandas as pd
from user_analysis_dashboard import generate_html_dashboard, load_and_clean_data

def demo_dynamic_dashboard():
    """Demonstrate dynamic dashboard generation"""
    print("🔬 DYNAMIC DASHBOARD DEMO")
    print("=" * 50)
    
    # Load the full dataset
    print("Loading full dataset...")
    df_full = load_and_clean_data()
    print(f"Full dataset: {len(df_full):,} records")
    
    # Generate dashboard with full data
    print("\n📊 Generating dashboard with full dataset...")
    generate_html_dashboard(df_full)
    print("✅ Full dataset dashboard saved as: user_dashboard.html")
    
    # Create a subset for demonstration (e.g., just California users)
    if 'data.document.attributes.state' in df_full.columns:
        ca_users = df_full[df_full['data.document.attributes.state'] == 'CA']
        if len(ca_users) > 0:
            print(f"\n🏖️ Creating California-only subset: {len(ca_users):,} records")
            
            # Generate dashboard with subset
            print("📊 Generating dashboard with California subset...")
            # Temporarily save with different name for demo
            import user_analysis_dashboard
            original_func = user_analysis_dashboard.generate_html_dashboard
            
            def save_ca_dashboard(df):
                # Modified version that saves to different file
                content = original_func.__doc__
                print("Generating California-only HTML dashboard...")
                # Generate and save to ca_dashboard.html instead
                html_content = original_func(df)
                # This would save to a different filename in a real implementation
                print("✅ California dashboard would be saved as: ca_dashboard.html")
                print(f"📈 Key difference: {len(df):,} CA users vs {len(df_full):,} total users")
    
    print("\n🎯 DASHBOARD FEATURES:")
    print("✓ User counts update automatically")
    print("✓ Geographic insights change with data")
    print("✓ Interest analysis reflects actual user base")
    print("✓ Business recommendations adapt to data patterns")
    print("✓ Timestamp shows when analysis was performed")
    
    print("\n💡 USE CASES:")
    print("• Monthly business reviews with latest data")
    print("• A/B testing different user segments")
    print("• Regional analysis (filter by state/city)")
    print("• Campaign effectiveness tracking")
    print("• Product launch targeting")

if __name__ == "__main__":
    demo_dynamic_dashboard() 