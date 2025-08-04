#!/usr/bin/env python3
"""
Show the exact flow of when the background task starts
"""

def show_flow():
    """Show the processing flow"""
    
    print("🔄 Campaign Reporting Flow")
    print("=" * 50)
    print()
    
    print("1. 📊 User clicks 'Generate Report'")
    print("2. 🔄 Fetch meta insights and Northbeam data")
    print("3. 📦 Merge data into comprehensive objects")
    print("4. 🔗 Merge ads with same name (if enabled)")
    print("5. 🧹 Clean NaN values")
    print("6. 💾 Save comprehensive_ads file to reports/")
    print("7. 🎬 START BACKGROUND TASK HERE ←")
    print("   - Extract ad IDs from meta insights")
    print("   - Check environment variables")
    print("   - Start background thread")
    print("   - Show status in Streamlit")
    print("8. 📊 Display report data in web app")
    print("9. 🔄 Background task continues running...")
    print()
    
    print("📁 Files Created:")
    print("   - comprehensive_ads_YYYYMMDD-YYYYMMDD.json (Step 6)")
    print("   - meta_adcreatives_raw_YYYYMMDD-YYYYMMDD.json (Background)")
    print("   - meta_adcreatives_processed_YYYYMMDD-YYYYMMDD.json (Background)")
    print()
    
    print("🎯 Key Points:")
    print("   ✅ Background task starts AFTER comprehensive_ads file is created")
    print("   ✅ User sees report data immediately")
    print("   ✅ Background task runs independently")
    print("   ✅ Files are created incrementally")
    print()
    
    print("🔍 How to Monitor:")
    print("   - Watch terminal for progress messages")
    print("   - Run: python3 monitor_background_task.py")
    print("   - Check: ls -la reports/meta_adcreatives_*")
    print("   - Look for Streamlit status expander")

if __name__ == "__main__":
    show_flow() 