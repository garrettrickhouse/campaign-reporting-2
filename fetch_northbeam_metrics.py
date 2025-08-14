#!/usr/bin/env python3
"""
Fetch Northbeam Available Metrics
This script fetches all available metrics from Northbeam's Data Export API.
Useful for discovering what metrics are available for your account.
"""

import os
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
NORTHBEAM_API_KEY = os.getenv('NORTHBEAM_API_KEY')
NORTHBEAM_DATA_CLIENT_ID = os.getenv('NORTHBEAM_DATA_CLIENT_ID')
NORTHBEAM_BASE_URL = "https://api.northbeam.io/v1"

def get_northbeam_headers():
    """Get headers for Northbeam API requests"""
    return {
        "accept": "application/json",
        "Authorization": NORTHBEAM_API_KEY,
        "Data-Client-ID": NORTHBEAM_DATA_CLIENT_ID
    }

def fetch_available_metrics():
    """
    Fetch all available metrics from Northbeam's Data Export API
    
    Returns:
        dict: JSON response containing available metrics or None if error
    """
    url = f"{NORTHBEAM_BASE_URL}/exports/metrics"
    headers = get_northbeam_headers()
    
    try:
        print("📊 Fetching available metrics from Northbeam...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        print(f"✅ Successfully fetched metrics")
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching metrics: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text}")
        return None

def display_metrics_summary(metrics_data):
    """
    Display a summary of available metrics in a readable format
    
    Args:
        metrics_data (dict): The metrics data from the API
    """
    if not metrics_data:
        print("❌ No metrics data to display")
        return
    
    print("\n" + "="*60)
    print("📊 NORTHBEAM AVAILABLE METRICS SUMMARY")
    print("="*60)
    
    # Check if it's a list or dict with different possible keys
    if isinstance(metrics_data, list):
        metrics_list = metrics_data
    elif isinstance(metrics_data, dict):
        # Try different possible keys where metrics might be stored
        if 'data' in metrics_data:
            metrics_list = metrics_data['data']
        elif 'metrics' in metrics_data:
            metrics_list = metrics_data['metrics']
        else:
            print("❌ Unexpected metrics data format")
            print(f"Data type: {type(metrics_data)}")
            print(f"Data keys: {list(metrics_data.keys())}")
            return
    else:
        print("❌ Unexpected metrics data format")
        print(f"Data type: {type(metrics_data)}")
        return
    
    print(f"📋 Total metrics available: {len(metrics_list)}")
    print("\n🔍 ALL AVAILABLE METRICS:")
    
    # Display all metrics in a simple list
    for i, metric in enumerate(metrics_list):
        metric_id = metric.get('id', 'Unknown')
        metric_label = metric.get('label', 'Unknown')
        print(f"   {i+1:3d}. {metric_id}: {metric_label}")
        
        # Show first 50 metrics, then indicate how many more
        if i >= 9:
            remaining = len(metrics_list) - 10
            print(f"   ... and {remaining} more metrics")
            break

def save_metrics_to_file(metrics_data, filename="northbeam_metrics.json"):
    """
    Save the metrics data to a JSON file for reference
    
    Args:
        metrics_data (dict): The metrics data from the API
        filename (str): Name of the file to save to
    """
    try:
        with open(filename, 'w') as f:
            json.dump(metrics_data, f, indent=2)
        print(f"\n💾 Metrics saved to: {filename}")
    except Exception as e:
        print(f"❌ Error saving metrics to file: {e}")

def test_metrics_fetch():
    """Test the complete metrics fetching workflow"""
    print("🚀 Testing Northbeam Metrics Fetch")
    print("=" * 50)
    
    # Check environment variables
    if not NORTHBEAM_API_KEY:
        print("❌ NORTHBEAM_API_KEY not found in environment variables")
        return
    
    if not NORTHBEAM_DATA_CLIENT_ID:
        print("❌ NORTHBEAM_DATA_CLIENT_ID not found in environment variables")
        return
    
    print("✅ Environment variables loaded successfully")
    
    # Fetch metrics
    print("\n📤 Fetching available metrics...")
    metrics_data = fetch_available_metrics()
    
    if metrics_data:
        # Display summary
        display_metrics_summary(metrics_data)
        
        # Save to file for reference
        save_metrics_to_file(metrics_data)
        
        print("\n✅ Metrics fetch test completed successfully!")
    else:
        print("❌ Failed to fetch metrics")

def main():
    """Main function"""
    test_metrics_fetch()

if __name__ == "__main__":
    main() 