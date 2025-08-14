#!/usr/bin/env python3
"""
Test script for Northbeam export functionality
Run this to test Northbeam exports independently of the main app
"""

import os
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
NORTHBEAM_DATA_CLIENT_ID = os.getenv('NORTHBEAM_DATA_CLIENT_ID')
NORTHBEAM_API_KEY = os.getenv('NORTHBEAM_API_KEY')
NORTHBEAM_BASE_URL = "https://api.northbeam.io/v1"

def get_northbeam_headers():
    """Get headers for Northbeam API requests"""
    return {
        "accept": "application/json",
        "Authorization": f"Basic {NORTHBEAM_API_KEY}",
        "Data-Client-ID": NORTHBEAM_DATA_CLIENT_ID
    }

def test_northbeam_connection():
    """Test basic connection to Northbeam API"""
    print("🔍 Testing Northbeam API connection...")
    
    # Test attribution models endpoint
    url = f"{NORTHBEAM_BASE_URL}/exports/attribution-models"
    try:
        response = requests.get(url, headers=get_northbeam_headers(), timeout=30)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Connection successful! Found {len(data.get('attribution_models', []))} attribution models")
            return True
        else:
            print(f"❌ Connection failed: HTTP {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False

def test_export_creation():
    """Test creating a small export for yesterday"""
    print("\n🚀 Testing export creation...")
    
    # Use yesterday's date for testing
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    url = f"{NORTHBEAM_BASE_URL}/exports/data-export"
    
    payload = {
        "period_type": "FIXED",
        "period_options": {
            "period_starting_at": f"{yesterday}T00:00:00Z",
            "period_ending_at": f"{yesterday}T23:59:59Z",
        },
        "attribution_options": {
            "attribution_models": ["last_touch_non_direct"],
            "attribution_windows": ["1"],
            "accounting_modes": ["accrual"]
        },
        "options": {
            "remove_zero_spend": False,
            "include_ids": True,
            "include_kind_and_platform": True
        },
        "time_granularity": "DAILY",
        "level": "ad",
        "metrics": [
            {"id": "spend", "label": "Spend"},
            {"id": "impressions", "label": "Impressions"}
        ]
    }
    
    try:
        response = requests.post(url, headers=get_northbeam_headers(), json=payload, timeout=60)
        
        if response.status_code == 201:
            export_id = response.json().get('id')
            print(f"✅ Export created successfully! ID: {export_id}")
            return export_id
        else:
            print(f"❌ Export creation failed: HTTP {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Export creation error: {e}")
        return None

def test_export_polling(export_id, timeout_seconds=120):
    """Test polling export status"""
    print(f"\n📊 Testing export status polling for ID: {export_id}")
    print(f"⏱️  Timeout: {timeout_seconds} seconds")
    
    url = f"{NORTHBEAM_BASE_URL}/exports/data-export/result/{export_id}"
    
    start_time = time.time()
    poll_count = 0
    
    while time.time() - start_time < timeout_seconds:
        poll_count += 1
        print(f"  🔄 Poll attempt {poll_count}...")
        
        try:
            response = requests.get(url, headers=get_northbeam_headers(), timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                status = data.get("status")
                
                print(f"  ↪ Status: {status}")
                
                if status in ["ready", "SUCCESS", "success", "COMPLETED"]:
                    result_links = data.get("result", [])
                    if result_links and len(result_links) > 0:
                        print(f"✅ Export ready! File URL: {result_links[0]}")
                        return result_links[0]
                    else:
                        print(f"✅ Export completed but no file URL found")
                        return None
                elif status in ["PENDING", "PROCESSING", "IN_PROGRESS"]:
                    print(f"  ⏳ Export still processing...")
                elif status in ["FAILED", "ERROR", "CANCELLED"]:
                    print(f"❌ Export failed with status: {status}")
                    if "error" in data:
                        print(f"  Error details: {data['error']}")
                    return None
                else:
                    print(f"  ⚠️ Unknown status: {status}")
                    
            elif response.status_code == 429:
                print(f"  ⚠️ Rate limit hit, waiting 30 seconds...")
                time.sleep(30)
                continue
            elif response.status_code == 404:
                print(f"❌ Export not found (404)")
                return None
            else:
                print(f"  ❌ HTTP {response.status_code}: {response.text}")
                
        except requests.exceptions.Timeout:
            print(f"  ⏰ Request timeout")
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request error: {e}")
        
        time.sleep(10)  # Wait 10 seconds between polls
    
    print(f"❌ Export polling timed out after {timeout_seconds} seconds")
    print(f"   - Total poll attempts: {poll_count}")
    return None

def main():
    """Main test function"""
    print("🧪 Northbeam Export Test Script")
    print("=" * 50)
    
    # Check environment variables
    if not NORTHBEAM_DATA_CLIENT_ID or not NORTHBEAM_API_KEY:
        print("❌ Missing required environment variables:")
        print(f"   - NORTHBEAM_DATA_CLIENT_ID: {'✅' if NORTHBEAM_DATA_CLIENT_ID else '❌'}")
        print(f"   - NORTHBEAM_API_KEY: {'✅' if NORTHBEAM_API_KEY else '❌'}")
        return
    
    print(f"✅ Environment variables loaded")
    print(f"   - Client ID: {NORTHBEAM_DATA_CLIENT_ID[:8]}...")
    print(f"   - API Key: {NORTHBEAM_API_KEY[:8]}...")
    
    # Test 1: Connection
    if not test_northbeam_connection():
        print("\n❌ Cannot proceed without API connection")
        return
    
    # Test 2: Export creation
    export_id = test_export_creation()
    if not export_id:
        print("\n❌ Cannot proceed without export ID")
        return
    
    # Test 3: Export polling
    file_url = test_export_polling(export_id)
    
    if file_url:
        print(f"\n🎉 SUCCESS: Export completed and ready for download!")
        print(f"   File URL: {file_url}")
    else:
        print(f"\n⚠️ Export may still be processing or failed")
        print(f"   Export ID: {export_id}")
        print(f"   Check status manually or increase timeout")

if __name__ == "__main__":
    main()
