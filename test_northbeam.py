#!/usr/bin/env python3
"""
Northbeam Report Testing Script

This script allows you to test the Northbeam report generation functionality:
1. Input start and end dates
2. Create Northbeam export
3. Save CSV to S3
4. Download CSV locally for review

Usage:
    python test_northbeam.py
"""

import os
import sys
import json
import time
import requests
import pandas as pd
import boto3
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ===== NORTHBEAM CONFIGURATION =====
NORTHBEAM_DATA_CLIENT_ID = os.getenv('NORTHBEAM_DATA_CLIENT_ID')
NORTHBEAM_API_KEY = os.getenv('NORTHBEAM_API_KEY')
NORTHBEAM_PLATFORM_ACCOUNT_ID = os.getenv('NORTHBEAM_PLATFORM_ACCOUNT_ID')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = 'us-east-1'
S3_BUCKET = os.getenv('S3_BUCKET')
NORTHBEAM_BASE_URL = "https://api.northbeam.io/v1"

# Attribution configuration
ATTRIBUTION_MODEL = "last_touch_non_direct"
ATTRIBUTION_WINDOW = "1"
ACCOUNTING_MODE_API = "accrual"  # For API payload
ACCOUNTING_MODE_FILTER = "Accrual performance"
NORTHBEAM_PLATFORM = "fb"

def get_northbeam_headers():
    """Headers for Northbeam API"""
    return {
        'accept': 'application/json',
        'Data-Client-ID': NORTHBEAM_DATA_CLIENT_ID,
        'Authorization': f'Bearer {NORTHBEAM_API_KEY}'
    }

def get_s3_client():
    """Get S3 client with credentials"""
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

def create_northbeam_export(start_date, end_date):
    """Create a Northbeam export"""
    
    time.sleep(2)
    
    url = f"{NORTHBEAM_BASE_URL}/exports/data-export"
    
    start_datetime = f"{start_date}T00:00:00Z"
    exclusive_end_date = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
    end_datetime = exclusive_end_date.strftime('%Y-%m-%dT00:00:00Z')

    # end_of_day = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
    # end_datetime = end_of_day.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # end_datetime = "2025-06-01T23:59:59Z"
    # end_datetime = "2025-06-02T00:00:00Z"
    
    
    print("Start: ", start_datetime)
    print("End: ", end_datetime)

    payload = {
        "period_type": "FIXED",
        "period_options": {
            "period_starting_at": f"{start_datetime}",
            "period_ending_at": f"{end_datetime}"
        },
        "attribution_options": {
            "attribution_models": [ATTRIBUTION_MODEL],
            "attribution_windows": [ATTRIBUTION_WINDOW],
            "accounting_modes": [ACCOUNTING_MODE_API]
        },
        "options": {
            "remove_zero_spend": False,
            "include_ids": True,
            "include_kind_and_platform": True
        },
        "time_granularity": "DAILY",
        "export_file_name": f"NB_{start_date.replace('-', '')}-{end_date.replace('-', '')}",
        "bucket_name": S3_BUCKET,
        "aws_role": "arn:aws:iam::881825931691:role/NorthbeamS3ExportRole",
        "level": "ad",
        "metrics": [
            { "id": "spend", "label": "Spend" },
            { "id": "impressions", "label": "Impressions" },
            { "id": "metaLinkClicks", "label": "meta_link_clicks" },
            { "id": "revAttributed", "label": "Attributed_Rev" },
            { "id": "txns", "label": "Transactions" },
            { "id": "roas", "label": "ROAS" },
            { "id": "meta3SVideoViewsDefault", "label": "Meta_3S_Video_Views" }
        ]
    }
    
    response = requests.post(url, headers=get_northbeam_headers(), json=payload)
    
    if response.status_code == 201:
        export_id = response.json().get('id')
        print(f"✅ Export created successfully! ID: {export_id}")
        return export_id
    elif response.status_code == 429:
        print(f"❌ Rate limit exceeded (429): {response.text}")
        print("⏱️ Waiting 60 seconds before retrying...")
        time.sleep(60)
        print("🔄 Retrying export creation...")
        response = requests.post(url, headers=get_northbeam_headers(), json=payload)
        if response.status_code == 201:
            export_id = response.json().get('id')
            print(f"✅ Export created successfully on retry! ID: {export_id}")
            return export_id
        else:
            print(f"❌ Export creation failed on retry: {response.status_code}")
            print(f"Response: {response.text}")
            return None
    else:
        print(f"❌ Export creation failed: {response.status_code}")
        print(f"Response: {response.text}")
        return None

def poll_northbeam_export_status(export_id, timeout_seconds=30, poll_interval=5):
    """Poll Northbeam for export status until ready"""
    
    url = f"{NORTHBEAM_BASE_URL}/exports/data-export/result/{export_id}"
    
    start_time = time.time()
    poll_count = 0
    while time.time() - start_time < timeout_seconds:
        poll_count += 1
        print(f"  🔄 Poll attempt {poll_count}...")
        
        response = requests.get(url, headers=get_northbeam_headers())
        
        if response.status_code == 200:
            data = response.json()
            status = data.get("status")
            
            print(f"  ↪ Status: {status}")
            
            if status in ["ready", "SUCCESS", "success"]:
                result_links = data.get("result", [])
                if result_links and len(result_links) > 0:
                    print(f"✅ Export ready.")
                    return result_links[0]
                else:
                    print(f"✅ Export completed, falling back to S3...")
                    return None
        elif response.status_code == 429:
            print(f"  ⚠️ Rate limit hit during polling, waiting 30 seconds...")
            time.sleep(30)
            continue
        
        time.sleep(poll_interval)
    
    print(f"❌ Export polling timed out")
    return None

def download_export_data(export_id, start_date, end_date):
    """Download the export data"""
    
    # Try direct download first
    direct_url = poll_northbeam_export_status(export_id)
    if direct_url:
        try:
            response = requests.get(direct_url)
            if response.status_code == 200:
                # Read CSV with specific dtype to ensure ID columns are treated as strings
                df = pd.read_csv(io.BytesIO(response.content), dtype={
                    'ad_id': str,
                    'campaign_id': str,
                    'adset_id': str
                })
                
                print(f"✅ Downloaded {len(df)} rows directly from Northbeam")
                return df
        except Exception as e:
            print(f"❌ Direct download failed: {e}")
    
    # Fallback to S3
    print(f"⚠️ Falling back to S3...")
    s3_client = get_s3_client()
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=100)
        if 'Contents' in response:
            matching_files = []
            for obj in response['Contents']:
                key = obj['Key']
                if key.startswith(f"NB_{start_date.replace('-', '')}-{end_date.replace('-', '')}") and key.endswith('.csv'):
                    matching_files.append({
                        'key': key,
                        'last_modified': obj['LastModified']
                    })
            
            if matching_files:
                matching_files.sort(key=lambda x: x['last_modified'], reverse=True)
                actual_file_key = matching_files[0]['key']
                print(f"📁 Found S3 file: {actual_file_key}")
                
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=actual_file_key)
                # Read CSV with specific dtype to ensure ID columns are treated as strings
                df = pd.read_csv(io.BytesIO(response['Body'].read()), dtype={
                    'ad_id': str,
                    'campaign_id': str,
                    'adset_id': str
                })
                print(f"✅ Downloaded {len(df)} rows from S3")
                
                return df
            else:
                print("❌ No matching files found in S3")
                return None
        else:
            print("❌ No files found in S3 bucket")
            return None
    except Exception as e:
        print(f"❌ S3 download failed: {e}")
        return None

def save_csv_to_s3(df, start_date, end_date):
    """Save CSV data to S3"""
    try:
        s3_client = get_s3_client()
        
        # Create CSV buffer
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Generate S3 key
        s3_key = f"campaign-reporting/raw/northbeam/northbeam_{start_date.replace('-', '')}-{end_date.replace('-', '')}.csv"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        print(f"✅ Saved CSV to S3: s3://{S3_BUCKET}/{s3_key}")
        return s3_key
    except Exception as e:
        print(f"❌ Failed to save CSV to S3: {e}")
        return None

def save_csv_locally(df, start_date, end_date):
    """Save CSV data locally"""
    try:
        # Create directory if it doesn't exist
        os.makedirs("campaign-reporting/raw/northbeam", exist_ok=True)
        
        # Generate filename
        filename = f"campaign-reporting/raw/northbeam/northbeam_{start_date.replace('-', '')}-{end_date.replace('-', '')}.csv"
        
        # Save CSV
        df.to_csv(filename, index=False)
        
        print(f"✅ Saved CSV locally: {filename}")
        return filename
    except Exception as e:
        print(f"❌ Failed to save CSV locally: {e}")
        return None

def filter_attribution_data(df, target_accounting_mode, target_platform):
    """Filter dataframe to specific attribution configuration and platform"""
    
    # Filter the data
    original_count = len(df)
    filtered_df = df
    
    # Filter by accounting mode
    if 'accounting_mode' in df.columns:
        filtered_df = filtered_df[filtered_df['accounting_mode'] == target_accounting_mode]
    else:
        print("   ⚠️ No accounting_mode column found")
    
    # Filter by platform
    if 'platform' in df.columns:
        filtered_df = filtered_df[filtered_df['platform'] == target_platform]
    else:
        print("   ⚠️ No platform column found")
    
    filtered_count = len(filtered_df)
    print(f"🔍 Filtered Northbeam data from {original_count} to {filtered_count} rows")
    
    return filtered_df

def main():
    """Main function to run the Northbeam test"""
    
    print("🚀 Northbeam Report Testing Script")
    print("=" * 50)
    
    # Check environment variables
    required_vars = [
        'NORTHBEAM_DATA_CLIENT_ID',
        'NORTHBEAM_API_KEY', 
        'NORTHBEAM_PLATFORM_ACCOUNT_ID',
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'S3_BUCKET'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file")
        return
    
    print("✅ Environment variables loaded successfully")
    
    # Get date inputs
    start_date = '2025-06-01' # YYYY-MM-DD
    end_date = '2025-06-01'
    
    # Validate dates
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        print("❌ Invalid date format. Please use YYYY-MM-DD")
        return
    
    print(f"\n🔄 Processing Northbeam data for {start_date} to {end_date}")
    
    # Step 1: Create export
    print("\n📊 Step 1: Creating Northbeam export...")
    export_id = create_northbeam_export(start_date, end_date)
    if not export_id:
        print("❌ Failed to create export")
        return
    
    # Step 2: Download data
    print("\n📥 Step 2: Downloading export data...")
    df = download_export_data(export_id, start_date, end_date)
    if df is None:
        print("❌ Failed to download data")
        return
    
    
    # Step 3: Filter data
    print("\n🔍 Step 3: Filtering attribution data...")
    filtered_df = filter_attribution_data(df, ACCOUNTING_MODE_FILTER, NORTHBEAM_PLATFORM)
    
    # Step 4: Print metrics for specific ad_id
    print("\n📊 Step 4: Printing metrics for ad_id 120225701404370716...")
    target_ad_id = "120225701404370716"
    ad_data = filtered_df[filtered_df['ad_id'] == target_ad_id]
    
    if not ad_data.empty:
        print(f"✅ Found data for ad_id: {target_ad_id}")
        for column in ad_data.columns:
            value = ad_data.iloc[0][column]
            print(f"   {column}: {value}")
    else:
        print(f"❌ No data found for ad_id: {target_ad_id}")
        print(f"Available ad_ids: {filtered_df['ad_id'].unique()[:10]}...")  # Show first 10 ad_ids
    
    # Step 5: Save to S3
    print("\n☁️ Step 5: Saving to S3...")
    s3_key = save_csv_to_s3(filtered_df, start_date, end_date)
    
    # Step 6: Save locally
    print("\n💾 Step 6: Saving locally...")
    local_file = save_csv_locally(filtered_df, start_date, end_date)
    
    # Summary
    print("\n" + "=" * 50)
    print("✅ Northbeam Test Complete!")
    print(f"📊 Data Summary:")
    print(f"   - Total rows: {len(filtered_df)}")
    print(f"   - Date range: {start_date} to {end_date}")
    print(f"   - S3 location: s3://{S3_BUCKET}/{s3_key}" if s3_key else "   - S3: Failed")
    print(f"   - Local file: {local_file}" if local_file else "   - Local: Failed")
    
    if local_file:
        print(f"\n📁 You can now review the CSV file at: {local_file}")

if __name__ == "__main__":
    main() 