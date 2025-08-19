#!/usr/bin/env python3
"""
Northbeam Client Module

This module consolidates all Northbeam API functionality including:
- Configuration and authentication
- Export creation and management
- Data fetching and processing
- Retry logic and error handling
"""

import os
import time
import requests
import pandas as pd
import boto3
import io
from datetime import datetime, date
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Northbeam configuration constants - these will be passed in from the calling application
# 
# Brand Name Configuration:
# - Set NORTHBEAM_BRAND_NAME in your .env file (e.g., NORTHBEAM_BRAND_NAME=thrivecausemetics)
# - If not set, defaults to "unknown"
# - Can also be passed explicitly when creating NorthbeamClient instance
# 
# File Organization:
# - Northbeam exports are saved directly to: northbeam_exports/{brand_name}_northbeam_{dates}
# - Cache files are saved to: campaign-reporting/raw/northbeam/northbeam_{dates}

class NorthbeamClient:
    """Client for interacting with Northbeam API and managing exports"""
    
    def __init__(self, attribution_model="last_touch_non_direct", attribution_window="1", 
                 accounting_mode_api="accrual", platform="fb", brand_name="unknown"):
        """Initialize Northbeam client with configuration from environment and parameters"""
        # Northbeam API configuration
        self.client_id = os.getenv('NORTHBEAM_DATA_CLIENT_ID')
        self.api_key = os.getenv('NORTHBEAM_API_KEY')
        self.platform_account_id = os.getenv('NORTHBEAM_PLATFORM_ACCOUNT_ID')
        self.base_url = "https://api.northbeam.io/v1"
        
        # AWS configuration
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = 'us-east-1'
        self.s3_bucket = os.getenv('S3_BUCKET')
        
        # Export configuration
        self.max_retries = 3
        self.retry_delay = 10
        self.poll_interval = 5
        self.polling_timeout = 60
        
        # Attribution configuration - passed in from calling application
        self.attribution_model = attribution_model
        self.attribution_window = attribution_window
        self.accounting_mode_api = accounting_mode_api
        self.platform = platform
        
        # Brand name: environment variable first, then parameter, then default
        self.brand_name = os.getenv('NORTHBEAM_BRAND_NAME', brand_name)
        
        # Validate required configuration
        if not all([self.client_id, self.api_key, self.platform_account_id]):
            raise ValueError("Missing required Northbeam environment variables")
    
    def get_headers(self) -> Dict[str, str]:
        """Get headers for Northbeam API requests"""
        return {
            'accept': 'application/json',
            'Data-Client-ID': self.client_id,
            'Authorization': f'Bearer {self.api_key}'
        }
    
    def get_s3_client(self):
        """Get S3 client with credentials"""
        return boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region
        )
    
    def create_export(self, start_date: date, end_date: date) -> Optional[str]:
        """Create a Northbeam export for the specified date range"""
        
        # Initial delay to avoid rate limits
        time.sleep(2)
        
        url = f"{self.base_url}/exports/data-export"
        
        start_datetime = f"{start_date}T00:00:00Z"
        end_datetime = f"{end_date}T23:59:59Z"

        print("Start datetime: ", start_datetime)
        print("End datetime: ", end_datetime)

        payload = {
            "period_type": "FIXED",
            "period_options": {
                "period_starting_at": start_datetime,
                "period_ending_at": end_datetime,
            },
            "attribution_options": {
                "attribution_models": [self.attribution_model],
                "attribution_windows": [self.attribution_window],
                "accounting_modes": [self.accounting_mode_api]
            },
            "options": {
                "remove_zero_spend": False,
                "include_ids": True,
                "include_kind_and_platform": True
            },
            "time_granularity": "DAILY",
            "export_file_name": f"{self.brand_name}_northbeam_{self._format_date_for_filename(start_date)}-{self._format_date_for_filename(end_date)}",
            "bucket_name": f"{self.s3_bucket}/northbeam_exports",
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

        for attempt in range(self.max_retries):
            try:
                response = requests.post(url, headers=self.get_headers(), json=payload, timeout=60)
                
                if response.status_code == 201:
                    export_id = response.json().get('id')
                    print(f"✅ Export created successfully! ID: {export_id}")
                    return export_id
                elif response.status_code == 429:
                    print(f"❌ Rate limit exceeded (429) on attempt {attempt + 1}: {response.text}")
                    print(f"⏱️ Waiting {self.retry_delay} seconds before retrying...")
                    time.sleep(self.retry_delay)
                    continue
                elif response.status_code == 400:
                    print(f"❌ Bad request (400): {response.text}")
                    # Don't retry on 400 errors as they're likely configuration issues
                    return None
                elif response.status_code >= 500:
                    print(f"❌ Server error ({response.status_code}) on attempt {attempt + 1}: {response.text}")
                    print(f"⏱️ Waiting {self.retry_delay} seconds before retrying...")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    print(f"❌ Export creation failed: {response.status_code}")
                    print(f"Response: {response.text}")
                    return None
                    
            except requests.exceptions.Timeout:
                print(f"⏰ Request timeout on attempt {attempt + 1}")
                print(f"⏱️ Waiting {self.retry_delay} seconds before retrying...")
                time.sleep(self.retry_delay)
                continue
            except requests.exceptions.RequestException as e:
                print(f"❌ Request error on attempt {attempt + 1}: {e}")
                print(f"⏱️ Waiting {self.retry_delay} seconds before retrying...")
                time.sleep(self.retry_delay)
                continue
        
        print(f"❌ Export creation failed after {self.max_retries} attempts")
        return None
    
    def poll_export_status(self, export_id: str, timeout_seconds: int = 20, poll_interval: int = 5) -> Optional[str]:
        """Poll Northbeam for export status until ready with configurable timeout and interval"""
        
        url = f"{self.base_url}/exports/data-export/result/{export_id}"
        
        start_time = time.time()
        poll_count = 0
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        while time.time() - start_time < timeout_seconds:
            poll_count += 1
            print(f"  🔄 Poll attempt {poll_count}...")
            
            try:
                response = requests.get(url, headers=self.get_headers(), timeout=60)
                
                if response.status_code == 200:
                    data = response.json()
                    status = data.get("status")
                    
                    print(f"  ↪ Status: {status}")
                    
                    # Reset error counter on successful response
                    consecutive_errors = 0
                    
                    if status in ["ready", "SUCCESS", "success", "COMPLETED"]:
                        result_links = data.get("result", [])
                        if result_links and len(result_links) > 0:
                            print(f"✅ Export ready. File URL: {result_links[0]}")
                            return result_links[0]
                        else:
                            print(f"✅ Export completed, falling back to S3...")
                            return None
                    elif status in ["PENDING", "PROCESSING", "IN_PROGRESS"]:
                        # Export is still processing, continue polling
                        print(f"  ⏳ Export still processing...")
                    elif status in ["FAILED", "ERROR", "CANCELLED"]:
                        print(f"❌ Export failed with status: {status}")
                        if "error" in data:
                            print(f"  Error details: {data['error']}")
                        return None
                    else:
                        print(f"  ⚠️ Unknown status: {status}")
                        
                elif response.status_code == 429:
                    consecutive_errors += 1
                    wait_time = self.retry_delay  # Consistent delay
                    print(f"  ⚠️ Rate limit hit during polling (attempt {consecutive_errors}), waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 404:
                    print(f"❌ Export not found (404) - may have been deleted or expired")
                    return None
                else:
                    consecutive_errors += 1
                    print(f"  ❌ HTTP {response.status_code}: {response.text}")
                    
            except requests.exceptions.Timeout:
                consecutive_errors += 1
                print(f"  ⏰ Request timeout on poll attempt {poll_count}")
            except requests.exceptions.RequestException as e:
                consecutive_errors += 1
                print(f"  ❌ Request error on poll attempt {poll_count}: {e}")
            
            # If we have too many consecutive errors, increase wait time
            if consecutive_errors >= max_consecutive_errors:
                print(f"  ⚠️ Too many consecutive errors, increasing wait time...")
                time.sleep(poll_interval * 2)
                consecutive_errors = 0  # Reset after longer wait
            else:
                time.sleep(poll_interval)
        
        print(f"❌ Export polling timed out after {timeout_seconds} seconds")
        print(f"   - Total poll attempts: {poll_count}")
        print(f"   - Export will be retried with increased timeout")
        return None
    
    def download_export_data(self, export_id: str, start_date: date, end_date: date, 
                            timeout_seconds: int = 20, poll_interval: int = 5) -> Optional[pd.DataFrame]:
        """Download the export data with configurable timeout and S3 fallback"""
        
        # Try direct download first with specified timeout and interval
        direct_url = self.poll_export_status(export_id, timeout_seconds=timeout_seconds, poll_interval=poll_interval)
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
                    
                    # Save to organized S3 folder for future cache hits
                    try:
                        s3_key = f"campaign-reporting/raw/northbeam/northbeam_{self._format_date_for_filename(start_date)}-{self._format_date_for_filename(end_date)}.csv"
                        self.save_to_s3(df, s3_key, content_type='text/csv')
                        print(f"💾 Saved Northbeam CSV to organized S3 folder: {s3_key}")
                    except Exception as e:
                        print(f"⚠️ Could not save to organized S3 folder: {e}")
                    
                    return df
            except Exception as e:
                print(f"❌ Direct download failed: {e}")
        
        # Fallback to S3 - check for existing processed data
        print(f"⚠️ Export timed out, checking S3 for existing data...")
        s3_client = self.get_s3_client()
        
        try:
            # First check for processed data in our organized campaign-reporting directory
            processed_key = f"campaign-reporting/raw/northbeam/northbeam_{self._format_date_for_filename(start_date)}-{self._format_date_for_filename(end_date)}.csv"
            if self.file_exists_in_s3(processed_key):
                print(f"📁 Found existing processed data in S3: {processed_key}")
                response = s3_client.get_object(Bucket=self.s3_bucket, Key=processed_key)
                df = pd.read_csv(io.BytesIO(response['Body'].read()), dtype={
                    'ad_id': str,
                    'campaign_id': str,
                    'adset_id': str
                })
                print(f"✅ Downloaded {len(df)} rows from existing S3 data")
                return df
            
            # Check for Northbeam export files in the northbeam_exports folder with the new naming pattern
            print(f"🔍 Searching northbeam_exports folder for Northbeam export files with date range {start_date} to {end_date}...")
            response = s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix="northbeam_exports/", MaxKeys=1000)
            
            if 'Contents' in response:
                matching_files = []
                for obj in response['Contents']:
                    key = obj['Key']
                    # Look for Northbeam export files with the new naming pattern: {brand_name}_northbeam_{start_date}-{end_date}
                    if (key.startswith(f'{self.brand_name}_northbeam_') and 
                        key.endswith('.csv') and
                        f"{self._format_date_for_filename(start_date)}" in key and
                        f"{self._format_date_for_filename(end_date)}" in key):
                        matching_files.append({
                            'key': key,
                            'last_modified': obj['LastModified'],
                            'size': obj['Size']
                        })
                
                if matching_files:
                    # Sort by last modified (newest first) and size (largest first)
                    matching_files.sort(key=lambda x: (x['last_modified'], x['size']), reverse=True)
                    best_match = matching_files[0]
                    print(f"📁 Found {len(matching_files)} matching Northbeam export files in root bucket")
                    print(f"📁 Using best match: {best_match['key']} (modified: {best_match['last_modified']}, size: {best_match['size']} bytes)")
                    
                    response = s3_client.get_object(Bucket=self.s3_bucket, Key=best_match['key'])
                    df = pd.read_csv(io.BytesIO(response['Body'].read()), dtype={
                        'ad_id': str,
                        'adset_id': str
                    })
                    print(f"✅ Downloaded {len(df)} rows from S3 fallback (root bucket)")
                    
                    # Save to organized S3 folder for future cache hits
                    try:
                        s3_key = f"campaign-reporting/raw/northbeam/northbeam_{self._format_date_for_filename(start_date)}-{self._format_date_for_filename(end_date)}.csv"
                        self.save_to_s3(df, s3_key, content_type='text/csv')
                        print(f"💾 Saved Northbeam CSV to organized S3 folder: {s3_key}")
                    except Exception as e:
                        print(f"⚠️ Could not save to organized S3 folder: {e}")
                    
                    return df
            
            print(f"❌ No matching Northbeam files found in S3 for date range {start_date} to {end_date}")
            return None
                
        except Exception as e:
            print(f"❌ S3 fallback failed: {e}")
            return None
    
    def fetch_data(self, start_date: date, end_date: date) -> Optional[pd.DataFrame]:
        """Fetch Northbeam data for the specified date range with retry logic"""
        
        if not start_date or not end_date:
            raise ValueError("start_date and end_date must be provided to fetch Northbeam data")
        
        print(f"\n🔄 Fetching Northbeam data for {start_date} to {end_date}...")
        
        # Progressive timeout strategy - use configured poll_interval consistently
        timeout_strategies = [
            self.polling_timeout,                    # 1st attempt: timeout
            self.polling_timeout * 2,               # 2nd attempt: timeout * 2
            self.polling_timeout * 3                # 3rd attempt: timeout * 3
        ]
        
        for attempt in range(1, self.max_retries + 1):
            print(f"📊 Attempt {attempt}/{self.max_retries}")
            
            try:
                # Create export
                export_id = self.create_export(start_date, end_date)
                if not export_id:
                    print(f"❌ Attempt {attempt}: Failed to create Northbeam export")
                    if attempt < self.max_retries:
                        print(f"⏳ Waiting {self.retry_delay} seconds before retry...")
                        time.sleep(self.retry_delay)
                    continue
                
                # Download data with progressive timeout but consistent poll_interval
                timeout_seconds = timeout_strategies[min(attempt - 1, len(timeout_strategies) - 1)]
                df = self.download_export_data(export_id, start_date, end_date, timeout_seconds, self.poll_interval)
                
                if df is not None:
                    # Filter and process the data
                    filtered_df = self._filter_attribution_data(df)
                    print(f"✅ Successfully fetched {len(filtered_df)} rows of Northbeam data")
                    return filtered_df
                else:
                    print(f"❌ Attempt {attempt}: Failed to download Northbeam data")
                    if attempt < self.max_retries:
                        print(f"⏳ Waiting {self.retry_delay} seconds before retry...")
                        time.sleep(self.retry_delay)
                    continue
                    
            except Exception as e:
                print(f"❌ Attempt {attempt}: Unexpected error: {e}")
                if attempt < self.max_retries:
                    print(f"⏳ Waiting {self.retry_delay} seconds before retry...")
                    time.sleep(self.retry_delay)
                continue
        
        print(f"❌ Failed to fetch Northbeam data after {self.max_retries} attempts")
        return None
    
    def _filter_attribution_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter attribution data based on accounting mode and platform"""
        original_count = len(df)
        
        # Filter by accounting mode and platform
        filtered_df = df[
            (df['accounting_mode'] == 'Accrual performance') &
            (df['platform'] == self.platform)
        ].copy()
        
        filtered_count = len(filtered_df)
        print(f"🔍 Filtered Northbeam data from {original_count} to {filtered_count} rows")
        
        return filtered_df
    
    def _format_date_for_filename(self, date_obj: date) -> str:
        """Format date for use in filenames"""
        return date_obj.strftime('%Y%m%d')
    
    def save_to_s3(self, data: Any, s3_key: str, content_type: str = 'application/json') -> bool:
        """Save data to S3 with proper content type handling"""
        try:
            s3_client = self.get_s3_client()
            
            if content_type == 'application/json':
                # Handle JSON data
                if isinstance(data, pd.DataFrame):
                    json_data = data.to_json(orient='records', indent=2)
                else:
                    json_data = data
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=json_data,
                    ContentType=content_type
                )
            elif content_type == 'text/csv':
                # Handle CSV data
                csv_buffer = io.StringIO()
                data.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=csv_data,
                    ContentType=content_type
                )
            else:
                # Handle other data types
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=data,
                    ContentType=content_type
                )
            
            print(f"✅ Saved to S3: s3://{self.s3_bucket}/{s3_key}")
            return True
            
        except Exception as e:
            print(f"⚠️ S3 access denied or unavailable: {e}")
            print(f"📁 Falling back to local storage only")
            return False
    
    def load_from_s3(self, s3_key: str) -> Optional[Any]:
        """Load data from S3"""
        try:
            s3_client = self.get_s3_client()
            response = s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            
            if s3_key.endswith('.csv'):
                data = pd.read_csv(response['Body'])
            elif s3_key.endswith('.json'):
                data = response['Body'].read().decode('utf-8')
            else:
                data = response['Body'].read()
            
            print(f"✅ Loaded from S3: s3://{self.s3_bucket}/{s3_key}")
            return data
            
        except Exception as e:
            print(f"⚠️ S3 access denied or unavailable: {e}")
            return None
    
    def file_exists_in_s3(self, s3_key: str) -> bool:
        """Check if a file exists in S3"""
        try:
            s3_client = self.get_s3_client()
            s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
            return True
        except Exception:
            return False


# Convenience functions for backward compatibility
def get_northbeam_headers(brand_name="unknown"):
    """Get headers for Northbeam API (backward compatibility)"""
    client = NorthbeamClient(brand_name=brand_name)
    return client.get_headers()

def create_northbeam_export(start_date, end_date, brand_name="unknown"):
    """Create a Northbeam export (backward compatibility)"""
    client = NorthbeamClient(brand_name=brand_name)
    return client.create_export(start_date, end_date)

def poll_northbeam_export_status(export_id, timeout_seconds=20, poll_interval=5, brand_name="unknown"):
    """Poll Northbeam for export status (backward compatibility)"""
    client = NorthbeamClient(brand_name=brand_name)
    return client.poll_export_status(export_id, timeout_seconds, poll_interval)

def download_export_data(export_id, start_date, end_date, timeout_seconds=20, poll_interval=5, brand_name="unknown"):
    """Download export data (backward compatibility)"""
    client = NorthbeamClient(brand_name=brand_name)
    return client.download_export_data(export_id, start_date, end_date, timeout_seconds, poll_interval)

def fetch_northbeam_data(date_from=None, date_to=None, 
                        attribution_model="last_touch_non_direct", attribution_window="1",
                        accounting_mode_api="accrual", platform="fb", brand_name="unknown"):
    """Fetch Northbeam data (backward compatibility)"""
    client = NorthbeamClient(attribution_model, attribution_window, accounting_mode_api, platform, brand_name)
    return client.fetch_data(date_from, date_to)
