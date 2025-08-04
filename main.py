import requests
import os
from dotenv import load_dotenv
import pandas as pd
import json
import time
import boto3
import io
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import threading
from datetime import datetime, date
import calendar
import time
import json
import traceback
import hashlib

# Load environment variables
load_dotenv()

# User Variables Config
MERGE_ADS_WITH_SAME_NAME = True
USE_NORTHBEAM_DATA = True  # Set to True to use Northbeam data for spend/revenue metrics

# Configuration - these will be set from frontend data
DATE_FROM = "2025-06-30" # Default start date
DATE_TO = "2025-07-01" # Default end date
TOP_N = 5
CORE_PRODUCTS = [["LLEM", "Mascara"], ["BEB"]] # ["IWEL"], ["BrowGel"], ["LipTint"]

AGENCY_CODES = ["RHM", "NRTV"]
AD_TYPE_KEYWORD_VIDEO = "e:video"
AD_TYPE_KEYWORD_STATIC = "e:static"
AD_TYPE_KEYWORD_CAROUSEL = "e:carousel"


# ===== NORTHBEAM CONFIGURATION =====
NORTHBEAM_DATA_CLIENT_ID = os.getenv('NORTHBEAM_DATA_CLIENT_ID')
NORTHBEAM_API_KEY = os.getenv('NORTHBEAM_API_KEY')
NORTHBEAM_PLATFORM_ACCOUNT_ID = os.getenv('NORTHBEAM_PLATFORM_ACCOUNT_ID')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = 'us-east-1'
S3_BUCKET = os.getenv('S3_BUCKET')
NORTHBEAM_BASE_URL = "https://api.northbeam.io/v1"


# ===== META GRAPH API CONFIGURATION =====
GRAPH_BASE = "https://graph.facebook.com/v23.0"  
META_SYSTEM_USER_ACCESS_TOKEN = os.getenv('META_SYSTEM_USER_ACCESS_TOKEN')
if not META_SYSTEM_USER_ACCESS_TOKEN:
    raise ValueError("META_SYSTEM_USER_ACCESS_TOKEN not set in .env file.")

# ===== AD ACCOUNT CONFIGURATION =====
AD_ACCOUNT_NAME = "Thrive Causemetics"
AD_ACCOUNT_ID = '753196138360184'
PAGE_ID = "445629222247515"  # Your specific page ID for better video access

# Attribution configuration
ATTRIBUTION_MODEL = "last_touch_non_direct"
ATTRIBUTION_WINDOW = "1"

DEBUG_MODE = True  # Set to True to use existing CSV/JSON files if available

ACCOUNTING_MODE_API = "accrual"  # For API payload
ACCOUNTING_MODE_FILTER = "Accrual performance"
NORTHBEAM_PLATFORM = "fb"

# ===== META API ENDPOINT CONFIGURATION =====
META_ENDPOINT = f'{GRAPH_BASE}/act_{AD_ACCOUNT_ID}/insights'

# ===== VIDEO FIELDS CONFIGURATION =====
VIDEO_FIELDS = ["id", "permalink_url", "source", "thumbnails"]


def format_date_for_filename(date_string):
    """Format date string to YYYYMMDD format for filenames"""
    return date_string.replace('-', '')

def get_meta_params(date_from, date_to):
    """Get Meta API parameters with dynamic date range"""
    return {
        "level": "ad",
        "fields": "ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,spend,impressions,actions{action_type,value},action_values{action_type,value}",
        "time_range[since]": date_from,
        "time_range[until]": date_to,
        "limit": 200,
        "access_token": META_SYSTEM_USER_ACCESS_TOKEN
    }


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

# ===== META HELPER FUNCTIONS =====

def create_session_with_retries():
    """Create a requests session with retry strategy"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def extract_video_views_from_actions(actions_data):
    """Extract video views (3s) from structured actions data"""
    try:
        if not actions_data:
            return 0
            
        for action in actions_data:
            if action.get("action_type") == "video_view":
                return int(action.get("value", 0))
        return 0
        
    except Exception as e:
        return 0

def extract_purchase_data(actions_data, action_values_data):
    """Extract purchase count and value from actions and action_values data"""
    try:
        purchase_count = 0
        purchase_value = 0.0
        
        # Get purchase count from actions
        for action in actions_data or []:
            if action.get("action_type") == "omni_purchase":
                purchase_count = int(action.get("value", 0))
                break
        
        # Get purchase value from action_values
        for action_value in action_values_data or []:
            if action_value.get("action_type") == "omni_purchase":
                purchase_value = float(action_value.get("value", 0))
                break
                
        return purchase_count, purchase_value
        
    except Exception as e:
        return 0, 0.0

def extract_link_clicks(ad_data):
    """Extract link clicks from Meta ad data actions array"""
    try:
        # Extract from actions[] array
        for action in ad_data.get("actions", []):
            if action["action_type"] == "link_click":
                return int(action["value"])

        # Default to 0 if missing
        return 0
        
    except Exception as e:
        return 0


def extract_ad_type_from_ad_name(ad_name, video_keyword="e:video", static_keyword="e:static", carousel_keyword="e:carousel"):
    """Classify ad type by searching ad name for keywords only"""
    ad_name_lower = ad_name.lower()

    if carousel_keyword.lower() in ad_name_lower:
        return "Carousel"
    elif video_keyword.lower() in ad_name_lower:
        return "Video"
    elif static_keyword.lower() in ad_name_lower:
        return "Static"
    else:
        return "Unknown"


def extract_campaign_type_from_name(campaign_name):
    """Extract campaign type from campaign name (value between 'n:' and next '_')"""
    try:
        if 'n:' in campaign_name:
            start_index = campaign_name.find('n:') + 2
            end_index = campaign_name.find('_', start_index)
            if end_index != -1:
                extracted_type = campaign_name[start_index:end_index]
            else:
                extracted_type = campaign_name[start_index:]
            
            # If we have configured campaign types, try to match the extracted type
            try:
                if CAMPAIGN_TYPES and isinstance(CAMPAIGN_TYPES, list):
                    # Check if the extracted type matches any configured campaign type
                    for campaign_type in CAMPAIGN_TYPES:
                        if isinstance(campaign_type, dict) and 'campaign_name' in campaign_type:
                            if extracted_type.lower() == campaign_type['campaign_name'].lower():
                                return campaign_type['campaign_name']
                        elif isinstance(campaign_type, str):
                            if extracted_type.lower() == campaign_type.lower():
                                return campaign_type
                    
                    # If no exact match, return the extracted type as-is
                    return extracted_type
                else:
                    return extracted_type
            except NameError:
                # CAMPAIGN_TYPES is not defined, return the extracted type as-is
                return extracted_type
        return "Unknown"
    except Exception as e:
        return "Unknown"

def extract_product_from_ad_name(ad_name):
    """Extract product from ad name (value between 'a:' and '_b:') and apply product merging logic"""
    try:
        if 'a:' in ad_name and '_b:' in ad_name:
            start_index = ad_name.find('a:') + 2
            end_index = ad_name.find('_b:', start_index)
            if end_index != -1:
                extracted_product = ad_name[start_index:end_index]
                
                # Apply product merging logic from CORE_PRODUCTS
                for product_group in CORE_PRODUCTS:
                    if isinstance(product_group, list) and extracted_product in product_group:
                        # Return the first product name as the label for merged products
                        return product_group[0]
                
                # If not in any merged group, return the original product
                return extracted_product
        return "Unknown"
    except Exception as e:
        return "Unknown"

def extract_creator_from_ad_name(ad_name):
    """Extract creator from ad name (look for TH# patterns or fallback to first token after '_b:')"""
    try:
        if '_b:' in ad_name:
            start_index = ad_name.find('_b:') + 3
            
            # Look for TH# patterns
            import re
            th_pattern = r'TH\d+'
            th_matches = re.findall(th_pattern, ad_name[start_index:])
            
            if th_matches:
                # Found TH# patterns, extract creators between dashes after each TH#
                creators = []
                current_pos = start_index
                
                for th_match in th_matches:
                    th_pos = ad_name.find(th_match, current_pos)
                    if th_pos != -1:
                        next_dash = ad_name.find('-', th_pos + len(th_match))
                        if next_dash != -1:
                            second_dash = ad_name.find('-', next_dash + 1)
                            if second_dash != -1:
                                creator = ad_name[next_dash + 1:second_dash]
                                if creator.replace('-', '').replace('_', '').isalpha():
                                    creators.append(creator)
                            else:
                                creator = ad_name[next_dash + 1:]
                                if creator.replace('-', '').replace('_', '').isalpha():
                                    creators.append(creator)
                        current_pos = th_pos + len(th_match)
                
                if creators:
                    return ", ".join(creators)
            
            # Fallback: Find the first dash after '_b:'
            dash_index = ad_name.find('-', start_index)
            if dash_index > start_index:
                creator = ad_name[start_index:dash_index]
                if creator.replace('-', '').replace('_', '').isalpha():
                    return creator
        return "Unknown"
    except Exception as e:
        return "Unknown"

def extract_agency_from_ad_name(ad_name):
    """Extract agency code from ad name"""
    ad_name_upper = ad_name.upper()
    
    # Check for agency codes if they are defined
    if AGENCY_CODES is not None:
        for agency_code in AGENCY_CODES:
            if agency_code in ad_name_upper:
                return agency_code
    
    return AD_ACCOUNT_NAME

def extract_ad_metadata(ad_name, campaign_name, video_views_3s=0, video_keyword=None, static_keyword=None, carousel_keyword=None, ad_format=None, video_id=None, image_hash=None):
    """Extract all metadata from ad name and campaign name"""
    
    return {
        "campaign_type": extract_campaign_type_from_name(campaign_name),
        "product": extract_product_from_ad_name(ad_name),
        "ad_type": extract_ad_type_from_ad_name(ad_name, AD_TYPE_KEYWORD_VIDEO, AD_TYPE_KEYWORD_STATIC, AD_TYPE_KEYWORD_CAROUSEL),
        "creator": extract_creator_from_ad_name(ad_name),
        "agency": extract_agency_from_ad_name(ad_name)
    }



def fetch_meta_insights():
    """Fetch Meta insights data (raw data only)"""
        
    # Get dynamic parameters using current global values
    # Safety check for DATE_FROM and DATE_TO - if not set, raise error
    if DATE_FROM is None or DATE_TO is None:
        raise ValueError("DATE_FROM and DATE_TO must be set before calling fetch_meta_insights")
    
    meta_params = get_meta_params(DATE_FROM, DATE_TO)
    
    ads_list = []
    next_url = META_ENDPOINT
    page_num = 1
    total_ads_retrieved = 0
    session = create_session_with_retries()

    while next_url:
        try:
            # print(f"🔄 Requesting insights page {page_num}...")
            
            if next_url == META_ENDPOINT:
                resp = session.get(next_url, params=meta_params, timeout=30)
            else:
                resp = session.get(next_url, timeout=30)
                
            if resp.status_code != 200:
                print(f"❌ Error {resp.status_code}: {resp.text}")
                break

            data = resp.json()
            ads_in_page = len(data.get("data", []))
            total_ads_retrieved += ads_in_page
            print(f"📦 Meta Insights Page {page_num}: Retrieved {ads_in_page} ads (Total so far: {total_ads_retrieved})")
            page_num += 1
            
            # Save raw data without processing
            for ad in data.get("data", []):
                try:
                    # Just extract basic performance metrics, no metadata processing
                    # --- Helper parsing ---
                    spend = safe_float_conversion(ad.get("spend", 0))
                    impressions = safe_float_conversion(ad.get("impressions", 0))
                    actions = ad.get("actions", [])
                    action_values = ad.get("action_values", [])

                    # --- Extracted metrics ---
                    video_views_3s = extract_video_views_from_actions(actions)
                    purchase_count, purchase_value = extract_purchase_data(actions, action_values)
                    purchase_roas = round(purchase_value / spend, 6) if spend > 0 else 0.0
                    link_clicks = extract_link_clicks(ad)

                    ad_data = {
                        "ad_id": ad.get("ad_id"),
                        "ad_name": ad.get("ad_name", ""),
                        "campaign_id": ad.get("campaign_id", ""),
                        "campaign_name": ad.get("campaign_name", ""),
                        "adset_id": ad.get("adset_id", ""),
                        "adset_name": ad.get("adset_name", ""),
                        "spend": spend,
                        "impressions": impressions,
                        "video_views_3s": video_views_3s,
                        "purchase_count": purchase_count,
                        "purchase_value": purchase_value,
                        "purchase_roas": purchase_roas,
                        "link_clicks": link_clicks,
                        "reporting_start_date": ad.get("date_start"),
                        "reporting_end_date": ad.get("date_stop"),
                        # "actions": ad.get("actions", []),
                        # "action_values": ad.get("action_values", [])
                    }
                    
                    ads_list.append(ad_data)

                except Exception as e:
                    print(f"⚠️ Skipped ad due to error: {e}")

            next_url = data.get("paging", {}).get("next")
            
            if next_url:
                time.sleep(0.5)

        except Exception as e:
            print(f"❌ Unexpected error on page {page_num}: {e}")
            break

    
    # Save raw meta insights immediately after fetching
    if ads_list:
        # Safety check for DATE_FROM and DATE_TO
        if DATE_FROM is None or DATE_TO is None:
            raise ValueError("DATE_FROM and DATE_TO must be set before saving meta insights")
        
        date_from_formatted = format_date_for_filename(DATE_FROM)
        date_to_formatted = format_date_for_filename(DATE_TO)
        meta_json_filename = f"reports/meta_insights_{date_from_formatted}-{date_to_formatted}.json"
        
        with open(meta_json_filename, 'w') as f:
            json.dump(ads_list, f, indent=2)
        print(f"💾 Saved raw Meta insights JSON: {meta_json_filename}")
    
    return ads_list

def filter_attribution_data(df, target_accounting_mode, target_platform):
    """Filter dataframe to specific attribution configuration and platform"""
    print(f"\n🔍 FILTERING NORTHBEAM DATA:")
    print(f"   - Target Accounting Mode: {target_accounting_mode}")
    print(f"   - Target Platform: {target_platform}")
    
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
    print(f"   - Filtered from {original_count} to {filtered_count} rows")
    
    return filtered_df

def create_northbeam_export(start_date, end_date):
    """Create a Northbeam export"""
    
    time.sleep(2)
    
    url = f"{NORTHBEAM_BASE_URL}/exports/data-export"
    
    payload = {
        "period_type": "FIXED",
        "period_options": {
            "period_starting_at": f"{start_date}T00:00:00Z",
            "period_ending_at": f"{end_date}T00:00:00Z"
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
    
    print(f"Northbeam export for {start_date} to {end_date}")
    print(f"   - Period Type: {payload['period_type']}")
    print(f"   - Attribution Model: {payload['attribution_options']['attribution_models']}")
    print(f"   - Attribution Window: {payload['attribution_options']['attribution_windows']} days")
    print(f"   - Accounting Mode: {payload['attribution_options']['accounting_modes']}")
    print(f"   - Time Granularity: {payload['time_granularity']}")
    print(f"   - Level: {payload['level']}")
    print(f"   - Metrics Requested: {len(payload['metrics'])} metrics")
    
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
                    print(f"✅ Export ready. File URL: {result_links[0]}")
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
                
                # Save CSV locally
                csv_filename = f"reports/northbeam_{start_date.replace('-', '')}-{end_date.replace('-', '')}.csv"
                df.to_csv(csv_filename, index=False)
                
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
                
                # Save CSV locally
                csv_filename = f"northbeam_{start_date.replace('-', '')}-{end_date.replace('-', '')}.csv"
                df.to_csv(csv_filename, index=False)
                print(f"💾 Saved Northbeam CSV locally: {csv_filename}")
                
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




def safe_float_conversion(value, default=0.0):
    """Safely convert a value to float, handling NaN, None, and empty strings"""
    if value is None or value == '' or (hasattr(value, 'isna') and value.isna()):
        return default
    try:
        result = float(value)
        # Check if the result is NaN and return default if so
        import math
        if math.isnan(result):
            return default
        return result
    except (ValueError, TypeError):
        return default

def clean_nan_values(obj):
    """Recursively clean NaN values from a data structure"""
    import math
    if isinstance(obj, dict):
        return {key: clean_nan_values(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return 0.0
    else:
        return obj

def merge_data(northbeam_data, meta_data):
    """Merge Northbeam and Meta data into comprehensive ad objects"""
    print("Merging Northbeam and Meta data...")
    
    # Convert northbeam_data DataFrame to list of dictionaries if needed
    if isinstance(northbeam_data, pd.DataFrame):
        northbeam_list = northbeam_data.to_dict('records')
    else:
        northbeam_list = northbeam_data
    
    # Create lookup dictionaries
    northbeam_lookup = {}
    for item in northbeam_list:
        ad_id = str(item.get('ad_id', ''))
        if ad_id:
            northbeam_lookup[ad_id] = item
    
    meta_lookup = {}
    for item in meta_data:
        ad_id = str(item.get('ad_id', ''))
        if ad_id:
            meta_lookup[ad_id] = item
    
    # Merge data
    comprehensive_ads = []
    all_ad_ids = set(northbeam_lookup.keys()) | set(meta_lookup.keys())
    
    for ad_id in all_ad_ids:
        northbeam_item = northbeam_lookup.get(ad_id, {})
        meta_item = meta_lookup.get(ad_id, {})
        
        # Extract ad name from either source
        ad_name = northbeam_item.get('ad_name') or meta_item.get('ad_name', 'Unknown')
        campaign_name = northbeam_item.get('campaign_name') or meta_item.get('campaign_name', 'Unknown')
        
        # Extract additional fields
        ad_set_id = northbeam_item.get('adset_id') or meta_item.get('adset_id', '')
        ad_set_name = northbeam_item.get('adset_name') or meta_item.get('adset_name', '')
        campaign_id = northbeam_item.get('campaign_id') or meta_item.get('campaign_id', '')
        
        # Create comprehensive ad object with proper schema
        ad_object = {
            'ad_ids': {
                'ad_id': ad_id,
                'ad_name': ad_name,
                'ad_set_id': ad_set_id,
                'ad_set_name': ad_set_name,
                'campaign_id': campaign_id,
                'campaign_name': campaign_name
            },
            'metadata': {
                'campaign_type': extract_campaign_type_from_name(campaign_name),
                'product': extract_product_from_ad_name(ad_name),
                'ad_type': extract_ad_type_from_ad_name(ad_name, AD_TYPE_KEYWORD_VIDEO, AD_TYPE_KEYWORD_STATIC, AD_TYPE_KEYWORD_CAROUSEL),
                'creator': extract_creator_from_ad_name(ad_name),
                'agency': extract_agency_from_ad_name(ad_name)
            },
            'filters': {
                'reporting_start_date': DATE_FROM,
                'reporting_end_date': DATE_TO
            },
            'metrics': {
                'northbeam': {
                    'ad_id': northbeam_item.get('ad_id', ''),
                    'ad_name': northbeam_item.get('ad_name', ''),
                    'campaign_id': northbeam_item.get('campaign_id', ''),
                    'campaign_name': northbeam_item.get('campaign_name', ''),
                    'adset_id': northbeam_item.get('adset_id', ''),
                    'adset_name': northbeam_item.get('adset_name', ''),
                    'accounting_mode': northbeam_item.get('accounting_mode', ''),
                    'attribution_model': northbeam_item.get('attribution_model', ''),
                    'attribution_window': northbeam_item.get('attribution_window', ''),
                    'spend': safe_float_conversion(northbeam_item.get('spend')),
                    'impressions': safe_float_conversion(northbeam_item.get('impressions')),
                    'meta_link_clicks': safe_float_conversion(northbeam_item.get('meta_link_clicks')),
                    'meta_3s_video_views': safe_float_conversion(northbeam_item.get('meta_3s_video_views')),
                    'attributed_rev': safe_float_conversion(northbeam_item.get('attributed_rev')),
                    'transactions': safe_float_conversion(northbeam_item.get('transactions')),
                    'roas': safe_float_conversion(northbeam_item.get('roas'))
                },
                'meta': {
                    'spend': safe_float_conversion(meta_item.get('spend')),
                    'impressions': safe_float_conversion(meta_item.get('impressions')),
                    'link_clicks': safe_float_conversion(meta_item.get('link_clicks')),
                    'purchase_value': safe_float_conversion(meta_item.get('purchase_value')),
                    'purchase_count': safe_float_conversion(meta_item.get('purchase_count')),
                    'purchase_roas': safe_float_conversion(meta_item.get('purchase_roas')),
                    'video_views_3s': safe_float_conversion(meta_item.get('video_views_3s'))
                }
            }
        }
        
        comprehensive_ads.append(ad_object)
    
    print(f"✅ Merged {len(comprehensive_ads)} ad objects")
    return comprehensive_ads

def get_metric_value(ad, metric_key, data_source='northbeam', default=0.0):
    """Get metric value from ad object, handling missing/invalid values"""
    # Safety check for USE_NORTHBEAM_DATA - if not set, default to True
    import sys
    current_module = sys.modules[__name__]
    use_northbeam = getattr(current_module, 'USE_NORTHBEAM_DATA', True)
    
    
    if use_northbeam:
        # When USE_NORTHBEAM_DATA is True, only use Northbeam data or return 0
        value = ad['metrics']['northbeam'].get(metric_key, default)
        result = float(value) if value != '' and value is not None else default
        return result
    else:
        # When USE_NORTHBEAM_DATA is False, only use Meta data
        # Map Northbeam keys to Meta keys
        meta_key_mapping = {
            'spend': 'spend',
            'impressions': 'impressions',
            'meta_link_clicks': 'link_clicks',
            'attributed_rev': 'purchase_value',
            'transactions': 'purchase_count',
            'meta_3s_video_views': 'video_views_3s',
            'roas': 'purchase_roas'  
        }
        
        meta_key = meta_key_mapping.get(metric_key, metric_key)
        value = ad['metrics']['meta'].get(meta_key, default)
        result = float(value) if value != '' and value is not None else default
        return result

def merge_ads_with_same_name(ad_objects):
    """Merge ads with the same name and aggregate their metrics"""
    print(f"DEBUG: Starting merge_ads_with_same_name ad_objects count: {len(ad_objects)}")
    
    merged_ads = {}
    ad_name_counts = {}  # Track how many ads have each name
    
    for i, ad in enumerate(ad_objects):
        ad_name = ad['ad_ids']['ad_name']
        ad_id = ad['ad_ids']['ad_id']
        
        # Track ad name occurrences
        if ad_name not in ad_name_counts:
            ad_name_counts[ad_name] = []
        ad_name_counts[ad_name].append(ad_id)
        
        
        if ad_name not in merged_ads:
            # Create a new merged ad object
            merged_ads[ad_name] = {
                'ad_ids': {
                    'ad_id': ad_id,  # Keep the first ad_id as primary
                    'ad_name': ad_name,
                    'ad_set_id': ad['ad_ids']['ad_set_id'],
                    'ad_set_name': ad['ad_ids']['ad_set_name'],
                    'campaign_id': ad['ad_ids']['campaign_id'],
                    'campaign_name': ad['ad_ids']['campaign_name'],
                    'all_ad_ids': [ad_id]  # Track all ad IDs
                },
                'metadata': ad['metadata'].copy(),
                'filters': ad['filters'].copy(),
                'metrics': {
                    'meta': {
                        'spend': float(ad['metrics']['meta']['spend']),
                        'impressions': float(ad['metrics']['meta']['impressions']),
                        'link_clicks': float(ad['metrics']['meta']['link_clicks']),
                        'purchase_value': float(ad['metrics']['meta']['purchase_value']),
                        'purchase_count': float(ad['metrics']['meta']['purchase_count']),
                        'purchase_roas': float(ad['metrics']['meta'].get('roas', 0.0)),
                        'video_views_3s': float(ad['metrics']['meta']['video_views_3s'])
                    },
                    'northbeam': {
                        'ad_id': ad['metrics']['northbeam']['ad_id'],
                        'ad_name': ad['metrics']['northbeam']['ad_name'],
                        'campaign_id': ad['metrics']['northbeam']['campaign_id'],
                        'campaign_name': ad['metrics']['northbeam']['campaign_name'],
                        'adset_id': ad['metrics']['northbeam']['adset_id'],
                        'adset_name': ad['metrics']['northbeam']['adset_name'],
                        'accounting_mode': ad['metrics']['northbeam']['accounting_mode'],
                        'attribution_model': ad['metrics']['northbeam']['attribution_model'],
                        'attribution_window': ad['metrics']['northbeam']['attribution_window'],
                        'spend': safe_float_conversion(ad['metrics']['northbeam']['spend']),
                        'impressions': safe_float_conversion(ad['metrics']['northbeam']['impressions']),
                        'meta_link_clicks': safe_float_conversion(ad['metrics']['northbeam']['meta_link_clicks']),
                        'attributed_rev': safe_float_conversion(ad['metrics']['northbeam']['attributed_rev']),
                        'transactions': safe_float_conversion(ad['metrics']['northbeam']['transactions']),
                        'roas': safe_float_conversion(ad['metrics']['northbeam']['roas']),
                        'meta_3s_video_views': safe_float_conversion(ad['metrics']['northbeam']['meta_3s_video_views'])
                    }
                }
            }
        else:
            # Add this ad_id to the list of all ad IDs
            merged_ads[ad_name]['ad_ids']['all_ad_ids'].append(ad_id)
            
            # Aggregate metrics
            # Meta metrics
            merged_ads[ad_name]['metrics']['meta']['spend'] += float(ad['metrics']['meta']['spend'])
            merged_ads[ad_name]['metrics']['meta']['impressions'] += float(ad['metrics']['meta']['impressions'])
            merged_ads[ad_name]['metrics']['meta']['link_clicks'] += float(ad['metrics']['meta']['link_clicks'])
            merged_ads[ad_name]['metrics']['meta']['purchase_value'] += float(ad['metrics']['meta']['purchase_value'])
            merged_ads[ad_name]['metrics']['meta']['purchase_count'] += float(ad['metrics']['meta']['purchase_count'])
            merged_ads[ad_name]['metrics']['meta']['video_views_3s'] += float(ad['metrics']['meta']['video_views_3s'])
            
            # Northbeam metrics
            merged_ads[ad_name]['metrics']['northbeam']['spend'] += safe_float_conversion(ad['metrics']['northbeam']['spend'])
            merged_ads[ad_name]['metrics']['northbeam']['impressions'] += safe_float_conversion(ad['metrics']['northbeam']['impressions'])
            merged_ads[ad_name]['metrics']['northbeam']['meta_link_clicks'] += safe_float_conversion(ad['metrics']['northbeam']['meta_link_clicks'])
            merged_ads[ad_name]['metrics']['northbeam']['attributed_rev'] += safe_float_conversion(ad['metrics']['northbeam']['attributed_rev'])
            merged_ads[ad_name]['metrics']['northbeam']['transactions'] += safe_float_conversion(ad['metrics']['northbeam']['transactions'])
            merged_ads[ad_name]['metrics']['northbeam']['meta_3s_video_views'] += safe_float_conversion(ad['metrics']['northbeam']['meta_3s_video_views'])
    
    # Calculate aggregated ROAS for both data sources
    for ad_name, merged_ad in merged_ads.items():
        # Calculate Northbeam ROAS
        if merged_ad['metrics']['northbeam']['spend'] > 0:
            merged_ad['metrics']['northbeam']['roas'] = merged_ad['metrics']['northbeam']['attributed_rev'] / merged_ad['metrics']['northbeam']['spend']
        
        # Calculate Meta ROAS
        if merged_ad['metrics']['meta']['spend'] > 0:
            merged_ad['metrics']['meta']['roas'] = merged_ad['metrics']['meta']['purchase_value'] / merged_ad['metrics']['meta']['spend']
    
    # Print merge statistics
    print(f"DEBUG: Merge statistics:")
    print(f"  - Original ads: {len(ad_objects)}")
    print(f"  - Merged ads: {len(merged_ads)}")
    print(f"  - Reduction: {len(ad_objects) - len(merged_ads)} ads")
    
    # Show which ad names had multiple ads
    # print(f"DEBUG: Ad names with multiple ads:")
    # for ad_name, ad_ids in ad_name_counts.items():
    #     if len(ad_ids) > 1:
    #         print(f"  - '{ad_name}': {len(ad_ids)} ads (IDs: {ad_ids})")
    
    result = list(merged_ads.values())
    return result

def calculate_campaign_metrics(ad_objects, filters=None, data_source='northbeam'):
    """
    Calculate campaign metrics from ad objects with flexible filtering and data source options
    
    Args:
        ad_objects (list): List of comprehensive ad objects
        filters (dict): Optional filters to apply:
            - campaign_type (str or list): Filter by specific campaign type(s)
            - product (str or list): Filter by specific product(s)
            - agency (str or list): Filter by specific agency(ies)
            - ad_type (str or list): Filter by specific ad type(s)
            - creator (str or list): Filter by specific creator(s)
        data_source (str): Which data source to use for metrics ('northbeam' or 'meta')
    
    Returns:
        dict: Calculated metrics
    """
    
    # Apply filters if provided
    filtered_ads = ad_objects
    if filters:
        filtered_ads = []
        for ad in ad_objects:
            include_ad = True
            
            # Check each filter
            if 'campaign_type' in filters and filters['campaign_type']:
                filter_value = filters['campaign_type']
                if isinstance(filter_value, list):
                    if ad['metadata']['campaign_type'] not in filter_value:
                        include_ad = False
                else:
                    if ad['metadata']['campaign_type'] != filter_value:
                        include_ad = False
                    
            if 'product' in filters and filters['product']:
                filter_value = filters['product']
                if isinstance(filter_value, list):
                    if ad['metadata']['product'] not in filter_value:
                        include_ad = False
                else:
                    if ad['metadata']['product'] != filter_value:
                        include_ad = False
                    
            if 'agency' in filters and filters['agency']:
                filter_value = filters['agency']
                if isinstance(filter_value, list):
                    if ad['metadata']['agency'] not in filter_value:
                        include_ad = False
                else:
                    if ad['metadata']['agency'] != filter_value:
                        include_ad = False
                    
            if 'ad_type' in filters and filters['ad_type']:
                filter_value = filters['ad_type']
                if isinstance(filter_value, list):
                    if ad['metadata']['ad_type'] not in filter_value:
                        include_ad = False
                else:
                    if ad['metadata']['ad_type'] != filter_value:
                        include_ad = False
                    
            if 'creator' in filters and filters['creator']:
                filter_value = filters['creator']
                if isinstance(filter_value, list):
                    if ad['metadata']['creator'] not in filter_value:
                        include_ad = False
                else:
                    if ad['metadata']['creator'] != filter_value:
                        include_ad = False
            
            if include_ad:
                filtered_ads.append(ad)
    
    total_ads = len(filtered_ads)
    total_spend = 0.0
    total_revenue = 0.0
    total_transactions = 0
    total_impressions = 0
    total_link_clicks = 0
    total_video_views = 0
    
    # Track impressions for video ads only (for thumbstop calculation)
    video_impressions = 0
    
    
    for ad in filtered_ads:
        # Determine which data source to use for each metric
        if data_source == 'northbeam':
            # Use Northbeam data for all metrics
            nb_metrics = ad['metrics']['northbeam']
            total_spend += safe_float_conversion(nb_metrics.get('spend'))
            total_revenue += safe_float_conversion(nb_metrics.get('attributed_rev'))
            total_transactions += safe_float_conversion(nb_metrics.get('transactions'))
            total_impressions += safe_float_conversion(nb_metrics.get('impressions'))
            total_link_clicks += safe_float_conversion(nb_metrics.get('meta_link_clicks'))
            
            # Track video metrics for thumbstop
            video_views_3s = safe_float_conversion(nb_metrics.get('meta_3s_video_views'))
            total_video_views += video_views_3s
            if video_views_3s > 0:
                video_impressions += safe_float_conversion(nb_metrics.get('impressions'))
            
        elif data_source == 'meta':
            # Use Meta data for all metrics
            meta_metrics = ad['metrics']['meta']
            total_spend += safe_float_conversion(meta_metrics.get('spend'))
            total_revenue += safe_float_conversion(meta_metrics.get('purchase_value'))
            total_transactions += safe_float_conversion(meta_metrics.get('purchase_count'))
            total_impressions += safe_float_conversion(meta_metrics.get('impressions'))
            total_link_clicks += safe_float_conversion(meta_metrics.get('link_clicks'))
            
            # Track video metrics for thumbstop
            video_views_3s = safe_float_conversion(meta_metrics.get('video_views_3s'))
            total_video_views += video_views_3s
            if video_views_3s > 0:
                video_impressions += safe_float_conversion(meta_metrics.get('impressions'))
    
    # Calculate derived metrics
    roas = total_revenue / total_spend if total_spend > 0 else 0
    ctr = (total_link_clicks / total_impressions * 100) if total_impressions > 0 else 0
    cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
    thumbstop = (total_video_views / video_impressions * 100) if video_impressions > 0 else 0
    aov = total_revenue / total_transactions if total_transactions > 0 else 0
    
    # Create filter description for reporting
    filter_description = "All Ads"
    if filters:
        active_filters = []
        for key, value in filters.items():
            if value:
                active_filters.append(f"{key}: {value}")
        if active_filters:
            filter_description = " | ".join(active_filters)
    
    
    return {
        'filter_description': filter_description,
        'data_source': data_source,
        'total_ads': total_ads,
        'total_spend': total_spend,
        'total_revenue': total_revenue,
        'total_transactions': total_transactions,
        'total_impressions': total_impressions,
        'total_link_clicks': total_link_clicks,
        'total_video_views': total_video_views,
        'roas': roas,
        'ctr': ctr,
        'cpm': cpm,
        'thumbstop': thumbstop,
        'aov': aov
    }

def get_available_filters(ad_objects):
    """
    Get all available filter values from the ad objects
    
    Returns:
        dict: Available filter options
    """
    filters = {
        'campaign_types': set(),
        'products': set(),
        'agencies': set(),
        'ad_types': set(),
        'creators': set()
    }
    
    for ad in ad_objects:
        metadata = ad['metadata']
        filters['campaign_types'].add(metadata.get('campaign_type', 'Unknown'))
        filters['products'].add(metadata.get('product', 'Unknown'))
        filters['agencies'].add(metadata.get('agency', 'Unknown'))
        filters['ad_types'].add(metadata.get('ad_type', 'Unknown'))
        filters['creators'].add(metadata.get('creator', 'Unknown'))
    
    # Convert sets to sorted lists
    return {key: sorted(list(value)) for key, value in filters.items()}

def print_campaign_metrics(metrics):
    """Print formatted campaign metrics"""
    print("\n" + "=" * 60)
    print("Campaign Metrics")
    print("=" * 60)
    print(f"📋 Filter: {metrics['filter_description']}")
    print(f"📊 Data Source: {metrics['data_source'].upper()}")
    print(f"📊 Total Ads: {metrics['total_ads']:,}")
    print(f"💰 Total Spend: ${metrics['total_spend']:,.2f}")
    print(f"📈 ROAS: {metrics['roas']:.2f}")
    print(f"🖱️ CTR: {metrics['ctr']:.2f}%")
    print(f"📱 CPM: ${metrics['cpm']:.2f}")
    print(f"👁️ Thumbstop: {metrics['thumbstop']:.2f}%")
    print(f"🛒 AOV: ${metrics['aov']:.2f}")
    print(f"💵 Total Revenue: ${metrics['total_revenue']:,.2f}")
    print(f"🛍️ Total Transactions: {metrics['total_transactions']:,}")

def generate_comprehensive_report(ad_objects, data_source='northbeam'):
    """
    Generate comprehensive report with multiple breakdowns
    
    Args:
        ad_objects (list): List of comprehensive ad objects
        data_source (str): Data source to use ('northbeam' or 'meta')
    """
    
    print("\n" + "=" * 80)
    print("COMPREHENSIVE CAMPAIGN REPORT")
    print("=" * 80)
    
    # Overall metrics
    overall_metrics = calculate_campaign_metrics(ad_objects, data_source=data_source)
    print_campaign_metrics(overall_metrics)
    
    # Get available filters
    available_filters = get_available_filters(ad_objects)
    
    # Campaign type breakdown (all campaign types found in data)
    print("\n" + "=" * 60)
    print("BREAKDOWN BY CAMPAIGN TYPE")
    print("=" * 60)
    
    for campaign_type in available_filters['campaign_types']:
        if campaign_type != 'Unknown':
            metrics = calculate_campaign_metrics(
                ad_objects, 
                filters={'campaign_type': campaign_type}, 
                data_source=data_source
            )
            print_campaign_metrics(metrics)
    
    # Product breakdown (Core products only)
    # print("\n" + "=" * 60)
    # print("BREAKDOWN BY PRODUCT")
    # print("=" * 60)
    
    # Get core products from CORE_PRODUCTS configuration
    # core_products = []
    # for product_group in CORE_PRODUCTS:
    #     if isinstance(product_group, list):
    #         core_products.extend(product_group)
    #     else:
    #         core_products.append(product_group)
    
    # for product in available_filters['products']:
    #     if product != 'Unknown' and product in core_products:
    #         metrics = calculate_campaign_metrics(
    #             ad_objects, 
    #             filters={'product': product}, 
    #             data_source=data_source
    #         )
    #         print_campaign_metrics(metrics)
    
    # Ad type breakdown
    # print("\n" + "=" * 60)
    # print("BREAKDOWN BY AD TYPE")
    # print("=" * 60)
    # for ad_type in available_filters['ad_types']:
    #     if ad_type != 'Unknown':
    #         metrics = calculate_campaign_metrics(
    #             ad_objects, 
    #             filters={'ad_type': ad_type}, 
    #             data_source=data_source
    #         )
    #         print_campaign_metrics(metrics)



def get_available_filter_options(ad_objects):
    """
    Get all available filter options for easy reference
    
    Returns:
        dict: Available filter options with counts
    """
    available_filters = get_available_filters(ad_objects)
    
    # Map filter type names to metadata field names
    filter_to_metadata_map = {
        'campaign_types': 'campaign_type',
        'products': 'product',
        'ad_types': 'ad_type',
        'creators': 'creator',
        'agencies': 'agency'
    }
    
    # Count occurrences for each filter value
    filter_counts = {}
    for filter_type, values in available_filters.items():
        filter_counts[filter_type] = {}
        metadata_field = filter_to_metadata_map.get(filter_type, filter_type)
        
        for value in values:
            count = sum(1 for ad in ad_objects 
                       if ad['metadata'].get(metadata_field) == value)
            filter_counts[filter_type][value] = count
    
    return filter_counts

def fetch_northbeam_data():
    """Fetch Northbeam data for the specified date range"""
    # Safety check for DATE_FROM and DATE_TO - if not set, raise error
    if DATE_FROM is None or DATE_TO is None:
        raise ValueError("DATE_FROM and DATE_TO must be set before calling fetch_northbeam_data")
    
    print(f"\n🔄 Fetching Northbeam data for {DATE_FROM} to {DATE_TO}...")
    
    # Create export
    export_id = create_northbeam_export(DATE_FROM, DATE_TO)
    if not export_id:
        print("❌ Failed to create Northbeam export")
        return None
    
    # Download data
    df = download_export_data(export_id, DATE_FROM, DATE_TO)
    if df is None:
        print("❌ Failed to download Northbeam data")
        return None
    
    # Filter data
    filtered_df = filter_attribution_data(df, ACCOUNTING_MODE_FILTER, NORTHBEAM_PLATFORM)
    
    # Save filtered data
    date_from_formatted = format_date_for_filename(DATE_FROM)
    date_to_formatted = format_date_for_filename(DATE_TO)
    csv_filename = f"reports/northbeam_{date_from_formatted}-{date_to_formatted}.csv"
    filtered_df.to_csv(csv_filename, index=False)
    print(f"💾 Saved Northbeam CSV: {csv_filename}")
    
    return filtered_df



def fetch_all_data_sequentially():
    """
    Fetch all required data sequentially and return comprehensive ad objects.
    Process: Fetch Meta data → Save → Fetch Northbeam data → Save → Merge into comprehensive objects
    """
    
    print(f"🚀 COMPREHENSIVE AD METRICS EXTRACTION")
    print("=" * 60)
    
    # Print configuration
    print(f"\n🎯 CONFIGURATION:")
    print(f"   - Date Range: {DATE_FROM} to {DATE_TO}")
    # Safety check for USE_NORTHBEAM_DATA
    use_northbeam = getattr(globals(), 'USE_NORTHBEAM_DATA', True)
    print(f"   - Data Source: {'Northbeam' if use_northbeam else 'Meta'}")
    print(f"   - Top N: {TOP_N}")
    
    print(f"\n🔍 STEP 1: CHECKING EXISTING DATA FILES...")
    
    # Check for existing data files
    # Safety check for DATE_FROM and DATE_TO
    if DATE_FROM is None or DATE_TO is None:
        raise ValueError("DATE_FROM and DATE_TO must be set before checking existing files")
    
    date_from_formatted = format_date_for_filename(DATE_FROM)
    date_to_formatted = format_date_for_filename(DATE_TO)
    
    meta_insights_file = f"reports/meta_insights_{date_from_formatted}-{date_to_formatted}.json"
    northbeam_file = f"reports/northbeam_{date_from_formatted}-{date_to_formatted}.csv"
    
    existing_files = {
        'meta_insights': None,
        'northbeam_data': None
    }
    
    # Check which files exist
    if os.path.exists(meta_insights_file):
        try:
            with open(meta_insights_file, 'r') as f:
                existing_files['meta_insights'] = json.load(f)
            print(f"✅ Found existing Meta insights: {len(existing_files['meta_insights'])} ads")
        except Exception as e:
            print(f"⚠️ Error loading existing Meta insights: {e}")
    
    if os.path.exists(northbeam_file):
        try:
            # Read CSV with specific dtype to ensure ID columns are treated as strings
            existing_files['northbeam_data'] = pd.read_csv(northbeam_file, dtype={
                'ad_id': str,
                'campaign_id': str,
                'adset_id': str
            })
            print(f"✅ Found existing Northbeam data: {len(existing_files['northbeam_data'])} rows")
        except Exception as e:
            print(f"⚠️ Error loading existing Northbeam data: {e}")
    
    # Initialize with existing data
    meta_insights = existing_files['meta_insights']
    northbeam_df = existing_files['northbeam_data']
    
    print(f"\n⚡ STEP 2: FETCHING MISSING RAW DATA SEQUENTIALLY...")
    
    # Fetch Meta insights first (if missing)
    if meta_insights is None:
        print("📊 STEP 2a: Fetching Meta insights...")
        try:
            meta_insights = fetch_meta_insights()
            print(f"✅ Meta insights fetched: {len(meta_insights) if meta_insights else 0} ads")
        except Exception as e:
            print(f"❌ Error fetching Meta insights: {e}")
            return None, None
    else:
        print("📊 Meta insights already available")
    
    # Fetch Northbeam data second (if missing)
    if northbeam_df is None:
        print("📊 STEP 2b: Fetching Northbeam data...")
        try:
            northbeam_df = fetch_northbeam_data()
            print(f"✅ Northbeam data fetched: {len(northbeam_df) if northbeam_df is not None else 0} rows")
        except Exception as e:
            print(f"❌ Error fetching Northbeam data: {e}")
            return None, None
    else:
        print("📊 Northbeam data already available")
    
    print(f"\n📊 FINAL DATA SUMMARY:")
    print(f"   - Meta insights: {len(meta_insights) if meta_insights else 0} ads")
    print(f"   - Northbeam data: {len(northbeam_df) if northbeam_df is not None else 0} rows")
    
    return meta_insights, northbeam_df

def main():
    """Main execution function"""
    try:

        print("🎯 Campaign Reporting Script")
        print(f"📅 Date Range: {DATE_FROM} to {DATE_TO}")
        print("=" * 60)
        
        # Create reports directory if it doesn't exist
        os.makedirs("reports", exist_ok=True)
        
        # Fetch data sequentially
        meta_insights, northbeam_df = fetch_all_data_sequentially()
        
        if meta_insights is None or northbeam_df is None:
            print("❌ Failed to fetch required data")
            return
        
        print(f"\n🔗 STEP 3: MERGING DATA INTO COMPREHENSIVE OBJECTS...")
        
        # Merge Northbeam and Meta data into comprehensive ad objects
        comprehensive_ads = merge_data(northbeam_df, meta_insights)

        # Merge ads with same name if enabled
        if MERGE_ADS_WITH_SAME_NAME:
            print(f"\n🔗 Merging ads with same name...")
            comprehensive_ads = merge_ads_with_same_name(comprehensive_ads)
            print(f"✅ Merged to {len(comprehensive_ads)} unique ads")

        # Clean any remaining NaN values before saving
        comprehensive_ads = clean_nan_values(comprehensive_ads)
        
        # Save comprehensive report
        date_from_formatted = format_date_for_filename(DATE_FROM)
        date_to_formatted = format_date_for_filename(DATE_TO)
        comprehensive_filename = f"reports/comprehensive_ads_{date_from_formatted}-{date_to_formatted}.json"
        
        with open(comprehensive_filename, 'w') as f:
            json.dump(comprehensive_ads, f, indent=2)
        
        print(f"💾 Saved comprehensive report: {comprehensive_filename}")
        
        if comprehensive_ads:
            # Generate comprehensive report with breakdowns - use the correct data source based on USE_NORTHBEAM_DATA setting
            data_source = 'northbeam' if USE_NORTHBEAM_DATA else 'meta'
            generate_comprehensive_report(comprehensive_ads, data_source=data_source)
            
            print(f"\n✅ Script completed successfully!")
            print(f"📁 Reports saved in 'reports/' directory")
            
            # Example of how to use the flexible function for specific filters
            # print(f"\n" + "=" * 60)
            # print("EXAMPLE: SPECIFIC FILTER QUERIES")
            # print("=" * 60)
            
            # Example 1: Prospecting campaigns only
            # prospecting_metrics = calculate_campaign_metrics(
            #     comprehensive_ads, 
            #     filters={'campaign_type': 'Prospecting'}, 
            #     data_source='northbeam'
            # )
            # print_campaign_metrics(prospecting_metrics)
            
            # Example 2: Video ads only
            # video_metrics = calculate_campaign_metrics(
            #     comprehensive_ads, 
            #     filters={'ad_type': 'Video'}, 
            #     data_source='northbeam'
            # )
            # print_campaign_metrics(video_metrics)
            
            # Example 3: Specific product
            # product_metrics = calculate_campaign_metrics(
            #     comprehensive_ads, 
            #     filters={'product': 'BEB'}, 
            #     data_source='northbeam'
            # )
            # print_campaign_metrics(product_metrics)
            
            # Show available filter options
            print(f"\n" + "=" * 60)
            print("AVAILABLE FILTER OPTIONS")
            print("=" * 60)
            filter_options = get_available_filter_options(comprehensive_ads)
            for filter_type, options in filter_options.items():
                print(f"\n{filter_type.upper()}:")
                for value, count in options.items():
                    if value != 'Unknown':
                        print(f"  - {value}: {count} ads")
            
        else:
            print("❌ Script failed to complete")
            
    except Exception as e:
        print(f"❌ Script failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()


