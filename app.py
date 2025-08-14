import streamlit as st
import pandas as pd
import json
import os
import requests
import time
import boto3
import io
from typing import Dict, List, Tuple, Optional, Set
from datetime import datetime, timedelta, date
import plotly.express as px
import plotly.graph_objects as go
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
# import threading  # Removed to fix infinite loop issue
import calendar
import traceback
import hashlib
import asyncio
# Import statements for main functions will be removed as we merge them directly

# Import media_urls_manager for handling media URLs
from media_urls_manager import (
    process_existing_media_urls,
    fetch_missing_media_urls,
    get_thumbnail_url_from_cache,
    load_media_urls_cache
)

# Import authentication module
from auth import require_authentication, show_logout_button

# ===== CONFIGURATION & CONSTANTS =====
# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# User Variables Config
# MERGE_ADS_WITH_SAME_NAME = True
# USE_NORTHBEAM_DATA = True  # Set to True to use Northbeam data for spend/revenue metrics
DOWNLOAD_REPORTS_LOCALLY = False  # Set to True to save all fetched/processed data locally (in addition to S3)
# Note: When DOWNLOAD_REPORTS_LOCALLY = False, files are only saved to S3, saving disk space

# Root directory for S3 storage (updated to match actual S3 bucket structure)
ROOT_DIRECTORY = "campaign-reporting"

# Configuration - these will be set from frontend data
# DATE_FROM = "2025-06-30" # Default start date
# DATE_TO = "2025-07-01" # Default end date
# TOP_N = 5
# CORE_PRODUCTS = [["LLEM", "Mascara"], ["BEB"], ["IWEL"], ["BrowGel"], ["LipTint"]]

CAMPAIGN_TYPES = [["Prospecting", 0.35], ["Prospecting+Remarketing", 0.69], ["Remarketing", 2.20]]
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

# Northbeam export configuration
NORTHBEAM_MAX_RETRIES = 3      # 3 retries default
NORTHBEAM_RETRY_DELAY = 10         # Wait between retry attempts (10 seconds)  

NORTHBEAM_POLL_INTERVAL = 5
NORTHBEAM_BASE_DELAY = 15          # Base for exponential backoff (15s, 30s, 60s)

META_REQUEST_TIMEOUT = 30            # Timeout for Meta API requests
META_RATE_LIMIT_DELAY = 0.5         # Delay between Meta API requests

# ===== META GRAPH API CONFIGURATION =====
META_API_VERSION = os.getenv('META_API_VERSION', 'v23.0')  # Use exact working version
GRAPH_BASE = f"https://graph.facebook.com/{META_API_VERSION}"  
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

# ===== STATUS MESSAGES CONFIGURATION =====
# Status messages now include timestamps and can be manually cleared

# ===== UTILITY FUNCTIONS =====
def get_top_spending_ad_thumbnail(ad_objects, group_key, group_value):
    """
    Get the thumbnail from the top spending ad in a group.
    
    Args:
        ad_objects: List of ad objects
        group_key: The metadata key to group by (e.g., 'product', 'creator', 'agency')
        group_value: The specific value to filter by
    
    Returns:
        str: Thumbnail URL from the top spending ad, or empty string if not found
    """
    if not ad_objects:
        return ""
    
    # Filter ads by the group criteria
    filtered_ads = []
    for ad in ad_objects:
        if group_key == 'product':
            # Handle product groups (check if ad's product is in the product group)
            ad_product = ad['metadata'].get('product', '')
            if ad_product == group_value:
                filtered_ads.append(ad)
        else:
            # For creator, agency, etc.
            if ad['metadata'].get(group_key, '') == group_value:
                filtered_ads.append(ad)
    
    if not filtered_ads:
        return ""
    
    # Sort by spend and get the top one
    top_ad = max(filtered_ads, key=lambda ad: get_metric_value(ad, 'spend', default=0))
    
    # Get the thumbnail for this ad
    ad_id = top_ad['ad_ids'].get('ad_id', '')
    if ad_id:
        return get_thumbnail_url_from_cache(ad_id)
    
    return ""

def format_date_for_filename(date_input):
    """Format date input to YYYYMMDD format for filenames"""
    if isinstance(date_input, str):
        return date_input.replace('-', '')
    elif hasattr(date_input, 'strftime'):
        return date_input.strftime("%Y%m%d")
    else:
        raise ValueError(f"Unsupported date type: {type(date_input)}")

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

def auto_hide_status_message(message_key, message_type="info", auto_hide_seconds=5):
    """
    Display a status message in the sidebar. New messages clear old ones.
    
    Args:
        message_key (str): The message to display
        message_type (str): Type of message ('info', 'success', 'warning', 'error')
        auto_hide_seconds (int): Seconds before message auto-hides (default: 5)
    """
    # Initialize session state for status messages if not exists
    if 'status_messages' not in st.session_state:
        st.session_state.status_messages = {}
    
    # Clear all existing messages - only show the latest one
    st.session_state.status_messages = {}
    
    # Create single message entry
    message_id = "latest_status"
    
    # Store message in session state with timestamp and auto-hide info
    st.session_state.status_messages[message_id] = {
        'type': message_type,
        'timestamp': time.time(),
        'auto_hide_seconds': auto_hide_seconds,
        'message': message_key
    }

def display_status_messages():
    """Display all active status messages in a compact, single location"""
    if 'status_messages' not in st.session_state or not st.session_state.status_messages:
        return
    
    # Create a compact status container
    with st.container():
        st.markdown("---")
        
        # Header with clear button and auto-refresh info
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.subheader("📊 Status Messages")
        with col2:
            if st.button("🗑️ Clear All", help="Remove all status messages"):
                st.session_state.status_messages = {}
                st.rerun()
        with col3:
            st.caption("🔄 Auto-refreshes")
        
        # Display messages in a compact format
        for message_id, message_data in list(st.session_state.status_messages.items()):
            # Calculate time remaining
            elapsed = time.time() - message_data['timestamp']
            remaining = max(0, message_data['auto_hide_seconds'] - elapsed)
            
            # Auto-remove expired messages
            if remaining <= 0:
                del st.session_state.status_messages[message_id]
                continue
            
            # Create progress bar for auto-hide countdown
            progress = 1 - (remaining / message_data['auto_hide_seconds'])
            
            # Display message with type-specific styling
            message_type = message_data['type']
            message_text = message_data['message']
            
            if message_type == "info":
                st.info(f"ℹ️ {message_text}")
            elif message_type == "success":
                st.success(f"✅ {message_text}")
            elif message_type == "warning":
                st.warning(f"⚠️ {message_text}")
            elif message_type == "error":
                st.error(f"❌ {message_text}")
            
            # Show countdown progress bar
            st.progress(progress, text=f"Auto-hide in {remaining:.1f}s")
            
            # Individual dismiss button
            if st.button(f"❌ Dismiss", key=f"dismiss_{message_id}", help="Remove this message"):
                del st.session_state.status_messages[message_id]
                st.rerun()
            
            st.markdown("---")
        
        # Note: Messages will auto-hide based on their timestamps
        # The page will refresh naturally as users interact with it

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

def save_file_to_s3(file_path, s3_key):
    """Save a local file to S3"""
    try:
        s3_client = get_s3_client()
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"✅ Saved to S3: s3://{S3_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        print(f"⚠️ S3 access denied or unavailable: {e}")
        print(f"📁 Falling back to local storage only")
        return False

def convert_dates_to_strings(obj):
    """Recursively convert date objects to ISO format strings for JSON serialization"""
    if isinstance(obj, dict):
        return {key: convert_dates_to_strings(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates_to_strings(item) for item in obj]
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif hasattr(obj, 'isoformat'):  # Handle other date-like objects
        return obj.isoformat()
    else:
        return obj

def save_json_to_s3(data, s3_key):
    """Save JSON data directly to S3"""
    try:
        s3_client = get_s3_client()
        # Convert any date objects to strings before JSON serialization
        serializable_data = convert_dates_to_strings(data)
        json_data = json.dumps(serializable_data, indent=2)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data,
            ContentType='application/json'
        )
        # print(f"✅ Saved JSON to S3: s3://{S3_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        print(f"⚠️ S3 access denied or unavailable: {e}")
        print(f"📁 Falling back to local storage only")
        return False

def save_csv_to_s3(df, s3_key):
    """Save CSV data directly to S3"""
    try:
        s3_client = get_s3_client()
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=csv_data,
            ContentType='text/csv'
        )
        print(f"✅ Saved CSV to S3: s3://{S3_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        print(f"❌ Failed to save CSV to S3: {e}")
        return False

def load_json_from_s3(s3_key):
    """Load JSON data from S3"""
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        # print(f"✅ Loaded JSON from S3: s3://{S3_BUCKET}/{s3_key}")
        return data
    except Exception as e:
        print(f"⚠️ S3 access denied or unavailable: {e}")
        print(f"📁 Falling back to local storage only")
        return None

def file_exists_in_s3(s3_key):
    """Check if a file exists in S3"""
    try:
        s3_client = get_s3_client()
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except Exception as e:
        # Check if it's a 404 error (file not found) vs actual S3 access issue
        if "404" in str(e) or "Not Found" in str(e):
            # File doesn't exist, which is normal - don't print error
            return False
        else:
            # Actual S3 access issue
            print(f"⚠️ S3 access denied or unavailable: {e}")
            print(f"📁 Falling back to local storage only")
            return False

def is_s3_available():
    """Check if S3 is available and accessible"""
    try:
        s3_client = get_s3_client()
        # Try a simple operation to test access - use list_objects instead of head_bucket
        # as it requires fewer permissions
        s3_client.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=1)
        return True
    except Exception as e:
        print(f"⚠️ S3 not available: {e}")
        return False

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

# ===== DATA EXTRACTION FUNCTIONS =====
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
        # Try to detect from other patterns in the ad name
        if 'video' in ad_name_lower:
            return "Video"
        elif 'static' in ad_name_lower:
            return "Static"
        elif 'carousel' in ad_name_lower:
            return "Carousel"
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
                
                # Apply product merging logic from CORE_PRODUCTS if available
                try:
                    for product_group in CORE_PRODUCTS:
                        if isinstance(product_group, list) and extracted_product in product_group:
                            # Return the first product name as the label for merged products
                            return product_group[0]
                except NameError:
                    # CORE_PRODUCTS not defined, return the original product
                    pass
                
                # If not in any merged group, return the original product
                return extracted_product
        return "Unknown"
    except Exception as e:
        return "Unknown"

def extract_creator_from_ad_name(ad_name):
    """Extract creator from ad name (first try simple _b: to -, then TH# patterns, then + fallback)"""
    try:
        if '_b:' in ad_name:
            start_index = ad_name.find('_b:') + 3
            
            # First option: Find the first dash after '_b:'
            dash_index = ad_name.find('-', start_index)
            if dash_index > start_index:
                creator = ad_name[start_index:dash_index]
                if creator.replace('-', '').replace('_', '').isalpha():
                    return creator
            
            # Second option: Look for TH# patterns
            import re
            th_pattern = r'TH\d+'
            th_matches = re.findall(th_pattern, ad_name[start_index:])
            
            if th_matches:
                # Found TH# patterns, extract the first creator after the first TH#
                first_th = th_matches[0]
                th_pos = ad_name.find(first_th, start_index)
                if th_pos != -1:
                    next_dash = ad_name.find('-', th_pos + len(first_th))
                    if next_dash != -1:
                        second_dash = ad_name.find('-', next_dash + 1)
                        if second_dash != -1:
                            creator = ad_name[next_dash + 1:second_dash]
                            if creator.replace('-', '').replace('_', '').isalpha():
                                return creator
                        else:
                            creator = ad_name[next_dash + 1:]
                            if creator.replace('-', '').replace('_', '').isalpha():
                                return creator
            
            # Third option: If no dash found, try plus sign as fallback
            plus_index = ad_name.find('+', start_index)
            if plus_index > start_index:
                creator = ad_name[start_index:plus_index]
                if creator.replace('+', '').replace('_', '').isalpha():
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

def extract_ad_metadata(ad_name, campaign_name):
    """Extract all metadata from ad name and campaign name"""
    
    return {
        "campaign_type": extract_campaign_type_from_name(campaign_name),
        "product": extract_product_from_ad_name(ad_name),
        "ad_type": extract_ad_type_from_ad_name(ad_name, AD_TYPE_KEYWORD_VIDEO, AD_TYPE_KEYWORD_STATIC, AD_TYPE_KEYWORD_CAROUSEL),
        "creator": extract_creator_from_ad_name(ad_name),
        "agency": extract_agency_from_ad_name(ad_name)
    }

# ===== API FUNCTIONS =====
def fetch_meta_insights(date_from=None, date_to=None):
    """Fetch Meta insights data (raw data only)"""
        
    # Get dynamic parameters using provided values
    # Safety check for date_from and date_to - if not set, raise error
    if date_from is None or date_to is None:
        raise ValueError("date_from and date_to must be provided to fetch_meta_insights")
    
    meta_params = get_meta_params(date_from, date_to)
    
    ads_list = []
    next_url = META_ENDPOINT
    page_num = 1
    total_ads_retrieved = 0
    session = create_session_with_retries()

    while next_url:
        try:
            # print(f"🔄 Requesting insights page {page_num}...")
            
            if next_url == META_ENDPOINT:
                resp = session.get(next_url, params=meta_params, timeout=META_REQUEST_TIMEOUT)
            else:
                resp = session.get(next_url, timeout=META_REQUEST_TIMEOUT)
                
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
                time.sleep(META_RATE_LIMIT_DELAY)

        except Exception as e:
            print(f"❌ Unexpected error on page {page_num}: {e}")
            break

    
    # Save raw meta insights immediately after fetching
    if ads_list:
        # Safety check for date_from and date_to
        if date_from is None or date_to is None:
            raise ValueError("date_from and date_to must be provided before saving meta insights")
        
        date_from_formatted = format_date_for_filename(date_from)
        date_to_formatted = format_date_for_filename(date_to)
        
        # Save to S3
        s3_key = f"{ROOT_DIRECTORY}/raw/meta_insights/meta_insights_{date_from_formatted}-{date_to_formatted}.json"
        save_json_to_s3(ads_list, s3_key)
        
        # Save locally if enabled
        if DOWNLOAD_REPORTS_LOCALLY:
            meta_json_filename = f"{ROOT_DIRECTORY}/raw/meta_insights/meta_insights_{date_from_formatted}-{date_to_formatted}.json"
            os.makedirs(f"{ROOT_DIRECTORY}/raw/meta_insights", exist_ok=True)
            with open(meta_json_filename, 'w') as f:
                json.dump(ads_list, f, indent=2)
            print(f"💾 Saved raw Meta insights JSON: {meta_json_filename}")
        
    
    return ads_list

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

def create_northbeam_export(start_date, end_date):
    """Create a Northbeam export"""
    
    # Initial delay to avoid rate limits
    time.sleep(2)
    
    url = f"{NORTHBEAM_BASE_URL}/exports/data-export"
    
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
        "export_file_name": f"northbeam_{format_date_for_filename(start_date)}-{format_date_for_filename(end_date)}",
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
    
    # Retry logic with exponential backoff
    base_delay = NORTHBEAM_BASE_DELAY  # Start with 15 seconds (15s, 30s, 60s exponential backoff)
    
    for attempt in range(NORTHBEAM_MAX_RETRIES):
        try:
            response = requests.post(url, headers=get_northbeam_headers(), json=payload, timeout=60)
            
            if response.status_code == 201:
                export_id = response.json().get('id')
                print(f"✅ Export created successfully! ID: {export_id}")
                return export_id
            elif response.status_code == 429:
                delay = NORTHBEAM_BASE_DELAY * (2 ** attempt)  # Exponential backoff
                print(f"❌ Rate limit exceeded (429) on attempt {attempt + 1}: {response.text}")
                print(f"⏱️ Waiting {delay} seconds before retrying...")
                time.sleep(delay)
                continue
            elif response.status_code == 400:
                print(f"❌ Bad request (400): {response.text}")
                # Don't retry on 400 errors as they're likely configuration issues
                return None
            elif response.status_code >= 500:
                delay = NORTHBEAM_BASE_DELAY * (2 ** attempt)
                print(f"❌ Server error ({response.status_code}) on attempt {attempt + 1}: {response.text}")
                print(f"⏱️ Waiting {delay} seconds before retrying...")
                time.sleep(delay)
                continue
            else:
                print(f"❌ Export creation failed: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            delay = NORTHBEAM_BASE_DELAY * (2 ** attempt)
            print(f"⏰ Request timeout on attempt {attempt + 1}")
            print(f"⏱️ Waiting {delay} seconds before retrying...")
            time.sleep(delay)
            continue
        except requests.exceptions.RequestException as e:
            delay = NORTHBEAM_BASE_DELAY * (2 ** attempt)
            print(f"❌ Request error on attempt {attempt + 1}: {e}")
            print(f"⏱️ Waiting {delay} seconds before retrying...")
            time.sleep(delay)
            continue
    
    print(f"❌ Export creation failed after {NORTHBEAM_MAX_RETRIES} attempts")
    return None

def poll_northbeam_export_status(export_id, timeout_seconds=20, poll_interval=5):
    """Poll Northbeam for export status until ready with configurable timeout and interval"""
    
    url = f"{NORTHBEAM_BASE_URL}/exports/data-export/result/{export_id}"
    
    start_time = time.time()
    poll_count = 0
    consecutive_errors = 0
    max_consecutive_errors = 3
    
    while time.time() - start_time < timeout_seconds:
        poll_count += 1
        print(f"  🔄 Poll attempt {poll_count}...")
        
        try:
            response = requests.get(url, headers=get_northbeam_headers(), timeout=60)
            
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
                wait_time = min(NORTHBEAM_BASE_DELAY * consecutive_errors, NORTHBEAM_BASE_DELAY * 12)  # Exponential backoff up to 3 minutes
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

def download_export_data(export_id, start_date, end_date, timeout_seconds=20, poll_interval=5):
    """Download the export data with configurable timeout and S3 fallback"""
    
    # Try direct download first with specified timeout and interval
    direct_url = poll_northbeam_export_status(export_id, timeout_seconds=timeout_seconds, poll_interval=poll_interval)
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
                
                # Save CSV locally (only if local saving is enabled)
                if DOWNLOAD_REPORTS_LOCALLY:
                    csv_filename = f"{ROOT_DIRECTORY}/raw/northbeam/northbeam_{format_date_for_filename(start_date)}-{format_date_for_filename(end_date)}.csv"
                    os.makedirs(f"{ROOT_DIRECTORY}/raw/northbeam", exist_ok=True)
                    df.to_csv(csv_filename, index=False)
                    print(f"💾 Saved Northbeam CSV locally: {csv_filename}")
                
                return df
        except Exception as e:
            print(f"❌ Direct download failed: {e}")
    
    # Fallback to S3 - check for existing processed data
    print(f"⚠️ Export timed out, checking S3 for existing data...")
    s3_client = get_s3_client()
    try:
        # First check for processed data in our campaign-reporting directory
        processed_key = f"{ROOT_DIRECTORY}/raw/northbeam/northbeam_{format_date_for_filename(start_date)}-{format_date_for_filename(end_date)}.csv"
        if file_exists_in_s3(processed_key):
            print(f"📁 Found existing processed data in S3: {processed_key}")
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=processed_key)
            df = pd.read_csv(io.BytesIO(response['Body'].read()), dtype={
                'ad_id': str,
                'campaign_id': str,
                'adset_id': str
            })
            print(f"✅ Downloaded {len(df)} rows from existing S3 data")
            return df
        
        # Fallback to checking Northbeam's S3 bucket for raw exports
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=100)
        if 'Contents' in response:
            matching_files = []
            for obj in response['Contents']:
                key = obj['Key']
                if key.startswith(f"northbeam_{format_date_for_filename(start_date)}-{format_date_for_filename(end_date)}") and key.endswith('.csv'):
                    matching_files.append({
                        'key': key,
                        'last_modified': obj['LastModified']
                    })
            
            if matching_files:
                matching_files.sort(key=lambda x: x['last_modified'], reverse=True)
                actual_file_key = matching_files[0]['key']
                print(f"📁 Found raw export in S3: {actual_file_key}")
                
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=actual_file_key)
                df = pd.read_csv(io.BytesIO(response['Body'].read()), dtype={
                    'ad_id': str,
                    'campaign_id': str,
                    'adset_id': str
                })
                print(f"✅ Downloaded {len(df)} rows from raw S3 export")
                
                # Save CSV locally (only if local saving is enabled)
                if DOWNLOAD_REPORTS_LOCALLY:
                    csv_filename = f"{ROOT_DIRECTORY}/raw/northbeam/northbeam_{format_date_for_filename(start_date)}-{format_date_for_filename(end_date)}.csv"
                    os.makedirs(f"{ROOT_DIRECTORY}/raw/northbeam", exist_ok=True)
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

def fetch_northbeam_data(date_from=None, date_to=None):
    """Fetch Northbeam data for the specified date range with exponential backoff retry logic"""
    # Safety check for date_from and date_to - if not set, raise error
    if date_from is None or date_to is None:
        raise ValueError("date_from and date_to must be provided to fetch_northbeam_data")
    
    print(f"\n🔄 Fetching Northbeam data for {date_from} to {date_to}...")
    
    # Exponential backoff configuration: (poll_interval, timeout)
    backoff_config = [
        (NORTHBEAM_POLL_INTERVAL, 15),                    # 1st attempt: every 5s for 15s
        (NORTHBEAM_POLL_INTERVAL * 2, 30),               # 2nd attempt: every 10s for 30s  
        (NORTHBEAM_POLL_INTERVAL * 3, 45)                # 3rd attempt: every 15s for 45s
    ]
    
    # Try up to 3 times with exponential backoff
    for attempt in range(1, NORTHBEAM_MAX_RETRIES + 1):
        print(f"📊 Attempt {attempt}/{NORTHBEAM_MAX_RETRIES}")
        
        # Get backoff settings for this attempt
        poll_interval, timeout = backoff_config[attempt - 1]
        print(f"⏱️  Polling: every {poll_interval}s for {timeout}s total")
        
        # Create export
        export_id = create_northbeam_export(date_from, date_to)
        if not export_id:
            print(f"❌ Attempt {attempt}: Failed to create Northbeam export")
            if attempt < NORTHBEAM_MAX_RETRIES:
                print("🔄 Retrying immediately...")
                continue
            else:
                print("❌ All attempts failed - giving up")
                return None
        
        # Download data with exponential backoff timeout and interval
        df = download_export_data(export_id, date_from, date_to, timeout, poll_interval)
        if df is not None:
            # Success! Filter and save data
            filtered_df = filter_attribution_data(df, ACCOUNTING_MODE_FILTER, NORTHBEAM_PLATFORM)
            
            # Save filtered data
            date_from_formatted = format_date_for_filename(date_from)
            date_to_formatted = format_date_for_filename(date_to)
            
            # Save to S3
            raw_northbeam_directory = f"{ROOT_DIRECTORY}/raw/northbeam/"
            raw_northbeam_filename = f"northbeam_{date_from_formatted}-{date_to_formatted}.csv"
            csv_file_path = raw_northbeam_directory + raw_northbeam_filename
            save_csv_to_s3(filtered_df, csv_file_path)
            
            # Save locally if enabled
            if DOWNLOAD_REPORTS_LOCALLY:
                os.makedirs(raw_northbeam_directory, exist_ok=True)
                filtered_df.to_csv(csv_file_path, index=False)
                print(f"💾 Saved Northbeam CSV: {csv_file_path}")
            else:
                print(f"💾 Northbeam CSV saved to S3 only (local saving disabled)")
            
            print(f"✅ Attempt {attempt} succeeded!")
            return filtered_df
        
        # This attempt failed
        print(f"❌ Attempt {attempt}: Failed to download Northbeam data")
        
        if attempt < NORTHBEAM_MAX_RETRIES:
            print(f"⏳ Waiting {NORTHBEAM_RETRY_DELAY} seconds before retry...")
            time.sleep(NORTHBEAM_RETRY_DELAY)  # Sleep between retries
            print("🔄 Retrying...")
        else:
            print("❌ All attempts failed - giving up")
    
    return None

def fetch_all_data_concurrently(date_from=None, date_to=None, use_cached_files=True, use_meta=True, use_northbeam=True):
    """
    Fetch all required data concurrently and return comprehensive ad objects.
    Process: Check existing data → Fetch missing data concurrently → Return data immediately
    """
    
    print(f"🚀 COMPREHENSIVE AD METRICS EXTRACTION (CONCURRENT)")
    print("=" * 60)
    
    # Print configuration
    print(f"\n🎯 CONFIGURATION:")
    print(f"   - Date Range: {date_from} to {date_to}")
    print(f"   - Data Sources: Meta: {'Yes' if use_meta else 'No'}, Northbeam: {'Yes' if use_northbeam else 'No'}")
    
    print(f"\n🔍 STEP 1: CHECKING EXISTING RAW DATA FILES...")
    
    # Safety check for date_from and date_to
    if date_from is None or date_to is None:
        raise ValueError("date_from and date_to must be provided")
    
    date_from_formatted = format_date_for_filename(date_from)
    date_to_formatted = format_date_for_filename(date_to)
    
    # Check for existing raw data files
    meta_insights_file = f"{ROOT_DIRECTORY}/raw/meta_insights/meta_insights_{date_from_formatted}-{date_to_formatted}.json"
    northbeam_file = f"{ROOT_DIRECTORY}/raw/northbeam/northbeam_{date_from_formatted}-{date_to_formatted}.csv"
    
    existing_files = {
        'meta_insights': None,
        'northbeam_data': None
    }
    
    # Check which files exist (S3 first, then local fallback)
    s3_meta_key = f"{ROOT_DIRECTORY}/raw/meta_insights/meta_insights_{date_from_formatted}-{date_to_formatted}.json"
    s3_northbeam_key = f"{ROOT_DIRECTORY}/raw/northbeam/northbeam_{date_from_formatted}-{date_to_formatted}.csv"
    
    # Check for Meta insights file (S3 first, then local fallback)
    if file_exists_in_s3(s3_meta_key):
        try:
            existing_files['meta_insights'] = load_json_from_s3(s3_meta_key)
            if existing_files['meta_insights']:
                print(f"✅ Found existing Meta insights in S3: {len(existing_files['meta_insights'])} ads")
        except Exception as e:
            print(f"⚠️ Error loading existing Meta insights from S3: {e}")
    else:
        print(f"❌ Meta insights not found in S3")
    
    # Fallback to local Meta insights file (only if local saving is enabled)
    if DOWNLOAD_REPORTS_LOCALLY and existing_files['meta_insights'] is None:
        if os.path.exists(meta_insights_file):
            try:
                with open(meta_insights_file, 'r') as f:
                    existing_files['meta_insights'] = json.load(f)
                print(f"✅ Found existing Meta insights locally: {len(existing_files['meta_insights'])} ads")
            except Exception as e:
                print(f"⚠️ Error loading existing Meta insights: {e}")
        else:
            print(f"📁 Meta insights not found locally")
    
    # Check for Northbeam data file (S3 first, then local fallback)
    if file_exists_in_s3(s3_northbeam_key):
        try:
            # Download CSV from S3 to temporary file
            s3_client = get_s3_client()
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_northbeam_key)
            csv_content = response['Body'].read().decode('utf-8')
            # Create a StringIO object to read CSV from memory
            csv_buffer = io.StringIO(csv_content)
            existing_files['northbeam_data'] = pd.read_csv(csv_buffer, dtype={
                'ad_id': str,
                'campaign_id': str,
                'adset_id': str
            })
            print(f"✅ Found existing Northbeam data in S3: {len(existing_files['northbeam_data'])} rows")
        except Exception as e:
            print(f"⚠️ Error loading existing Northbeam data from S3: {e}")
    else:
        print(f"❌ Northbeam data not found in S3")
    
    # Fallback to local Northbeam data file (only if local saving is enabled)
    if DOWNLOAD_REPORTS_LOCALLY and existing_files['northbeam_data'] is None:
        print(f"🔍 Checking for local Northbeam data: {northbeam_file}")
        if os.path.exists(northbeam_file):
            try:
                # Read CSV with specific dtype to ensure ID columns are treated as strings
                existing_files['northbeam_data'] = pd.read_csv(northbeam_file, dtype={
                    'ad_id': str,
                    'campaign_id': str,
                    'adset_id': str
                })
                print(f"✅ Found existing Northbeam data locally: {len(existing_files['northbeam_data'])} rows")
            except Exception as e:
                print(f"⚠️ Error loading existing Northbeam data: {e}")
        else:
            print(f"📁 Northbeam data not found locally")
    
    # Initialize with existing data
    meta_insights = existing_files['meta_insights']
    northbeam_df = existing_files['northbeam_data']
    
    print(f"\n⚡ STEP 2: FETCHING MISSING RAW DATA CONCURRENTLY...")
    
    # Check if we should use cached files only
    if use_cached_files:
        # Check if we have the required files for this date range based on selected sources
        missing_files = []
        if use_meta and meta_insights is None:
            missing_files.append("Meta insights")
        if use_northbeam and northbeam_df is None:
            missing_files.append("Northbeam data")
        
        if not missing_files:
            print("✅ All required cached files found - using existing data only")
            return meta_insights, northbeam_df
    
    # Prepare concurrent fetching
    import concurrent.futures
    import threading
    
    # Shared variables for concurrent access
    meta_insights_result = {'data': meta_insights, 'error': None}
    northbeam_result = {'data': northbeam_df, 'error': None}
    
    def fetch_meta_concurrent():
        """Fetch Meta insights in a separate thread"""
        try:
            if meta_insights is None:
                print("📊 Fetching Meta insights concurrently...")
                result = fetch_meta_insights(date_from, date_to)
                meta_insights_result['data'] = result
                print(f"✅ Meta insights fetched concurrently: {len(result) if result else 0} ads")
            else:
                print("📊 Meta insights already available")
        except Exception as e:
            print(f"❌ Error fetching Meta insights concurrently: {e}")
            meta_insights_result['error'] = e
    
    def fetch_northbeam_concurrent():
        """Fetch Northbeam data in a separate thread"""
        try:
            if northbeam_df is None:
                print("📊 Fetching Northbeam data concurrently...")
                result = fetch_northbeam_data(date_from, date_to)
                northbeam_result['data'] = result
                print(f"✅ Northbeam data fetched concurrently: {len(result) if result is not None else 0} rows")
            else:
                print("📊 Northbeam data already available")
        except Exception as e:
            print(f"❌ Error fetching Northbeam data concurrently: {e}")
            northbeam_result['error'] = e
    
    # Start concurrent fetching only for selected sources
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        if use_meta and meta_insights is None:
            futures.append(executor.submit(fetch_meta_concurrent))
        if use_northbeam and northbeam_df is None:
            futures.append(executor.submit(fetch_northbeam_concurrent))
        
        # Wait for all futures to complete
        if futures:
            concurrent.futures.wait(futures)
    
    # Check for errors only for selected sources
    if use_meta and meta_insights_result['error']:
        print(f"❌ Meta insights fetch failed: {meta_insights_result['error']}")
        if not use_northbeam:
            return None, None  # Only return None if Meta was the only source
    
    if use_northbeam and northbeam_result['error']:
        print(f"❌ Northbeam data fetch failed: {northbeam_result['error']}")
        if not use_meta:
            return None, None  # Only return None if Northbeam was the only source
    
    print(f"\n📊 FINAL DATA SUMMARY:")
    if use_meta:
        print(f"   - Meta insights: {len(meta_insights_result['data']) if meta_insights_result['data'] else 0} ads")
    if use_northbeam:
        print(f"   - Northbeam data: {len(northbeam_result['data']) if northbeam_result['data'] is not None else 0} rows")
    
    return meta_insights_result['data'], northbeam_result['data']

def fetch_all_data_sequentially(date_from=None, date_to=None, use_cached_files=True):
    """
    Fetch all required data sequentially and return comprehensive ad objects.
    Process: Always fetch fresh data → Save to S3 → Merge into comprehensive objects → Save to S3
    """
    
    print(f"🚀 COMPREHENSIVE AD METRICS EXTRACTION")
    print("=" * 60)
    
    # Print configuration
    print(f"\n🎯 CONFIGURATION:")
    print(f"   - Date Range: {date_from} to {date_to}")
    # Safety check for USE_NORTHBEAM_DATA
    use_northbeam = getattr(globals(), 'USE_NORTHBEAM_DATA', True)
    print(f"   - Data Source: {'Northbeam' if use_northbeam else 'Meta'}")
    
    print(f"\n🔍 STEP 1: CHECKING EXISTING RAW DATA FILES...")
    
    # Safety check for date_from and date_to
    if date_from is None or date_to is None:
        raise ValueError("date_from and date_to must be provided")
    
    date_from_formatted = format_date_for_filename(date_from)
    date_to_formatted = format_date_for_filename(date_to)
    
    # Check for existing raw data files
    meta_insights_file = f"{ROOT_DIRECTORY}/raw/meta_insights/meta_insights_{date_from_formatted}-{date_to_formatted}.json"
    northbeam_file = f"{ROOT_DIRECTORY}/raw/northbeam/northbeam_{date_from_formatted}-{date_to_formatted}.csv"
    
    existing_files = {
        'meta_insights': None,
        'northbeam_data': None
    }
    
    # Check which files exist (S3 first, then local fallback)
    s3_meta_key = f"{ROOT_DIRECTORY}/raw/meta_insights/meta_insights_{date_from_formatted}-{date_to_formatted}.json"
    s3_northbeam_key = f"{ROOT_DIRECTORY}/raw/northbeam/northbeam_{date_from_formatted}-{date_to_formatted}.csv"
    
    # Check for Meta insights file (S3 first, then local fallback)
    # print(f"🔍 Checking for Meta insights: {s3_meta_key}")
    if file_exists_in_s3(s3_meta_key):
        try:
            existing_files['meta_insights'] = load_json_from_s3(s3_meta_key)
            if existing_files['meta_insights']:
                print(f"✅ Found existing Meta insights in S3: {len(existing_files['meta_insights'])} ads")
        except Exception as e:
            print(f"⚠️ Error loading existing Meta insights from S3: {e}")
    else:
        print(f"❌ Meta insights not found in S3")
    
    # Fallback to local Meta insights file (only if local saving is enabled)
    if DOWNLOAD_REPORTS_LOCALLY and existing_files['meta_insights'] is None:
        # print(f"🔍 Checking for local Meta insights: {meta_insights_file}")
        if os.path.exists(meta_insights_file):
            try:
                with open(meta_insights_file, 'r') as f:
                    existing_files['meta_insights'] = json.load(f)
                print(f"✅ Found existing Meta insights locally: {len(existing_files['meta_insights'])} ads")
            except Exception as e:
                print(f"⚠️ Error loading existing Meta insights: {e}")
        else:
            print(f"📁 Meta insights not found locally")
    
    # Check for Northbeam data file (S3 first, then local fallback)
    # print(f"🔍 Checking for Northbeam data: {s3_northbeam_key}")
    if file_exists_in_s3(s3_northbeam_key):
        try:
            # Download CSV from S3 to temporary file
            s3_client = get_s3_client()
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_northbeam_key)
            csv_content = response['Body'].read().decode('utf-8')
            # Create a StringIO object to read CSV from memory
            csv_buffer = io.StringIO(csv_content)
            existing_files['northbeam_data'] = pd.read_csv(csv_buffer, dtype={
                'ad_id': str,
                'campaign_id': str,
                'adset_id': str
            })
            print(f"✅ Found existing Northbeam data in S3: {len(existing_files['northbeam_data'])} rows")
        except Exception as e:
            print(f"⚠️ Error loading existing Northbeam data from S3: {e}")
    else:
        print(f"❌ Northbeam data not found in S3")
    
    # Fallback to local Northbeam data file (only if local saving is enabled)
    if DOWNLOAD_REPORTS_LOCALLY and existing_files['northbeam_data'] is None:
        print(f"🔍 Checking for local Northbeam data: {northbeam_file}")
        if os.path.exists(northbeam_file):
            try:
                # Read CSV with specific dtype to ensure ID columns are treated as strings
                existing_files['northbeam_data'] = pd.read_csv(northbeam_file, dtype={
                    'ad_id': str,
                    'campaign_id': str,
                    'adset_id': str
                })
                print(f"✅ Found existing Northbeam data locally: {len(existing_files['northbeam_data'])} rows")
            except Exception as e:
                print(f"⚠️ Error loading existing Northbeam data: {e}")
        else:
            print(f"📁 Northbeam data not found locally")
    
    # Initialize with existing data
    meta_insights = existing_files['meta_insights']
    northbeam_df = existing_files['northbeam_data']
    
    print(f"\n⚡ STEP 2: FETCHING MISSING RAW DATA SEQUENTIALLY...")
    
    # Check if we should use cached files only
    if use_cached_files:
        
        # Check if we have the required files for this date range
        missing_files = []
        if meta_insights is None:
            missing_files.append("Meta insights")

        if northbeam_df is None:
            missing_files.append("Northbeam data")

        if not missing_files:
            print("✅ All required cached files found - using existing data only")
            return meta_insights, northbeam_df
        
    # Fetch Meta insights first (if missing)
    if meta_insights is None:
        print("\n📊 STEP 2a: Fetching Meta insights...")
        try:
            meta_insights = fetch_meta_insights(date_from, date_to)
            print(f"✅ Meta insights fetched: {len(meta_insights) if meta_insights else 0} ads")
        except Exception as e:
            print(f"❌ Error fetching Meta insights: {e}")
            return None, None
    else:
        print("📊 Meta insights already available")
    
    # Fetch Northbeam data second (if missing)
    if northbeam_df is None:
        print("\n📊 STEP 2b: Fetching Northbeam data...")
        try:
            northbeam_df = fetch_northbeam_data(date_from, date_to)
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

# ===== DATA PROCESSING FUNCTIONS =====
def merge_data(northbeam_data, meta_data, date_from=None, date_to=None):
    """Merge Northbeam and Meta data into comprehensive ad objects"""
    
    # Handle empty or None Northbeam data gracefully
    if northbeam_data is None or (isinstance(northbeam_data, pd.DataFrame) and len(northbeam_data) == 0):
        print("⚠️ No Northbeam data available - creating Meta-only comprehensive ads")
        northbeam_list = []
    else:
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
                'reporting_start_date': date_from,
                'reporting_end_date': date_to
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
                    'spend': safe_float_conversion(northbeam_item.get('spend', 0)),
                    'impressions': safe_float_conversion(northbeam_item.get('impressions', 0)),
                    'meta_link_clicks': safe_float_conversion(northbeam_item.get('meta_link_clicks', 0)),
                    'meta_3s_video_views': safe_float_conversion(northbeam_item.get('meta_3s_video_views', 0)),
                    'attributed_rev': safe_float_conversion(northbeam_item.get('attributed_rev', 0)),
                    'transactions': safe_float_conversion(northbeam_item.get('transactions', 0)),
                    'roas': safe_float_conversion(northbeam_item.get('roas', 0))
                },
                'meta': {
                    'spend': safe_float_conversion(meta_item.get('spend', 0)),
                    'impressions': safe_float_conversion(meta_item.get('impressions', 0)),
                    'link_clicks': safe_float_conversion(meta_item.get('link_clicks', 0)),
                    'purchase_value': safe_float_conversion(meta_item.get('purchase_value', 0)),
                    'purchase_count': safe_float_conversion(meta_item.get('purchase_count', 0)),
                    'purchase_roas': safe_float_conversion(meta_item.get('purchase_roas', 0)),
                    'video_views_3s': safe_float_conversion(meta_item.get('video_views_3s', 0))
                }
            }
        }
        
        comprehensive_ads.append(ad_object)
    
    print("✅ Merged Northbeam and Meta data")

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

def merge_ads_with_same_name(ad_objects, merge_by_campaign_type=True):
    """
    Merge ads based on specified criteria and aggregate their metrics
    
    Args:
        ad_objects: List of ad objects to merge
        merge_by_campaign_type: If True, merge by campaign type AND ad name. 
                               If False, merge by ad name only (regardless of campaign type)
    
    Returns:
        List of merged ad objects
    """
    
    merged_ads = {}
    ad_name_counts = {}  # Track how many ads have each name
    
    for i, ad in enumerate(ad_objects):
        ad_name = ad['ad_ids']['ad_name']
        ad_id = ad['ad_ids']['ad_id']
        
        # Create merge key based on requirements
        if merge_by_campaign_type:
            campaign_type = ad['metadata'].get('campaign_type', 'Unknown')
            merge_key = f"{campaign_type}_{ad_name}"
        else:
            merge_key = ad_name
        
        # Track ad name occurrences
        if ad_name not in ad_name_counts:
            ad_name_counts[ad_name] = []
        ad_name_counts[ad_name].append(ad_id)
        
        
        if merge_key not in merged_ads:
            # Create a new merged ad object
            merged_ads[merge_key] = {
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
                # Track all campaign types for merged ads
                'campaign_types': [ad['metadata'].get('campaign_type', 'Unknown')],
                # Track merge count
                'merged_count': 1,
                'metrics': {
                    'meta': {
                        'spend': float(ad['metrics']['meta']['spend']),
                        'impressions': float(ad['metrics']['meta']['impressions']),
                        'link_clicks': float(ad['metrics']['meta']['link_clicks']),
                        'purchase_value': float(ad['metrics']['meta']['purchase_value']),
                        'purchase_count': float(ad['metrics']['meta']['purchase_count']),
                        'purchase_roas': float(ad['metrics']['meta'].get('purchase_roas', 0.0)),
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
            merged_ads[merge_key]['ad_ids']['all_ad_ids'].append(ad_id)
            
            # Increment merge count
            merged_ads[merge_key]['merged_count'] += 1
            
            # Track campaign types for merged ads
            current_campaign_type = ad['metadata'].get('campaign_type', 'Unknown')
            if current_campaign_type not in merged_ads[merge_key]['campaign_types']:
                merged_ads[merge_key]['campaign_types'].append(current_campaign_type)
            
            # Aggregate metrics
            # Meta metrics
            merged_ads[merge_key]['metrics']['meta']['spend'] += float(ad['metrics']['meta']['spend'])
            merged_ads[merge_key]['metrics']['meta']['impressions'] += float(ad['metrics']['meta']['impressions'])
            merged_ads[merge_key]['metrics']['meta']['link_clicks'] += float(ad['metrics']['meta']['link_clicks'])
            merged_ads[merge_key]['metrics']['meta']['purchase_value'] += float(ad['metrics']['meta']['purchase_value'])
            merged_ads[merge_key]['metrics']['meta']['purchase_count'] += float(ad['metrics']['meta']['purchase_count'])
            merged_ads[merge_key]['metrics']['meta']['video_views_3s'] += float(ad['metrics']['meta']['video_views_3s'])
            
            # Northbeam metrics
            merged_ads[merge_key]['metrics']['northbeam']['spend'] += safe_float_conversion(ad['metrics']['northbeam']['spend'])
            merged_ads[merge_key]['metrics']['northbeam']['impressions'] += safe_float_conversion(ad['metrics']['northbeam']['impressions'])
            merged_ads[merge_key]['metrics']['northbeam']['meta_link_clicks'] += safe_float_conversion(ad['metrics']['northbeam']['meta_link_clicks'])
            merged_ads[merge_key]['metrics']['northbeam']['attributed_rev'] += safe_float_conversion(ad['metrics']['northbeam']['attributed_rev'])
            merged_ads[merge_key]['metrics']['northbeam']['transactions'] += safe_float_conversion(ad['metrics']['northbeam']['transactions'])
            merged_ads[merge_key]['metrics']['northbeam']['meta_3s_video_views'] += safe_float_conversion(ad['metrics']['northbeam']['meta_3s_video_views'])
    
    # Calculate aggregated ROAS for both data sources
    for merge_key, merged_ad in merged_ads.items():
        # Calculate Northbeam ROAS
        if merged_ad['metrics']['northbeam']['spend'] > 0:
            merged_ad['metrics']['northbeam']['roas'] = merged_ad['metrics']['northbeam']['attributed_rev'] / merged_ad['metrics']['northbeam']['spend']
        
        # Calculate Meta ROAS
        if merged_ad['metrics']['meta']['spend'] > 0:
            merged_ad['metrics']['meta']['purchase_roas'] = merged_ad['metrics']['meta']['purchase_value'] / merged_ad['metrics']['meta']['spend']
        
        # Update campaign type display for merged ads
        if 'campaign_types' in merged_ad and len(merged_ad['campaign_types']) > 1:
            # Sort campaign types alphabetically for consistent display
            sorted_campaign_types = sorted(merged_ad['campaign_types'])
            merged_ad['metadata']['campaign_type'] = ', '.join(sorted_campaign_types)
    
    # Print merge statistics with appropriate message
    if merge_by_campaign_type:
        print(f"✅ Merged {len(ad_objects) - len(merged_ads)} ads with same name and campaign type ({len(ad_objects)} ➡️ {len(merged_ads)})")
    else:
        print(f"✅ Merged {len(ad_objects) - len(merged_ads)} ads with same name regardless of campaign ({len(ad_objects)} ➡️ {len(merged_ads)})")
    
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

# Google API imports
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# MetaAdCreativesProcessor class has been replaced with preview URLs functionality




# ===== GOOGLE API CONFIGURATION =====
GOOGLE_CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials/creative-audit-tool-aaa3858bf2cb.json')
SCOPES = ['https://www.googleapis.com/auth/drive']

GENERATE_GOOGLE_DOC = True  # Set to True to generate Google Doc from web display

# Default configuration values - these will be overridden by frontend inputs
DEFAULT_DATE_FROM = date.today() - timedelta(days=7)  # 7 days before today
DEFAULT_DATE_TO = date.today() - timedelta(days=1)    # yesterday
DEFAULT_TOP_N = 5
DEFAULT_CORE_PRODUCTS = [["LLEM", "Mascara"], ["BEB"], ["IWEL"], ["BrowGel"], ["LipTint"]]

DEFAULT_MERGE_ADS_WITH_SAME_NAME = True
DEFAULT_USE_NORTHBEAM_DATA = True

# Page configuration
st.set_page_config(
    page_title="Campaign Reporting Dashboard",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .metric-value {
        font-size: 1.5rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #666;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #666;
    }
</style>
""", unsafe_allow_html=True)

def format_currency(value):
    """Format value as currency"""
    return f"${value:,.2f}"

def format_percentage(value):
    """Format value as percentage"""
    return f"{value:.2f}%"

def format_roas(value):
    """Format ROAS value to 6 decimal places"""
    return f"{value:.6f}"

def create_metric_card(label, value, format_func=str, subtitle=None):
    """Create a metric card with label and formatted value"""
    if subtitle:
        title_html = f'{label} <span style="font-size: 0.8rem; color: #888; font-style: italic;">{subtitle}</span>'
    else:
        title_html = label
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{title_html}</div>
        <div class="metric-value">{format_func(value)}</div>
    </div>
    """, unsafe_allow_html=True)

def get_target_roas(campaign_type):
    """Get target ROAS for a campaign type from CAMPAIGN_TYPES"""
    for campaign_info in CAMPAIGN_TYPES:
        if isinstance(campaign_info, list) and len(campaign_info) >= 2:
            if campaign_info[0] == campaign_type:
                return campaign_info[1]
    return None

# ===== GOOGLE DRIVE INTEGRATION =====

def get_google_api_service():
    """Get Google Drive API service"""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)
    
    docs_service = build('docs', 'v1', credentials=credentials)
    drive_service = build('drive', 'v3', credentials=credentials)
    return docs_service, drive_service

def upload_to_google_drive(file_path, file_name):
    """Upload file to Google Drive"""
    _, drive_service = get_google_api_service()
    try:
        file_metadata = {'name': file_name}
        media = MediaFileUpload(file_path, resumable=True)
        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        file_id = file.get('id')
        print(f"✅ File uploaded to Google Drive: {file_name} (ID: {file_id})")
        return file_id
    except HttpError as error:
        print(f"❌ Upload error: {error}")
        return None

def convert_to_google_docs(file_id, doc_title):
    """Convert uploaded file to Google Docs"""
    docs_service, drive_service = get_google_api_service()
    try:
        copied_file = drive_service.files().copy(
            fileId=file_id,
            body={'mimeType': 'application/vnd.google-apps.document', 'name': doc_title}
        ).execute()
        doc_id = copied_file.get('id')
        print(f"✅ Converted to Google Doc: {doc_title} (ID: {doc_id})")
        return doc_id
    except HttpError as error:
        print(f"❌ Convert error: {error}")
        return None

def make_document_shareable(doc_id):
    """Make Google Doc publicly accessible"""
    _, drive_service = get_google_api_service()
    try:
        drive_service.permissions().create(
            fileId=doc_id,
            body={'type': 'anyone', 'role': 'writer'}
        ).execute()
        file = drive_service.files().get(fileId=doc_id, fields='webViewLink').execute()
        shareable_link = file.get('webViewLink')
        print(f"✅ Document made shareable: {doc_id}")
        return shareable_link
    except HttpError as error:
        print(f"❌ Permission error: {error}")
        return None

def export_report_to_google_doc(report_file_path, doc_title="Thrive Causemetics Campaign Analysis"):
    """Export markdown report to Google Drive as shareable Google Doc"""
    print("📁 Using existing report file for Google Drive upload.")
    
    file_id = upload_to_google_drive(report_file_path, doc_title + ".md")
    if not file_id:
        print("❌ Upload to Google Drive failed.")
        return None
    
    doc_id = convert_to_google_docs(file_id, doc_title)
    if not doc_id:
        print("❌ Conversion to Google Doc failed.")
        return None
    
    shareable_link = make_document_shareable(doc_id)
    if shareable_link:
        print(f"\n✅ Shareable Google Doc Link:\n{shareable_link}")
    else:
        print("❌ Failed to get shareable link.")
    
    return shareable_link

def generate_markdown_report(ad_objects, date_from, date_to, top_n, core_products_input, merge_ads, use_northbeam):
    """Generate comprehensive markdown report from ad objects"""
    

    
    # Get data source for display
    data_source_display = "Northbeam" if use_northbeam else "Meta"
    
    # Calculate overall metrics
    data_source = 'northbeam' if use_northbeam else 'meta'
    overall_metrics = calculate_campaign_metrics(ad_objects, data_source=data_source)
    
    # Generate report header
    report = f"""# Thrive Causemetics Campaign Analysis Report

**Date Range:** {date_from} to {date_to}  
**Account:** Thrive Causemetics  
**Data Source:** {data_source_display}  
**Total Ads Analyzed:** {overall_metrics['total_ads']:,}  
**Merge Ads with Same Name:** {'Yes' if merge_ads else 'No'}

## 📊 Executive Summary

### Overall Performance Metrics

| Metric | Value |
|--------|-------|
| Total Ads | {overall_metrics['total_ads']:,} |
| Total Spend | ${overall_metrics['total_spend']:,.2f} |
| Total Revenue | ${overall_metrics['total_revenue']:,.2f} |
| ROAS | {overall_metrics['roas']:.6f}x |
| CTR | {overall_metrics['ctr']:.2f}% |
| CPM | ${overall_metrics['cpm']:.2f} |
| Thumbstop | {overall_metrics['thumbstop']:.1f}% |
| AOV | ${overall_metrics['aov']:.2f} |

### Top {top_n} Ads by Spend

| Rank | Ad Name | Campaign | Product | Ad Type | Creator | Agency | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|---------|----------|---------|---------|---------|--------|-------|------|-----|-----|-----------|-----|
"""
    
    # Get top N ads
    ads_data = []
    for ad in ad_objects:
        ads_data.append({
            'ad_name': ad['ad_ids']['ad_name'],
            'campaign': ad['metadata'].get('campaign_type', 'Unknown'),
            'product': ad['metadata'].get('product', 'Unknown'),
            'ad_type': ad['metadata'].get('ad_type', 'Unknown'),
            'creator': ad['metadata'].get('creator', 'Unknown'),
            'agency': ad['metadata'].get('agency', 'Unknown'),
            'spend': get_metric_value(ad, 'spend', data_source),
            'roas': get_metric_value(ad, 'roas', data_source),
            'ctr': (get_metric_value(ad, 'meta_link_clicks', data_source) / get_metric_value(ad, 'impressions', data_source) * 100) if get_metric_value(ad, 'impressions', data_source) > 0 else 0,
            'cpm': (get_metric_value(ad, 'spend', data_source) / get_metric_value(ad, 'impressions', data_source) * 1000) if get_metric_value(ad, 'impressions', data_source) > 0 else 0,
            'thumbstop': (get_metric_value(ad, 'meta_3s_video_views', data_source) / get_metric_value(ad, 'impressions', data_source) * 100) if get_metric_value(ad, 'impressions', data_source) > 0 else 0,
            'aov': get_metric_value(ad, 'attributed_rev', data_source) / get_metric_value(ad, 'transactions', data_source) if get_metric_value(ad, 'transactions', data_source) > 0 else 0
        })
    
    # Sort by spend and get top N
    ads_df = pd.DataFrame(ads_data)
    ads_df = ads_df.sort_values('spend', ascending=False)
    top_ads = ads_df.head(top_n)
    
    for i, (_, ad) in enumerate(top_ads.iterrows(), 1):
        # report += f"| {i} | [{ad['ad_name']}](www.google.com) | {ad['campaign']} | {ad['product']} | {ad['ad_type']} | {ad['creator']} | {ad['agency']} | ${ad['spend']:,.2f} | {ad['roas']:.2f}x | {ad['ctr']:.2f}% | ${ad['cpm']:.2f} | {ad['thumbstop']:.1f}% | ${ad['aov']:.2f} |\n"
        report += f"| {i} | {ad['ad_name']} | {ad['campaign']} | {ad['product']} | {ad['ad_type']} | {ad['creator']} | {ad['agency']} | ${ad['spend']:,.2f} | {ad['roas']:.6f}x | {ad['ctr']:.2f}% | ${ad['cpm']:.2f} | {ad['thumbstop']:.1f}% | ${ad['aov']:.2f} |\n"
    
    # Top N Products
    report += f"""

### Top {top_n} Products by Spend

| Rank | Product | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|---------|-----------|-------|------|-----|-----|-----------|-----|
"""
    
    products_df = calculate_aggregated_metrics(ad_objects, 'product', 10000)
    top_products = products_df.head(top_n)
    
    for i, (product, row) in enumerate(top_products.iterrows(), 1):
        report += f"| {i} | {product} | {int(row['Ads Count'])} | ${row['Spend']:,.2f} | {row['ROAS']:.6f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
    
    # Top N Creators
    report += f"""

### Top {top_n} Creators by Spend

| Rank | Creator | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|---------|-----------|-------|------|-----|-----|-----------|-----|
"""
    
    creators_df = calculate_aggregated_metrics(ad_objects, 'creator', 10000)
    top_creators = creators_df.head(top_n)
    
    for i, (creator, row) in enumerate(top_creators.iterrows(), 1):
        report += f"| {i} | {creator} | {int(row['Ads Count'])} | ${row['Spend']:,.2f} | {row['ROAS']:.6f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
    
    # Top N Agencies
    report += f"""

### Top {top_n} Agencies by Spend

| Rank | Agency | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|--------|-----------|-------|------|-----|-----|-----------|-----|
"""
    
    agencies_df = calculate_aggregated_metrics(ad_objects, 'agency', 10)
    top_agencies = agencies_df.head(top_n)
    
    for i, (agency, row) in enumerate(top_agencies.iterrows(), 1):
        report += f"| {i} | {agency} | {int(row['Ads Count'])} | ${row['Spend']:,.2f} | {row['ROAS']:.6f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
    
    # Campaign Analysis
    report += "\n## 📈 Campaign Analysis\n"
    
    # Use hard-coded campaign types from CAMPAIGN_TYPES
    campaigns = []
    for campaign_type in CAMPAIGN_TYPES:
        if isinstance(campaign_type, list) and len(campaign_type) > 0:
            campaigns.append(campaign_type[0])  # Use the first element (campaign name)
        elif isinstance(campaign_type, str):
            campaigns.append(campaign_type)
    
    # Filter out any empty or invalid campaign names
    campaigns = [c for c in campaigns if c and c.strip()]
    
    # Get available products from configuration
    available_products = []
    if core_products_input:
        core_products_list = []
        for line in core_products_input.strip().split('\n'):
            if line.strip():
                products = [p.strip() for p in line.split(',') if p.strip()]
                if products:
                    core_products_list.append(products)
        
        # Use the first product in each group as the display name
        for product_group in core_products_list:
            if len(product_group) > 0:
                available_products.append(product_group[0])
    else:
        # Fallback to DEFAULT_CORE_PRODUCTS
        for product_group in DEFAULT_CORE_PRODUCTS:
            if len(product_group) > 0:
                available_products.append(product_group[0])
    
    # Process each campaign
    for campaign in campaigns:
        # Filter ads by campaign
        campaign_ads = [ad for ad in ad_objects if ad['metadata'].get('campaign_type') == campaign]
        
        if not campaign_ads:
            continue
        
        # Calculate campaign metrics
        campaign_metrics = calculate_campaign_metrics(campaign_ads, data_source=data_source)
        
        report += f"""

### {campaign} Campaign

**Summary:**
- Total Ads: {campaign_metrics['total_ads']:,}
- Total Spend: ${campaign_metrics['total_spend']:,.2f}
- Total Revenue: ${campaign_metrics['total_revenue']:,.2f}
- ROAS: {campaign_metrics['roas']:.6f}x
- CTR: {campaign_metrics['ctr']:.2f}%
- CPM: ${campaign_metrics['cpm']:.2f}
- Thumbstop: {campaign_metrics['thumbstop']:.1f}%
- AOV: ${campaign_metrics['aov']:.2f}

#### Product Breakdown
"""
        
        # Process each core product for this campaign
        for product in available_products:
            # Find the product group that contains this product
            product_group = None
            if core_products_input:
                core_products_list = []
                for line in core_products_input.strip().split('\n'):
                    if line.strip():
                        products = [p.strip() for p in line.split(',') if p.strip()]
                        if products:
                            core_products_list.append(products)
                
                for group in core_products_list:
                    if product in group:
                        product_group = group
                        break
            else:
                # Use DEFAULT_CORE_PRODUCTS
                for group in DEFAULT_CORE_PRODUCTS:
                    if product in group:
                        product_group = group
                        break
            
            if not product_group:
                continue
            
            # Filter ads by product group
            product_ads = []
            for ad in campaign_ads:
                ad_product = ad['metadata'].get('product', 'Unknown')
                if ad_product in product_group:
                    product_ads.append(ad)
            
            if not product_ads:
                continue
            
            # Calculate product metrics
            product_metrics = calculate_campaign_metrics(product_ads, data_source=data_source)
            
            report += f"""

##### {product} ({campaign})

**Summary:**
- Total Ads: {product_metrics['total_ads']:,}
- Total Spend: ${product_metrics['total_spend']:,.2f}
- Total Revenue: ${product_metrics['total_revenue']:,.2f}
- ROAS: {product_metrics['roas']:.6f}x
- CTR: {product_metrics['ctr']:.2f}%
- CPM: ${product_metrics['cpm']:.2f}
- Thumbstop: {product_metrics['thumbstop']:.1f}%
- AOV: ${product_metrics['aov']:.2f}

**Top Ads by Spend:**

| Rank | Ad Name | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|---------|-------|------|-----|-----|-----------|-----|
"""
            
            # Get top ads for this product
            product_ads_data = []
            for ad in product_ads:
                product_ads_data.append({
                    'ad_name': ad['ad_ids']['ad_name'],
                    'spend': get_metric_value(ad, 'spend', data_source),
                    'roas': get_metric_value(ad, 'roas', data_source),
                    'ctr': (get_metric_value(ad, 'meta_link_clicks', data_source) / get_metric_value(ad, 'impressions', data_source) * 100) if get_metric_value(ad, 'impressions', data_source) > 0 else 0,
                    'cpm': (get_metric_value(ad, 'spend', data_source) / get_metric_value(ad, 'impressions', data_source) * 1000) if get_metric_value(ad, 'impressions', data_source) > 0 else 0,
                    'thumbstop': (get_metric_value(ad, 'meta_3s_video_views', data_source) / get_metric_value(ad, 'impressions', data_source) * 100) if get_metric_value(ad, 'impressions', data_source) > 0 else 0,
                    'aov': get_metric_value(ad, 'attributed_rev', data_source) / get_metric_value(ad, 'transactions', data_source) if get_metric_value(ad, 'transactions', data_source) > 0 else 0
                })
            
            product_ads_df = pd.DataFrame(product_ads_data)
            product_ads_df = product_ads_df.sort_values('spend', ascending=False)
            top_product_ads = product_ads_df.head(top_n)
            
            for i, (_, ad) in enumerate(top_product_ads.iterrows(), 1):
                report += f"| {i} | {ad['ad_name']} | ${ad['spend']:,.2f} | {ad['roas']:.6f}x | {ad['ctr']:.2f}% | ${ad['cpm']:.2f} | {ad['thumbstop']:.1f}% | ${ad['aov']:.2f} |\n"
            
            # Get top creators for this product
            product_creators_df = calculate_aggregated_metrics(product_ads, 'creator', 10000)
            top_product_creators = product_creators_df.head(top_n)
            
            if not top_product_creators.empty:
                report += f"""

**Top Creators by Spend:**

| Rank | Creator | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|---------|-----------|-------|------|-----|-----|-----------|-----|
"""
                
                for i, (creator, row) in enumerate(top_product_creators.iterrows(), 1):
                    report += f"| {i} | {creator} | {int(row['Ads Count'])} | ${row['Spend']:,.2f} | {row['ROAS']:.6f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
    
    return report

def calculate_aggregated_metrics(ad_objects, group_by_field, top_n=10):
    """Calculate aggregated metrics for a specific field (product, creator, agency)"""
    if not ad_objects:
        return pd.DataFrame()
    
    # Debug: Print the current USE_NORTHBEAM_DATA setting

    use_northbeam = getattr(main, 'USE_NORTHBEAM_DATA', True)
    data_source = 'northbeam' if use_northbeam else 'meta'
    
    # Group ads by the specified field
    grouped_data = {}
    for ad in ad_objects:
        group_value = ad['metadata'].get(group_by_field, 'Unknown')
        if group_value not in grouped_data:
            grouped_data[group_value] = {
                'ads': [],
                'total_spend': 0,
                'total_revenue': 0,
                'total_transactions': 0,
                'total_impressions': 0,
                'total_link_clicks': 0,
                'total_video_views': 0,
                'video_impressions': 0
            }
        
        grouped_data[group_value]['ads'].append(ad)
        
        # Aggregate metrics using get_metric_value to respect USE_NORTHBEAM_DATA setting
        grouped_data[group_value]['total_spend'] += get_metric_value(ad, 'spend', data_source)
        grouped_data[group_value]['total_revenue'] += get_metric_value(ad, 'attributed_rev', data_source)
        grouped_data[group_value]['total_transactions'] += get_metric_value(ad, 'transactions', data_source)
        grouped_data[group_value]['total_impressions'] += get_metric_value(ad, 'impressions', data_source)
        grouped_data[group_value]['total_link_clicks'] += get_metric_value(ad, 'meta_link_clicks', data_source)
        grouped_data[group_value]['total_video_views'] += get_metric_value(ad, 'meta_3s_video_views', data_source)
        
        # Track video impressions for thumbstop calculation
        if get_metric_value(ad, 'meta_3s_video_views', data_source) > 0:
            grouped_data[group_value]['video_impressions'] += get_metric_value(ad, 'impressions', data_source)
    
    # Calculate derived metrics
    results = []
    for group_value, data in grouped_data.items():
        if data['total_spend'] > 0:
            roas = data['total_revenue'] / data['total_spend']
            ctr = (data['total_link_clicks'] / data['total_impressions'] * 100) if data['total_impressions'] > 0 else 0
            cpm = (data['total_spend'] / data['total_impressions'] * 1000) if data['total_impressions'] > 0 else 0
            thumbstop = (data['total_video_views'] / data['video_impressions'] * 100) if data['video_impressions'] > 0 else 0
            aov = data['total_revenue'] / data['total_transactions'] if data['total_transactions'] > 0 else 0
            
            results.append({
                group_by_field: group_value,
                'Ads Count': len(data['ads']),
                'Spend': data['total_spend'],
                'ROAS': roas,
                'CTR': ctr,
                'CPM': cpm,
                'Thumbstop': thumbstop,
                'AOV': aov,
                'Revenue': data['total_revenue'],
                'Transactions': data['total_transactions']
            })
    
    # Convert to DataFrame and sort by spend
    df = pd.DataFrame(results)
    if not df.empty:
        # Set the group_by_field as the index so we can access it properly
        df = df.set_index(group_by_field)
        df = df.sort_values('Spend', ascending=False).head(top_n)
    
    
    return df

def display_summary_tab(ad_objects, top_n=DEFAULT_TOP_N):
    """Display the summary tab with overall metrics and top N tables"""
    st.header("📊 Campaign Summary")
    
    # Use the currently selected view source from session state
    current_view_source = st.session_state.get('current_view_source', 'Meta')
    data_source = 'northbeam' if current_view_source == 'Northbeam' else 'meta'
    overall_metrics = calculate_campaign_metrics(ad_objects, data_source=data_source)
    
    # Display metrics in a grid
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        create_metric_card("Total Ads With Delivery", overall_metrics['total_ads'])
        create_metric_card("Total Spend", overall_metrics['total_spend'], format_currency)
    
    with col2:
        create_metric_card("ROAS", overall_metrics['roas'], format_roas)
        create_metric_card("CTR", overall_metrics['ctr'], format_percentage)
    
    with col3:
        create_metric_card("CPM", overall_metrics['cpm'], format_currency)
        create_metric_card("Thumbstop", overall_metrics['thumbstop'], format_percentage)
    
    with col4:
        create_metric_card("AOV", overall_metrics['aov'], format_currency)
        create_metric_card("Total Revenue", overall_metrics['total_revenue'], format_currency)
    
    st.markdown("---")
    
    # Top N Ads table
    st.subheader(f"🏆 Top {top_n} Ads")
    
    # Note: Background processing runs independently - links will appear automatically when ready
    
    # Create ads dataframe
    ads_data = []
    
    # Handle both dict and list formats
    if isinstance(ad_objects, dict):
        # Convert dict to list of ad objects
        ad_list = list(ad_objects.values())
    else:
        ad_list = ad_objects
    
    
    # Merge ads with same name regardless of campaign for All Ads view
    merged_ads = merge_ads_with_same_name(ad_list, merge_by_campaign_type=False)
    
    for ad in merged_ads:
        # Get URL for the primary ad_id (first one in the merged group)
        ad_id = ad['ad_ids'].get('ad_id', '')
        ad_type = ad['metadata'].get('ad_type', 'Unknown')
        primary_url, thumbnail_url = get_ad_url(ad_id, ad_type) if ad_id else ("", "")
        
        ads_data.append({
            'Thumbnail': get_thumbnail_url_from_cache(ad_id),
            'Link': primary_url,  # Will be empty if not found in processed file
            'Ad Type': ad['metadata'].get('ad_type', 'Unknown'),
            'Ad Name': ad['ad_ids']['ad_name'],
            'Merged': ad.get('merged_count', 1),  # Show how many ads were merged
            'Campaign Type': ad['metadata'].get('campaign_type', 'Unknown'),
            'Product': ad['metadata'].get('product', 'Unknown'),
            'Creator': ad['metadata'].get('creator', 'Unknown'),
            'Agency': ad['metadata'].get('agency', 'Unknown'),
            'Spend': get_metric_value(ad, 'spend'),
            'Revenue': get_metric_value(ad, 'attributed_rev'),
            'Transactions': get_metric_value(ad, 'transactions'),
            'Impressions': get_metric_value(ad, 'impressions'),
            'Link Clicks': get_metric_value(ad, 'meta_link_clicks'),
            'Video Views': get_metric_value(ad, 'meta_3s_video_views'),
            'ROAS': get_metric_value(ad, 'roas'),
            'CTR': (get_metric_value(ad, 'meta_link_clicks') / get_metric_value(ad, 'impressions') * 100) if get_metric_value(ad, 'impressions') > 0 else 0,
            'CPM': (get_metric_value(ad, 'spend') / get_metric_value(ad, 'impressions') * 1000) if get_metric_value(ad, 'impressions') > 0 else 0,
            'Thumbstop': (get_metric_value(ad, 'meta_3s_video_views') / get_metric_value(ad, 'impressions') * 100) if get_metric_value(ad, 'impressions') > 0 else 0,
            'AOV': get_metric_value(ad, 'attributed_rev') / get_metric_value(ad, 'transactions') if get_metric_value(ad, 'transactions') > 0 else 0
        })

    
    ads_df = pd.DataFrame(ads_data)
    ads_df = ads_df.sort_values('Spend', ascending=False)
    
    # Show top N ads
    top_ads_df = ads_df.head(top_n)
    
    # Remove Link Type column from display (keep it for emoji logic)
    display_columns = [col for col in top_ads_df.columns if col != 'Link Type']
    top_ads_df = top_ads_df[display_columns]
    
    # Create display dataframe with thumbnail column
    display_df = top_ads_df.copy()
    
    st.dataframe(
        display_df,
        column_config={
            "Thumbnail": st.column_config.ImageColumn(
                "Thumbnail",
                width="small"
            ),
            "Link": st.column_config.LinkColumn(
                "Link",
                help="Click to view ad preview",
                display_text="🔗"
            )
        },
        use_container_width=True,
        hide_index=True
    )
    
    # Show all ads in expander
    with st.expander(f"📊 Show all {len(ads_df)} ads"):
        # Create display dataframe with thumbnail column
        display_ads_df = ads_df.copy()
        
        st.dataframe(
            display_ads_df,
            column_config={
                "Thumbnail": st.column_config.ImageColumn(
                    "Thumbnail",
                    width="small"
                ),
                "Link": st.column_config.LinkColumn(
                    "Link",
                    help="Click to view ad preview",
                    display_text="🔗"
                )
            },
            use_container_width=True,
            hide_index=True
        )
    
    st.markdown("---")
    
    # Top N Products table
    st.subheader(f"📦 Top {top_n} Products")
    products_df = calculate_aggregated_metrics(ad_objects, 'product', 10000)  # Get all products
    
    if not products_df.empty:
        # Show top N products
        top_products_df = products_df.head(top_n)
        # Capitalize the product column name
        top_products_df_display = top_products_df.reset_index()
        
        # Add thumbnail column for each product first
        # After reset_index(), the first column is the group_by_field (product)
        first_column_name = top_products_df_display.columns[0]
        top_products_df_display['Thumbnail'] = top_products_df_display[first_column_name].apply(
            lambda product: get_top_spending_ad_thumbnail(ad_objects, 'product', product)
        )
        
        # Now reorder columns to put Thumbnail first
        columns = ['Thumbnail'] + [col for col in top_products_df_display.columns if col != 'Thumbnail']
        top_products_df_display = top_products_df_display[columns]
        
        st.dataframe(
            top_products_df_display,
            column_config={
                "Thumbnail": st.column_config.ImageColumn(
                    "Thumbnail",
                    width="small"
                )
            },
            use_container_width=True,
            hide_index=True
                )
        
        # Show all products in expander
        with st.expander(f"📦 Show all {len(products_df)} products"):
            all_products_df_display = products_df.reset_index()
            
            # Add thumbnail column for each product first
            # After reset_index(), the first column is the group_by_field (product)
            first_column_name = all_products_df_display.columns[0]
            all_products_df_display['Thumbnail'] = all_products_df_display[first_column_name].apply(
                lambda product: get_top_spending_ad_thumbnail(ad_objects, 'product', product)
            )
            
            # Now reorder columns to put Thumbnail first
            columns = ['Thumbnail'] + [col for col in all_products_df_display.columns if col != 'Thumbnail']
            all_products_df_display = all_products_df_display[columns]
            
            st.dataframe(
                all_products_df_display,
                column_config={
                    "Thumbnail": st.column_config.ImageColumn(
                        "Thumbnail",
                        width="small"
                    )
                },
                use_container_width=True,
                hide_index=True
            )
    
    st.markdown("---")
    
    # Top N Creators table
    st.subheader(f"👥 Top {top_n} Creators")
    
    # Group by creator using the same approach as other tabs
    creators_grouped_df = ads_df.groupby('Creator').agg({
        'Spend': 'sum',
        'Revenue': 'sum',
        'Transactions': 'sum',
        'Impressions': 'sum',
        'Link Clicks': 'sum',
        'Video Views': 'sum'
    }).reset_index()
    
    # Calculate derived metrics for grouped data
    creators_grouped_df['ROAS'] = creators_grouped_df['Revenue'] / creators_grouped_df['Spend']
    creators_grouped_df['CTR'] = (creators_grouped_df['Link Clicks'] / creators_grouped_df['Impressions'] * 100).fillna(0)
    creators_grouped_df['CPM'] = (creators_grouped_df['Spend'] / creators_grouped_df['Impressions'] * 1000).fillna(0)
    creators_grouped_df['Thumbstop'] = (creators_grouped_df['Video Views'] / creators_grouped_df['Impressions'] * 100).fillna(0)
    creators_grouped_df['AOV'] = (creators_grouped_df['Revenue'] / creators_grouped_df['Transactions']).fillna(0)
    
    # Sort by spend (descending)
    creators_grouped_df = creators_grouped_df.sort_values('Spend', ascending=False)
    
    if not creators_grouped_df.empty:
        # Show top N creators
        top_creators_df = creators_grouped_df.head(top_n).copy()
        
        # Add thumbnail column for each creator first
        top_creators_df['Thumbnail'] = top_creators_df['Creator'].apply(
            lambda creator: get_top_spending_ad_thumbnail(ad_objects, 'creator', creator)
        )
        
        # Now reorder columns to put Thumbnail first
        columns = ['Thumbnail'] + [col for col in top_creators_df.columns if col != 'Thumbnail']
        top_creators_df = top_creators_df[columns]
        
        st.dataframe(
            top_creators_df,
            column_config={
                "Thumbnail": st.column_config.ImageColumn(
                    "Thumbnail",
                    width="small"
                )
            },
            use_container_width=True,
            hide_index=True
        )
        
        # Show all creators in expander
        with st.expander(f"👥 Show all {len(creators_grouped_df)} creators"):
            # Add thumbnail column for all creators first
            all_creators_df = creators_grouped_df.copy()
            all_creators_df['Thumbnail'] = all_creators_df['Creator'].apply(
                lambda creator: get_top_spending_ad_thumbnail(ad_objects, 'creator', creator)
            )
            
            # Now reorder columns to put Thumbnail first
            columns = ['Thumbnail'] + [col for col in all_creators_df.columns if col != 'Thumbnail']
            all_creators_df = all_creators_df[columns]
            
            st.dataframe(
                all_creators_df,
                column_config={
                    "Thumbnail": st.column_config.ImageColumn(
                        "Thumbnail",
                        width="small"
                    )
                },
                use_container_width=True,
                hide_index=True
            )
    
    st.markdown("---")
    
    # Top Agencies table
    st.subheader("🏢 Top Agencies")
    agencies_df = calculate_aggregated_metrics(ad_objects, 'agency', 10)
    
    if not agencies_df.empty:
        # Create display dataframe with formatted values
        display_agencies_df = agencies_df.copy()
        
        # Format the display values
        display_agencies_df['Spend_Display'] = display_agencies_df['Spend'].apply(format_currency)
        display_agencies_df['ROAS_Display'] = display_agencies_df['ROAS'].apply(lambda x: f"{x:.6f}")
        display_agencies_df['CTR_Display'] = display_agencies_df['CTR'].apply(lambda x: f"{x:.2f}%")
        display_agencies_df['CPM_Display'] = display_agencies_df['CPM'].apply(lambda x: f"${x:.2f}")
        display_agencies_df['Thumbstop_Display'] = display_agencies_df['Thumbstop'].apply(lambda x: f"{x:.2f}%")
        display_agencies_df['AOV_Display'] = display_agencies_df['AOV'].apply(lambda x: f"${x:.2f}")
        
        # Reset index to get agency names as a column
        display_agencies_df = display_agencies_df.reset_index()
        
        # Select and rename columns for display
        display_agencies_df = display_agencies_df[['agency', 'Spend_Display', 'ROAS_Display', 'CTR_Display', 'CPM_Display', 'Thumbstop_Display', 'AOV_Display']]
        display_agencies_df.columns = ['Agency', 'Spend', 'ROAS', 'CTR', 'CPM', 'Thumbstop', 'AOV']
        
        # Add thumbnail column for each agency first
        # After reset_index(), the first column is the group_by_field (agency)
        first_column_name = display_agencies_df.columns[0]
        display_agencies_df['Thumbnail'] = display_agencies_df[first_column_name].apply(
            lambda agency: get_top_spending_ad_thumbnail(ad_objects, 'agency', agency)
        )
        
        # Now reorder columns to put Thumbnail first
        columns = ['Thumbnail'] + [col for col in display_agencies_df.columns if col != 'Thumbnail']
        display_agencies_df = display_agencies_df[columns]
        
        # The data is already sorted by calculate_aggregated_metrics, so we can use it directly
        st.dataframe(
            display_agencies_df,
            column_config={
                "Thumbnail": st.column_config.ImageColumn(
                    "Thumbnail",
                    width="small"
                )
            },
            use_container_width=True,
            hide_index=True
        )

def display_all_ads_tab(ad_objects):
    """Display the All Ads tab with comprehensive filtering and sorting capabilities"""
    
    # Debug: Print the current USE_NORTHBEAM_DATA setting

    use_northbeam = getattr(main, 'USE_NORTHBEAM_DATA', True)
    
    st.header("📋 All Ads")
    
    try:
        # Create the main dataframe with all ad data
        ads_data = []
        
        # Handle both dict and list formats
        if isinstance(ad_objects, dict):
            # Convert dict to list of ad objects
            ad_list = list(ad_objects.values())
        else:
            ad_list = ad_objects
        
        # Merge ads with same name regardless of campaign for All Ads view
        merged_ads = merge_ads_with_same_name(ad_list, merge_by_campaign_type=False)
        print(f"DEBUG: After merging, {len(merged_ads)} unique ads")
        
        for i, ad in enumerate(merged_ads):
            try:
                # Debug: Check which metrics source is being used
                if use_northbeam:
                    metrics = ad['metrics']['northbeam']
                    # Northbeam metric keys
                    spend_key = 'spend'
                    revenue_key = 'attributed_rev'
                    transactions_key = 'transactions'
                    impressions_key = 'impressions'
                    link_clicks_key = 'meta_link_clicks'
                    video_views_key = 'meta_3s_video_views'
                    roas_key = 'roas'
                else:
                    metrics = ad['metrics']['meta']
                    # Meta metric keys
                    spend_key = 'spend'
                    revenue_key = 'purchase_value'
                    transactions_key = 'purchase_count'
                    impressions_key = 'impressions'
                    link_clicks_key = 'link_clicks'
                    video_views_key = 'video_views_3s'
                    roas_key = 'purchase_roas'
                
                # Get ad URL from processed data (use primary ad_id from merged group)
                ad_id = ad['ad_ids'].get('ad_id', '')
                ad_type = ad['metadata'].get('ad_type', 'Unknown')
                primary_url, thumbnail_url = get_ad_url(ad_id, ad_type) if ad_id else ("", "")
                
                ads_data.append({
                    'Link': primary_url,
                    'Ad Type': ad['metadata'].get('ad_type', 'Unknown'),
                    'Thumbnail': thumbnail_url,  # Hidden fallback column
                    'Ad Name': ad['ad_ids'].get('ad_name', 'Unknown'),
                    'Merged': ad.get('merged_count', 1),  # Show how many ads were merged
                    'Campaign Type': ad['metadata'].get('campaign_type', 'Unknown'),
                    'Product': ad['metadata'].get('product', 'Unknown'),
                    'Creator': ad['metadata'].get('creator', 'Unknown'),
                    'Agency': ad['metadata'].get('agency', 'Unknown'),
                    'Spend': metrics.get(spend_key, 0),
                    'Revenue': metrics.get(revenue_key, 0),
                    'Transactions': metrics.get(transactions_key, 0),
                    'Impressions': metrics.get(impressions_key, 0),
                    'Link Clicks': metrics.get(link_clicks_key, 0),
                    'Video Views': metrics.get(video_views_key, 0),
                    'ROAS': metrics.get(roas_key, 0),
                    'CTR': (metrics.get(link_clicks_key, 0) / metrics.get(impressions_key, 1) * 100) if metrics.get(impressions_key, 0) > 0 else 0,
                    'CPM': (metrics.get(spend_key, 0) / metrics.get(impressions_key, 1) * 1000) if metrics.get(impressions_key, 0) > 0 else 0,
                    'Thumbstop': (metrics.get(video_views_key, 0) / metrics.get(impressions_key, 1) * 100) if metrics.get(impressions_key, 0) > 0 else 0,
                    'AOV': metrics.get(revenue_key, 0) / metrics.get(transactions_key, 1) if metrics.get(transactions_key, 0) > 0 else 0
                })
            except Exception as e:
                print(f"DEBUG: Error processing ad {i}: {str(e)}")
                st.warning(f"Error processing ad {i}: {str(e)}")
                continue
        
        
        if not ads_data:
            print("DEBUG: No valid ad data found")
            st.error("No valid ad data found")
            return
            
        df = pd.DataFrame(ads_data)
        
        # Ensure all required columns exist
        required_columns = ['Campaign Type', 'Product', 'Ad Type', 'Creator', 'Agency']
        for col in required_columns:
            if col not in df.columns:
                df[col] = 'Unknown'
                st.warning(f"Added missing column: {col}")
        
        # Fill NaN values
        df = df.fillna('Unknown')
        
    except Exception as e:
        print(f"DEBUG: Error creating dataframe: {str(e)}")
        st.error(f"Error creating dataframe: {str(e)}")
        st.exception(e)
        return
    
    # Display all ads without filtering
    display_df = df.copy()
    
    # Create tabs for deifferent views
    tab1, tab2, tab3 = st.tabs(["📊 All Ads", "👥 Creator Analysis", "📦 Product Analysis"])
    
    with tab1:
        st.subheader(f"📊 All Ads ({len(display_df)} ads)")
        
        # Display the dataframe with clickable URLs
        # Create display dataframe with fallback logic
        display_df = df.copy()
        
        # Use thumbnail as fallback when Link is empty
        for idx, row in display_df.iterrows():
            if not row['Link'] and row['Thumbnail']:
                display_df.at[idx, 'Link'] = row['Thumbnail']
        
        # Remove Thumbnail column from display
        display_df = display_df[[col for col in display_df.columns if col != 'Thumbnail']]
        
        st.dataframe(
            display_df,
            column_config={
                "Link": st.column_config.LinkColumn(
                    "Link",
                    help="Click to view ad",
                    display_text="🔗"
                )
            },
            use_container_width=True,
            height=400,
            hide_index=True
        )
        
        # Download button
        csv = display_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"campaign_ads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    with tab2:
        st.subheader("👥 Creator Analysis")
        
        # Group by creator
        grouped_df = display_df.groupby('Creator').agg({
            'Spend': 'sum',
            'Revenue': 'sum',
            'Transactions': 'sum',
            'Impressions': 'sum',
            'Link Clicks': 'sum',
            'Video Views': 'sum'
        }).reset_index()
        
        # Calculate derived metrics for grouped data
        grouped_df['ROAS'] = grouped_df['Revenue'] / grouped_df['Spend']
        grouped_df['CTR'] = (grouped_df['Link Clicks'] / grouped_df['Impressions'] * 100).fillna(0)
        grouped_df['CPM'] = (grouped_df['Spend'] / grouped_df['Impressions'] * 1000).fillna(0)
        grouped_df['Thumbstop'] = (grouped_df['Video Views'] / grouped_df['Impressions'] * 100).fillna(0)
        grouped_df['AOV'] = (grouped_df['Revenue'] / grouped_df['Transactions']).fillna(0)
        
        # Sort by spend (descending)
        grouped_df = grouped_df.sort_values('Spend', ascending=False)
        
        st.subheader(f"📊 Creator Analysis ({len(grouped_df)} creators)")
        
        # Display the dataframe
        st.dataframe(grouped_df, use_container_width=True, height=400)
        
        # Download button
        csv = grouped_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"creator_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    with tab3:
        st.subheader("📦 Product Analysis")
        
        # Group by product
        grouped_df = display_df.groupby('Product').agg({
            'Spend': 'sum',
            'Revenue': 'sum',
            'Transactions': 'sum',
            'Impressions': 'sum',
            'Link Clicks': 'sum',
            'Video Views': 'sum'
        }).reset_index()
        
        # Calculate derived metrics for grouped data
        grouped_df['ROAS'] = grouped_df['Revenue'] / grouped_df['Spend']
        grouped_df['CTR'] = (grouped_df['Link Clicks'] / grouped_df['Impressions'] * 100).fillna(0)
        grouped_df['CPM'] = (grouped_df['Spend'] / grouped_df['Impressions'] * 1000).fillna(0)
        grouped_df['Thumbstop'] = (grouped_df['Video Views'] / grouped_df['Impressions'] * 100).fillna(0)
        grouped_df['AOV'] = (grouped_df['Revenue'] / grouped_df['Transactions']).fillna(0)
        
        # Sort by spend (descending)
        grouped_df = grouped_df.sort_values('Spend', ascending=False)
        
        st.subheader(f"📊 Product Analysis ({len(grouped_df)} products)")
        
        # Display the dataframe
        st.dataframe(grouped_df, use_container_width=True, height=400)
        
        # Download button
        csv = grouped_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"product_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

def display_creator_analysis_tab(ad_objects):
    """Display the Creator Analysis tab"""
    st.header("👥 Creator Analysis")
    
    # Create the main dataframe with all ad data
    ads_data = []
    for ad in ad_objects:
        ads_data.append({
            'Creator': ad['metadata']['creator'],
            'Product': ad['metadata']['product'],
            'Spend': get_metric_value(ad, 'spend'),
            'Revenue': get_metric_value(ad, 'attributed_rev'),
            'Transactions': get_metric_value(ad, 'transactions'),
            'Impressions': get_metric_value(ad, 'impressions'),
            'Link Clicks': get_metric_value(ad, 'meta_link_clicks'),
            'Video Views': get_metric_value(ad, 'meta_3s_video_views')
        })
    
    df = pd.DataFrame(ads_data)
    
    # Group by creator
    grouped_df = df.groupby('Creator').agg({
        'Spend': 'sum',
        'Revenue': 'sum',
        'Transactions': 'sum',
        'Impressions': 'sum',
        'Link Clicks': 'sum',
        'Video Views': 'sum'
    }).reset_index()
    
    # Calculate derived metrics for grouped data
    grouped_df['ROAS'] = grouped_df['Revenue'] / grouped_df['Spend']
    grouped_df['CTR'] = (grouped_df['Link Clicks'] / grouped_df['Impressions'] * 100).fillna(0)
    grouped_df['CPM'] = (grouped_df['Spend'] / grouped_df['Impressions'] * 1000).fillna(0)
    grouped_df['Thumbstop'] = (grouped_df['Video Views'] / grouped_df['Impressions'] * 100).fillna(0)
    grouped_df['AOV'] = (grouped_df['Revenue'] / grouped_df['Transactions']).fillna(0)
    
    # Sort by spend (descending)
    grouped_df = grouped_df.sort_values('Spend', ascending=False)
    
    display_df = grouped_df.copy()
    
    # Format the display dataframe
    display_df_formatted = display_df.copy()
    
    if 'Spend' in display_df_formatted.columns:
        display_df_formatted['Spend'] = display_df_formatted['Spend'].apply(format_currency)
    if 'Revenue' in display_df_formatted.columns:
        display_df_formatted['Revenue'] = display_df_formatted['Revenue'].apply(format_currency)
    if 'ROAS' in display_df_formatted.columns:
        display_df_formatted['ROAS'] = display_df_formatted['ROAS'].apply(lambda x: f"{x:.2f}")
    if 'CTR' in display_df_formatted.columns:
        display_df_formatted['CTR'] = display_df_formatted['CTR'].apply(lambda x: f"{x:.2f}%")
    if 'CPM' in display_df_formatted.columns:
        display_df_formatted['CPM'] = display_df_formatted['CPM'].apply(lambda x: f"${x:.2f}")
    if 'Thumbstop' in display_df_formatted.columns:
        display_df_formatted['Thumbstop'] = display_df_formatted['Thumbstop'].apply(lambda x: f"{x:.2f}%")
    if 'AOV' in display_df_formatted.columns:
        display_df_formatted['AOV'] = display_df_formatted['AOV'].apply(lambda x: f"${x:.2f}")
    if 'Transactions' in display_df_formatted.columns:
        display_df_formatted['Transactions'] = display_df_formatted['Transactions'].apply(lambda x: f"{x:,.0f}")
    if 'Impressions' in display_df_formatted.columns:
        display_df_formatted['Impressions'] = display_df_formatted['Impressions'].apply(lambda x: f"{x:,.0f}")
    if 'Link Clicks' in display_df_formatted.columns:
        display_df_formatted['Link Clicks'] = display_df_formatted['Link Clicks'].apply(lambda x: f"{x:,.0f}")
    if 'Video Views' in display_df_formatted.columns:
        display_df_formatted['Video Views'] = display_df_formatted['Video Views'].apply(lambda x: f"{x:,.0f}")
    
    # Display results
    st.subheader(f"📊 Creator Analysis ({len(display_df)} creators)")
    
    # Display the dataframe with raw numbers for proper sorting
    st.dataframe(display_df, use_container_width=True, height=400)
    
    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name=f"creator_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

def display_product_analysis_tab(ad_objects):
    """Display the Product Analysis tab"""
    st.header("📦 Product Analysis")
    
    # Create the main dataframe with all ad data
    ads_data = []
    for ad in ad_objects:
        ads_data.append({
            'Creator': ad['metadata']['creator'],
            'Product': ad['metadata']['product'],
            'Spend': get_metric_value(ad, 'spend'),
            'Revenue': get_metric_value(ad, 'attributed_rev'),
            'Transactions': get_metric_value(ad, 'transactions'),
            'Impressions': get_metric_value(ad, 'impressions'),
            'Link Clicks': get_metric_value(ad, 'meta_link_clicks'),
            'Video Views': get_metric_value(ad, 'meta_3s_video_views')
        })
    
    df = pd.DataFrame(ads_data)
    
    # Group by product
    grouped_df = df.groupby('Product').agg({
        'Spend': 'sum',
        'Revenue': 'sum',
        'Transactions': 'sum',
        'Impressions': 'sum',
        'Link Clicks': 'sum',
        'Video Views': 'sum'
    }).reset_index()
    
    # Calculate derived metrics for grouped data
    grouped_df['ROAS'] = grouped_df['Revenue'] / grouped_df['Spend']
    grouped_df['CTR'] = (grouped_df['Link Clicks'] / grouped_df['Impressions'] * 100).fillna(0)
    grouped_df['CPM'] = (grouped_df['Spend'] / grouped_df['Impressions'] * 1000).fillna(0)
    grouped_df['Thumbstop'] = (grouped_df['Video Views'] / grouped_df['Impressions'] * 100).fillna(0)
    grouped_df['AOV'] = (grouped_df['Revenue'] / grouped_df['Transactions']).fillna(0)
    
    # Sort by spend (descending)
    grouped_df = grouped_df.sort_values('Spend', ascending=False)
    
    display_df = grouped_df.copy()
    
    # Format the display dataframe
    display_df_formatted = display_df.copy()
    
    if 'Spend' in display_df_formatted.columns:
        display_df_formatted['Spend'] = display_df_formatted['Spend'].apply(format_currency)
    if 'Revenue' in display_df_formatted.columns:
        display_df_formatted['Revenue'] = display_df_formatted['Revenue'].apply(format_currency)
    if 'ROAS' in display_df_formatted.columns:
        display_df_formatted['ROAS'] = display_df_formatted['ROAS'].apply(lambda x: f"{x:.2f}")
    if 'CTR' in display_df_formatted.columns:
        display_df_formatted['CTR'] = display_df_formatted['CTR'].apply(lambda x: f"{x:.2f}%")
    if 'CPM' in display_df_formatted.columns:
        display_df_formatted['CPM'] = display_df_formatted['CPM'].apply(lambda x: f"${x:.2f}")
    if 'Thumbstop' in display_df_formatted.columns:
        display_df_formatted['Thumbstop'] = display_df_formatted['Thumbstop'].apply(lambda x: f"{x:.2f}%")
    if 'AOV' in display_df_formatted.columns:
        display_df_formatted['AOV'] = display_df_formatted['AOV'].apply(lambda x: f"${x:.2f}")
    if 'Transactions' in display_df_formatted.columns:
        display_df_formatted['Transactions'] = display_df_formatted['Transactions'].apply(lambda x: f"{x:,.0f}")
    if 'Impressions' in display_df_formatted.columns:
        display_df_formatted['Impressions'] = display_df_formatted['Impressions'].apply(lambda x: f"{x:,.0f}")
    if 'Link Clicks' in display_df_formatted.columns:
        display_df_formatted['Link Clicks'] = display_df_formatted['Link Clicks'].apply(lambda x: f"{x:,.0f}")
    if 'Video Views' in display_df_formatted.columns:
        display_df_formatted['Video Views'] = display_df_formatted['Video Views'].apply(lambda x: f"{x:,.0f}")
    
    # Display results
    st.subheader(f"📊 Product Analysis ({len(display_df)} products)")
    
    # Display the dataframe with raw numbers for proper sorting
    st.dataframe(display_df, use_container_width=True, height=400)
    
    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name=f"product_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

def display_campaign_explorer_tab(ad_objects, top_n=DEFAULT_TOP_N, core_products_input=None):
    """Display the Campaign Explorer tab with campaign and product filtering using tabs"""
    st.header("🎯 Campaign Explorer")
    
    # Use the currently selected view source from session state
    current_view_source = st.session_state.get('current_view_source', 'Meta')
    
    # Use hard-coded campaign types from CAMPAIGN_TYPES
    campaigns = []
    for campaign_type in CAMPAIGN_TYPES:
        if isinstance(campaign_type, list) and len(campaign_type) > 0:
            campaigns.append(campaign_type[0])  # Use the first element (campaign name)
        elif isinstance(campaign_type, str):
            campaigns.append(campaign_type)
    
    # Filter out any empty or invalid campaign names
    campaigns = [c for c in campaigns if c and c.strip()]
    
    # Get available products from frontend configuration
    available_products = []
    if core_products_input:
        core_products_list = []
        for line in core_products_input.strip().split('\n'):
            if line.strip():
                products = [p.strip() for p in line.split(',') if p.strip()]
                if products:
                    core_products_list.append(products)
        
        # Use the first product in each group as the display name
        for product_group in core_products_list:
            if len(product_group) > 0:
                available_products.append(product_group[0])
    else:
        # Fallback to DEFAULT_CORE_PRODUCTS if no frontend input
        for product_group in DEFAULT_CORE_PRODUCTS:
            if len(product_group) > 0:
                available_products.append(product_group[0])
    
    # Add "All Products" option
    available_products = ["All Products"] + available_products
    
    if not campaigns:
        st.warning("No campaigns available. Please generate a report first.")
        return
    
    # Create campaign tabs
    campaign_tabs = st.tabs(campaigns)
    
    # Process each campaign tab
    for i, campaign in enumerate(campaigns):
        with campaign_tabs[i]:
            st.subheader(f"📊 {campaign} Campaigns")
            
            # Filter ads by selected campaign
            campaign_ads = [ad for ad in ad_objects if ad['metadata'].get('campaign_type') == campaign]
            
            if not campaign_ads:
                st.warning(f"No ads found for campaign: {campaign}")
                continue
            
            # Campaign summary metrics - use the currently selected view source
            data_source = 'northbeam' if current_view_source == 'Northbeam' else 'meta'
            campaign_metrics = calculate_campaign_metrics(campaign_ads, data_source=data_source)
            
            # Display metrics in a grid
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                create_metric_card("Total Ads With Delivery", campaign_metrics['total_ads'])
                create_metric_card("Total Spend", campaign_metrics['total_spend'], format_currency)
            
            with col2:
                # Get target ROAS for this campaign type
                target_roas = get_target_roas(campaign)
                subtitle = f"(Target: {target_roas})" if target_roas is not None else None
                create_metric_card("ROAS", campaign_metrics['roas'], format_roas, subtitle)
                create_metric_card("CTR", campaign_metrics['ctr'], format_percentage)
            
            with col3:
                create_metric_card("CPM", campaign_metrics['cpm'], format_currency)
                create_metric_card("Thumbstop", campaign_metrics['thumbstop'], format_percentage)
            
            with col4:
                create_metric_card("AOV", campaign_metrics['aov'], format_currency)
                create_metric_card("Total Revenue", campaign_metrics['total_revenue'], format_currency)
            
            st.markdown("---")
            
            # Create product tabs for this campaign
            product_tabs = st.tabs(available_products)
            
            # Process each product tab
            for j, product in enumerate(available_products):
                with product_tabs[j]:
                    st.subheader(f"📦 {product} - {campaign}")
                    
                    # Filter ads by selected product
                    if product == "All Products":
                        product_ads = campaign_ads
                    else:
                        # Find the product group that contains this product
                        product_group = None
                        if core_products_input:
                            core_products_list = []
                            for line in core_products_input.strip().split('\n'):
                                if line.strip():
                                    products = [p.strip() for p in line.split(',') if p.strip()]
                                    if products:
                                        core_products_list.append(products)
                            
                            for group in core_products_list:
                                if product in group:
                                    product_group = group
                                    break
                        else:
                            # Fallback to DEFAULT_CORE_PRODUCTS
                            for group in DEFAULT_CORE_PRODUCTS:
                                if product in group:
                                    product_group = group
                                    break
                        
                        # Filter ads that match any product in the group
                        if product_group:
                            product_ads = [ad for ad in campaign_ads if ad['metadata'].get('product') in product_group]
                        else:
                            # If no group found, filter by exact product match
                            product_ads = [ad for ad in campaign_ads if ad['metadata'].get('product') == product]
                    
                    if not product_ads:
                        st.info(f"No ads found for {product} in {campaign} campaign")
                        continue
                    
                    # Show product summary - use the currently selected view source
                    product_metrics = calculate_campaign_metrics(product_ads, data_source=data_source)
                    
                    # Display product metrics in a consistent card format
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        create_metric_card("Ads With Delivery", product_metrics['total_ads'])
                        create_metric_card("Spend", product_metrics['total_spend'], format_currency)
                    
                    with col2:
                        # Get target ROAS for this campaign type
                        target_roas = get_target_roas(campaign)
                        subtitle = f"(Target: {target_roas})" if target_roas is not None else None
                        create_metric_card("ROAS", product_metrics['roas'], format_roas, subtitle)
                        create_metric_card("CTR", product_metrics['ctr'], format_percentage)
                    
                    with col3:
                        create_metric_card("CPM", product_metrics['cpm'], format_currency)
                        create_metric_card("Thumbstop", product_metrics['thumbstop'], format_percentage)
                    
                    with col4:
                        create_metric_card("AOV", product_metrics['aov'], format_currency)
                        create_metric_card("Revenue", product_metrics['total_revenue'], format_currency)
                    
                    st.markdown("---")
                    
                    # Top N Ads for selected campaign and product
                    st.subheader(f"🏆 Top {top_n} Ads")
                    
                    # Create ads dataframe for the filtered data
                    ads_data = []
                    for ad in product_ads:
                        # Get ad URL from processed data
                        ad_id = ad['ad_ids'].get('ad_id', '')
                        ad_type = ad['metadata'].get('ad_type', 'Unknown')
                        primary_url, thumbnail_url = get_ad_url(ad_id, ad_type) if ad_id else ("", "")

                        ads_data.append({
                            'Thumbnail': get_thumbnail_url_from_cache(ad_id),
                            'Link': primary_url,
                            'Ad Type': ad['metadata'].get('ad_type', 'Unknown'),
                            'Ad Name': ad['ad_ids']['ad_name'],
                            'Merged': ad.get('merged_count', 1),  # Show how many ads were merged
                            'Product': ad['metadata'].get('product', 'Unknown'),
                            'Creator': ad['metadata'].get('creator', 'Unknown'),
                            'Agency': ad['metadata'].get('agency', 'Unknown'),
                            'Spend': get_metric_value(ad, 'spend', data_source),
                            'Revenue': get_metric_value(ad, 'attributed_rev', data_source),
                            'Transactions': get_metric_value(ad, 'transactions', data_source),
                            'Impressions': get_metric_value(ad, 'impressions', data_source),
                            'Link Clicks': get_metric_value(ad, 'meta_link_clicks', data_source),
                            'Video Views': get_metric_value(ad, 'meta_3s_video_views', data_source),
                            'ROAS': get_metric_value(ad, 'roas', data_source),
                            'CTR': (get_metric_value(ad, 'meta_link_clicks', data_source) / get_metric_value(ad, 'impressions', data_source) * 100) if get_metric_value(ad, 'impressions', data_source) > 0 else 0,
                            'CPM': (get_metric_value(ad, 'spend', data_source) / get_metric_value(ad, 'impressions', data_source) * 1000) if get_metric_value(ad, 'impressions', data_source) > 0 else 0,
                            'Thumbstop': (get_metric_value(ad, 'meta_3s_video_views', data_source) / get_metric_value(ad, 'impressions', data_source) * 100) if get_metric_value(ad, 'impressions', data_source) > 0 else 0,
                            'AOV': get_metric_value(ad, 'attributed_rev', data_source) / get_metric_value(ad, 'transactions', data_source) if get_metric_value(ad, 'transactions', data_source) > 0 else 0
                        })
                    
                    ads_df = pd.DataFrame(ads_data)
                    # Sort by raw numeric values before formatting
                    ads_df = ads_df.sort_values('Spend', ascending=False)
                    
                    # Show top N ads by default
                    display_ads_df = ads_df.head(top_n).copy()
                    
                    if not display_ads_df.empty:
                        # Format the dataframe for display
                        display_ads_df_formatted = display_ads_df.copy()
                        display_ads_df_formatted['Spend'] = display_ads_df_formatted['Spend'].apply(format_currency)
                        display_ads_df_formatted['Revenue'] = display_ads_df_formatted['Revenue'].apply(format_currency)
                        display_ads_df_formatted['ROAS'] = display_ads_df_formatted['ROAS'].apply(lambda x: f"{x:.2f}")
                        display_ads_df_formatted['CTR'] = display_ads_df_formatted['CTR'].apply(lambda x: f"{x:.2f}%")
                        display_ads_df_formatted['CPM'] = display_ads_df_formatted['CPM'].apply(lambda x: f"${x:.2f}")
                        display_ads_df_formatted['Thumbstop'] = display_ads_df_formatted['Thumbstop'].apply(lambda x: f"{x:.2f}%")
                        display_ads_df_formatted['AOV'] = display_ads_df_formatted['AOV'].apply(lambda x: f"${x:.2f}")
                        display_ads_df_formatted['Transactions'] = display_ads_df_formatted['Transactions'].apply(lambda x: f"{x:,.0f}")
                        display_ads_df_formatted['Impressions'] = display_ads_df_formatted['Impressions'].apply(lambda x: f"{x:,.0f}")
                        display_ads_df_formatted['Link Clicks'] = display_ads_df_formatted['Link Clicks'].apply(lambda x: f"{x:,.0f}")
                        display_ads_df_formatted['Video Views'] = display_ads_df_formatted['Video Views'].apply(lambda x: f"{x:,.0f}")
                        
                        # Create display dataframe with thumbnail column
                        display_ads_df = display_ads_df.copy()
                        
                        # Use LinkColumn for clickable URLs
                        st.dataframe(
                            display_ads_df,
                            column_config={
                                "Thumbnail": st.column_config.ImageColumn(
                                    "Thumbnail",
                                    width="small"
                                ),
                                "Link": st.column_config.LinkColumn(
                                    "Link",
                                    help="Click to view ad preview",
                                    display_text="🔗"
                                )
                            },
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # Show all ads in expander
                        with st.expander(f"📊 Show all {len(ads_df)} ads"):
                            # Create display dataframe with thumbnail column
                            display_all_ads_df = ads_df.copy()
                            
                            st.dataframe(
                                display_all_ads_df,
                                column_config={
                                    "Thumbnail": st.column_config.ImageColumn(
                                        "Thumbnail",
                                        width="small"
                                    ),
                                    "Link": st.column_config.LinkColumn(
                                        "Link",
                                        help="Click to view ad preview",
                                        display_text="🔗"
                                    )
                                },
                                use_container_width=True,
                                hide_index=True
                            )
                    else:
                        st.info("No ads data available for the selected filters.")
                    
                    st.markdown("---")
                    
                    # Top N Creators for selected campaign and product
                    st.subheader(f"👥 Top {top_n} Creators")
                    
                    # Group by creator using the same approach as All Ads tab
                    creators_grouped_df = ads_df.groupby('Creator').agg({
                        'Spend': 'sum',
                        'Revenue': 'sum',
                        'Transactions': 'sum',
                        'Impressions': 'sum',
                        'Link Clicks': 'sum',
                        'Video Views': 'sum'
                    }).reset_index()
                    
                    # Calculate derived metrics for grouped data
                    creators_grouped_df['ROAS'] = creators_grouped_df['Revenue'] / creators_grouped_df['Spend']
                    creators_grouped_df['CTR'] = (creators_grouped_df['Link Clicks'] / creators_grouped_df['Impressions'] * 100).fillna(0)
                    creators_grouped_df['CPM'] = (creators_grouped_df['Spend'] / creators_grouped_df['Impressions'] * 1000).fillna(0)
                    creators_grouped_df['Thumbstop'] = (creators_grouped_df['Video Views'] / creators_grouped_df['Impressions'] * 100).fillna(0)
                    creators_grouped_df['AOV'] = (creators_grouped_df['Revenue'] / creators_grouped_df['Transactions']).fillna(0)
                    
                    # Sort by spend (descending)
                    creators_grouped_df = creators_grouped_df.sort_values('Spend', ascending=False)
                    
                    # Show top N creators by default
                    display_creators_df = creators_grouped_df.head(top_n).copy()
                    
                    if not display_creators_df.empty:
                        # Add thumbnail column for each creator first
                        display_creators_df['Thumbnail'] = display_creators_df['Creator'].apply(
                            lambda creator: get_top_spending_ad_thumbnail(ad_objects, 'creator', creator)
                        )
                        
                        # Now reorder columns to put Thumbnail first
                        columns = ['Thumbnail'] + [col for col in display_creators_df.columns if col != 'Thumbnail']
                        display_creators_df = display_creators_df[columns]
                        
                        # Format the dataframe for display
                        display_creators_df_formatted = display_creators_df.copy()
                        display_creators_df_formatted['Spend'] = display_creators_df_formatted['Spend'].apply(format_currency)
                        display_creators_df_formatted['ROAS'] = display_creators_df_formatted['ROAS'].apply(lambda x: f"{x:.2f}")
                        display_creators_df_formatted['CTR'] = display_creators_df_formatted['CTR'].apply(lambda x: f"{x:.2f}%")
                        display_creators_df_formatted['CPM'] = display_creators_df_formatted['CPM'].apply(lambda x: f"${x:.2f}")
                        display_creators_df_formatted['Thumbstop'] = display_creators_df_formatted['Thumbstop'].apply(lambda x: f"{x:.2f}%")
                        display_creators_df_formatted['AOV'] = display_creators_df_formatted['AOV'].apply(lambda x: f"${x:.2f}")
                        
                        # Use raw numbers for proper sorting, let Streamlit handle display
                        st.dataframe(
                            display_creators_df,
                            column_config={
                                "Thumbnail": st.column_config.ImageColumn(
                                    "Thumbnail",
                                    width="small"
                                )
                            },
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # Show all creators in expander
                        with st.expander(f"👥 Show all {len(creators_grouped_df)} creators"):
                            # Add thumbnail column for all creators first
                            all_creators_df = creators_grouped_df.copy()
                            all_creators_df['Thumbnail'] = all_creators_df['Creator'].apply(
                                lambda creator: get_top_spending_ad_thumbnail(ad_objects, 'creator', creator)
                            )
                            
                            # Now reorder columns to put Thumbnail first
                            columns = ['Thumbnail'] + [col for col in all_creators_df.columns if col != 'Thumbnail']
                            all_creators_df = all_creators_df[columns]
                            
                            # Format all creators dataframe
                            all_creators_formatted = all_creators_df.copy()
                            all_creators_formatted['Spend'] = all_creators_formatted['Spend'].apply(format_currency)
                            all_creators_formatted['ROAS'] = all_creators_formatted['ROAS'].apply(lambda x: f"{x:.2f}")
                            all_creators_formatted['CTR'] = all_creators_formatted['CTR'].apply(lambda x: f"{x:.2f}%")
                            all_creators_formatted['CPM'] = all_creators_formatted['CPM'].apply(lambda x: f"${x:.2f}")
                            all_creators_formatted['Thumbstop'] = all_creators_formatted['Thumbstop'].apply(lambda x: f"{x:.2f}%")
                            all_creators_formatted['AOV'] = all_creators_df['AOV'].apply(lambda x: f"${x:.2f}")
                            
                            # Use raw numbers for proper sorting, let Streamlit handle display
                            st.dataframe(
                                all_creators_df,
                                column_config={
                                    "Thumbnail": st.column_config.ImageColumn(
                                        "Thumbnail",
                                        width="small"
                                    )
                                },
                                use_container_width=True,
                                hide_index=True
                            )
                    else:
                        st.info("No creator data available for the selected filters.")

def display_product_creator_explorer_tab(ad_objects):
    """Display the Product/Creator Explorer tab with filtering and metrics"""
    st.header("🔍 Product/Creator Explorer")
    
    # Cache available filters in session state to avoid recomputation
    if 'product_creator_available_filters' not in st.session_state:
        st.session_state.product_creator_available_filters = get_available_filters(ad_objects)
    
    available_filters = st.session_state.product_creator_available_filters
    products = available_filters['products']
    creators = available_filters['creators']
    
    # Pre-compute all product and creator combinations for instant loading
    if 'product_creator_all_combinations' not in st.session_state:
        # Get all unique products and creators
        all_products = set()
        all_creators = set()
        for ad in ad_objects:
            all_products.add(ad['metadata'].get('product', 'Unknown'))
            all_creators.add(ad['metadata'].get('creator', 'Unknown'))
        
        # Sort by count (highest to lowest)
        product_counts = {}
        creator_counts = {}
        for ad in ad_objects:
            product = ad['metadata'].get('product', 'Unknown')
            creator = ad['metadata'].get('creator', 'Unknown')
            product_counts[product] = product_counts.get(product, 0) + 1
            creator_counts[creator] = creator_counts.get(creator, 0) + 1
        
        sorted_products = sorted(product_counts.items(), key=lambda x: x[1], reverse=True)
        sorted_creators = sorted(creator_counts.items(), key=lambda x: x[1], reverse=True)
        
        # Pre-compute all combinations
        combinations = {}
        product_options = ["All Products"] + [product for product, count in sorted_products]
        creator_options = ["All Creators"] + [creator for creator, count in sorted_creators]
        
        for product in product_options:
            for creator in creator_options:
                key = f"{product}_{creator}"
                filtered_ads = []
                for ad in ad_objects:
                    product_match = product == "All Products" or ad['metadata'].get('product') == product
                    creator_match = creator == "All Creators" or ad['metadata'].get('creator') == creator
                    if product_match and creator_match:
                        filtered_ads.append(ad)
                combinations[key] = {
                    'ads': filtered_ads,
                    'product': product,
                    'creator': creator,
                    'product_count': product_counts.get(product, 0) if product != "All Products" else len(ad_objects),
                    'creator_count': creator_counts.get(creator, 0) if creator != "All Creators" else len(ad_objects)
                }
        
        st.session_state.product_creator_all_combinations = combinations
        st.session_state.product_creator_product_options = product_options
        st.session_state.product_creator_creator_options = creator_options
    
    # Create two side-by-side dropdowns for Product and Creator selection
    col1, col2 = st.columns(2)
    
    with col1:
        selected_product = st.selectbox(
            "📦 Select Product",
            options=st.session_state.product_creator_product_options,
            index=0,
            key="product_creator_product_select",
            help="Products ranked by total number of ads"
        )
    
    with col2:
        selected_creator = st.selectbox(
            "👥 Select Creator",
            options=st.session_state.product_creator_creator_options,
            index=0,
            key="product_creator_creator_select",
            help="Creators ranked by total number of ads"
        )
    
    # Get pre-computed filtered ads
    filter_key = f"{selected_product}_{selected_creator}"
    combination_data = st.session_state.product_creator_all_combinations[filter_key]
    filtered_ads = combination_data['ads']
    
    # Calculate metrics for filtered ads
    if filtered_ads:
        # Calculate aggregated metrics
        total_ads = len(filtered_ads)
        total_spend = sum(get_metric_value(ad, 'spend') for ad in filtered_ads)
        total_revenue = sum(get_metric_value(ad, 'attributed_rev') for ad in filtered_ads)
        total_transactions = sum(get_metric_value(ad, 'transactions') for ad in filtered_ads)
        total_impressions = sum(get_metric_value(ad, 'impressions') for ad in filtered_ads)
        total_link_clicks = sum(get_metric_value(ad, 'meta_link_clicks') for ad in filtered_ads)
        total_video_views = sum(get_metric_value(ad, 'meta_3s_video_views') for ad in filtered_ads)
        
        # Calculate derived metrics
        roas = total_revenue / total_spend if total_spend > 0 else 0
        ctr = (total_link_clicks / total_impressions * 100) if total_impressions > 0 else 0
        cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
        thumbstop = (total_video_views / total_impressions * 100) if total_impressions > 0 else 0
        aov = (total_revenue / total_transactions) if total_transactions > 0 else 0
        
        # Display metric cards
        st.subheader("📊 Summary Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            create_metric_card("Total Ads With Delivery", total_ads)
            create_metric_card("Total Spend", total_spend, format_currency)
        
        with col2:
            create_metric_card("ROAS", roas, format_roas)
            create_metric_card("CTR", ctr, format_percentage)
        
        with col3:
            create_metric_card("CPM", cpm, format_currency)
            create_metric_card("Thumbstop", thumbstop, format_percentage)
        
        with col4:
            create_metric_card("AOV", aov, format_currency)
            create_metric_card("Total Revenue", total_revenue, format_currency)
        
        st.markdown("---")
        
        # Display filtered ads table
        st.subheader(f"📋 Ads Table ({len(filtered_ads)} ads)")
        
        # Create ads dataframe
        ads_data = []
        for ad in filtered_ads:
            # Get ad URL from processed data
            ad_id = ad['ad_ids'].get('ad_id', '')
            ad_type = ad['metadata'].get('ad_type', 'Unknown')
            primary_url, thumbnail_url = get_ad_url(ad_id, ad_type) if ad_id else ("", "")
            
            ads_data.append({
                'Thumbnail': get_thumbnail_url_from_cache(ad_id),
                'Link': primary_url,
                'Ad Type': ad['metadata'].get('ad_type', 'Unknown'),
                'Ad Name': ad['ad_ids']['ad_name'],
                'Campaign Type': ad['metadata'].get('campaign_type', 'Unknown'),
                'Product': ad['metadata'].get('product', 'Unknown'),
                'Creator': ad['metadata'].get('creator', 'Unknown'),
                'Agency': ad['metadata'].get('agency', 'Unknown'),
                'Spend': get_metric_value(ad, 'spend'),
                'Revenue': get_metric_value(ad, 'attributed_rev'),
                'Transactions': get_metric_value(ad, 'transactions'),
                'Impressions': get_metric_value(ad, 'impressions'),
                'Link Clicks': get_metric_value(ad, 'meta_link_clicks'),
                'Video Views': get_metric_value(ad, 'meta_3s_video_views'),
                'ROAS': get_metric_value(ad, 'roas'),
                'CTR': (get_metric_value(ad, 'meta_link_clicks') / get_metric_value(ad, 'impressions') * 100) if get_metric_value(ad, 'impressions') > 0 else 0,
                'CPM': (get_metric_value(ad, 'spend') / get_metric_value(ad, 'impressions') * 1000) if get_metric_value(ad, 'impressions') > 0 else 0,
                'Thumbstop': (get_metric_value(ad, 'meta_3s_video_views') / get_metric_value(ad, 'impressions') * 100) if get_metric_value(ad, 'impressions') > 0 else 0,
                'AOV': get_metric_value(ad, 'attributed_rev') / get_metric_value(ad, 'transactions') if get_metric_value(ad, 'transactions') > 0 else 0
            })
        
        ads_df = pd.DataFrame(ads_data)
        ads_df = ads_df.sort_values('Spend', ascending=False)
        
        # Format the display dataframe
        display_df_formatted = ads_df.copy()
        
        if 'Spend' in display_df_formatted.columns:
            display_df_formatted['Spend'] = display_df_formatted['Spend'].apply(format_currency)
        if 'Revenue' in display_df_formatted.columns:
            display_df_formatted['Revenue'] = display_df_formatted['Revenue'].apply(format_currency)
        if 'ROAS' in display_df_formatted.columns:
            display_df_formatted['ROAS'] = display_df_formatted['ROAS'].apply(lambda x: f"{x:.2f}")
        if 'CTR' in display_df_formatted.columns:
            display_df_formatted['CTR'] = display_df_formatted['CTR'].apply(lambda x: f"{x:.2f}%")
        if 'CPM' in display_df_formatted.columns:
            display_df_formatted['CPM'] = display_df_formatted['CPM'].apply(lambda x: f"${x:.2f}")
        if 'Thumbstop' in display_df_formatted.columns:
            display_df_formatted['Thumbstop'] = display_df_formatted['Thumbstop'].apply(lambda x: f"{x:.2f}%")
        if 'AOV' in display_df_formatted.columns:
            display_df_formatted['AOV'] = display_df_formatted['AOV'].apply(lambda x: f"${x:.2f}")
        if 'Transactions' in display_df_formatted.columns:
            display_df_formatted['Transactions'] = display_df_formatted['Transactions'].apply(lambda x: f"{x:,.0f}")
        if 'Impressions' in display_df_formatted.columns:
            display_df_formatted['Impressions'] = display_df_formatted['Impressions'].apply(lambda x: f"{x:,.0f}")
        if 'Link Clicks' in display_df_formatted.columns:
            display_df_formatted['Link Clicks'] = display_df_formatted['Link Clicks'].apply(lambda x: f"{x:,.0f}")
        if 'Video Views' in display_df_formatted.columns:
            display_df_formatted['Video Views'] = display_df_formatted['Video Views'].apply(lambda x: f"{x:,.0f}")
        
        # Create display dataframe with thumbnail column
        display_df_formatted = display_df_formatted.copy()
        
        st.dataframe(
            display_df_formatted,
            column_config={
                "Thumbnail": st.column_config.ImageColumn(
                    "Thumbnail",
                    width="small"
                ),
                "Link": st.column_config.LinkColumn(
                    "Link",
                    help="Click to view ad preview",
                    display_text="🔗"
                )
            },
            use_container_width=True,
            hide_index=True
        )
        

        
    else:
        st.warning("⚠️ No ads found matching the selected filters. Please try different combinations.")

def main():
    # Check authentication first
    if not require_authentication():
        return
    
    st.title("🎯 Campaign Reporting Dashboard")
    

    
    st.markdown("---")
    
    # Initialize session state for data persistence
    if 'comprehensive_ads' not in st.session_state:
        st.session_state.comprehensive_ads = None
    if 'report_config' not in st.session_state:
        st.session_state.report_config = None
    
    
    

    

    
    # Sidebar with configuration
    st.sidebar.header("⚙️ Configuration")
    
    # Editable configuration
    st.sidebar.subheader("📅 Date Range")
    
    # Initialize session state for dates if not exists
    if 'date_from' not in st.session_state:
        st.session_state.date_from = DEFAULT_DATE_FROM
    if 'date_to' not in st.session_state:
        st.session_state.date_to = DEFAULT_DATE_TO

    date_from = st.sidebar.date_input(
        "Start Date",
        value=st.session_state.date_from,
        format="YYYY-MM-DD"
    )
    date_to = st.sidebar.date_input(
        "End Date (Inclusive)", 
        value=st.session_state.date_to,
        format="YYYY-MM-DD"
    )
    
    # Update session state when dates change manually
    if date_from != st.session_state.date_from:
        st.session_state.date_from = date_from
    if date_to != st.session_state.date_to:
        st.session_state.date_to = date_to
    
    st.sidebar.subheader("📊 Settings")
    top_n = st.sidebar.number_input("Top N (# ads/groups to display)", min_value=1, max_value=50, value=DEFAULT_TOP_N, key="top_n")
    merge_ads = st.sidebar.checkbox("Merge Ads with Same Name", value=DEFAULT_MERGE_ADS_WITH_SAME_NAME, key="merge_ads", help="Combine ads with identical names and campaign types, aggregate their metrics")
    # Data source selection - at least one must be selected
    st.sidebar.subheader("🔌 Data Sources")
    
    # Initialize data source session state if not exists
    if 'use_meta' not in st.session_state:
        st.session_state.use_meta = True
    if 'use_northbeam' not in st.session_state:
        st.session_state.use_northbeam = DEFAULT_USE_NORTHBEAM_DATA
    
    # Data source selection for NEXT report generation
    # These checkboxes control what data will be fetched when you click "Generate Report"
    
    # Ensure at least one source is always selected
    if not st.session_state.use_meta and not st.session_state.use_northbeam:
        st.session_state.use_meta = True
    
    use_meta = st.sidebar.checkbox(
        "Meta", 
        key="use_meta",
        help="Fetch data from Meta Ads API for the next report"
    )
    
    use_northbeam = st.sidebar.checkbox(
        "Northbeam", 
        key="use_northbeam",
        help="Fetch data from Northbeam API for spend/revenue metrics in the next report"
    )
    
    # Update session state and ensure at least one source is selected
    if use_meta != st.session_state.use_meta:
        st.session_state.use_meta = use_meta
        if not use_meta and not use_northbeam:
            st.session_state.use_northbeam = True
            use_northbeam = True
    
    if use_northbeam != st.session_state.use_northbeam:
        st.session_state.use_northbeam = use_northbeam
        if not use_meta and not use_northbeam:
            st.session_state.use_meta = True
            use_meta = True
    
    
    use_cached_files = st.sidebar.checkbox("Use Cached Files", value=True, key="use_cached_files", 
                                          help="Uses previously fetched data for date range to speed up processing. Uncheck to fetch fresh data.")
    
    # Note: Cache management is now handled by media_urls_manager and Streamlit session state
    
    # Update the global variables for instant switching
    main.USE_NORTHBEAM_DATA = use_northbeam
    main.USE_META_DATA = use_meta
    
    st.sidebar.subheader("📦 Core Products")
    
    # Convert default core products to text format
    default_core_products_text = ""
    for product_group in DEFAULT_CORE_PRODUCTS:
        if product_group:
            default_core_products_text += ", ".join(product_group) + "\n"
    default_core_products_text = default_core_products_text.strip()
    
    core_products_input = st.sidebar.text_area(
        "One product per line", 
        value=default_core_products_text, 
        height=150, 
        key="core_products",
        help="Separate multiple codes for the same product with a comma."
    )
    
    # Convert to string format for API calls
    date_from_str = date_from.strftime("%Y-%m-%d")
    date_to_str = date_to.strftime("%Y-%m-%d")
    
    # Check if configuration has changed and clear cached data if needed
    if st.session_state.comprehensive_ads and st.session_state.report_config:
            config = st.session_state.report_config
            # Check if configuration has changed (excluding data source settings for instant switching)
            config_changed = (
                config.get('date_from') != date_from_str or
                config.get('date_to') != date_to_str or
                config.get('top_n') != top_n or
                config.get('merge_ads') != merge_ads or
                config.get('core_products_input') != core_products_input
            )
            
            # Only clear cache if non-northbeam settings changed
            if config_changed:
                st.session_state.comprehensive_ads = None
                st.session_state.report_config = None
                # Clear Google doc state when configuration changes
                if 'google_doc_link' in st.session_state:
                    del st.session_state.google_doc_link
                if 'markdown_content' in st.session_state:
                    del st.session_state.markdown_content
                if 'report_filename' in st.session_state:
                    del st.session_state.report_filename
                if 'is_generating_google_doc' in st.session_state:
                    del st.session_state.is_generating_google_doc
                auto_hide_status_message("🔄 Configuration changed. Please click 'Generate Report' to fetch fresh data with the new settings.", "info")
            # Note: Data source checkboxes no longer affect the dropdown for existing reports
            # The dropdown shows what data was actually fetched, not what will be fetched next
    
    
    # Generate Report Button
    
    generate_button = st.sidebar.button("🔄 Generate Report", type="primary")
    
    # Status display in sidebar
    if 'status_messages' in st.session_state and st.session_state.status_messages:
        # Show only the most recent status message
        latest_message = list(st.session_state.status_messages.values())[-1]
        message_type = latest_message['type']
        message_text = latest_message['message']
        
        st.sidebar.markdown("---")
        st.sidebar.subheader("📊 Status")
        
        if message_type == "info":
            st.sidebar.info(f"ℹ️ {message_text}")
        elif message_type == "success":
            st.sidebar.success(f"✅ {message_text}")
        elif message_type == "warning":
            st.sidebar.warning(f"⚠️ {message_text}")
        elif message_type == "error":
            st.sidebar.error(f"❌ {message_text}")
        
        # Clear button for status
        if st.sidebar.button("🗑️ Clear Status", key="clear_status"):
            st.session_state.status_messages = {}
            st.rerun()
    
    # Add logout button at the bottom of sidebar
    st.sidebar.markdown("---")
    show_logout_button()
    

    


    # Main content area
    if generate_button:
        # Generate report with status messages
        
        try:
            # Temporarily update the global variables for this session
            main.DATE_FROM = date_from
            main.DATE_TO = date_to
            main.TOP_N = top_n
            main.MERGE_ADS_WITH_SAME_NAME = merge_ads
            main.USE_NORTHBEAM_DATA = use_northbeam
            
            # Debug: Show what we're trying to fetch
            auto_hide_status_message(f"🔄 Starting data fetch for {date_from} to {date_to} (Northbeam: {use_northbeam})", "info")
            
            # Parse core products from user-friendly format
            if core_products_input:
                core_products_list = []
                for line in core_products_input.strip().split('\n'):
                    if line.strip():
                        # Split by comma and clean up
                        products = [p.strip() for p in line.split(',') if p.strip()]
                        if products:
                            core_products_list.append(products)
                main.CORE_PRODUCTS = core_products_list
            else:
                main.CORE_PRODUCTS = DEFAULT_CORE_PRODUCTS
            
            # Always generate fresh comprehensive ad objects to ensure merge setting is respected
            

            
            # Define filename for saving
            date_from_formatted = date_from.strftime("%Y%m%d")
            date_to_formatted = date_to.strftime("%Y%m%d")
            comprehensive_filename = f"{ROOT_DIRECTORY}/processed/comprehensive_ads/comprehensive_ads_{date_from_formatted}-{date_to_formatted}.json"
            
            # Note: Cache management is now handled by media_urls_manager and Streamlit session state
            
            # Update progress - Step 1: Checking existing data
            auto_hide_status_message("🔍 Step 1/4: Fetching Meta & Northbeam data. This may take a few minutes.", "info")
            
                        # Fetch data using concurrent configuration for better performance
            meta_insights, northbeam_df = fetch_all_data_concurrently(date_from, date_to, use_cached_files, use_meta, use_northbeam)
            
            # Update progress - Step 2: Data fetched
            auto_hide_status_message("📊 Step 2/4: Data fetched successfully", "info")
            
            # Debug: Show what we got from selected sources
            sources_fetched = []
            if use_meta and meta_insights:
                sources_fetched.append(f"Meta: {len(meta_insights)} ads")
            if use_northbeam and northbeam_df is not None:
                sources_fetched.append(f"Northbeam: {len(northbeam_df)} rows")
            
            if sources_fetched:
                auto_hide_status_message(f"📊 Data fetched: {', '.join(sources_fetched)}", "info")
            else:
                auto_hide_status_message("⚠️ No data fetched from selected sources", "warning")
            
            # Check for Northbeam data failure and provide user guidance
            if use_northbeam and (northbeam_df is None or (isinstance(northbeam_df, pd.DataFrame) and len(northbeam_df) == 0)):
                # Show warning but continue with Meta data only
                auto_hide_status_message("⚠️ Northbeam data unavailable. Try generating again if you need Northbeam metrics.", "warning")
                st.warning("""
                **⚠️ The system failed to fetch Northbeam data.

                Click **"Generate Report"** again to retry— this typically resolves the issue.
                
                If the problem persists, **Check your Northbeam API credentials**
                
                """)
            elif not use_northbeam:
                # Meta-only mode, so this is expected
                auto_hide_status_message("📊 Meta-only mode: Northbeam data not required", "info")
            
            # Apply filtering to Northbeam data if it exists and is selected
            if use_northbeam and northbeam_df is not None and len(northbeam_df) > 0:
                # Import the filtering function
                                                        # Removed import - functions are now merged into app.py
                northbeam_df = filter_attribution_data(northbeam_df, ACCOUNTING_MODE_FILTER, NORTHBEAM_PLATFORM)
                auto_hide_status_message(f"🔍 After filtering: {len(northbeam_df)} Northbeam rows", "info")
            
            # Check if we have at least one data source
            if meta_insights is None and northbeam_df is None:
                auto_hide_status_message("❌ Failed to fetch data from all selected sources", "error")
                return
            elif use_meta and meta_insights is None:
                auto_hide_status_message("❌ Failed to fetch Meta data", "error")
                return
            
            # Update progress - Step 3: Merging data
            sources_to_merge = []
            if use_meta and meta_insights:
                sources_to_merge.append("Meta")
            if use_northbeam and northbeam_df is not None:
                sources_to_merge.append("Northbeam")
            
            auto_hide_status_message(f"🔄 Step 3/4: Merging {' and '.join(sources_to_merge)} data...", "info")
            
            # Merge data into comprehensive objects
            comprehensive_ads = merge_data(northbeam_df, meta_insights, date_from, date_to)
            
            # Merge ads with same name (if enabled)
            if merge_ads:
                comprehensive_ads = merge_ads_with_same_name(comprehensive_ads, merge_by_campaign_type=True)
            
            # Clean any remaining NaN values
            comprehensive_ads = clean_nan_values(comprehensive_ads)
            
            # Update progress - Step 4: Saving data
            auto_hide_status_message("💾 Step 4/4: Saving comprehensive data...", "info")
            
            # Save comprehensive ad objects to reports directory
            if comprehensive_ads:
                # Save to S3
                s3_key = f"{ROOT_DIRECTORY}/processed/comprehensive_ads/comprehensive_ads_{date_from_formatted}-{date_to_formatted}.json"
                save_json_to_s3(comprehensive_ads, s3_key)
                
                # Save locally if enabled
                if DOWNLOAD_REPORTS_LOCALLY:
                    os.makedirs(f"{ROOT_DIRECTORY}/processed/comprehensive_ads", exist_ok=True)
                    with open(comprehensive_filename, 'w') as f:
                        json.dump(comprehensive_ads, f, indent=2)
                
                # Store data in session state for persistence across reruns
                st.session_state.comprehensive_ads = comprehensive_ads
                st.session_state.report_config = {
                    'date_from': date_from_str,  # Store string format for consistency
                    'date_to': date_to_str,      # Store string format for consistency
                    'top_n': top_n,
                    'core_products_input': core_products_input,
                    'merge_ads': merge_ads,
                    'use_northbeam': use_northbeam,
                    'use_meta': use_meta,
                    'date_from_formatted': date_from_formatted,
                    'date_to_formatted': date_to_formatted,
                    'fetched_data_sources': {
                        'meta': use_meta,
                        'northbeam': use_northbeam
                    }
                }
                
                # Debug: Show final data
                auto_hide_status_message(f"✅ Report generated successfully! {len(comprehensive_ads)} comprehensive ads created", "success")
                
                # Process existing media URLs immediately for instant display
                comprehensive_ads = process_existing_media_urls(comprehensive_ads)
                
                # Start background fetch for missing media URLs in a separate thread
                # This ensures the UI is not blocked while fetching URLs
                import threading
                def background_fetch():
                    try:
                        fetch_missing_media_urls(comprehensive_ads)
                    except Exception as e:
                        print(f"⚠️ Background media URL fetching failed: {e}")
                
                thread = threading.Thread(target=background_fetch, daemon=True)
                thread.start()
                print(f"🚀 Started background media URL fetching thread")
                    
            else:
                auto_hide_status_message("❌ Failed to generate report. Please check the console for errors.", "error")
                
        except Exception as e:
            auto_hide_status_message(f"❌ Error generating report: {str(e)}", "error")
            st.exception(e)
    
    # Background ad creatives processing removed - now using preview URLs directly
    
    # Display report and Google Doc generation (using session state data)
    
    if st.session_state.comprehensive_ads and st.session_state.report_config:
        # Check if cached data matches current date range
        config = st.session_state.report_config
        current_date_from = date_from
        current_date_to = date_to
        
        # Validate that cached data matches current date range
        # Convert current dates to strings for comparison with cached data
        current_date_from_str = current_date_from.strftime("%Y-%m-%d")
        current_date_to_str = current_date_to.strftime("%Y-%m-%d")
        
        if (config.get('date_from') != current_date_from_str or 
            config.get('date_to') != current_date_to_str):
            # Clear cached data if date range doesn't match
            st.session_state.comprehensive_ads = None
            st.session_state.report_config = None
            auto_hide_status_message("🔄 Date range changed. Please click 'Generate Report' to fetch fresh data for the new date range.", "info")
            return
        
        # Display context information in a clean, minimal layout
        # Use a unique key for the container to prevent re-rendering issues
        with st.container():
            # Single row with 4 columns: Date range, merge ads, data source, Google doc button
            col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
            
            with col1:
                st.caption(f"📅 Date Range: {config['date_from']} to {config['date_to']}")
            
            with col2:
                merge_status = "On" if config['merge_ads'] else "Off"
                st.caption(f"🔗 Merge Ads: {merge_status}")
            
            with col3:
                # Data source selection for CURRENT report viewing
                # This dropdown shows what data is available from the fetched report                
                # Create dynamic data source dropdown based on what data was actually fetched
                available_sources = []
                fetched_sources = config.get('fetched_data_sources', {})
                if fetched_sources.get('meta', True):
                    available_sources.append("Meta")
                if fetched_sources.get('northbeam', True):
                    available_sources.append("Northbeam")
                
                # Default to Northbeam if available, otherwise first available source
                if 'current_view_source' not in st.session_state:
                    if "Northbeam" in available_sources:
                        st.session_state.current_view_source = "Northbeam"
                    else:
                        st.session_state.current_view_source = available_sources[0] if available_sources else "Meta"
                
                # Create dropdown for data source selection
                if len(available_sources) > 1:
                    # Use columns to put label and dropdown on same line
                    label_col, dropdown_col = st.columns([1, 2])
                    with label_col:
                        st.caption(f"🟢 Data Source:")

                    with dropdown_col:
                        selected_source = st.selectbox(
                            "Select data source",
                            options=available_sources,
                            index=available_sources.index(st.session_state.current_view_source) if st.session_state.current_view_source in available_sources else 0,
                            key="data_source_selector",
                            label_visibility="collapsed"
                        )
                    
                    # Update session state when selection changes
                    if selected_source != st.session_state.current_view_source:
                        st.session_state.current_view_source = selected_source
                        st.rerun()
                    
                    data_source_display = selected_source
                    data_source_color = "🟢" if selected_source == "Northbeam" else "🔵"
                else:
                    # Only one source available, show as static text
                    data_source_display = available_sources[0] if available_sources else "Meta"
                    data_source_color = "🟢" if data_source_display == "Northbeam" else "🔵"
                    
                    # Use columns to put label and value on same line
                    label_col, value_col = st.columns([1, 2])
                    with label_col:
                        st.caption(f"🟢 Data Source:")

                    with value_col:
                        st.caption(f"{data_source_display}")
            
            with col4:
                # Google Doc generation button with unique key
                button_key = f"generate_google_doc_{config['date_from']}_{config['date_to']}"
                
                # Only show button if not currently generating
                if not st.session_state.get('is_generating_google_doc', False):
                    if st.button("📄 Generate Google Doc", type="secondary", key=button_key):
                        # Set generating flag and clear existing state
                        st.session_state.is_generating_google_doc = True
                        if 'google_doc_link' in st.session_state:
                            del st.session_state.google_doc_link
                        if 'markdown_content' in st.session_state:
                            del st.session_state.markdown_content
                        if 'report_filename' in st.session_state:
                            del st.session_state.report_filename
                        st.rerun()
                
                # Show spinner if generating
                if st.session_state.get('is_generating_google_doc', False):
                    with st.spinner(""):
                        try:
                            # Generate markdown report using the currently selected view source
                            current_view_source = st.session_state.get('current_view_source', 'Meta')
                            use_northbeam_for_report = current_view_source == "Northbeam"
                            
                            markdown_content = generate_markdown_report(
                                st.session_state.comprehensive_ads,
                                config['date_from'],
                                config['date_to'],
                                config['top_n'],
                                config['core_products_input'],
                                config['merge_ads'],
                                use_northbeam_for_report
                            )
                            
                            # Save markdown file to S3 and locally (if enabled)
                            # Use stored formatted dates directly
                            date_from_formatted = config['date_from_formatted']
                            date_to_formatted = config['date_to_formatted']
                            
                            # S3 path
                            s3_key = f"{ROOT_DIRECTORY}/reports/campaign_analysis_report_{date_from_formatted}-{date_to_formatted}.md"
                            
                            # Local path (same directory structure as S3)
                            local_filename = f"{ROOT_DIRECTORY}/reports/campaign_analysis_report_{date_from_formatted}-{date_to_formatted}.md"
                            
                            # Save to S3 (always overwrite)
                            try:
                                s3_client = get_s3_client()
                                s3_client.put_object(
                                    Bucket=S3_BUCKET,
                                    Key=s3_key,
                                    Body=markdown_content,
                                    ContentType='text/markdown'
                                )
                                print(f"✅ Saved markdown report to S3: s3://{S3_BUCKET}/{s3_key}")
                            except Exception as e:
                                print(f"⚠️ Failed to save markdown report to S3: {e}")
                            
                            # Save locally if enabled (always overwrite)
                            if DOWNLOAD_REPORTS_LOCALLY:
                                os.makedirs(f"{ROOT_DIRECTORY}/reports", exist_ok=True)
                                with open(local_filename, 'w') as f:
                                    f.write(markdown_content)
                                print(f"💾 Saved markdown report locally: {local_filename}")
                            else:
                                print(f"💾 Markdown report saved to S3 only (local saving disabled)")
                            
                            # Store the local filename for download button
                            report_filename = local_filename if DOWNLOAD_REPORTS_LOCALLY else f"{ROOT_DIRECTORY}/reports/campaign_analysis_report_{date_from_formatted}-{date_to_formatted}.md"
                            
                            # Export to Google Doc
                            doc_title = f"Thrive Causemetics Campaign Analysis {config['date_from']} to {config['date_to']}"
                            shareable_link = export_report_to_google_doc(report_filename, doc_title)
                            
                            if shareable_link:
                                # Store the link in session state to display below the button
                                st.session_state.google_doc_link = shareable_link
                                st.session_state.markdown_content = markdown_content
                                st.session_state.report_filename = report_filename
                                # Clear generating flag
                                st.session_state.is_generating_google_doc = False
                                # No status message - keep UI clean
                            else:
                                auto_hide_status_message("❌ Failed to create Google Doc", "error")
                                st.session_state.is_generating_google_doc = False
                                
                        except Exception as e:
                            auto_hide_status_message(f"❌ Error creating Google Doc: {str(e)}", "error")
                            st.exception(e)
                            st.session_state.is_generating_google_doc = False
                
                # Display Google Doc link and download button if available
                if 'google_doc_link' in st.session_state:
                    st.markdown(f"**📄 [View Google Doc]({st.session_state.google_doc_link})**")
                    
                    # Download .md file button
                    if 'markdown_content' in st.session_state and 'report_filename' in st.session_state:
                        markdown_content = st.session_state.markdown_content
                        report_filename = st.session_state.report_filename
                        
                        # Create download button for the markdown file with unique key
                        download_key = f"download_md_file_{config['date_from']}_{config['date_to']}"
                        st.download_button(
                            label="📥 Download .md File",
                            data=markdown_content,
                            file_name=os.path.basename(report_filename),
                            mime="text/markdown",
                            key=download_key
                        )
        
        
        # Export functionality can be added later if needed
        
        # Create tabs with minimal spacing
        tab1, tab2, tab3 = st.tabs(["📊 All Ads Summary", "🎯 Campaign Explorer", "🔍 Product/Creator Explorer"])
        
        with tab1:
            display_summary_tab(st.session_state.comprehensive_ads, config['top_n'])
        
        with tab2:
            display_campaign_explorer_tab(st.session_state.comprehensive_ads, config['top_n'], config['core_products_input'])
        
        with tab3:
            display_product_creator_explorer_tab(st.session_state.comprehensive_ads)
    
    else:
        # Welcome screen
        st.write("""
        **To get started:**\n
        ⬅️ Select Date Range, Configure Settings, and Generate Report in the sidebar
        """)
        

# ===== CACHE MANAGEMENT =====
# Note: Cache management is now handled by media_urls_manager and Streamlit session state

def list_s3_reports():
    """List all markdown reports in S3"""
    try:
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{ROOT_DIRECTORY}/reports/",
            MaxKeys=100
        )
        
        reports = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.md') and 'campaign_analysis_report' in key:
                    # Extract date range from filename
                    filename = key.split('/')[-1]
                    if filename.startswith('campaign_analysis_report_'):
                        date_part = filename.replace('campaign_analysis_report_', '').replace('.md', '')
                        if '-' in date_part:
                            date_from, date_to = date_part.split('-')
                            # Format dates for display
                            try:
                                date_from_formatted = f"{date_from[:4]}-{date_from[4:6]}-{date_from[6:8]}"
                                date_to_formatted = f"{date_to[:4]}-{date_to[4:6]}-{date_to[6:8]}"
                                reports.append({
                                    'key': key,
                                    'filename': filename,
                                    'date_from': date_from_formatted,
                                    'date_to': date_to_formatted,
                                    'last_modified': obj['LastModified']
                                })
                            except:
                                # Skip if date parsing fails
                                continue
        
        # Sort by last modified (newest first)
        reports.sort(key=lambda x: x['last_modified'], reverse=True)
        return reports
    except Exception as e:
        print(f"⚠️ Error listing S3 reports: {e}")
        return []

def get_ad_url(ad_id: str, ad_type: str = None) -> tuple[str, str]:
    """
    Get preview URL and thumbnail URL from media URLs cache based on ad_id.
    Now uses the media_urls_manager system for better performance and caching.
    
    Args:
        ad_id: The ad ID to look up
        ad_type: The ad type (not used in media URLs approach, kept for compatibility)
    
    Returns:
        Tuple of (preview_url, thumbnail_url)
    """
    try:
        # Load media URLs cache from media_urls_manager
        media_cache = load_media_urls_cache()
        
        if not media_cache:
            return "", ""
        
        ad_id_str = str(ad_id)
        if ad_id_str not in media_cache:
            return "", ""
        
        cached_data = media_cache[ad_id_str]
        
        if isinstance(cached_data, dict):
            # New format - preview URL and thumbnail
            preview_url = cached_data.get('preview_url', '')
            thumbnail_url = cached_data.get('thumbnail_url', '')
        elif isinstance(cached_data, str):
            # Old format - just preview URL
            preview_url = cached_data
            thumbnail_url = ''
        else:
            preview_url = ''
            thumbnail_url = ''
        
        return preview_url, thumbnail_url
        
    except Exception as e:
        print(f"Error getting media URLs for ad {ad_id}: {e}")
        return "", ""

def detect_ad_type_from_name(ad_name: str) -> str:
    """
    Detect ad type from ad name
    
    Args:
        ad_name: The ad name to analyze
        
    Returns:
        ad_type: "video", "static", "carousel", or "unknown"
    """
    ad_name_lower = ad_name.lower()
    
    if 'video' in ad_name_lower:
        return 'video'
    elif 'static' in ad_name_lower:
        return 'static'
    elif 'carousel' in ad_name_lower:
        return 'carousel'
    else:
        return 'unknown'

# ===== PREVIEW URL CACHING FUNCTIONS =====
# Note: These functions have been replaced by media_urls_manager functions
# - get_most_recent_cache_file() -> media_urls_manager.get_most_recent_cache_file()
# - load_preview_urls_cache() -> load_media_urls_cache()
# - save_preview_urls_cache() -> save_media_urls_cache() (handled internally by media_urls_manager)

# ===== PREVIEW URL CACHING FUNCTIONS =====
# Note: These functions have been replaced by media_urls_manager functions
# - load_preview_urls_cache() -> load_media_urls_cache()
# - save_preview_urls_cache() -> save_media_urls_cache() (handled internally by media_urls_manager)

# ===== BACKGROUND URL FETCHING SYSTEM =====
# Note: This functionality has been replaced by media_urls_manager.fetch_missing_media_urls()
# The function is now called directly in the main function for better integration

# ===== STANDALONE FUNCTION FOR EXTERNAL USE =====
# ===== STANDALONE FUNCTION FOR EXTERNAL USE =====
# Note: This function has been replaced by media_urls_manager.get_preview_urls_for_ads()
# Use the media_urls_manager module for all media URL operations

if __name__ == "__main__":
    main() 