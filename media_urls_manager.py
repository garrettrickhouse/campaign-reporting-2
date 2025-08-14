import json
import os
import time
import boto3
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ===== CONFIGURATION & CONSTANTS =====
# Meta Graph API Configuration
META_GRAPH_API_VERSION = os.getenv('META_GRAPH_API_VERSION')
GRAPH_BASE = f"https://graph.facebook.com/v{META_GRAPH_API_VERSION}"  
META_SYSTEM_USER_ACCESS_TOKEN = os.getenv('META_SYSTEM_USER_ACCESS_TOKEN')
AD_ACCOUNT_ID = os.getenv('META_AD_ACCOUNT_ID')

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = 'us-east-1'
S3_BUCKET = os.getenv('S3_BUCKET')

# Local storage configuration
DOWNLOAD_REPORTS_LOCALLY = True  # Set to True to save all fetched/processed data locally (in addition to S3)

ROOT_DIRECTORY = "campaign-reporting"

# ===== UTILITY FUNCTIONS =====
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

def get_s3_client():
    """Get S3 client with credentials"""
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

def save_json_to_s3(data, s3_key):
    """Save JSON data directly to S3"""
    try:
        s3_client = get_s3_client()
        json_data = json.dumps(data, indent=2)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data,
            ContentType='application/json'
        )
        print(f"✅ Saved JSON to S3: s3://{S3_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        print(f"❌ Failed to save JSON to S3: {e}")
        return False

# ===== MEDIA URLS CACHING FUNCTIONS =====
# Global cache to avoid repeated loading
_media_cache_singleton = None
_cache_loaded = False

def get_most_recent_cache_file():
    """Find the most recent media URLs cache file in S3"""
    try:
        s3_client = get_s3_client()
        
        # List all media URLs cache files
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{ROOT_DIRECTORY}/processed/media_urls_"
        )
        
        if 'Contents' not in response:
            return None
        
        # Find the most recent file
        cache_files = [obj['Key'] for obj in response['Contents'] 
                      if obj['Key'].endswith('.json')]
        
        if not cache_files:
            return None
        
        # Sort by last modified time (most recent first)
        cache_files_with_dates = []
        for key in cache_files:
            try:
                # Extract date from filename (media_urls_YYYYMMDD.json)
                date_str = key.split('_')[-1].replace('.json', '')
                if len(date_str) == 8:  # YYYYMMDD format
                    cache_files_with_dates.append((key, date_str))
            except:
                continue
        
        if not cache_files_with_dates:
            return None
        
        # Sort by date (newest first)
        cache_files_with_dates.sort(key=lambda x: x[1], reverse=True)
        return cache_files_with_dates[0][0]  # Return the most recent file key
        
    except Exception as e:
        print(f"⚠️ Could not list cache files: {e}")
        return None

def load_media_urls_cache():
    """Load media URLs cache from S3 with local fallback (singleton pattern)"""
    global _media_cache_singleton, _cache_loaded
    
    # Return cached version if already loaded
    if _cache_loaded and _media_cache_singleton is not None:
        return _media_cache_singleton
    
    try:
        # Try to load from S3 first
        most_recent_s3 = get_most_recent_cache_file()
        
        if most_recent_s3:
            try:
                s3_client = get_s3_client()
                response = s3_client.get_object(
                    Bucket=S3_BUCKET,
                    Key=most_recent_s3
                )
                cache_data = json.loads(response['Body'].read().decode('utf-8'))
                
                # Extract date from filename for logging
                date_str = most_recent_s3.split('_')[-1].replace('.json', '')
                print(f"✅ Loaded {len(cache_data)} cached media URLs from S3 {date_str}")
                
                # Store in singleton and mark as loaded
                _media_cache_singleton = cache_data
                _cache_loaded = True
                return cache_data
            except Exception as e:
                print(f"⚠️ Failed to load from S3: {e}, trying local files...")
        
        # Fallback to local files
        processed_dir = f"{ROOT_DIRECTORY}/processed"
        if os.path.exists(processed_dir):
            local_files = []
            for file in os.listdir(processed_dir):
                if file.startswith('media_urls_') and file.endswith('.json'):
                    local_files.append(os.path.join(processed_dir, file))
            
            if local_files:
                # Get the most recent file
                local_files.sort(reverse=True)
                latest_file = local_files[0]
                
                try:
                    with open(latest_file, 'r') as f:
                        cache_data = json.load(f)
                    
                    date_str = os.path.basename(latest_file).split('_')[-1].replace('.json', '')
                    print(f"✅ Loaded {len(cache_data)} cached media URLs from local file {date_str}")
                    
                    # Store in singleton and mark as loaded
                    _media_cache_singleton = cache_data
                    _cache_loaded = True
                    return cache_data
                except Exception as e:
                    print(f"⚠️ Failed to load local file {latest_file}: {e}")
        
        print("📋 No existing media URLs cache found")
        
        # Store empty cache in singleton and mark as loaded
        _media_cache_singleton = {}
        _cache_loaded = True
        return {}
        
    except Exception as e:
        print(f"⚠️ Could not load media URLs cache: {e}")
        
        # Store empty cache in singleton and mark as loaded
        _media_cache_singleton = {}
        _cache_loaded = True
        return {}

def save_media_urls_cache(cache_data):
    """Save media URLs cache to S3 and locally in processed directory"""
    try:
        # Create filename with current date
        current_date = datetime.now().strftime("%Y%m%d")
        cache_filename = f"media_urls_{current_date}.json"
        
        # Save to S3 (in processed directory)
        s3_key = f"{ROOT_DIRECTORY}/processed/{cache_filename}"
        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(cache_data, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Saved {len(cache_data)} media URLs to S3 cache ({current_date})")
        
        # Save locally if enabled
        if DOWNLOAD_REPORTS_LOCALLY:
            try:
                # Create directory structure
                os.makedirs(f"{ROOT_DIRECTORY}/processed", exist_ok=True)
                
                # Generate local filename
                local_filename = f"{ROOT_DIRECTORY}/processed/{cache_filename}"
                
                # Save JSON file
                with open(local_filename, 'w') as f:
                    json.dump(cache_data, f, indent=2)
                print(f"💾 Saved media URLs locally: {local_filename}")
            except Exception as e:
                print(f"⚠️ Failed to save media URLs locally: {e}")
        
    except Exception as e:
        print(f"❌ Failed to save media URLs cache: {e}")

# ===== MEDIA URLS PROCESSING FUNCTIONS =====
def _extract_ad_id(ad):
    """Extract ad_id from ad object, handling both flat and nested structures"""
    if isinstance(ad, dict):
        # Try nested structure first (comprehensive ads format)
        if 'ad_ids' in ad and isinstance(ad['ad_ids'], dict):
            return ad['ad_ids'].get('ad_id')
        # Try flat structure (legacy format)
        elif 'ad_id' in ad:
            return ad['ad_id']
    return None

def process_existing_media_urls(ad_objects, media_cache=None):
    """Process existing cached media URLs immediately for instant display"""
    if not ad_objects:
        return ad_objects
    
    print(f"🔗 Processing existing media URLs for {len(ad_objects)} ads...")
    
    # Load existing cache if not provided
    if media_cache is None:
        media_cache = load_media_urls_cache()
    
    # Create lookup dictionary for quick access, handling nested structure
    ad_lookup = {}
    for ad in ad_objects:
        ad_id = _extract_ad_id(ad)
        if ad_id:
            ad_lookup[ad_id] = ad
    
    # Process existing cached URLs immediately
    cached_count = 0
    for ad_id, ad in ad_lookup.items():
        if ad_id in media_cache and media_cache[ad_id]:
            cached_data = media_cache[ad_id]
            if isinstance(cached_data, dict):
                # New format - preview URL and thumbnail
                ad['link'] = cached_data.get('preview_url', '')
                ad['thumbnail_url'] = cached_data.get('thumbnail_url', '')
                cached_count += 1
            elif isinstance(cached_data, str):
                # Old format - just preview URL
                ad['link'] = cached_data
                ad['thumbnail_url'] = ''
                cached_count += 1
    
    print(f"📋 Applied {cached_count} cached media URLs for instant display")
    return ad_objects

def get_thumbnail_url_from_cache(ad_id, media_cache=None):
    """Get thumbnail URL from media URLs cache"""
    try:
        # Load cache if not provided
        if media_cache is None:
            media_cache = load_media_urls_cache()
        
        if ad_id in media_cache and media_cache[ad_id]:
            cached_data = media_cache[ad_id]
            if isinstance(cached_data, dict):
                return cached_data.get('thumbnail_url', '')
            elif isinstance(cached_data, str):
                # Old format - no thumbnail
                return ''
        return ''
    except Exception as e:
        print(f"⚠️ Error loading thumbnail URL from cache: {e}")
        return ''

def fetch_missing_media_urls(ad_objects, media_cache=None):
    """Background task to fetch missing media URLs (preview links and thumbnails)"""
    if not ad_objects:
        return
    
    print(f"🔗 Background: Fetching missing media URLs for {len(ad_objects)} ads...")
    
    # Load existing cache if not provided
    if media_cache is None:
        media_cache = load_media_urls_cache()
    
    # Extract unique ad IDs (filter out invalid ones and non-Facebook ads)
    ad_ids = []
    for ad in ad_objects:
        ad_id = _extract_ad_id(ad)
        if ad_id and ad_id != 'nan' and str(ad_id).lower() != 'nan':
            # Check if this ad has data (comprehensive ads format)
            has_meta_data = ad.get('metrics', {}).get('meta', {})
            has_northbeam_data = ad.get('metrics', {}).get('northbeam', {})
            
            if has_meta_data or has_northbeam_data:
                ad_ids.append(ad_id)
    
    ad_ids = list(set(ad_ids))  # Remove duplicates
    
    if not ad_ids:
        print("⚠️ No valid ad IDs found for preview links")
        return
    
    # Create lookup dictionary for quick access, handling nested structure
    ad_lookup = {}
    for ad in ad_objects:
        ad_id = _extract_ad_id(ad)
        if ad_id:
            ad_lookup[ad_id] = ad
    
    # Check which ad IDs need new URLs
    missing_ad_ids = []
    for ad_id in ad_ids:
        if ad_id not in media_cache or not media_cache[ad_id]:
            missing_ad_ids.append(ad_id)
        elif isinstance(media_cache[ad_id], str):
            # Old format - force refresh to get thumbnail URL
            missing_ad_ids.append(ad_id)
    
    if not missing_ad_ids:
        print("✅ Background: All media URLs already cached!")
        return
    
    print(f"📋 Background: Need to fetch {len(missing_ad_ids)} new URLs")
    
    # Process missing ad IDs in batches of 50
    batch_size = 50
    session = create_session_with_retries()
    new_urls_found = 0
    
    for i in range(0, len(missing_ad_ids), batch_size):
        batch_ad_ids = missing_ad_ids[i:i + batch_size]
        
        # Create batch requests
        batch_requests = []
        for ad_id in batch_ad_ids:
            batch_requests.append({
                "method": "GET",
                "relative_url": f"{ad_id}?fields=preview_shareable_link,creative{{thumbnail_url}}"
            })
        
        try:
            # Execute batch request
            response = session.post(
                f"{GRAPH_BASE}/",
                data={
                    "access_token": META_SYSTEM_USER_ACCESS_TOKEN,
                    "batch": json.dumps(batch_requests)
                },
                timeout=30
            )
            response.raise_for_status()
            
            batch_results = response.json()
            
            # Process results
            for idx, result in enumerate(batch_results):
                if idx >= len(batch_ad_ids):
                    break
                
                ad_id = batch_ad_ids[idx]
                
                if result.get("code") == 200:
                    try:
                        ad_data = json.loads(result["body"])
                        preview_url = ad_data.get("preview_shareable_link", "")
                        
                        # Extract thumbnail URL from nested creative object
                        thumbnail_url = ""
                        if 'creative' in ad_data and ad_data['creative']:
                            thumbnail_url = ad_data['creative'].get('thumbnail_url', '')
                        
                        # Update the ad object with the preview URL only
                        if ad_id in ad_lookup:
                            ad_lookup[ad_id]['link'] = preview_url
                        
                        # Add to cache (store both preview URL and thumbnail)
                        if preview_url or thumbnail_url:
                            if ad_id not in media_cache:
                                media_cache[ad_id] = {}
                            media_cache[ad_id]['preview_url'] = preview_url
                            media_cache[ad_id]['thumbnail_url'] = thumbnail_url
                            new_urls_found += 1
                            
                    except json.JSONDecodeError as e:
                        print(f"⚠️ Error parsing response for ad {ad_id}: {e}")
                else:
                    print(f"⚠️ Failed to get preview link for ad {ad_id}: {result.get('code')}")
            
            print(f"📦 Processed batch {i//batch_size + 1}/{(len(missing_ad_ids) + batch_size - 1)//batch_size}")
            
            # Save cache incrementally after each batch to preserve progress
            if new_urls_found > 0:
                save_media_urls_cache(media_cache)
                print(f"💾 Saved batch {i//batch_size + 1} - {new_urls_found} URLs cached so far")
            
        except Exception as e:
            print(f"❌ Batch request failed: {e}")
        
        # Rate limiting
        time.sleep(0.5)
    
    print(f"✅ Background: Processed media URLs for {len(ad_ids)} ads ({new_urls_found} total new)")

# ===== MAIN MEDIA URLS FUNCTION =====
def get_media_urls_for_ads(ad_objects, use_cache=True, background_fetch=True):
    """
    Main function to get media URLs (preview links and thumbnails) for a list of ad objects.
    
    Args:
        ad_objects (list): List of ad objects with 'ad_id' field
        use_cache (bool): Whether to use cached URLs (default: True)
        background_fetch (bool): Whether to fetch missing URLs in background (default: True)
    
    Returns:
        list: Updated ad objects with 'link' and 'thumbnail_url' fields populated
    """
    if not ad_objects:
        return ad_objects
    
    print(f"🔗 Processing media URLs for {len(ad_objects)} ads...")
    
    # Load existing cache
    if use_cache:
        media_cache = load_media_urls_cache()
    else:
        media_cache = {}
    
    # Process existing cached URLs immediately for instant display
    ad_objects = process_existing_media_urls(ad_objects, media_cache)
    
    # Background fetch for missing URLs if requested
    if background_fetch:
        # Start background task (in a real web app, this would be async)
        fetch_missing_media_urls(ad_objects, media_cache)
    
    return ad_objects

# ===== STANDALONE FUNCTIONS FOR EXTERNAL USE =====
def get_preview_urls_for_ads(ad_objects, use_cache=True):
    """
    Standalone function to get preview URLs for a list of ad objects.
    This maintains backward compatibility with existing code.
    
    Args:
        ad_objects (list): List of ad objects with 'ad_id' field
        use_cache (bool): Whether to use cached URLs (default: True)
    
    Returns:
        list: Updated ad objects with 'link' field populated
    """
    return get_media_urls_for_ads(ad_objects, use_cache=use_cache, background_fetch=False)

def get_thumbnail_urls_for_ads(ad_objects, use_cache=True):
    """
    Standalone function to get thumbnail URLs for a list of ad objects.
    
    Args:
        ad_objects (list): List of ad objects with 'ad_id' field
        use_cache (bool): Whether to use cached URLs (default: True)
    
    Returns:
        list: Updated ad objects with 'thumbnail_url' field populated
    """
    if not ad_objects:
        return ad_objects
    
    # Load existing cache
    if use_cache:
        media_cache = load_media_urls_cache()
    else:
        media_cache = {}
    
    # Process existing cached URLs immediately
    for ad in ad_objects:
        ad_id = ad.get('ad_id')
        if ad_id:
            ad['thumbnail_url'] = get_thumbnail_url_from_cache(ad_id, media_cache)
    
    return ad_objects

# ===== CACHE MANAGEMENT FUNCTIONS =====
def clear_media_urls_cache():
    """Clear the media URLs cache (useful for testing or forcing refresh)"""
    global _media_cache_singleton, _cache_loaded
    
    try:
        # Clear singleton cache
        _media_cache_singleton = None
        _cache_loaded = False
        
        # Clear local cache files
        processed_dir = f"{ROOT_DIRECTORY}/processed"
        if os.path.exists(processed_dir):
            for file in os.listdir(processed_dir):
                if file.startswith('media_urls_') and file.endswith('.json'):
                    file_path = os.path.join(processed_dir, file)
                    os.remove(file_path)
                    print(f"🗑️ Removed local cache file: {file}")
        
        print("✅ Media URLs cache cleared")
        return True
    except Exception as e:
        print(f"❌ Failed to clear cache: {e}")
        return False

def get_cache_stats():
    """Get statistics about the media URLs cache"""
    try:
        media_cache = load_media_urls_cache()
        
        total_entries = len(media_cache)
        preview_urls = 0
        thumbnail_urls = 0
        old_format = 0
        
        for ad_id, data in media_cache.items():
            if isinstance(data, dict):
                if data.get('preview_url'):
                    preview_urls += 1
                if data.get('thumbnail_url'):
                    thumbnail_urls += 1
            elif isinstance(data, str):
                old_format += 1
                preview_urls += 1
        
        return {
            'total_entries': total_entries,
            'preview_urls': preview_urls,
            'thumbnail_urls': thumbnail_urls,
            'old_format_entries': old_format,
            'new_format_entries': total_entries - old_format
        }
    except Exception as e:
        print(f"❌ Failed to get cache stats: {e}")
        return {}

# ===== EXAMPLE USAGE =====
if __name__ == "__main__":
    # Example of how to use this module
    print("🔗 Media URLs Manager")
    print("This module provides functions to fetch and cache Meta ad media URLs (preview URLs and thumbnails).")
    print("\nMain functions:")
    print("- get_media_urls_for_ads(ad_objects, use_cache=True, background_fetch=True): Main function for getting both preview URLs and thumbnails")
    print("- get_preview_urls_for_ads(ad_objects, use_cache=True): Get preview URLs only (backward compatible)")
    print("- get_thumbnail_urls_for_ads(ad_objects, use_cache=True): Get thumbnail URLs only")
    print("- process_existing_media_urls(ad_objects, media_cache=None): Process cached URLs immediately")
    print("- fetch_missing_media_urls(ad_objects, media_cache=None): Background fetch for missing URLs")
    print("- load_media_urls_cache(): Load existing cache")
    print("- save_media_urls_cache(cache_data): Save cache to S3/local")
    print("- clear_media_urls_cache(): Clear cache")
    print("- get_cache_stats(): Get cache statistics")
    
    print("\nExample usage:")
    print("from media_urls_manager import get_media_urls_for_ads")
    print("")
    print("# Your ad objects should have 'ad_id' field")
    print("ad_objects = [{'ad_id': '123456789', 'ad_name': 'Test Ad'}]")
    print("")
    print("# Get both preview URLs and thumbnails")
    print("updated_ads = get_media_urls_for_ads(ad_objects)")
    print("")
    print("# Each ad will now have 'link' and 'thumbnail_url' fields")
    print("for ad in updated_ads:")
    print("    print(f\"Ad: {ad['ad_name']}, Preview: {ad.get('link', 'No link')}, Thumbnail: {ad.get('thumbnail_url', 'No thumbnail')}\")")
    
    print("\n# Cache management")
    print("from media_urls_manager import get_cache_stats, clear_media_urls_cache")
    print("stats = get_cache_stats()")
    print("print(f\"Cache stats: {stats}\")")
    print("# clear_media_urls_cache()  # Uncomment to clear cache")
