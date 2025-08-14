import json
import os
import requests
import time
import boto3
from typing import Dict, List, Tuple, Optional, Set
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ===== CONFIGURATION & CONSTANTS =====
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = 'us-east-1'
S3_BUCKET = os.getenv('S3_BUCKET')
DOWNLOAD_REPORTS_LOCALLY = False  # Set to True to save all fetched/processed data locally (in addition to S3)

# ===== UTILITY FUNCTIONS =====
def get_s3_client():
    """Get S3 client with credentials"""
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

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
        return True
    except Exception as e:
        print(f"⚠️ S3 access denied or unavailable: {e}")
        print(f"📁 Falling back to local storage only")
        return False

def load_json_from_s3(s3_key):
    """Load JSON data from S3"""
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
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

def get_master_urls_filename():
    """Generate master URLs filename with current date"""
    current_date = datetime.now().strftime("%Y%m%d")
    return f"campaign-reporting/processed/master_urls/master_urls_{current_date}.json"

# ===== CACHE MANAGEMENT =====
# Global flag to control whether to use cached files
USE_CACHED_FILES = True

# Global cache for processed data to avoid repeated S3 calls
_PROCESSED_DATA_CACHE = None
_CACHE_LOADED = False

def set_use_cached_files(value: bool):
    """Set whether to use cached files for meta_insights and northbeam data"""
    global USE_CACHED_FILES
    USE_CACHED_FILES = value
    print(f"🔄 Use cached files set to: {value}")

def get_processed_data_cache():
    """Get cached processed data, loading from S3/local if needed"""
    global _processed_data_cache
    
    # Return cached data if available
    if _processed_data_cache is not None:
        print(f"✅ Using cached processed data ({len(_processed_data_cache)} ads)")
        return _processed_data_cache
    
    try:
        # Try to find the most recent master URLs file
        try:
            # List all master URLs files in S3
            s3_client = get_s3_client()
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix="campaign-reporting/processed/master_urls/master_urls_",
                MaxKeys=100
            )
            
            if 'Contents' in response:
                # Sort by date (newest first)
                files = sorted(
                    [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')],
                    reverse=True
                )
                
                if files:
                    # Load the most recent file
                    most_recent_key = files[0]
                    try:
                        processed_data = load_json_from_s3(most_recent_key)
                        if processed_data:
                            _processed_data_cache = processed_data
                            return processed_data
                    except Exception as e:
                        print(f"⚠️ Error loading most recent master URLs from S3: {e}")
            
        except Exception as e:
            print(f"⚠️ Error listing master URLs files in S3: {e}")
        
        # Fallback to local files (only if DOWNLOAD_REPORTS_LOCALLY is True)
        if DOWNLOAD_REPORTS_LOCALLY:
            try:
                local_dir = "campaign-reporting/processed/master_urls"
                if os.path.exists(local_dir):
                    files = [f for f in os.listdir(local_dir) if f.startswith("master_urls_") and f.endswith(".json")]
                    if files:
                        # Sort by date (newest first)
                        files.sort(reverse=True)
                        most_recent_file = os.path.join(local_dir, files[0])
                        with open(most_recent_file, 'r') as f:
                            processed_data = json.load(f)
                            print(f"✅ Loaded most recent master URLs locally: {most_recent_file}")
                            _processed_data_cache = processed_data
                            return processed_data
            except Exception as e:
                print(f"⚠️ Error loading local master URLs: {e}")
        else:
            print("📁 Skipping local file lookup (DOWNLOAD_REPORTS_LOCALLY = False)")
        
        print("❌ No processed data found in S3 or local storage")
        return None
        
    except Exception as e:
        print(f"Error loading processed data: {e}")
        return None

def _get_cached_processed_data():
    """Get cached processed data, loading it once if needed"""
    global _PROCESSED_DATA_CACHE, _CACHE_LOADED
    
    if not _CACHE_LOADED:
        _PROCESSED_DATA_CACHE = get_processed_data_cache()
        _CACHE_LOADED = True
        if _PROCESSED_DATA_CACHE:
            print(f"✅ Loaded processed data cache ({len(_PROCESSED_DATA_CACHE)} ads)")
        else:
            print("⚠️ No processed data available")
    
    return _PROCESSED_DATA_CACHE

def clear_processed_data_cache():
    """Clear the processed data cache to force reload"""
    global _PROCESSED_DATA_CACHE, _CACHE_LOADED
    _PROCESSED_DATA_CACHE = None
    _CACHE_LOADED = False
    print("🔄 Cleared processed data cache")

# ===== META AD CREATIVES PROCESSOR =====
class MetaAdCreativesProcessor:
    """Clean and efficient processor for Meta ad creatives URLs"""
    
    def __init__(self, access_token: str, ad_account_id: str, page_id: str = None, graph_base: str = "https://graph.facebook.com/v23.0"):
        self.access_token = access_token
        self.ad_account_id = ad_account_id
        self.page_id = page_id
        self.graph_base = graph_base
        self.batch_size = 50
        
    def get_filename(self, file_type: str, date_from: str = None, date_to: str = None) -> str:
        """Generate standardized filenames"""
        if file_type == "raw":
            # Raw file is date-specific
            date_from_clean = date_from.replace('-', '') if date_from else "temp"
            date_to_clean = date_to.replace('-', '') if date_to else "temp"
            return f"campaign-reporting/raw/meta_adcreatives/meta_adcreatives_{date_from_clean}-{date_to_clean}.json"
        elif file_type == "processed":
            # Processed file uses current date
            return get_master_urls_filename()
        else:
            return f"campaign-reporting/raw/meta_adcreatives/meta_adcreatives_{file_type}.json"
    
    def load_processed_data(self, date_from: str = None, date_to: str = None) -> Dict:
        """Load existing processed data - finds most recent master URLs file"""
        # Try to find the most recent master URLs file
        try:
            # List all master URLs files in S3
            s3_client = get_s3_client()
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix="campaign-reporting/processed/master_urls/master_urls_",
                MaxKeys=100
            )
            
            if 'Contents' in response:
                # Sort by date (newest first)
                files = sorted(
                    [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')],
                    reverse=True
                )
                
                if files:
                    # Load the most recent file
                    most_recent_key = files[0]
                    try:
                        data = load_json_from_s3(most_recent_key)
                        if data:
                            return data
                    except Exception as e:
                        print(f"⚠️ Error loading most recent master URLs from S3: {e}")
            
        except Exception as e:
            print(f"⚠️ Error listing master URLs files in S3: {e}")
        
        # Fallback to local files
        try:
            local_dir = "campaign-reporting/processed/master_urls"
            if os.path.exists(local_dir):
                files = [f for f in os.listdir(local_dir) if f.startswith("master_urls_") and f.endswith(".json")]
                if files:
                    # Sort by date (newest first)
                    files.sort(reverse=True)
                    most_recent_file = os.path.join(local_dir, files[0])
                    with open(most_recent_file, 'r') as f:
                        data = json.load(f)
                        print(f"✅ Loaded most recent master URLs locally: {most_recent_file}")
                        return data
        except Exception as e:
            print(f"⚠️ Error loading local master URLs: {e}")
        
        return {}
    
    def save_processed_data(self, data: Dict, date_from: str = None, date_to: str = None):
        """Save processed data"""
        filename = self.get_filename("processed", date_from, date_to)
        
        # Save to S3
        s3_key = filename
        save_json_to_s3(data, s3_key)
        
        # Save locally if enabled
        if DOWNLOAD_REPORTS_LOCALLY:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
        else:
            print(f"💾 Master URLs saved to S3 only (local saving disabled)")
        
        # Clear the processed data cache to force reload of updated data
        clear_processed_data_cache()
        print("🔄 Cleared processed data cache to ensure fresh data on next access")
    
    def save_raw_data(self, data: Dict, date_from: str, date_to: str):
        """Save raw adcreatives data"""
        filename = self.get_filename("raw", date_from, date_to)
        
        # Save to S3
        s3_key = filename
        save_json_to_s3(data, s3_key)
        
        # Save locally if enabled
        if DOWNLOAD_REPORTS_LOCALLY:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
        else:
            print(f"💾 Meta ad creatives raw data saved to S3 only (local saving disabled)")
    
    def identify_missing_ads(self, ad_ids: List[str], processed_data: Dict) -> List[str]:
        """Step 1: Identify ads missing media URLs or needing next priority attempt"""
        missing_ads = []
        
        for ad_id in ad_ids:
            ad_id_str = str(ad_id)
            
            # Check if ad exists in processed data
            if ad_id_str not in processed_data:
                missing_ads.append(ad_id_str)
                continue
            
            ad_data = processed_data[ad_id_str]
            
            # Check if we have successfully obtained URLs
            has_video_urls = bool(ad_data.get("video_source_url") or ad_data.get("video_permalink_url"))
            has_image_urls = bool(ad_data.get("image_url") or ad_data.get("image_permalink_url"))
            
            # Check if we have priority tracking and haven't exhausted all attempts
            current_priority = ad_data.get("priority", 0)
            max_priority_attempts = ad_data.get("max_priority_attempts", 0)
            
            # Include ad if:
            # 1. We haven't successfully obtained URLs yet, OR
            # 2. We haven't exhausted all priority attempts yet
            if not (has_video_urls or has_image_urls) or (max_priority_attempts > 0 and current_priority < max_priority_attempts - 1):
                missing_ads.append(ad_id_str)
        
        return missing_ads
    
    def fetch_raw_adcreatives(self, ad_ids: List[str], date_from: str, date_to: str) -> Dict:
        """Steps 2-3: Batch fetch raw adcreatives and save"""
        print(f"🔄 Fetching raw adcreatives for {len(ad_ids)} ads...")
        
        raw_data = {}
        headers = {'Authorization': f'Bearer {self.access_token}'}
        
        # Creative fields for comprehensive data extraction
        fields = (
            "thumbnail_url,"
            "asset_feed_spec{ad_formats,images{hash,url,adlabels},videos{video_id,adlabels,thumbnail_url,thumbnail_hash},"
            "asset_customization_rules},"
            "object_story_spec{link_data{image_hash,picture,preferred_image_tags,preferred_video_tags},"
            "template_data{child_attachments},video_data{video_id,image_url,image_hash},"
            "photo_data{url,image_hash}}"
        )
        
        # Process in batches
        for i in range(0, len(ad_ids), self.batch_size):
            batch_ads = ad_ids[i:i + self.batch_size]
            
            # Prepare batch requests
            batch_requests = []
            for ad_id in batch_ads:
                batch_requests.append({
                    "method": "GET",
                    "relative_url": f"{ad_id}/adcreatives?fields={fields}"
                })
            
            # Execute batch request
            try:
                response = requests.post(
                    f"{self.graph_base}/",
                    data={
                        "access_token": self.access_token,
                        "batch": json.dumps(batch_requests)
                    },
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                
                batch_results = response.json()
                
                # Process results
                for idx, result in enumerate(batch_results):
                    if idx >= len(batch_ads):
                        break
                    
                    ad_id = batch_ads[idx]
                    
                    if result.get("code") == 200:
                        try:
                            creative_data = json.loads(result["body"])
                            raw_data[ad_id] = creative_data
                        except json.JSONDecodeError as e:
                            raw_data[ad_id] = {"error": f"JSON parse error: {e}"}
                    else:
                        raw_data[ad_id] = {"error": f"API error {result.get('code')}"}
                
            except Exception as e:
                print(f"❌ Batch request failed: {e}")
                for ad_id in batch_ads:
                    if ad_id not in raw_data:
                        raw_data[ad_id] = {"error": f"Request failed: {e}"}
            
            time.sleep(0.2)  # Rate limiting
            print(f"📦 Processed {min(i + self.batch_size, len(ad_ids))}/{len(ad_ids)} ads")
        
        # Save raw data
        self.save_raw_data(raw_data, date_from, date_to)
        print(f"✅ Raw adcreatives saved")
        
        return raw_data

    def update_thumbnails_for_existing_ads(self, raw_data: Dict, processed_data: Dict) -> Dict:
        """Update video_thumbnail_url for existing ads that have raw data available"""
        print(f"🖼️ Updating thumbnails for existing ads with raw data...")
        
        updated_count = 0
        for ad_id, creative_data in raw_data.items():
            if "error" in creative_data or "data" not in creative_data:
                continue
            
            if not creative_data["data"]:
                continue
            
            # Only update if ad exists in processed data and has empty video_thumbnail_url
            if ad_id in processed_data and not processed_data[ad_id].get("video_thumbnail_url"):
                creative = creative_data["data"][0]
                
                # Extract video thumbnail from asset feed (priority 1)
                asset_feed = creative.get("asset_feed_spec", {})
                videos = asset_feed.get("videos", [])
                if videos:
                    # Get priority 1 video (lowest priority number)
                    customization_rules = asset_feed.get("asset_customization_rules", [])
                    priority_map = {}
                    
                    for rule in customization_rules:
                        video_label = rule.get("video_label", {})
                        if video_label.get("id") and rule.get("priority"):
                            priority_map[video_label["id"]] = rule["priority"]
                    
                    # Find video with priority 1
                    priority_1_video = None
                    for video in videos:
                        adlabels = video.get("adlabels", [])
                        label_id = adlabels[0].get("id") if adlabels else None
                        priority = priority_map.get(label_id, 999) if label_id else 999
                        
                        if priority == 1:
                            priority_1_video = video
                            break
                    
                    # If no priority 1 found, use first video
                    if not priority_1_video and videos:
                        priority_1_video = videos[0]
                    
                    if priority_1_video:
                        video_thumbnail_url = priority_1_video.get("thumbnail_url", "")
                        if video_thumbnail_url:
                            processed_data[ad_id]["video_thumbnail_url"] = video_thumbnail_url
                            updated_count += 1
                            print(f"✅ Updated video_thumbnail_url for ad {ad_id}")
        
        print(f"🖼️ Updated thumbnails for {updated_count} existing ads")
        return processed_data
    
    def add_thumbnails_to_processed(self, raw_data: Dict, processed_data: Dict, missing_ads: List[str]) -> Dict:
        """Step 4: Add missing ads to processed data with thumbnails"""
        print(f"🖼️ Adding thumbnails for {len(missing_ads)} ads...")
        
        for ad_id in missing_ads:
            if ad_id in raw_data and "error" not in raw_data[ad_id]:
                # Extract thumbnail from raw data
                thumbnail_url = ""
                video_thumbnail_url = ""
                creative_data = raw_data[ad_id]
                
                if "data" in creative_data and creative_data["data"]:
                    creative = creative_data["data"][0]
                    thumbnail_url = creative.get("thumbnail_url", "")
                    
                    # Extract video thumbnail from asset feed (priority 1)
                    asset_feed = creative.get("asset_feed_spec", {})
                    videos = asset_feed.get("videos", [])
                    if videos:
                        # Get priority 1 video (lowest priority number)
                        customization_rules = asset_feed.get("asset_customization_rules", [])
                        priority_map = {}
                        
                        for rule in customization_rules:
                            video_label = rule.get("video_label", {})
                            if video_label.get("id") and rule.get("priority"):
                                priority_map[video_label["id"]] = rule["priority"]
                        
                        # Find video with priority 1
                        priority_1_video = None
                        for video in videos:
                            adlabels = video.get("adlabels", [])
                            label_id = adlabels[0].get("id") if adlabels else None
                            priority = priority_map.get(label_id, 999) if label_id else 999
                            
                            if priority == 1:
                                priority_1_video = video
                                break
                        
                        # If no priority 1 found, use first video
                        if not priority_1_video and videos:
                            priority_1_video = videos[0]
                        
                        if priority_1_video:
                            video_thumbnail_url = priority_1_video.get("thumbnail_url", "")
                
                # Initialize processed entry
                processed_data[ad_id] = {
                    "ad_id": ad_id,
                    "thumbnail_url": thumbnail_url,  # Basic thumbnail from ad creative
                    "video_thumbnail_url": video_thumbnail_url,  # High-quality thumbnail from asset feed
                    "video_source_url": "",
                    "video_permalink_url": "",
                    "image_url": "",
                    "image_permalink_url": "",
                    "video_id": "",
                    "image_hash": "",
                    "priority": 0,
                    "max_priority_attempts": 0,  # Will be set when we extract assets
                    "all_video_ids": [],  # Track all available video IDs
                    "all_image_hashes": []  # Track all available image hashes
                }
        
        return processed_data
    
    def extract_media_assets(self, asset_feed: Dict, object_story: Dict) -> List[Tuple[str, str, float, str]]:
        """Extract and prioritize media assets from creative data"""
        assets = []
        
        # 1. Asset feed videos (with priority)
        videos = asset_feed.get("videos", [])
        if videos:
            customization_rules = asset_feed.get("asset_customization_rules", [])
            priority_map = {}
            
            for rule in customization_rules:
                video_label = rule.get("video_label", {})
                if video_label.get("id") and rule.get("priority"):
                    priority_map[video_label["id"]] = rule["priority"]
            
            for idx, video in enumerate(videos):
                video_id = video.get("video_id")
                if video_id:
                    adlabels = video.get("adlabels", [])
                    label_id = adlabels[0].get("id") if adlabels else None
                    priority = priority_map.get(label_id, 999) if label_id else 999
                    assets.append(("video", video_id, priority, f"asset_feed_{idx}"))
        
        # 2. Asset feed images (with priority)
        images = asset_feed.get("images", [])
        if images:
            customization_rules = asset_feed.get("asset_customization_rules", [])
            priority_map = {}
            
            for rule in customization_rules:
                image_label = rule.get("image_label", {})
                if image_label.get("id") and rule.get("priority"):
                    priority_map[image_label["id"]] = rule["priority"]
            
            for idx, image in enumerate(images):
                image_hash = image.get("hash")
                if image_hash:
                    adlabels = image.get("adlabels", [])
                    label_id = adlabels[0].get("id") if adlabels else None
                    priority = priority_map.get(label_id, 999) if label_id else 999
                    assets.append(("image", image_hash, priority, f"asset_feed_{idx}"))
        
        # 3. Object story assets (lower priority)
        video_data = object_story.get("video_data", {})
        if video_data.get("video_id"):
            assets.append(("video", video_data["video_id"], 1000, "object_story_video"))
        
        photo_data = object_story.get("photo_data", {})
        if photo_data.get("image_hash"):
            assets.append(("image", photo_data["image_hash"], 1000, "object_story_photo"))
        
        link_data = object_story.get("link_data", {})
        if link_data.get("image_hash"):
            assets.append(("image", link_data["image_hash"], 1000, "object_story_link"))
        
        # Sort by priority (lower number = higher priority)
        assets.sort(key=lambda x: x[2])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_assets = []
        for asset in assets:
            if asset[1] not in seen:
                seen.add(asset[1])
                unique_assets.append(asset)
        
        return unique_assets
    
    def get_page_token(self) -> Optional[str]:
        """Get page access token for enhanced permissions"""
        if not self.page_id:
            return None
        
        try:
            url = f"{self.graph_base}/{self.page_id}?fields=access_token"
            headers = {'Authorization': f'Bearer {self.access_token}'}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data.get("access_token")
        except Exception as e:
            print(f"❌ Failed to get page token: {e}")
            return None
    
    def batch_fetch_video_urls(self, video_ids: List[str]) -> Dict[str, Dict]:
        """Batch fetch video URLs with thumbnails"""
        if not video_ids:
            return {}
        
        print(f"🎬 Fetching URLs for {len(video_ids)} videos...")
        
        video_urls = {}
        tokens = [("system", self.access_token)]
        
        # Add page token if available
        page_token = self.get_page_token()
        if page_token:
            tokens.insert(0, ("page", page_token))
        
        # Try tokens in order (page first, then system)
        for token_name, token in tokens:
            if len(video_urls) >= len(video_ids):
                break
            
            remaining_videos = [vid for vid in video_ids if vid not in video_urls]
            if not remaining_videos:
                break
            
            # Process in batches
            for i in range(0, len(remaining_videos), self.batch_size):
                batch_videos = remaining_videos[i:i + self.batch_size]
                
                batch_requests = []
                for video_id in batch_videos:
                    batch_requests.append({
                        "method": "GET",
                        "relative_url": f"{video_id}?fields=id,permalink_url,source,thumbnails"
                    })
                
                try:
                    response = requests.post(
                        f"{self.graph_base}/",
                        data={
                            "access_token": token,
                            "batch": json.dumps(batch_requests)
                        },
                        headers={'Authorization': f'Bearer {token}'},
                        timeout=30
                    )
                    response.raise_for_status()
                    
                    batch_results = response.json()
                    
                    for idx, result in enumerate(batch_results):
                        if idx >= len(batch_videos):
                            break
                        
                        video_id = batch_videos[idx]
                        
                        if result.get("code") == 200:
                            try:
                                video_data = json.loads(result["body"])
                                
                                # Extract thumbnail
                                thumbnail_url = ""
                                thumbnails = video_data.get("thumbnails", {}).get("data", [])
                                if thumbnails:
                                    preferred = next((t for t in thumbnails if t.get("is_preferred")), None)
                                    thumbnail_url = (preferred or thumbnails[0]).get("uri", "")
                                
                                # Ensure permalink URL has full Facebook domain
                                permalink_url = video_data.get("permalink_url", "")
                                if permalink_url and permalink_url.startswith('/'):
                                    permalink_url = f"https://www.facebook.com{permalink_url}"
                                
                                video_urls[video_id] = {
                                    "source": video_data.get("source", ""),
                                    "permalink": permalink_url,
                                    "thumbnail": thumbnail_url
                                }
                            except json.JSONDecodeError:
                                continue
                
                except Exception as e:
                    print(f"❌ Video batch failed with {token_name}: {e}")
                    continue
                
                time.sleep(0.2)
        
        print(f"✅ Retrieved {len(video_urls)} video URLs")
        return video_urls
    
    def batch_fetch_image_urls(self, image_hashes: List[str]) -> Dict[str, Dict]:
        """Batch fetch image URLs"""
        if not image_hashes:
            return {}
        
        print(f"🖼️ Fetching URLs for {len(image_hashes)} images...")
        
        image_urls = {}
        
        # Process in batches using adimages endpoint
        for i in range(0, len(image_hashes), self.batch_size):
            batch_hashes = image_hashes[i:i + self.batch_size]
            
            try:
                url = f"{self.graph_base}/act_{self.ad_account_id}/adimages"
                params = {
                    'hashes': json.dumps(batch_hashes),
                    'fields': 'url,permalink_url',
                    'access_token': self.access_token
                }
                
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                for img in data.get("data", []):
                    # Extract hash from id (format: "account_id:hash")
                    img_id = img.get("id", "")
                    if ":" in img_id:
                        hash_part = img_id.split(":")[1]
                        # Ensure permalink URL has full Facebook domain
                        permalink_url = img.get("permalink_url", "")
                        if permalink_url and permalink_url.startswith('/'):
                            permalink_url = f"https://www.facebook.com{permalink_url}"
                        
                        image_urls[hash_part] = {
                            "url": img.get("url", ""),
                            "permalink": permalink_url
                        }
                
            except Exception as e:
                print(f"❌ Image batch failed: {e}")
                continue
            
            time.sleep(0.2)
        
        print(f"✅ Retrieved {len(image_urls)} image URLs")
        return image_urls

    def process_media_urls(self, raw_data: Dict, processed_data: Dict, ad_types: Dict) -> Dict:
        """Steps 5-6: Process raw data to get media URLs by ad type"""
        print(f"🔄 Processing media URLs by ad type...")
        
        # Group ads by type and extract prioritized assets
        # All ads will be processed as "mixed" (like carousel) to handle both video and image assets
        mixed_ads = {}
        
        # Process ads from raw_data (new ads)
        for ad_id, creative_data in raw_data.items():
            if "error" in creative_data or "data" not in creative_data:
                continue
            
            if not creative_data["data"]:
                continue
            
            creative = creative_data["data"][0]
            asset_feed = creative.get("asset_feed_spec", {})
            object_story = creative.get("object_story_spec", {})
            
            # Extract prioritized assets
            assets = self.extract_media_assets(asset_feed, object_story)
            if not assets:
                continue
            
            # Store all available assets in processed data for future retries
            if ad_id in processed_data:
                video_assets = [a for a in assets if a[0] == "video"]
                image_assets = [a for a in assets if a[0] == "image"]
                
                processed_data[ad_id]["all_video_ids"] = [a[1] for a in video_assets]
                processed_data[ad_id]["all_image_hashes"] = [a[1] for a in image_assets]
                processed_data[ad_id]["max_priority_attempts"] = len(assets)
            
            # All ads are processed as mixed (like carousel) to handle both video and image assets
            mixed_ads[ad_id] = {
                "assets": assets,
                "ad_type": ad_types.get(ad_id, "").lower()
            }
        
        # Process existing ads that need URL processing (not in raw_data)
        existing_ads_processed = 0
        for ad_id, ad_data in processed_data.items():
            # Skip if we already processed this ad from raw_data
            if ad_id in raw_data:
                continue
            
            # Check if this ad needs URL processing
            has_video_urls = bool(ad_data.get("video_source_url") or ad_data.get("video_permalink_url"))
            has_image_urls = bool(ad_data.get("image_url") or ad_data.get("image_permalink_url"))
            current_priority = ad_data.get("priority", 0)
            max_priority_attempts = ad_data.get("max_priority_attempts", 0)
            
            # Skip if we have URLs or exhausted attempts
            if (has_video_urls or has_image_urls) or (max_priority_attempts > 0 and current_priority >= max_priority_attempts - 1):
                continue
            
            # Create assets from stored video_ids and image_hashes
            all_video_ids = ad_data.get("all_video_ids", [])
            all_image_hashes = ad_data.get("all_image_hashes", [])
            
            assets = []
            for video_id in all_video_ids:
                assets.append(("video", video_id, 999, f"stored_video_{video_id}"))
            for image_hash in all_image_hashes:
                assets.append(("image", image_hash, 999, f"stored_image_{image_hash}"))
            
            if not assets:
                continue
            
            existing_ads_processed += 1
            
            # All existing ads are processed as mixed (like carousel)
            mixed_ads[ad_id] = {
                "assets": assets,
                "ad_type": ad_types.get(ad_id, "").lower()
            }
        
        print(f"📊 Processed {existing_ads_processed} existing ads for URL fetching")
        
        # Process all ads as mixed (like carousel) to handle both video and image assets
        if mixed_ads:
            video_ids = []
            image_hashes = []
            video_to_ads = {}
            hash_to_ads = {}
            
            for ad_id, ad_info in mixed_ads.items():
                assets = ad_info["assets"]
                ad_type = ad_info["ad_type"]
                
                # Get current priority attempt
                current_priority = processed_data[ad_id].get("priority", 0)
                
                # Find video and image assets at current priority
                video_assets = [a for a in assets if a[0] == "video"]
                image_assets = [a for a in assets if a[0] == "image"]
                
                # Try video at current priority
                if video_assets and current_priority < len(video_assets):
                    video_id = video_assets[current_priority][1]
                    video_ids.append(video_id)
                    video_to_ads[video_id] = ad_id
                    processed_data[ad_id]["video_id"] = video_id
                
                # Try image at current priority
                if image_assets and current_priority < len(image_assets):
                    image_hash = image_assets[current_priority][1]
                    image_hashes.append(image_hash)
                    hash_to_ads[image_hash] = ad_id
                    processed_data[ad_id]["image_hash"] = image_hash
                
                # If no assets at current priority, increment for next attempt
                if (not video_assets or current_priority >= len(video_assets)) and (not image_assets or current_priority >= len(image_assets)):
                    print(f"⚠️ Ad {ad_id}: No more assets to try (current priority: {current_priority})")
            
            # Track which ads had failed attempts to avoid double incrementing
            failed_ads = set()
            
            # Fetch video URLs
            if video_ids:
                video_urls = self.batch_fetch_video_urls(video_ids)
                print(f"🔍 DEBUG: Retrieved {len(video_urls)} video URLs out of {len(video_ids)} requested")
                
                for video_id, urls in video_urls.items():
                    if video_id in video_to_ads:
                        ad_id = video_to_ads[video_id]
                        current_priority = processed_data[ad_id].get("priority", 0)
                        ad_type = mixed_ads[ad_id]["ad_type"]
                        
                        # Only update if URLs are missing or different
                        if not processed_data[ad_id].get("video_source_url") or processed_data[ad_id]["video_source_url"] != urls.get("source", ""):
                            processed_data[ad_id]["video_source_url"] = urls.get("source", "")
                        if not processed_data[ad_id].get("video_permalink_url") or processed_data[ad_id]["video_permalink_url"] != urls.get("permalink", ""):
                            processed_data[ad_id]["video_permalink_url"] = urls.get("permalink", "")
                        # Store high-quality video thumbnail
                        if urls.get("thumbnail"):
                            processed_data[ad_id]["video_thumbnail_url"] = urls["thumbnail"]
                        
                        # Check if we got URLs - if not, increment priority for next attempt
                        source_url = urls.get("source", "")
                        permalink_url = urls.get("permalink", "")
                        
                        if not source_url and not permalink_url:
                            failed_ads.add(ad_id)

                # Handle failed video URL fetches (when video_urls is empty)
                if not video_urls and video_ids:
                    print(f"🚨 All video URL fetches failed for {len(video_ids)} videos - marking ads as failed")
                    for video_id in video_ids:
                        if video_id in video_to_ads:
                            ad_id = video_to_ads[video_id]
                            failed_ads.add(ad_id)
            
            # Fetch image URLs
            if image_hashes:
                image_urls = self.batch_fetch_image_urls(image_hashes)
                print(f"🔍 DEBUG: Retrieved {len(image_urls)} image URLs out of {len(image_hashes)} requested")
                
                for image_hash, urls in image_urls.items():
                    if image_hash in hash_to_ads:
                        ad_id = hash_to_ads[image_hash]
                        current_priority = processed_data[ad_id].get("priority", 0)
                        ad_type = mixed_ads[ad_id]["ad_type"]
                        
                        # Only update if URLs are missing or different
                        if not processed_data[ad_id].get("image_url") or processed_data[ad_id]["image_url"] != urls.get("url", ""):
                            processed_data[ad_id]["image_url"] = urls.get("url", "")
                        if not processed_data[ad_id].get("image_permalink_url") or processed_data[ad_id]["image_permalink_url"] != urls.get("permalink", ""):
                            processed_data[ad_id]["image_permalink_url"] = urls.get("permalink", "")
                        
                        # Check if we got URLs - if not, mark as failed
                        image_url = urls.get("url", "")
                        permalink_url = urls.get("permalink", "")
                        
                        if not image_url and not permalink_url:
                            failed_ads.add(ad_id)
                            
                # Handle failed image URL fetches (when image_urls is empty)
                if not image_urls and image_hashes:
                    print(f"🚨 All image URL fetches failed for {len(image_hashes)} images - marking ads as failed")
                    for image_hash in image_hashes:
                        if image_hash in hash_to_ads:
                            ad_id = hash_to_ads[image_hash]
                            failed_ads.add(ad_id)
            
            # Increment priority for all failed ads (only once per ad)
            for ad_id in failed_ads:
                current_priority = processed_data[ad_id].get("priority", 0)
                processed_data[ad_id]["priority"] = current_priority + 1
        
        print(f"✅ Media URL processing completed")
        return processed_data
    
    def process_ads(self, ad_ids: List[str], ad_types: Dict[str, str], date_from: str, date_to: str) -> Dict:
        """Main processing function following the 6-step process"""
        
        # Step 1: Load existing processed data
        processed_data = self.load_processed_data()  # No date parameters for evolving document
        
        # Identify ads that need raw creative fetching
        # Include new ads and existing ads that have empty video_thumbnail_url
        ads_for_raw_fetch = []
        for ad_id in ad_ids:
            ad_id_str = str(ad_id)
            if ad_id_str not in processed_data:
                # New ad - needs raw data
                ads_for_raw_fetch.append(ad_id_str)
            elif not processed_data[ad_id_str].get("video_thumbnail_url"):
                # Existing ad with empty video_thumbnail_url - needs raw data to update thumbnail
                ads_for_raw_fetch.append(ad_id_str)
        
        print(f"📊 Found {len(ads_for_raw_fetch)} ads needing raw creative fetching")
        
        # Steps 2-3: Fetch raw adcreatives for new ads only (temporary file)
        raw_data = {}
        if ads_for_raw_fetch:
            raw_data = self.fetch_raw_adcreatives(ads_for_raw_fetch, date_from, date_to)
            
            # Step 4: Add thumbnails to processed data for new ads
            processed_data = self.add_thumbnails_to_processed(raw_data, processed_data, ads_for_raw_fetch)
        
        # Step 4b: Update thumbnails for existing ads that have raw data available
        # This handles cases where existing ads in master_urls don't have video_thumbnail_url
        if raw_data:
            processed_data = self.update_thumbnails_for_existing_ads(raw_data, processed_data)
        
        # Steps 5-6: Process media URLs by ad type (for ALL ads that need URL processing)
        # This includes both new ads and existing ads that need to try next priority
        ads_needing_urls = []
        
        # Add ads from current report that need URL processing
        current_ad_ids = set(str(ad_id) for ad_id in ad_ids)
        
        for ad_id_str, ad_data in processed_data.items():
            # Only process ads that are in the current report
            if ad_id_str not in current_ad_ids:
                continue
                
            has_video_urls = bool(ad_data.get("video_source_url") or ad_data.get("video_permalink_url"))
            has_image_urls = bool(ad_data.get("image_url") or ad_data.get("image_permalink_url"))
            current_priority = ad_data.get("priority", 0)
            max_priority_attempts = ad_data.get("max_priority_attempts", 0)
            
            # Include if we don't have URLs yet or haven't exhausted attempts
            if not (has_video_urls or has_image_urls) or (max_priority_attempts > 0 and current_priority < max_priority_attempts - 1):
                ads_needing_urls.append(ad_id_str)
                
        if ads_needing_urls:
            processed_data = self.process_media_urls(raw_data, processed_data, ad_types)
        
        # Save final processed data (evolving document)
        self.save_processed_data(processed_data)
        
        # Debug: Check if any priorities were updated
        priority_updates = 0
        for ad_id, ad_data in processed_data.items():
            if ad_data.get("priority", 0) > 0:
                priority_updates += 1
        
        if priority_updates > 0:
            print(f"💾 Saved {priority_updates} ads with priority > 0 to master_urls")
        else:
            print(f"💾 Saved master_urls (all priorities are 0)")
        
        # Summary
        total_with_media = sum(1 for ad in processed_data.values() 
                              if ad.get("video_source_url") or ad.get("video_permalink_url") or 
                                 ad.get("image_url") or ad.get("image_permalink_url"))
        
        print(f"✅ Processing complete: {total_with_media}/{len(processed_data)} ads have media URLs")
        
        return processed_data

# ===== UTILITY FUNCTIONS =====
def process_meta_ad_urls(ad_ids: List[str], ad_types: Dict[str, str], 
                        access_token: str, ad_account_id: str, 
                        date_from: str, date_to: str, page_id: str = None) -> Dict:
    """
    Main function to process Meta ad URLs
    
    Args:
        ad_ids: List of ad IDs to process
        ad_types: Dictionary mapping ad_id -> ad_type (e.g., "video", "static", "carousel")
        access_token: Meta system user access token
        ad_account_id: Meta ad account ID
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        page_id: Optional page ID for enhanced permissions
    
    Returns:
        Dictionary with processed ad data including URLs
    """
    processor = MetaAdCreativesProcessor(
        access_token=access_token,
        ad_account_id=ad_account_id,
        page_id=page_id
    )
    
    return processor.process_ads(ad_ids, ad_types, date_from, date_to)

def get_ad_url(ad_id: str, ad_type: str = None) -> tuple[str, str]:
    """
    Get URL from meta_adcreatives_processed.json based on ad_id and ad_type
    Returns both primary URL (matching ad type) and thumbnail URL
    
    Args:
        ad_id: The ad ID to look up
        ad_type: The ad type (video, static, carousel) - if None or "Unknown", will try all URL types
    
    Returns:
        Tuple of (primary_url, thumbnail_url) where primary_url matches the ad type
    """
    try:
        processed_data = _get_cached_processed_data()
        
        if processed_data is None:
            return "", ""
        
        ad_id_str = str(ad_id)
        if ad_id_str not in processed_data:
            return "", ""
        
        ad_data = processed_data[ad_id_str]
        
        # Get thumbnail URL - prefer high-quality video thumbnail if available
        thumbnail_url = ad_data.get("video_thumbnail_url", "") or ad_data.get("thumbnail_url", "")
        
        # Get primary URL based on ad type
        primary_url = ""
        
        if ad_type and ad_type.lower() != "unknown":
            # For video ads, prefer permalink_url, then source_url
            if ad_type.lower() in ["video", "carousel"]:
                if ad_data.get("video_permalink_url"):
                    primary_url = ad_data["video_permalink_url"]
                elif ad_data.get("video_source_url"):
                    primary_url = ad_data["video_source_url"]
            
            # For static/image ads, prefer image permalink_url, then image_url
            elif ad_type.lower() in ["static", "image"]:
                if ad_data.get("image_permalink_url"):
                    primary_url = ad_data["image_permalink_url"]
                elif ad_data.get("image_url"):
                    primary_url = ad_data["image_url"]
        
        # If no type-specific URL found, try all URL types in order of preference
        if not primary_url:
            if ad_data.get("video_permalink_url"):
                primary_url = ad_data["video_permalink_url"]
            elif ad_data.get("video_source_url"):
                primary_url = ad_data["video_source_url"]
            elif ad_data.get("image_permalink_url"):
                primary_url = ad_data["image_permalink_url"]
            elif ad_data.get("image_url"):
                primary_url = ad_data["image_url"]
        
        return primary_url, thumbnail_url
        
    except Exception as e:
        print(f"Error getting URL for ad {ad_id}: {e}")
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

def list_s3_reports():
    """List all markdown reports in S3"""
    try:
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix="campaign-reporting/reports/",
            MaxKeys=100
        )
        
        reports = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.md'):
                    try:
                        # Extract date from filename
                        filename = os.path.basename(key)
                        if 'campaign_analysis_report_' in filename:
                            date_part = filename.replace('campaign_analysis_report_', '').replace('.md', '')
                            if len(date_part) == 8:  # YYYYMMDD format
                                date_from_formatted = date_part[:4] + '-' + date_part[4:6] + '-' + date_part[6:8]
                                date_to_formatted = date_from_formatted  # Single date reports
                            elif len(date_part) == 17:  # YYYYMMDD-YYYYMMDD format
                                date_parts = date_part.split('-')
                                if len(date_parts) == 2:
                                    date_from_formatted = date_parts[0][:4] + '-' + date_parts[0][4:6] + '-' + date_parts[0][6:8]
                                    date_to_formatted = date_parts[1][:4] + '-' + date_parts[1][4:6] + '-' + date_parts[1][6:8]
                                else:
                                    date_from_formatted = "Unknown"
                                    date_to_formatted = "Unknown"
                            else:
                                date_from_formatted = "Unknown"
                                date_to_formatted = "Unknown"
                            
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
