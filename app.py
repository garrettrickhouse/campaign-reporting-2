import streamlit as st
import pandas as pd
import json
import os
import requests
import time
from typing import Dict, List, Tuple, Optional, Set
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from main import (
    AGENCY_CODES,
    fetch_all_data_sequentially, calculate_campaign_metrics, get_available_filters,
    merge_data, merge_ads_with_same_name, clean_nan_values, format_date_for_filename, get_metric_value
)

# Google API imports
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

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
            # Raw file is temporary and date-specific
            date_from_clean = date_from.replace('-', '') if date_from else "temp"
            date_to_clean = date_to.replace('-', '') if date_to else "temp"
            return f"reports/meta_adcreatives_raw_{date_from_clean}-{date_to_clean}.json"
        elif file_type == "processed":
            # Processed file is a single evolving document
            return "reports/meta_adcreatives_processed.json"
        else:
            return f"reports/meta_adcreatives_{file_type}.json"
    
    def load_processed_data(self, date_from: str = None, date_to: str = None) -> Dict:
        """Load existing processed data"""
        filename = self.get_filename("processed", date_from, date_to)
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return json.load(f)
        return {}
    
    def save_processed_data(self, data: Dict, date_from: str = None, date_to: str = None):
        """Save processed data"""
        filename = self.get_filename("processed", date_from, date_to)
        os.makedirs("reports", exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
    
    def save_raw_data(self, data: Dict, date_from: str, date_to: str):
        """Save raw adcreatives data (temporary)"""
        filename = self.get_filename("raw", date_from, date_to)
        os.makedirs("reports", exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
    
    def identify_missing_ads(self, ad_ids: List[str], processed_data: Dict) -> List[str]:
        """Step 1: Identify ads missing media URLs"""
        missing_ads = []
        
        for ad_id in ad_ids:
            ad_id_str = str(ad_id)
            
            # Check if ad exists in processed data
            if ad_id_str not in processed_data:
                missing_ads.append(ad_id_str)
                continue
            
            ad_data = processed_data[ad_id_str]
            
            # Check if ad has any media URLs (excluding thumbnail)
            has_video_urls = bool(ad_data.get("video_source_url") or ad_data.get("video_permalink_url"))
            has_image_urls = bool(ad_data.get("image_url") or ad_data.get("image_permalink_url"))
            
            if not has_video_urls and not has_image_urls:
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
    
    def add_thumbnails_to_processed(self, raw_data: Dict, processed_data: Dict, missing_ads: List[str]) -> Dict:
        """Step 4: Add missing ads to processed data with thumbnails"""
        print(f"🖼️ Adding thumbnails for {len(missing_ads)} ads...")
        
        for ad_id in missing_ads:
            if ad_id in raw_data and "error" not in raw_data[ad_id]:
                # Extract thumbnail from raw data
                thumbnail_url = ""
                creative_data = raw_data[ad_id]
                
                if "data" in creative_data and creative_data["data"]:
                    creative = creative_data["data"][0]
                    thumbnail_url = creative.get("thumbnail_url", "")
                
                # Initialize processed entry
                processed_data[ad_id] = {
                    "ad_id": ad_id,
                    "thumbnail_url": thumbnail_url,
                    "video_source_url": "",
                    "video_permalink_url": "",
                    "image_url": "",
                    "image_permalink_url": "",
                    "video_id": "",
                    "image_hash": "",
                    "priority": 0
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
        video_ads = {}
        image_ads = {}
        carousel_ads = {}
        
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
            
            # Classify by ad type
            ad_type = ad_types.get(ad_id, "").lower()
            
            if "video" in ad_type:
                video_assets = [a for a in assets if a[0] == "video"]
                if video_assets:
                    video_ads[ad_id] = video_assets
            elif "static" in ad_type or "image" in ad_type:
                image_assets = [a for a in assets if a[0] == "image"]
                if image_assets:
                    image_ads[ad_id] = image_assets
            else:  # carousel or unknown
                carousel_ads[ad_id] = assets
        
        # Process video ads
        if video_ads:
            video_ids = []
            video_to_ads = {}
            
            for ad_id, assets in video_ads.items():
                video_id = assets[0][1]  # Use highest priority video
                priority_index = 0  # Highest priority asset
                video_ids.append(video_id)
                video_to_ads[video_id] = ad_id
                # Only update if video_id is missing or different
                if processed_data[ad_id].get("video_id") != video_id:
                    processed_data[ad_id]["video_id"] = video_id
                    processed_data[ad_id]["priority"] = priority_index
            
            video_urls = self.batch_fetch_video_urls(video_ids)
            
            for video_id, urls in video_urls.items():
                if video_id in video_to_ads:
                    ad_id = video_to_ads[video_id]
                    # Only update if URLs are missing or different
                    if not processed_data[ad_id].get("video_source_url") or processed_data[ad_id]["video_source_url"] != urls.get("source", ""):
                        processed_data[ad_id]["video_source_url"] = urls.get("source", "")
                    if not processed_data[ad_id].get("video_permalink_url") or processed_data[ad_id]["video_permalink_url"] != urls.get("permalink", ""):
                        processed_data[ad_id]["video_permalink_url"] = urls.get("permalink", "")
                    if urls.get("thumbnail") and not processed_data[ad_id].get("thumbnail_url"):
                        processed_data[ad_id]["thumbnail_url"] = urls["thumbnail"]
        
        # Process image ads
        if image_ads:
            image_hashes = []
            hash_to_ads = {}
            
            for ad_id, assets in image_ads.items():
                image_hash = assets[0][1]  # Use highest priority image
                priority_index = 0  # Highest priority asset
                image_hashes.append(image_hash)
                hash_to_ads[image_hash] = ad_id
                # Only update if image_hash is missing or different
                if processed_data[ad_id].get("image_hash") != image_hash:
                    processed_data[ad_id]["image_hash"] = image_hash
                    processed_data[ad_id]["priority"] = priority_index
            
            image_urls = self.batch_fetch_image_urls(image_hashes)
            
            for image_hash, urls in image_urls.items():
                if image_hash in hash_to_ads:
                    ad_id = hash_to_ads[image_hash]
                    # Only update if URLs are missing or different
                    if not processed_data[ad_id].get("image_url") or processed_data[ad_id]["image_url"] != urls.get("url", ""):
                        processed_data[ad_id]["image_url"] = urls.get("url", "")
                    if not processed_data[ad_id].get("image_permalink_url") or processed_data[ad_id]["image_permalink_url"] != urls.get("permalink", ""):
                        processed_data[ad_id]["image_permalink_url"] = urls.get("permalink", "")
        
        # Process carousel ads (try both video and image)
        if carousel_ads:
            carousel_video_ids = []
            carousel_image_hashes = []
            video_to_ads = {}
            hash_to_ads = {}
            
            for ad_id, assets in carousel_ads.items():
                # Find first video and image assets
                video_priority = None
                image_priority = None
                
                for idx, (asset_type, asset_id, priority, source) in enumerate(assets):
                    if asset_type == "video" and ad_id not in video_to_ads.values() and video_priority is None:
                        carousel_video_ids.append(asset_id)
                        video_to_ads[asset_id] = ad_id
                        # Only update if video_id is missing or different
                        if processed_data[ad_id].get("video_id") != asset_id:
                            processed_data[ad_id]["video_id"] = asset_id
                        video_priority = idx
                    
                    if asset_type == "image" and ad_id not in hash_to_ads.values() and image_priority is None:
                        carousel_image_hashes.append(asset_id)
                        hash_to_ads[asset_id] = ad_id
                        # Only update if image_hash is missing or different
                        if processed_data[ad_id].get("image_hash") != asset_id:
                            processed_data[ad_id]["image_hash"] = asset_id
                        if image_priority is None:
                            image_priority = idx
                
                # Set priority to the highest priority asset found (lowest index)
                final_priority = min(p for p in [video_priority, image_priority] if p is not None)
                processed_data[ad_id]["priority"] = final_priority
            
            # Fetch carousel video URLs
            if carousel_video_ids:
                video_urls = self.batch_fetch_video_urls(carousel_video_ids)
                for video_id, urls in video_urls.items():
                    if video_id in video_to_ads:
                        ad_id = video_to_ads[video_id]
                        # Only update if URLs are missing or different
                        if not processed_data[ad_id].get("video_source_url") or processed_data[ad_id]["video_source_url"] != urls.get("source", ""):
                            processed_data[ad_id]["video_source_url"] = urls.get("source", "")
                        if not processed_data[ad_id].get("video_permalink_url") or processed_data[ad_id]["video_permalink_url"] != urls.get("permalink", ""):
                            processed_data[ad_id]["video_permalink_url"] = urls.get("permalink", "")
                        if urls.get("thumbnail") and not processed_data[ad_id].get("thumbnail_url"):
                            processed_data[ad_id]["thumbnail_url"] = urls["thumbnail"]
            
            # Fetch carousel image URLs
            if carousel_image_hashes:
                image_urls = self.batch_fetch_image_urls(carousel_image_hashes)
                for image_hash, urls in image_urls.items():
                    if image_hash in hash_to_ads:
                        ad_id = hash_to_ads[image_hash]
                        # Only update if URLs are missing or different
                        if not processed_data[ad_id].get("image_url") or processed_data[ad_id]["image_url"] != urls.get("url", ""):
                            processed_data[ad_id]["image_url"] = urls.get("url", "")
                        if not processed_data[ad_id].get("image_permalink_url") or processed_data[ad_id]["image_permalink_url"] != urls.get("permalink", ""):
                            processed_data[ad_id]["image_permalink_url"] = urls.get("permalink", "")
        
        print(f"✅ Media URL processing completed")
        return processed_data
    
    def process_ads(self, ad_ids: List[str], ad_types: Dict[str, str], date_from: str, date_to: str) -> Dict:
        """Main processing function following the 6-step process"""
        print(f"🚀 Processing {len(ad_ids)} ads for media URLs...")
        
        # Step 1: Load existing processed data and identify missing ads
        processed_data = self.load_processed_data()  # No date parameters for evolving document
        missing_ads = self.identify_missing_ads(ad_ids, processed_data)
        
        print(f"📊 Found {len(missing_ads)} ads needing URL processing")
        
        if not missing_ads:
            print("✅ All ads already have media URLs")
            return processed_data
        
        # Steps 2-3: Fetch raw adcreatives (temporary file)
        raw_data = self.fetch_raw_adcreatives(missing_ads, date_from, date_to)
        
        # Step 4: Add thumbnails to processed data
        processed_data = self.add_thumbnails_to_processed(raw_data, processed_data, missing_ads)
        
        # Steps 5-6: Process media URLs by ad type
        processed_data = self.process_media_urls(raw_data, processed_data, ad_types)
        
        # Save final processed data (evolving document)
        self.save_processed_data(processed_data)
        
        # Summary
        total_with_media = sum(1 for ad in processed_data.values() 
                              if ad.get("video_source_url") or ad.get("video_permalink_url") or 
                                 ad.get("image_url") or ad.get("image_permalink_url"))
        
        print(f"✅ Processing complete: {total_with_media}/{len(processed_data)} ads have media URLs")
        
        return processed_data


# Usage function
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

# ===== GOOGLE API CONFIGURATION =====
GOOGLE_CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials/creative-audit-tool-aaa3858bf2cb.json')
SCOPES = ['https://www.googleapis.com/auth/drive']

GENERATE_GOOGLE_DOC = True  # Set to True to generate Google Doc from web display

# Default configuration values - these will be overridden by frontend inputs
DEFAULT_DATE_FROM = "2025-06-30"
DEFAULT_DATE_TO = "2025-07-01"
DEFAULT_TOP_N = 5
DEFAULT_CORE_PRODUCTS = [["LLEM", "Mascara"], ["BEB"]]
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
</style>
""", unsafe_allow_html=True)

def format_currency(value):
    """Format value as currency"""
    return f"${value:,.2f}"

def format_percentage(value):
    """Format value as percentage"""
    return f"{value:.2f}%"

def create_metric_card(label, value, format_func=str):
    """Create a metric card with label and formatted value"""
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{format_func(value)}</div>
    </div>
    """, unsafe_allow_html=True)

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
    
    # Import main module to access configuration
    import main
    
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
| ROAS | {overall_metrics['roas']:.2f}x |
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
        report += f"| {i} | {ad['ad_name']} | {ad['campaign']} | {ad['product']} | {ad['ad_type']} | {ad['creator']} | {ad['agency']} | ${ad['spend']:,.2f} | {ad['roas']:.2f}x | {ad['ctr']:.2f}% | ${ad['cpm']:.2f} | {ad['thumbstop']:.1f}% | ${ad['aov']:.2f} |\n"
    
    # Top N Products
    report += f"""

### Top {top_n} Products by Spend

| Rank | Product | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|---------|-----------|-------|------|-----|-----|-----------|-----|
"""
    
    products_df = calculate_aggregated_metrics(ad_objects, 'product', 10000)
    top_products = products_df.head(top_n)
    
    for i, (product, row) in enumerate(top_products.iterrows(), 1):
        report += f"| {i} | {product} | {int(row['Ads Count'])} | ${row['Spend']:,.2f} | {row['ROAS']:.2f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
    
    # Top N Creators
    report += f"""

### Top {top_n} Creators by Spend

| Rank | Creator | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|---------|-----------|-------|------|-----|-----|-----------|-----|
"""
    
    creators_df = calculate_aggregated_metrics(ad_objects, 'creator', 10000)
    top_creators = creators_df.head(top_n)
    
    for i, (creator, row) in enumerate(top_creators.iterrows(), 1):
        report += f"| {i} | {creator} | {int(row['Ads Count'])} | ${row['Spend']:,.2f} | {row['ROAS']:.2f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
    
    # Top N Agencies
    report += f"""

### Top {top_n} Agencies by Spend

| Rank | Agency | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|--------|-----------|-------|------|-----|-----|-----------|-----|
"""
    
    agencies_df = calculate_aggregated_metrics(ad_objects, 'agency', 10)
    top_agencies = agencies_df.head(top_n)
    
    for i, (agency, row) in enumerate(top_agencies.iterrows(), 1):
        report += f"| {i} | {agency} | {int(row['Ads Count'])} | ${row['Spend']:,.2f} | {row['ROAS']:.2f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
    
    # Campaign Analysis
    report += "\n## 📈 Campaign Analysis\n"
    
    # Get available campaigns
    campaigns = list(set([ad['metadata'].get('campaign_type', 'Unknown') for ad in ad_objects]))
    campaigns = [c for c in campaigns if c != 'Unknown']
    campaigns.sort()
    
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
- ROAS: {campaign_metrics['roas']:.2f}x
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
- ROAS: {product_metrics['roas']:.2f}x
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
                report += f"| {i} | {ad['ad_name']} | ${ad['spend']:,.2f} | {ad['roas']:.2f}x | {ad['ctr']:.2f}% | ${ad['cpm']:.2f} | {ad['thumbstop']:.1f}% | ${ad['aov']:.2f} |\n"
            
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
                    report += f"| {i} | {creator} | {int(row['Ads Count'])} | ${row['Spend']:,.2f} | {row['ROAS']:.2f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
    
    return report

def calculate_aggregated_metrics(ad_objects, group_by_field, top_n=10):
    """Calculate aggregated metrics for a specific field (product, creator, agency)"""
    if not ad_objects:
        return pd.DataFrame()
    
    # Debug: Print the current USE_NORTHBEAM_DATA setting
    import main
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
    
    # Debug: Print the current USE_NORTHBEAM_DATA setting
    import main
    use_northbeam = getattr(main, 'USE_NORTHBEAM_DATA', True)
    
    # Overall metrics - use the correct data source based on USE_NORTHBEAM_DATA setting
    data_source = 'northbeam' if use_northbeam else 'meta'
    overall_metrics = calculate_campaign_metrics(ad_objects, data_source=data_source)
    
    # Display metrics in a grid
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        create_metric_card("Total Ads", overall_metrics['total_ads'])
        create_metric_card("Total Spend", overall_metrics['total_spend'], format_currency)
    
    with col2:
        create_metric_card("ROAS", overall_metrics['roas'])
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
    
    # Create ads dataframe
    ads_data = []
    for ad in ad_objects:
        # Get ad URL from processed data
        ad_id = ad['ad_ids'].get('ad_id')
        ad_type = ad['metadata'].get('ad_type')  # Use existing ad_type from comprehensive ad object
        
        link_url = get_ad_url_from_processed(ad_id, ad_type) if ad_id else ""
        
        ads_data.append({
            'Link': link_url,  # Will be empty if not found in processed file
            'Ad Name': ad['ad_ids']['ad_name'],
            'Campaign Type': ad['metadata'].get('campaign_type', 'Unknown'),
            'Product': ad['metadata'].get('product', 'Unknown'),
            'Ad Type': ad['metadata'].get('ad_type', 'Unknown'),
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
    
    st.dataframe(
        top_ads_df,
        column_config={
            "Link": st.column_config.LinkColumn(
                "Link",
                help="Click to view ad",
                display_text="🔗"
            )
        },
        use_container_width=True
    )
    
    # Show all ads in expander
    with st.expander(f"📊 Show all {len(ads_df)} ads"):
        st.dataframe(
            ads_df,
            column_config={
                "Link": st.column_config.LinkColumn(
                    "Link",
                    help="Click to view ad",
                    display_text="🔗"
                )
            },
            use_container_width=True
        )
    
    st.markdown("---")
    
    # Top N Products table
    st.subheader(f"📦 Top {top_n} Products")
    products_df = calculate_aggregated_metrics(ad_objects, 'product', 10000)  # Get all products
    
    if not products_df.empty:
        # Show top N products
        top_products_df = products_df.head(top_n)
        st.dataframe(top_products_df, use_container_width=True)
        
        # Show all products in expander
        with st.expander(f"📦 Show all {len(products_df)} products"):
            st.dataframe(products_df, use_container_width=True)
    
    st.markdown("---")
    
    # Top N Creators table
    st.subheader(f"👥 Top {top_n} Creators")
    creators_df = calculate_aggregated_metrics(ad_objects, 'creator', 10000)  # Get all creators
    
    if not creators_df.empty:
        # Show top N creators
        top_creators_df = creators_df.head(top_n)
        st.dataframe(top_creators_df, use_container_width=True)
        
        # Show all creators in expander
        with st.expander(f"👥 Show all {len(creators_df)} creators"):
            st.dataframe(creators_df, use_container_width=True)
    
    st.markdown("---")
    
    # Top Agencies table
    st.subheader("🏢 Top Agencies")
    agencies_df = calculate_aggregated_metrics(ad_objects, 'agency', 10)
    
    if not agencies_df.empty:
        # Create display dataframe with formatted values
        display_agencies_df = agencies_df.copy()
        
        # Format the display values
        display_agencies_df['Spend_Display'] = display_agencies_df['Spend'].apply(format_currency)
        display_agencies_df['ROAS_Display'] = display_agencies_df['ROAS'].apply(lambda x: f"{x:.2f}")
        display_agencies_df['CTR_Display'] = display_agencies_df['CTR'].apply(lambda x: f"{x:.2f}%")
        display_agencies_df['CPM_Display'] = display_agencies_df['CPM'].apply(lambda x: f"${x:.2f}")
        display_agencies_df['Thumbstop_Display'] = display_agencies_df['Thumbstop'].apply(lambda x: f"{x:.2f}%")
        display_agencies_df['AOV_Display'] = display_agencies_df['AOV'].apply(lambda x: f"${x:.2f}")
        
        # Reset index to get agency names as a column
        display_agencies_df = display_agencies_df.reset_index()
        
        # Select and rename columns for display
        display_agencies_df = display_agencies_df[['agency', 'Spend_Display', 'ROAS_Display', 'CTR_Display', 'CPM_Display', 'Thumbstop_Display', 'AOV_Display']]
        display_agencies_df.columns = ['Agency', 'Spend', 'ROAS', 'CTR', 'CPM', 'Thumbstop', 'AOV']
        
        # The data is already sorted by calculate_aggregated_metrics, so we can use it directly
        st.dataframe(display_agencies_df, use_container_width=True)

def display_all_ads_tab(ad_objects):
    """Display the All Ads tab with comprehensive filtering and sorting capabilities"""
    
    # Debug: Print the current USE_NORTHBEAM_DATA setting
    import main
    use_northbeam = getattr(main, 'USE_NORTHBEAM_DATA', True)
    
    st.header("📋 All Ads")
    
    try:
        # Create the main dataframe with all ad data
        ads_data = []
        for i, ad in enumerate(ad_objects):
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
                
                # Get ad URL from processed data
                ad_id = ad['ad_ids'].get('ad_id')
                ad_type = ad['metadata'].get('ad_type')  # Use existing ad_type from comprehensive ad object
                
                link_url = get_ad_url_from_processed(ad_id, ad_type) if ad_id else ""
                
                ads_data.append({
                    'Link': link_url,
                    'Ad Name': ad['ad_ids'].get('ad_name', 'Unknown'),
                    'Campaign Type': ad['metadata'].get('campaign_type', 'Unknown'),
                    'Product': ad['metadata'].get('product', 'Unknown'),
                    'Ad Type': ad['metadata'].get('ad_type', 'Unknown'),
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
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["📊 All Ads", "👥 Creator Analysis", "📦 Product Analysis"])
    
    with tab1:
        st.subheader(f"📊 All Ads ({len(display_df)} ads)")
        
        # Display the dataframe with clickable URLs
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
            height=400
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
        
        # Display the dataframe with raw numbers for proper sorting
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
        
        # Display the dataframe with raw numbers for proper sorting
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
    
    # Debug: Print the current USE_NORTHBEAM_DATA setting
    import main
    use_northbeam = getattr(main, 'USE_NORTHBEAM_DATA', True)
    
    # Get available campaigns dynamically from data
    campaigns = list(set([ad['metadata'].get('campaign_type', 'Unknown') for ad in ad_objects]))
    campaigns = [c for c in campaigns if c != 'Unknown']
    campaigns.sort()
    
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
            
            # Campaign summary metrics - use the correct data source based on USE_NORTHBEAM_DATA setting
            data_source = 'northbeam' if use_northbeam else 'meta'
            campaign_metrics = calculate_campaign_metrics(campaign_ads, data_source=data_source)
            
            # Display metrics in a grid
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                create_metric_card("Total Ads", campaign_metrics['total_ads'])
                create_metric_card("Total Spend", campaign_metrics['total_spend'], format_currency)
            
            with col2:
                create_metric_card("ROAS", campaign_metrics['roas'])
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
                    
                    # Show product summary - use the correct data source based on USE_NORTHBEAM_DATA setting
                    product_metrics = calculate_campaign_metrics(product_ads, data_source=data_source)
                    
                    # Display product metrics in a consistent card format
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        create_metric_card("Ads", product_metrics['total_ads'])
                        create_metric_card("Spend", product_metrics['total_spend'], format_currency)
                    
                    with col2:
                        create_metric_card("ROAS", product_metrics['roas'])
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
                        ad_id = ad['ad_ids'].get('ad_id')
                        ad_type = ad['metadata'].get('ad_type')  # Use existing ad_type from comprehensive ad object
                        
                        link_url = get_ad_url_from_processed(ad_id, ad_type) if ad_id else ""

                        ads_data.append({
                            'Link': link_url,
                            'Ad Name': ad['ad_ids']['ad_name'],
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
                        
                        # Use LinkColumn for clickable URLs
                        st.dataframe(
                            display_ads_df,
                            column_config={
                                "Link": st.column_config.LinkColumn(
                                    "Link",
                                    help="Click to view ad",
                                    display_text="🔗"
                                )
                            },
                            use_container_width=True
                        )
                        
                        # Show all ads in expander
                        with st.expander(f"📊 Show all {len(ads_df)} ads"):
                            st.dataframe(
                                ads_df,
                                column_config={
                                    "Link": st.column_config.LinkColumn(
                                        "Link",
                                        help="Click to view ad",
                                        display_text="🔗"
                                    )
                                },
                                use_container_width=True
                            )
                    else:
                        st.info("No ads data available for the selected filters.")
                    
                    st.markdown("---")
                    
                    # Top N Creators for selected campaign and product
                    st.subheader(f"👥 Top {top_n} Creators")
                    
                    # Calculate aggregated metrics for creators (get all creators, not just top N)
                    # We need to bypass the top_n limit in the function to get all creators
                    creators_df = calculate_aggregated_metrics(product_ads, 'creator', 10000)  # Large number to get all creators
                    # Ensure proper sorting by raw numeric values
                    if not creators_df.empty:
                        creators_df = creators_df.sort_values('Spend', ascending=False)
                    
                    # Show top N creators by default
                    display_creators_df = creators_df.head(top_n).copy()
                    
                    if not display_creators_df.empty:
                        # Reset index to get creator names as a column
                        display_creators_df = display_creators_df.reset_index()
                        
                        display_creators_df_formatted = display_creators_df.copy()
                        display_creators_df_formatted['Spend'] = display_creators_df_formatted['Spend'].apply(format_currency)
                        display_creators_df_formatted['ROAS'] = display_creators_df_formatted['ROAS'].apply(lambda x: f"{x:.2f}")
                        display_creators_df_formatted['CTR'] = display_creators_df_formatted['CTR'].apply(lambda x: f"{x:.2f}%")
                        display_creators_df_formatted['CPM'] = display_creators_df_formatted['CPM'].apply(lambda x: f"${x:.2f}")
                        display_creators_df_formatted['Thumbstop'] = display_creators_df_formatted['Thumbstop'].apply(lambda x: f"{x:.2f}%")
                        display_creators_df_formatted['AOV'] = display_creators_df_formatted['AOV'].apply(lambda x: f"${x:.2f}")
                        
                        # Drop the extra columns for display
                        display_creators_df_formatted = display_creators_df_formatted[['creator', 'Spend', 'ROAS', 'CTR', 'CPM', 'Thumbstop', 'AOV']]
                        display_creators_df_formatted.columns = ['Creator', 'Spend', 'ROAS', 'CTR', 'CPM', 'Thumbstop', 'AOV']
                        
                        # Use raw numbers for proper sorting, let Streamlit handle display
                        st.dataframe(display_creators_df, use_container_width=True)
                        
                        # Show all creators in expander
                        with st.expander(f"👥 Show all {len(creators_df)} creators"):
                            # Reset index for all creators dataframe
                            all_creators_formatted = creators_df.reset_index().copy()
                            all_creators_formatted['Spend'] = all_creators_formatted['Spend'].apply(format_currency)
                            all_creators_formatted['ROAS'] = all_creators_formatted['ROAS'].apply(lambda x: f"{x:.2f}")
                            all_creators_formatted['CTR'] = all_creators_formatted['CTR'].apply(lambda x: f"{x:.2f}%")
                            all_creators_formatted['CPM'] = all_creators_formatted['CPM'].apply(lambda x: f"${x:.2f}")
                            all_creators_formatted['Thumbstop'] = all_creators_formatted['Thumbstop'].apply(lambda x: f"{x:.2f}%")
                            all_creators_formatted['AOV'] = all_creators_formatted['AOV'].apply(lambda x: f"${x:.2f}")
                            
                            # Drop the extra columns for display
                            all_creators_formatted = all_creators_formatted[['creator', 'Spend', 'ROAS', 'CTR', 'CPM', 'Thumbstop', 'AOV']]
                            all_creators_formatted.columns = ['Creator', 'Spend', 'ROAS', 'CTR', 'CPM', 'Thumbstop', 'AOV']
                            
                            # Use raw numbers for proper sorting, let Streamlit handle display
                            st.dataframe(creators_df, use_container_width=True)
                    else:
                        st.info("No creator data available for the selected filters.")

def main():
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
    col1, col2 = st.sidebar.columns(2)
    with col1:
        date_from = st.text_input("From Date", value=DEFAULT_DATE_FROM, key="date_from")
    with col2:
        date_to = st.text_input("To Date", value=DEFAULT_DATE_TO, key="date_to")
    
    st.sidebar.subheader("📊 Settings")
    top_n = st.sidebar.number_input("Top N", min_value=1, max_value=50, value=DEFAULT_TOP_N, key="top_n")
    merge_ads = st.sidebar.checkbox("Merge Ads with Same Name", value=DEFAULT_MERGE_ADS_WITH_SAME_NAME, key="merge_ads")
    use_northbeam = st.sidebar.checkbox("Use Northbeam Data", value=DEFAULT_USE_NORTHBEAM_DATA, key="use_northbeam")
    

    

    
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
        height=100, 
        key="core_products",
        help="To group product codes, separate codes with comma."
    )
    
    
    # Parse and display core products count
    core_products_count = 0
    if core_products_input:
        core_products_list = []
        for line in core_products_input.strip().split('\n'):
            if line.strip():
                products = [p.strip() for p in line.split(',') if p.strip()]
                if products:
                    core_products_list.append(products)
        core_products_count = len(core_products_list)
    
    

    
    # Generate Report Form
    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 Generate Report")
    
    with st.sidebar.form("generate_report"):
        generate_button = st.form_submit_button("🔄 Generate Report", type="primary")

    # Main content area
    if generate_button:
        with st.spinner("🔄 Generating report..."):
            try:
                # Temporarily update the global variables for this session
                import main
                main.DATE_FROM = date_from
                main.DATE_TO = date_to
                main.TOP_N = top_n
                main.MERGE_ADS_WITH_SAME_NAME = merge_ads
                main.USE_NORTHBEAM_DATA = use_northbeam
                
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
                
                # Clear previous background task status
                if 'background_task_status' in st.session_state:
                    del st.session_state.background_task_status
                
                # Define filename for saving
                date_from_formatted = date_from.replace('-', '')
                date_to_formatted = date_to.replace('-', '')
                comprehensive_filename = f"reports/comprehensive_ads_{date_from_formatted}-{date_to_formatted}.json"
                
                # Fetch data using the updated configuration
                meta_insights, northbeam_df = fetch_all_data_sequentially()
                
                # Apply filtering to Northbeam data if it exists
                if northbeam_df is not None:
                    # Import the filtering function
                    from main import filter_attribution_data, ACCOUNTING_MODE_FILTER, NORTHBEAM_PLATFORM
                    northbeam_df = filter_attribution_data(northbeam_df, ACCOUNTING_MODE_FILTER, NORTHBEAM_PLATFORM)
                
                if meta_insights is None or northbeam_df is None:
                    st.error("❌ Failed to fetch required data")
                    return
                
                # Merge data into comprehensive objects
                comprehensive_ads = merge_data(northbeam_df, meta_insights)
                
                # Merge ads with same name (if enabled)
                if merge_ads:
                    print(f"DEBUG: Applying merge_ads_with_same_name (merge_ads={merge_ads})")
                    comprehensive_ads = merge_ads_with_same_name(comprehensive_ads)
                else:
                    print(f"DEBUG: Skipping merge (merge_ads={merge_ads})")
                
                # Clean any remaining NaN values
                comprehensive_ads = clean_nan_values(comprehensive_ads)
                
                # Save comprehensive ad objects to reports directory
                if comprehensive_ads:
                    with open(comprehensive_filename, 'w') as f:
                        json.dump(comprehensive_ads, f, indent=2)
                    
                    # Store data in session state for persistence across reruns
                    st.session_state.comprehensive_ads = comprehensive_ads
                    st.session_state.report_config = {
                        'date_from': date_from,
                        'date_to': date_to,
                        'top_n': top_n,
                        'core_products_input': core_products_input,
                        'merge_ads': merge_ads,
                        'use_northbeam': use_northbeam,
                        'date_from_formatted': date_from_formatted,
                        'date_to_formatted': date_to_formatted
                    }
                    
                    # Start background task for Meta ad creatives processing AFTER comprehensive_ads is created
                    if meta_insights and len(meta_insights) > 0:
                        try:
                            # Extract ad IDs and types from meta insights
                            ad_ids = []
                            ad_types = {}
                            
                            for ad in meta_insights:
                                ad_id = ad.get('ad_id')
                                if ad_id:
                                    ad_ids.append(str(ad_id))
                                    # Extract ad type from ad name or use default
                                    ad_name = ad.get('ad_name', '')
                                    # Simple ad type detection from ad name
                                    ad_type = 'unknown'
                                    if 'video' in ad_name.lower():
                                        ad_type = 'video'
                                    elif 'static' in ad_name.lower():
                                        ad_type = 'static'
                                    elif 'carousel' in ad_name.lower():
                                        ad_type = 'carousel'
                                    ad_types[str(ad_id)] = ad_type
                            
                            if ad_ids:
                                print(f"🎬 Starting background Meta ad creatives processing for {len(ad_ids)} ads...")
                                
                                # Get Meta API credentials from environment or config
                                import os
                                access_token = os.getenv('META_SYSTEM_USER_ACCESS_TOKEN')
                                ad_account_id = os.getenv('META_AD_ACCOUNT_ID')
                                page_id = os.getenv('META_PAGE_ID')  # Optional
                                
                                if access_token and ad_account_id:
                                    # Start background processing
                                    def background_process_creatives():
                                        try:
                                            processed_data = process_meta_ad_urls(
                                                ad_ids=ad_ids,
                                                ad_types=ad_types,
                                                access_token=access_token,
                                                ad_account_id=ad_account_id,
                                                date_from=date_from,
                                                date_to=date_to,
                                                page_id=page_id
                                            )
                                            print(f"✅ Background Meta ad creatives processing completed: {len(processed_data)} ads processed")
                                        except Exception as e:
                                            print(f"❌ Background Meta ad creatives processing failed: {e}")
                                    
                                    # Start the background task
                                    import threading
                                    background_thread = threading.Thread(target=background_process_creatives)
                                    background_thread.daemon = True
                                    background_thread.start()
                                    
                                    print(f"🔄 Background Meta ad creatives processing started for {len(ad_ids)} ads")
                                    
                                    # Store background task status in session state
                                    st.session_state.background_task_status = f"🎬 Background task started: Processing Meta ad creatives for {len(ad_ids)} ads"
                                else:
                                    print("⚠️ Meta API credentials not found. Skipping ad creatives processing.")
                                    print("Set META_SYSTEM_USER_ACCESS_TOKEN and META_AD_ACCOUNT_ID environment variables to enable this feature.")
                                    st.warning("⚠️ Meta API credentials not found. Set META_SYSTEM_USER_ACCESS_TOKEN and META_AD_ACCOUNT_ID environment variables to enable ad creatives processing.")
                            else:
                                print("⚠️ No ad IDs found in meta insights. Skipping ad creatives processing.")
                                st.info("ℹ️ No ad IDs found in meta insights. Skipping ad creatives processing.")
                            
                        except Exception as e:
                            print(f"❌ Error starting background Meta ad creatives processing: {e}")
                            st.warning(f"⚠️ Background Meta ad creatives processing failed: {str(e)}")
                            # Continue with main processing even if background task fails

                if comprehensive_ads:
                    pass  # Report generated successfully message will be shown in session state display
                else:
                    st.error("❌ Failed to generate report. Please check the console for errors.")
                    
            except Exception as e:
                st.error(f"❌ Error generating report: {str(e)}")
                st.exception(e)
    
    # Display report and Google Doc generation (using session state data)
    if st.session_state.comprehensive_ads:
        # Display context information in a clean, minimal layout
        config = st.session_state.report_config
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.caption("✅ Report generated successfully!")
            # Display background task status if available
            if hasattr(st.session_state, 'background_task_status'):
                st.caption(st.session_state.background_task_status)
        
        with col2:
            st.caption(f"📅 Date Range: {config['date_from']} to {config['date_to']}")
        
        with col3:
            data_source_display = "Northbeam" if config['use_northbeam'] else "Meta"
            st.caption(f"📊 Data Source: {data_source_display}")
        
        with col4:
            merge_status = "On" if config['merge_ads'] else "Off"
            st.caption(f"🔗 Merge Ads: {merge_status}")
        
        st.markdown("---")
        
        # Create tabs
        tab1, tab2 = st.tabs(["📊 All Ads Summary", "🎯 Campaign Explorer"])
        
        with tab1:
            display_summary_tab(st.session_state.comprehensive_ads, config['top_n'])
        
        with tab2:
            display_campaign_explorer_tab(st.session_state.comprehensive_ads, config['top_n'], config['core_products_input'])
        
        # Add Google Doc generation button
        st.markdown("---")
        st.subheader("📄 Export Report")
        
        col1, col2 = st.columns([1, 3])
        with col1:
            generate_doc_button = st.button("📄 Generate Google Doc", type="secondary")
        
        with col2:
            st.caption("Generate a comprehensive markdown report and upload to Google Drive as a shareable Google Doc")
        
        if generate_doc_button:
            with st.spinner("📄 Generating Google Doc..."):
                try:
                    # Generate markdown report
                    report_markdown = generate_markdown_report(
                        st.session_state.comprehensive_ads, 
                        config['date_from'], 
                        config['date_to'], 
                        config['top_n'], 
                        config['core_products_input'], 
                        config['merge_ads'], 
                        config['use_northbeam']
                    )
                    
                    # Save markdown to file
                    report_filename = f"reports/campaign_analysis_report_{config['date_from_formatted']}-{config['date_to_formatted']}.md"
                    with open(report_filename, 'w') as f:
                        f.write(report_markdown)
                    
                    st.success(f"✅ Markdown report generated: {report_filename}")
                    
                    # Upload to Google Drive
                    doc_title = f"Thrive Causemetics Campaign Analysis - {config['date_from']} to {config['date_to']}"
                    shareable_link = export_report_to_google_doc(report_filename, doc_title)
                    
                    if shareable_link:
                        st.success("✅ Google Doc created successfully!")
                        st.markdown(f"**Shareable Link:** {shareable_link}")
                        
                        # Add download button for local file
                        with open(report_filename, 'r') as f:
                            markdown_content = f.read()
                        st.download_button(
                            label="📥 Download Markdown Report",
                            data=markdown_content,
                            file_name=f"campaign_analysis_report_{config['date_from_formatted']}-{config['date_to_formatted']}.md",
                            mime="text/markdown"
                        )
                    else:
                        st.warning("⚠️ Google Doc creation failed, but markdown report was saved locally")
                        st.info(f"Local file: {report_filename}")
                        
                except Exception as e:
                    st.error(f"❌ Error generating Google Doc: {str(e)}")
                    st.exception(e)
    
    else:
        # Welcome screen
        st.write("""
        This dashboard provides comprehensive analytics by campaigns & products.
        
        **To get started:**
        1. Review and edit the configuration in the sidebar
        2. Click "Generate Report" to create a new analysis
        3. Explore the Summary and Details tabs
        """)
        

def get_ad_url_from_processed(ad_id: str, ad_type: str = None) -> str:
    """
    Get URL from meta_adcreatives_processed.json based on ad_id and ad_type
    
    Args:
        ad_id: The ad ID to look up
        ad_type: The ad type (video, static, carousel) - if None, will try to detect from ad name
    
    Returns:
        URL string or empty string if not found
    """
    try:
        processed_file = "reports/meta_adcreatives_processed.json"
        if not os.path.exists(processed_file):
            return ""
        
        with open(processed_file, 'r') as f:
            processed_data = json.load(f)
        
        ad_id_str = str(ad_id)
        if ad_id_str not in processed_data:
            return ""
        
        ad_data = processed_data[ad_id_str]
        
        # If ad_type is not provided, return empty string
        if ad_type is None:
            return ""
        
        # For video ads, prefer permalink_url, then source_url
        if ad_type.lower() in ["video", "carousel"]:
            if ad_data.get("video_permalink_url"):
                return ad_data["video_permalink_url"]
            elif ad_data.get("video_source_url"):
                return ad_data["video_source_url"]
        
        # For static/image ads, prefer image permalink_url, then image_url
        elif ad_type.lower() in ["static", "image"]:
            if ad_data.get("image_permalink_url"):
                return ad_data["image_permalink_url"]
            elif ad_data.get("image_url"):
                return ad_data["image_url"]
        
        # Fallback: try video URLs first, then image URLs
        if ad_data.get("video_permalink_url"):
            return ad_data["video_permalink_url"]
        elif ad_data.get("video_source_url"):
            return ad_data["video_source_url"]
        elif ad_data.get("image_permalink_url"):
            return ad_data["image_permalink_url"]
        elif ad_data.get("image_url"):
            return ad_data["image_url"]
        
        return ""
        
    except Exception as e:
        print(f"Error getting URL for ad {ad_id}: {e}")
        return ""

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

if __name__ == "__main__":
    main() 