#!/usr/bin/env python3
"""
Test script to test URL lookup with real ad IDs from the loaded data
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import get_processed_data_cache, get_ad_url_from_processed

def test_real_ad_urls():
    """Test ad URL lookup with real ad IDs from the loaded data"""
    print("🔍 Testing ad URL lookup with real ad IDs...")
    
    processed_data = get_processed_data_cache()
    
    if processed_data:
        print(f"✅ Loaded processed data with {len(processed_data)} ads")
        
        # Test with the first few real ad IDs
        sample_ad_ids = list(processed_data.keys())[:5]
        print(f"📋 Testing with real ad IDs: {sample_ad_ids}")
        
        for ad_id in sample_ad_ids:
            url = get_ad_url_from_processed(ad_id)
            ad_data = processed_data[ad_id]
            
            # Show what URL types are available for this ad
            available_urls = []
            if ad_data.get("video_permalink_url"):
                available_urls.append("video_permalink_url")
            if ad_data.get("video_source_url"):
                available_urls.append("video_source_url")
            if ad_data.get("image_permalink_url"):
                available_urls.append("image_permalink_url")
            if ad_data.get("image_url"):
                available_urls.append("image_url")
            
            print(f"   Ad {ad_id}:")
            print(f"     URL: {url}")
            print(f"     Available URL types: {available_urls}")
            print()
    else:
        print("❌ Failed to load processed data")

if __name__ == "__main__":
    print("🧪 Testing real ad URL lookup...")
    test_real_ad_urls()
    print("\n✅ Testing complete!") 