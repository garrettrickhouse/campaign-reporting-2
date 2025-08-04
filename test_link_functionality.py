#!/usr/bin/env python3
"""
Test the link functionality that connects processed.json with web display
"""

import os
import json
from app import get_ad_url_from_processed, detect_ad_type_from_name

def test_link_functionality():
    """Test the link functionality"""
    
    print("🔗 Testing Link Functionality")
    print("=" * 40)
    
    # Test ad type detection (for reference)
    print("\n📝 Testing Ad Type Detection (for reference):")
    test_names = [
        "video_ad_test",
        "static_image_ad", 
        "carousel_ad_test",
        "unknown_ad_name"
    ]
    
    for name in test_names:
        ad_type = detect_ad_type_from_name(name)
        print(f"   '{name}' -> {ad_type}")
    
    # Test URL retrieval
    print("\n🔗 Testing URL Retrieval:")
    
    processed_file = "reports/meta_adcreatives_processed.json"
    if os.path.exists(processed_file):
        with open(processed_file, 'r') as f:
            processed_data = json.load(f)
        
        print(f"   ✅ Processed file found with {len(processed_data)} ads")
        
        # Test a few sample ads
        sample_ads = list(processed_data.items())[:5]
        
        for ad_id, ad_data in sample_ads:
            print(f"\n   📊 Ad ID: {ad_id}")
            
            # Test different ad types
            for ad_type in ["video", "static", "carousel"]:
                url = get_ad_url_from_processed(ad_id, ad_type)
                if url:
                    print(f"      {ad_type}: {url[:50]}...")
                else:
                    print(f"      {ad_type}: No URL found")
            
            # Test without specifying ad_type (should return empty)
            url = get_ad_url_from_processed(ad_id)
            if url:
                print(f"      no ad_type: {url[:50]}...")
            else:
                print(f"      no ad_type: No URL found (expected)")
                
    else:
        print(f"   ❌ Processed file not found: {processed_file}")
        print("   ℹ️ Run the background task first to generate processed data")
    
    print("\n✅ Link functionality test completed")

if __name__ == "__main__":
    test_link_functionality() 