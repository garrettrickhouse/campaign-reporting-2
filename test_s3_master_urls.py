#!/usr/bin/env python3
"""
Test script to debug S3 master_urls loading
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import get_processed_data_cache, get_ad_url_from_processed, get_s3_client, S3_BUCKET

def test_s3_connection():
    """Test S3 connection and list master_urls files"""
    print("🔍 Testing S3 connection...")
    
    try:
        s3_client = get_s3_client()
        print(f"✅ S3 client created successfully")
        
        # List master_urls files
        print(f"📁 Listing master_urls files in s3://{S3_BUCKET}/processed/master_urls/")
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix="processed/master_urls/master_urls_",
            MaxKeys=10
        )
        
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
            print(f"✅ Found {len(files)} master_urls files:")
            for file in files:
                print(f"   - {file}")
        else:
            print("❌ No master_urls files found")
            
    except Exception as e:
        print(f"❌ S3 connection failed: {e}")

def test_processed_data_cache():
    """Test the processed data cache loading"""
    print("\n🔍 Testing processed data cache...")
    
    processed_data = get_processed_data_cache()
    
    if processed_data:
        print(f"✅ Loaded processed data with {len(processed_data)} ads")
        
        # Test a few ad IDs
        sample_ad_ids = list(processed_data.keys())[:3]
        print(f"📋 Sample ad IDs: {sample_ad_ids}")
        
        for ad_id in sample_ad_ids:
            url = get_ad_url_from_processed(ad_id)
            print(f"   Ad {ad_id}: {url}")
    else:
        print("❌ Failed to load processed data")

def test_ad_url_lookup():
    """Test ad URL lookup with sample ad IDs"""
    print("\n🔍 Testing ad URL lookup...")
    
    # Test with some sample ad IDs (you can replace these with actual ad IDs from your data)
    test_ad_ids = [
        "123456789012345",  # Replace with actual ad ID
        "234567890123456",  # Replace with actual ad ID
    ]
    
    for ad_id in test_ad_ids:
        url = get_ad_url_from_processed(ad_id)
        print(f"Ad {ad_id}: {url}")

if __name__ == "__main__":
    print("🧪 Testing S3 master_urls loading...")
    test_s3_connection()
    test_processed_data_cache()
    test_ad_url_lookup()
    print("\n✅ Testing complete!") 