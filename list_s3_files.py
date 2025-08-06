#!/usr/bin/env python3
"""
List all files in S3 bucket to debug the master_urls issue
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import get_s3_client, S3_BUCKET

def list_s3_files():
    """List all files in the S3 bucket"""
    print(f"🔍 Listing all files in s3://{S3_BUCKET}/")
    
    try:
        s3_client = get_s3_client()
        
        # List all files
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            MaxKeys=100
        )
        
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
            print(f"✅ Found {len(files)} files:")
            for file in files:
                print(f"   - {file}")
        else:
            print("❌ No files found in bucket")
            
    except Exception as e:
        print(f"❌ Error listing files: {e}")

def list_processed_files():
    """List files in the processed directory"""
    print(f"\n🔍 Listing files in s3://{S3_BUCKET}/processed/")
    
    try:
        s3_client = get_s3_client()
        
        # List files in processed directory
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix="processed/",
            MaxKeys=100
        )
        
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
            print(f"✅ Found {len(files)} files in processed/:")
            for file in files:
                print(f"   - {file}")
        else:
            print("❌ No files found in processed/ directory")
            
    except Exception as e:
        print(f"❌ Error listing processed files: {e}")

if __name__ == "__main__":
    print("🧪 Listing S3 files...")
    list_s3_files()
    list_processed_files()
    print("\n✅ Listing complete!") 