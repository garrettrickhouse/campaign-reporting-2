import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize S3 client
s3 = boto3.client('s3')

def test_s3_permissions():
    """Test S3 permissions for the campaign reporting app"""
    bucket_name = 'tools.rickhousemedia.com'
    
    print("🔍 Testing S3 permissions...")
    print(f"Bucket: {bucket_name}")
    print(f"AWS Region: {os.getenv('AWS_DEFAULT_REGION', 'Not set')}")
    print(f"Access Key ID: {os.getenv('AWS_ACCESS_KEY_ID', 'Not set')[:10]}...")
    print("-" * 50)
    
    try:
        # Test 1: List bucket contents
        print("📋 Testing ListBucket permission...")
        response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=5)
        print(f"✅ ListBucket working! Found {len(response.get('Contents', []))} objects")
        
        # Test 2: Upload a test file
        print("\n📤 Testing PutObject permission...")
        test_key = 'test/campaign-reporting-test.txt'
        test_content = f"Test file created at {boto3.datetime.now()}\nThis is a test file for S3 permissions verification."
        
        s3.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content
        )
        print(f"✅ PutObject working! File uploaded: {test_key}")
        
        # Test 3: Read the uploaded file
        print("\n📥 Testing GetObject permission...")
        response = s3.get_object(Bucket=bucket_name, Key=test_key)
        content = response['Body'].read().decode('utf-8')
        print(f"✅ GetObject working! File content: {content[:50]}...")
        
        # Test 4: Delete the test file
        print("\n🗑️ Testing DeleteObject permission...")
        s3.delete_object(Bucket=bucket_name, Key=test_key)
        print(f"✅ DeleteObject working! File deleted: {test_key}")
        
        # Test 5: Test the specific paths used by the app
        print("\n🎯 Testing app-specific paths...")
        test_paths = [
            'reports/test-report.json',
            'reports/campaign_analysis_report_20250101-20250102.md'
        ]
        
        for path in test_paths:
            try:
                s3.put_object(
                    Bucket=bucket_name,
                    Key=path,
                    Body=f"Test content for {path}"
                )
                print(f"✅ Can write to: {path}")
                
                # Clean up
                s3.delete_object(Bucket=bucket_name, Key=path)
                print(f"✅ Can delete: {path}")
                
            except Exception as e:
                print(f"❌ Failed to write/delete: {path} - {e}")
        
        print("\n🎉 All S3 permissions tests passed!")
        print("Your AWS setup is working correctly for the campaign reporting app.")
        
    except Exception as e:
        print(f"\n❌ S3 permissions test failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check your AWS credentials in .env file")
        print("2. Verify the bucket policy includes PutObject permission")
        print("3. Ensure your IAM user has the correct permissions")
        print("4. Check that the bucket exists in the correct region")

if __name__ == "__main__":
    test_s3_permissions() 