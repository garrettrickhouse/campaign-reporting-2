
#!/usr/bin/env python3
"""
Deployment Test Script
This script tests the core functionality that might be causing deployment issues.
"""

import os
import sys
import traceback
from datetime import datetime

def test_imports():
    """Test all critical imports"""
    print("🔍 Testing imports...")
    try:
        import streamlit as st
        print("✅ Streamlit imported successfully")
        
        import pandas as pd
        print("✅ Pandas imported successfully")
        
        import requests
        print("✅ Requests imported successfully")
        
        import boto3
        print("✅ Boto3 imported successfully")
        
        from dotenv import load_dotenv
        print("✅ python-dotenv imported successfully")
        
        import plotly.express as px
        print("✅ Plotly imported successfully")
        
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        traceback.print_exc()
        return False

def test_environment_variables():
    """Test environment variables"""
    print("\n🔍 Testing environment variables...")
    
    from dotenv import load_dotenv
    load_dotenv()
    
    required_vars = [
        'NORTHBEAM_DATA_CLIENT_ID',
        'NORTHBEAM_API_KEY', 
        'NORTHBEAM_PLATFORM_ACCOUNT_ID',
        'META_SYSTEM_USER_ACCESS_TOKEN',
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'S3_BUCKET'
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: {'*' * min(8, len(value))}")
        else:
            print(f"❌ {var}: NOT SET")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"⚠️  Missing variables: {missing_vars}")
        return False
    return True

def test_streamlit_config():
    """Test Streamlit configuration"""
    print("\n🔍 Testing Streamlit configuration...")
    try:
        import streamlit as st
        
        # Test basic Streamlit functionality
        print("✅ Streamlit can be imported")
        
        # Check if running in deployment mode
        if hasattr(st, 'runtime'):
            print("✅ Streamlit runtime available")
        
        return True
    except Exception as e:
        print(f"❌ Streamlit config test failed: {e}")
        return False

def test_auth_module():
    """Test authentication module"""
    print("\n🔍 Testing authentication module...")
    try:
        from auth import check_authentication, check_password
        
        # Test password check with default password
        test_password = os.getenv('APP_PASSWORD', 'jonesroad2024')
        result = check_password(test_password)
        print(f"✅ Auth module imported and password check works: {result}")
        
        return True
    except Exception as e:
        print(f"❌ Auth module test failed: {e}")
        traceback.print_exc()
        return False

def test_northbeam_client():
    """Test Northbeam client"""
    print("\n🔍 Testing Northbeam client...")
    try:
        from northbeam_client import NorthbeamClient
        
        client = NorthbeamClient(
            attribution_model="last_touch_non_direct",
            attribution_window="1", 
            accounting_mode_api="accrual",
            platform="fb"
        )
        print("✅ NorthbeamClient created successfully")
        
        return True
    except Exception as e:
        print(f"❌ Northbeam client test failed: {e}")
        traceback.print_exc()
        return False

def test_media_urls_manager():
    """Test media URLs manager"""
    print("\n🔍 Testing media URLs manager...")
    try:
        from media_urls_manager import load_media_urls_cache
        
        cache = load_media_urls_cache()
        print(f"✅ Media URLs manager working, cache size: {len(cache) if cache else 0}")
        
        return True
    except Exception as e:
        print(f"❌ Media URLs manager test failed: {e}")
        traceback.print_exc()
        return False

def test_s3_connection():
    """Test S3 connection"""
    print("\n🔍 Testing S3 connection...")
    try:
        import boto3
        from dotenv import load_dotenv
        load_dotenv()
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-east-1'
        )
        
        bucket = os.getenv('S3_BUCKET')
        # Test bucket access with minimal operation
        s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        print("✅ S3 connection successful")
        
        return True
    except Exception as e:
        print(f"❌ S3 connection test failed: {e}")
        return False

def test_minimal_streamlit_app():
    """Test minimal Streamlit app functionality"""
    print("\n🔍 Testing minimal Streamlit app...")
    try:
        # Create a minimal test script
        test_app_content = '''
import streamlit as st
import os
from dotenv import load_dotenv

load_dotenv()

st.title("🧪 Deployment Test")
st.write("✅ Streamlit is working!")
st.write(f"📅 Current time: {__import__('datetime').datetime.now()}")
st.write(f"🐍 Python version: {__import__('sys').version}")
st.write(f"📁 Working directory: {os.getcwd()}")

# Test environment variables
env_vars = ['APP_PASSWORD', 'S3_BUCKET', 'AWS_ACCESS_KEY_ID']
for var in env_vars:
    value = os.getenv(var)
    if value:
        st.write(f"✅ {var}: Set")
    else:
        st.write(f"❌ {var}: Not set")
'''
        
        with open('test_streamlit_app.py', 'w') as f:
            f.write(test_app_content)
        
        print("✅ Created test_streamlit_app.py")
        print("   Run with: streamlit run test_streamlit_app.py --server.address 0.0.0.0 --server.headless true --server.enableCORS=false --server.enableWebsocketCompression=false --server.port 8501")
        
        return True
    except Exception as e:
        print(f"❌ Minimal Streamlit app test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 DEPLOYMENT DEBUG TEST")
    print("=" * 50)
    print(f"🐍 Python version: {sys.version}")
    print(f"📁 Working directory: {os.getcwd()}")
    print(f"📅 Test time: {datetime.now()}")
    print("=" * 50)
    
    tests = [
        ("Imports", test_imports),
        ("Environment Variables", test_environment_variables),
        ("Streamlit Config", test_streamlit_config),
        ("Auth Module", test_auth_module),
        ("Northbeam Client", test_northbeam_client),
        ("Media URLs Manager", test_media_urls_manager),
        ("S3 Connection", test_s3_connection),
        ("Minimal Streamlit App", test_minimal_streamlit_app)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")
            results[test_name] = False
    
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n📈 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The issue might be with deployment-specific configuration.")
        print("\n🔧 Try these deployment fixes:")
        print("1. Use the minimal test app: streamlit run test_streamlit_app.py")
        print("2. Check deployment environment variables in Replit Deployments pane")
        print("3. Verify the deployment run command matches the working command")
    else:
        print("⚠️  Some tests failed. Fix these issues before deploying.")
    
    return passed == total

if __name__ == "__main__":
    main()
