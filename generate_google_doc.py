#!/usr/bin/env python3
"""
Independent Google Doc Generator

This script generates Google Docs from comprehensive_ads files without relying on Streamlit.
It can be run independently to create reports from existing data files.
"""

import os
import json
import sys
from datetime import datetime
import pandas as pd

# Google API imports
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# Import functions from app module
from app import (
    calculate_campaign_metrics, get_metric_value, merge_ads_with_same_name,
    clean_nan_values, format_date_for_filename
)

# ===== CONFIGURATION & CONSTANTS =====
CAMPAIGN_TYPES = [["Prospecting", 0.35], ["Prospecting+Remarketing", 0.69], ["Remarketing", 2.20]]

# ===== GOOGLE API CONFIGURATION =====
GOOGLE_CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials/creative-audit-tool-aaa3858bf2cb.json')
SCOPES = ['https://www.googleapis.com/auth/drive']

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

def calculate_aggregated_metrics(ad_objects, group_by_field, top_n=10):
    """Calculate aggregated metrics for a specific field (product, creator, agency)"""
    if not ad_objects:
        return pd.DataFrame()
    
    # Determine data source from the ad objects
    use_northbeam = True  # Default to Northbeam
    if ad_objects and 'metrics' in ad_objects[0]:
        if 'northbeam' in ad_objects[0]['metrics']:
            use_northbeam = True
        elif 'meta' in ad_objects[0]['metrics']:
            use_northbeam = False
    
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
        
        # Aggregate metrics using get_metric_value
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
        df = df.sort_values('Spend', ascending=False).head(top_n)
    
    return df

def generate_markdown_report(ad_objects, date_from, date_to, top_n=5, core_products_input=None, merge_ads=True, use_northbeam=True):
    """Generate comprehensive markdown report from ad objects"""
    
    print("🔍 DEBUG: generate_markdown_report called")
    print(f"🔍 DEBUG: ad_objects count: {len(ad_objects) if ad_objects else 0}")
    print(f"🔍 DEBUG: date_from: {date_from}")
    print(f"🔍 DEBUG: date_to: {date_to}")
    print(f"🔍 DEBUG: top_n: {top_n}")
    print(f"🔍 DEBUG: core_products_input: {core_products_input}")
    print(f"🔍 DEBUG: merge_ads: {merge_ads}")
    print(f"🔍 DEBUG: use_northbeam: {use_northbeam}")
    
    try:
        # Get data source for display
        data_source_display = "Northbeam" if use_northbeam else "Meta"
        
        # Calculate overall metrics
        data_source = 'northbeam' if use_northbeam else 'meta'
        print(f"🔍 DEBUG: data_source: {data_source}")
        
        # Validate inputs
        if not ad_objects:
            raise ValueError("No ad objects provided")
        
        if not date_from or not date_to:
            raise ValueError("Date range is required")
        
        if top_n <= 0:
            raise ValueError("Top N must be greater than 0")
        
        print("🔍 DEBUG: Input validation passed")
        
        # Calculate overall metrics
        print("🔍 DEBUG: About to calculate overall metrics")
        overall_metrics = calculate_campaign_metrics(ad_objects, data_source=data_source)
        print("🔍 DEBUG: Overall metrics calculated")
        print(f"🔍 DEBUG: Overall metrics: {overall_metrics}")
        
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
        print(f"🔍 DEBUG: Processing {len(ad_objects)} ads")
        for i, ad in enumerate(ad_objects):
            try:
                print(f"🔍 DEBUG: Processing ad {i+1}/{len(ad_objects)}")
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
                print(f"🔍 DEBUG: Ad {i+1} processed successfully")
            except Exception as e:
                print(f"Warning: Error processing ad {i}: {e}")
                continue
        
        print(f"🔍 DEBUG: Processed {len(ads_data)} ads successfully")
        
        # Sort by spend and get top N
        ads_df = pd.DataFrame(ads_data)
        ads_df = ads_df.sort_values('spend', ascending=False)
        top_ads = ads_df.head(top_n)
        
        for i, (_, ad) in enumerate(top_ads.iterrows(), 1):
            report += f"| {i} | {ad['ad_name']} | {ad['campaign']} | {ad['product']} | {ad['ad_type']} | {ad['creator']} | {ad['agency']} | ${ad['spend']:,.2f} | {ad['roas']:.2f}x | {ad['ctr']:.2f}% | ${ad['cpm']:.2f} | {ad['thumbstop']:.1f}% | ${ad['aov']:.2f} |\n"
        
        # Top N Products
        report += f"""

### Top {top_n} Products by Spend

| Rank | Product | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|---------|-----------|-------|------|-----|-----|-----------|-----|
"""
        
        products_df = calculate_aggregated_metrics(ad_objects, 'product', 10000)
        products_df = products_df.sort_values('Spend', ascending=False)
        top_products = products_df.head(top_n)
        
        for i, (product, row) in enumerate(top_products.iterrows(), 1):
            report += f"| {i} | {product} | {row['Ads Count']} | ${row['Spend']:,.2f} | {row['ROAS']:.2f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
        
        # Top N Creators
        report += f"""

### Top {top_n} Creators by Spend

| Rank | Creator | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|---------|-----------|-------|------|-----|-----|-----------|-----|
"""
        
        creators_df = calculate_aggregated_metrics(ad_objects, 'creator', 10000)
        creators_df = creators_df.sort_values('Spend', ascending=False)
        top_creators = creators_df.head(top_n)
        
        for i, (creator, row) in enumerate(top_creators.iterrows(), 1):
            report += f"| {i} | {creator} | {row['Ads Count']} | ${row['Spend']:,.2f} | {row['ROAS']:.2f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
        
        # Top N Agencies
        report += f"""

### Top {top_n} Agencies by Spend

| Rank | Agency | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|--------|-----------|-------|------|-----|-----|-----------|-----|
"""
        
        agencies_df = calculate_aggregated_metrics(ad_objects, 'agency', 10000)
        agencies_df = agencies_df.sort_values('Spend', ascending=False)
        top_agencies = agencies_df.head(top_n)
        
        for i, (agency, row) in enumerate(top_agencies.iterrows(), 1):
            report += f"| {i} | {agency} | {row['Ads Count']} | ${row['Spend']:,.2f} | {row['ROAS']:.2f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
        
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
            # Fallback to default core products
            default_core_products = [["LLEM", "Mascara"], ["BEB"]]
            for product_group in default_core_products:
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
                    # Use default core products
                    default_core_products = [["LLEM", "Mascara"], ["BEB"]]
                    for group in default_core_products:
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
                product_creators_df = product_creators_df.sort_values('Spend', ascending=False)
                top_product_creators = product_creators_df.head(top_n)
                
                if not top_product_creators.empty:
                    report += f"""

**Top Creators by Spend:**

| Rank | Creator | Total Ads | Spend | ROAS | CTR | CPM | Thumbstop | AOV |
|------|---------|-----------|-------|------|-----|-----|-----------|-----|
"""
                    
                    for i, (creator, row) in enumerate(top_product_creators.iterrows(), 1):
                        report += f"| {i} | {creator} | {row['Ads Count']} | ${row['Spend']:,.2f} | {row['ROAS']:.2f}x | {row['CTR']:.2f}% | ${row['CPM']:.2f} | {row['Thumbstop']:.1f}% | ${row['AOV']:.2f} |\n"
        
        return report
        
    except Exception as e:
        print(f"Error generating markdown report: {e}")
        import traceback
        print("🔍 DEBUG: Full traceback in generate_markdown_report:")
        traceback.print_exc()
        return f"# Error Generating Report\n\nAn error occurred while generating the report: {str(e)}"
    
    finally:
        print("🔍 DEBUG: generate_markdown_report function completed")

def load_comprehensive_ads(file_path):
    """Load comprehensive ads from JSON file or S3"""
    try:
        # Try local file first
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                comprehensive_ads = json.load(f)
            print(f"✅ Loaded {len(comprehensive_ads)} ads from local file: {file_path}")
            return comprehensive_ads
        
        # Try S3 if local file doesn't exist
        try:
            import boto3
            from dotenv import load_dotenv
            load_dotenv()
            
            s3_bucket = os.getenv('S3_BUCKET')
            aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            aws_region = 'us-east-1'
            
            if s3_bucket and aws_access_key_id and aws_secret_access_key:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=aws_region
                )
                
                            # Convert local path to S3 key
            s3_key = file_path
            # Handle both old and new file structures
            if file_path.startswith('reports/') or file_path.startswith('processed/'):
                s3_key = file_path
                
                response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
                comprehensive_ads = json.loads(response['Body'].read().decode('utf-8'))
                print(f"✅ Loaded {len(comprehensive_ads)} ads from S3: s3://{s3_bucket}/{s3_key}")
                return comprehensive_ads
                
        except Exception as s3_error:
            print(f"⚠️ S3 loading failed: {s3_error}")
        
        print(f"❌ File not found locally or in S3: {file_path}")
        return None
        
    except Exception as e:
        print(f"❌ Error loading comprehensive ads from {file_path}: {e}")
        return None

def main():
    """Main function to generate Google Doc from comprehensive ads file"""
    
    # Check if comprehensive ads file is provided as argument
    if len(sys.argv) < 2:
        print("Usage: python generate_google_doc.py <comprehensive_ads_file> [date_from] [date_to] [top_n]")
        print("Example: python generate_google_doc.py campaign-reporting/processed/comprehensive_ads/comprehensive_ads_20250630-20250701.json 2025-06-30 2025-07-01 5")
        return
    
    comprehensive_file = sys.argv[1]
    
    # Optional parameters
    date_from = sys.argv[2] if len(sys.argv) > 2 else "2025-06-30"
    date_to = sys.argv[3] if len(sys.argv) > 3 else "2025-07-01"
    top_n = int(sys.argv[4]) if len(sys.argv) > 4 else 5
    
    print(f"📁 Loading comprehensive ads from: {comprehensive_file}")
    print(f"📅 Date range: {date_from} to {date_to}")
    print(f"📊 Top N: {top_n}")
    
    # Load comprehensive ads
    comprehensive_ads = load_comprehensive_ads(comprehensive_file)
    if not comprehensive_ads:
        print("❌ Failed to load comprehensive ads")
        return
    
    # Generate markdown report
    print("📝 Generating markdown report...")
    report_markdown = generate_markdown_report(
        comprehensive_ads,
        date_from,
        date_to,
        top_n,
        core_products_input=None,  # Use default core products
        merge_ads=True,
        use_northbeam=True
    )
    
    if not report_markdown or report_markdown.startswith("# Error"):
        print("❌ Failed to generate markdown report")
        return
    
    # Save markdown to file
    date_from_formatted = date_from.replace('-', '')
    date_to_formatted = date_to.replace('-', '')
    data_source = "northbeam"  # Default to northbeam for this script
    report_filename = f"campaign-reporting/reports/campaign_analysis/campaign_analysis_{data_source}_{date_from_formatted}-{date_to_formatted}.md"
    
    # Ensure reports directory exists
    os.makedirs("campaign-reporting/reports/campaign_analysis", exist_ok=True)
    
    with open(report_filename, 'w') as f:
        f.write(report_markdown)
    
    print(f"✅ Markdown report generated: {report_filename}")
    
    # Check if Google credentials are available
    if not os.path.exists(GOOGLE_CREDENTIALS_FILE):
        print(f"⚠️ Google credentials file not found: {GOOGLE_CREDENTIALS_FILE}")
        print("Markdown report saved locally. Please set up Google Cloud credentials to upload to Google Drive.")
        return
    
    # Upload to Google Drive
    print("☁️ Uploading to Google Drive...")
    doc_title = f"Thrive Causemetics Campaign Analysis - {date_from} to {date_to}"
    
    try:
        shareable_link = export_report_to_google_doc(report_filename, doc_title)
        
        if shareable_link:
            print("✅ Google Doc created successfully!")
            print(f"Shareable Link: {shareable_link}")
        else:
            print("⚠️ Google Doc creation failed, but markdown report was saved locally")
            print(f"Local file: {report_filename}")
            
    except Exception as e:
        print(f"❌ Error uploading to Google Drive: {str(e)}")
        print("Markdown report was saved locally")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 