# Northbeam Report Testing Script

This standalone script allows you to test the Northbeam report generation functionality without running the full Streamlit application.

## Features

- **Date Input**: Enter custom start and end dates for testing
- **Export Creation**: Creates Northbeam data exports via API
- **S3 Integration**: Saves CSV files to S3 bucket
- **Local Download**: Downloads CSV files locally for review
- **Data Filtering**: Applies proper attribution and platform filters
- **Error Handling**: Comprehensive error handling and retry logic

## Prerequisites

1. **Environment Variables**: Ensure your `.env` file contains:
   ```
   NORTHBEAM_DATA_CLIENT_ID=your_client_id
   NORTHBEAM_API_KEY=your_api_key
   NORTHBEAM_PLATFORM_ACCOUNT_ID=your_platform_account_id
   AWS_ACCESS_KEY_ID=your_aws_access_key
   AWS_SECRET_ACCESS_KEY=your_aws_secret_key
   S3_BUCKET=your_s3_bucket_name
   ```

2. **Python Dependencies**: Install required packages:
   ```bash
   pip install pandas requests boto3 python-dotenv
   ```

## Usage

1. **Run the script**:
   ```bash
   python test_northbeam.py
   ```

2. **Enter date range** when prompted:
   ```
   Start date (YYYY-MM-DD): 2025-06-30
   End date (YYYY-MM-DD): 2025-07-01
   ```

3. **Monitor the process**:
   - The script will create a Northbeam export
   - Poll for completion status
   - Download the CSV data
   - Filter by attribution settings
   - Save to S3 and locally

## Output

The script will:
- Create a CSV file in `campaign-reporting/raw/northbeam/`
- Save the same file to your S3 bucket
- Display a summary of the data retrieved

## Example Output

```
🚀 Northbeam Report Testing Script
==================================================
✅ Environment variables loaded successfully

📅 Enter date range:
Start date (YYYY-MM-DD): 2025-06-30
End date (YYYY-MM-DD): 2025-07-01

🔄 Processing Northbeam data for 2025-06-30 to 2025-07-01

📊 Step 1: Creating Northbeam export...
Northbeam export for 2025-06-30 to 2025-07-01
   - Period Type: FIXED
   - Attribution Model: ['last_touch_non_direct']
   - Attribution Window: ['1'] days
   - Accounting Mode: ['accrual']
   - Time Granularity: DAILY
   - Level: ad
   - Metrics Requested: 7 metrics
✅ Export created successfully! ID: abc123

📥 Step 2: Downloading export data...
  🔄 Poll attempt 1...
  ↪ Status: processing
  🔄 Poll attempt 2...
  ↪ Status: ready
✅ Export ready. File URL: https://...
✅ Downloaded 1,234 rows directly from Northbeam

🔍 Step 3: Filtering attribution data...
🔍 Filtered Northbeam data from 1,234 to 1,200 rows

☁️ Step 4: Saving to S3...
✅ Saved CSV to S3: s3://your-bucket/campaign-reporting/raw/northbeam/northbeam_20250630-20250701.csv

💾 Step 5: Saving locally...
✅ Saved CSV locally: campaign-reporting/raw/northbeam/northbeam_20250630-20250701.csv

==================================================
✅ Northbeam Test Complete!
📊 Data Summary:
   - Total rows: 1,200
   - Date range: 2025-06-30 to 2025-07-01
   - S3 location: s3://your-bucket/campaign-reporting/raw/northbeam/northbeam_20250630-20250701.csv
   - Local file: campaign-reporting/raw/northbeam/northbeam_20250630-20250701.csv

📁 You can now review the CSV file at: campaign-reporting/raw/northbeam/northbeam_20250630-20250701.csv
```

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**: Ensure all required variables are set in your `.env` file
2. **API Rate Limits**: The script includes retry logic for rate limits
3. **S3 Permissions**: Verify your AWS credentials have proper S3 access
4. **Date Format**: Use YYYY-MM-DD format for dates

### Error Messages

- `❌ Export creation failed`: Check your Northbeam API credentials
- `❌ S3 download failed`: Verify AWS credentials and S3 bucket permissions
- `❌ Missing required environment variables`: Check your `.env` file

## Data Structure

The generated CSV includes these columns:
- `ad_id`: Unique ad identifier
- `campaign_id`: Campaign identifier
- `adset_id`: Ad set identifier
- `ad_name`: Ad name
- `campaign_name`: Campaign name
- `adset_name`: Ad set name
- `spend`: Ad spend amount
- `impressions`: Number of impressions
- `meta_link_clicks`: Link clicks
- `attributed_rev`: Attributed revenue
- `transactions`: Number of transactions
- `roas`: Return on ad spend
- `meta_3s_video_views`: 3-second video views
- `accounting_mode`: Accounting mode used
- `attribution_model`: Attribution model used
- `platform`: Platform (fb for Facebook)

## Configuration

The script uses the same configuration as the main application:
- **Attribution Model**: `last_touch_non_direct`
- **Attribution Window**: `1` day
- **Accounting Mode**: `accrual`
- **Platform**: `fb` (Facebook)
- **Level**: `ad` (ad-level data) 