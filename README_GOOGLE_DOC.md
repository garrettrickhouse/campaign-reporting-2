# Google Doc Generator

This document explains how to use the independent Google Doc generator script that creates comprehensive campaign analysis reports from existing comprehensive_ads files.

## Overview

The `generate_google_doc.py` script is an independent tool that generates Google Docs from comprehensive_ads JSON files without relying on Streamlit. This solves the issue where the Google Doc generation button in the Streamlit app was crashing due to Streamlit's state management.

## Usage

### Basic Usage

```bash
python3 generate_google_doc.py <comprehensive_ads_file> [date_from] [date_to] [top_n]
```

### Example

```bash
python3 generate_google_doc.py reports/comprehensive_ads_20250630-20250701.json 2025-06-30 2025-07-01 5
```

### Parameters

- `comprehensive_ads_file` (required): Path to the comprehensive ads JSON file
- `date_from` (optional): Start date in YYYY-MM-DD format (default: 2025-06-30)
- `date_to` (optional): End date in YYYY-MM-DD format (default: 2025-07-01)
- `top_n` (optional): Number of top items to show in each section (default: 5)

## What the Script Does

1. **Loads Data**: Reads the comprehensive ads JSON file
2. **Generates Report**: Creates a comprehensive markdown report with:
   - Executive summary with overall metrics
   - Top ads by spend
   - Top products by spend
   - Top creators by spend
   - Top agencies by spend
   - Campaign analysis with product breakdowns
3. **Saves Locally**: Saves the markdown report to the `reports/` directory
4. **Uploads to Google Drive**: If Google credentials are available:
   - Uploads the markdown file to Google Drive
   - Converts it to a Google Doc
   - Makes it publicly shareable
   - Provides a shareable link

## Requirements

### Google API Setup

To upload to Google Drive, you need:

1. **Google Cloud Project** with Drive API enabled
2. **Service Account** with appropriate permissions
3. **Credentials File** at `credentials/creative-audit-tool-aaa3858bf2cb.json`

### Python Dependencies

The script requires the same dependencies as the main app:
- `googleapiclient`
- `pandas`
- `json`
- `os`
- `sys`

## Output Files

### Local Files

The script creates:
- `reports/campaign_analysis_report_YYYYMMDD-YYYYMMDD.md` - The markdown report

### Google Drive

If credentials are available:
- Uploads the markdown file to Google Drive
- Converts it to a Google Doc
- Makes it publicly shareable
- Returns a shareable link

## Error Handling

The script includes comprehensive error handling:

- **Missing File**: Shows usage instructions if no file is provided
- **Invalid Data**: Handles errors in data processing gracefully
- **Google API Errors**: Continues with local file creation if Google upload fails
- **Debug Information**: Provides detailed logging for troubleshooting

## Integration with Streamlit App

The Streamlit app (`app.py`) now:

1. **Removes the Google Doc button** that was causing crashes
2. **Adds instructions** for using the independent script
3. **Maintains all other functionality** for data analysis and visualization

## Example Workflow

1. **Generate Data**: Use the Streamlit app to generate comprehensive ads data
2. **Generate Report**: Use the independent script to create Google Docs
3. **Share Results**: Use the provided Google Doc link to share results

## Troubleshooting

### Common Issues

1. **"Google credentials file not found"**
   - Ensure the credentials file exists at the specified path
   - Check that the service account has appropriate permissions

2. **"Failed to load comprehensive ads"**
   - Verify the JSON file exists and is valid
   - Check that the file contains the expected data structure

3. **"Error uploading to Google Drive"**
   - Check Google Cloud project settings
   - Verify API quotas and permissions
   - The script will still save the markdown file locally

### Debug Information

The script provides detailed debug output including:
- Number of ads processed
- Data source detection (Northbeam vs Meta)
- Processing progress
- Error details with full tracebacks

## Benefits of This Approach

1. **Reliability**: Independent of Streamlit's state management
2. **Flexibility**: Can be run from command line or automated
3. **Robustness**: Comprehensive error handling
4. **Maintainability**: Clean separation of concerns
5. **Reusability**: Can process any comprehensive_ads file 