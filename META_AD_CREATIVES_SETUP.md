# Meta Ad Creatives Processing Setup

This feature automatically fetches video and image URLs for Meta ads in the background after retrieving meta insights.

## Files Created

The processor will create two types of files in the `reports/` directory:

1. **Raw Data**: `meta_adcreatives_raw_YYYYMMDD-YYYYMMDD.json` (Date-specific)
   - Contains raw adcreatives data from Meta API
   - Created fresh for each run based on missing ad_ids
   - Overwrites existing file if same date range is used
   - Date-specific for each run

2. **Processed Data**: `meta_adcreatives_processed.json` (Evolving)
   - Single document containing all processed ad data with video/image URLs
   - Incrementally updated with new ad_ids and missing URLs
   - Not tied to date ranges - accumulates all processed ads
   - Preserves existing data and only adds/updates missing fields

## Environment Variables Required

Set these environment variables to enable the feature:

```bash
export META_SYSTEM_USER_ACCESS_TOKEN="your_meta_access_token"
export META_AD_ACCOUNT_ID="your_ad_account_id"
export META_PAGE_ID="your_page_id"  # Optional, for enhanced permissions
```

## How It Works

1. **Background Task**: After meta insights are retrieved and displayed, a background thread starts processing ad creatives
2. **Load Existing Data**: Loads the evolving `meta_adcreatives_processed.json` file
3. **Identify Missing Ads**: Compares current ad_ids with existing processed data to find missing ads or ads with missing URLs
4. **Fetch Raw Data**: Only fetches raw adcreatives for missing ads (creates temporary date-specific raw file)
5. **Process URLs**: Extracts video/image URLs for missing ads
6. **Incremental Update**: Updates the processed file with new data while preserving existing data
7. **Evolving Document**: The processed file grows over time, accumulating all processed ads

## File Structure

### Raw Data Format
```json
{
  "ad_id": {
    "data": [
      {
        "thumbnail_url": "...",
        "asset_feed_spec": {
          "videos": [...],
          "images": [...],
          "asset_customization_rules": [...]
        },
        "object_story_spec": {
          "video_data": {...},
          "photo_data": {...},
          "link_data": {...}
        }
      }
    ]
  }
}
```

### Processed Data Format
```json
{
  "ad_id": {
    "ad_id": "ad_id",
    "thumbnail_url": "https://...",
    "video_source_url": "https://...",
    "video_permalink_url": "https://...",
    "image_url": "https://...",
    "image_permalink_url": "https://...",
    "video_id": "video_id",
    "image_hash": "image_hash",
    "priority": 0
  }
}
```

## Usage

The feature is automatically triggered when you generate a report in the web app. No additional action is required.

## Testing

Run the test script to verify functionality:

```bash
python3 test_meta_processor.py
```

## Troubleshooting

1. **Missing Environment Variables**: Set the required environment variables
2. **API Rate Limits**: The processor includes rate limiting (0.2s between batches)
3. **Permission Issues**: Ensure your access token has the required permissions
4. **File Permissions**: Ensure the `reports/` directory is writable

## Permissions Required

Your Meta access token needs these permissions:
- `ads_read`
- `ads_management` (for some video access)
- `pages_read_engagement` (if using page token) 