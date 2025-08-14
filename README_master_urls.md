# Master URLs Manager

This module contains all the functionality for retrieving and storing master URLs for Meta ad creatives. It was extracted from the main `app.py` file to provide a clean, focused interface for managing ad creative URLs.

## Features

- **MetaAdCreativesProcessor**: Main class for processing Meta ad creatives and extracting media URLs
- **S3 Integration**: Automatic saving and loading from S3 with local fallback
- **Caching**: Built-in caching system to avoid repeated S3 calls
- **Batch Processing**: Efficient batch processing of multiple ads
- **Priority System**: Intelligent retry system with priority-based asset selection
- **Media Type Support**: Handles videos, images, and carousel ads

## Installation

1. Install the required dependencies:
```bash
pip install -r master_urls_requirements.txt
```

2. Set up environment variables in a `.env` file:
```bash
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
S3_BUCKET=your_s3_bucket_name
```

## Usage

### Basic Usage

```python
from master_urls_manager import process_meta_ad_urls

# Process ad creatives for a list of ad IDs
ad_ids = ["123456789", "987654321"]
ad_types = {"123456789": "video", "987654321": "static"}

# Process the ads
processed_data = process_meta_ad_urls(
    ad_ids=ad_ids,
    ad_types=ad_types,
    access_token="your_meta_access_token",
    ad_account_id="your_ad_account_id",
    date_from="2025-01-01",
    date_to="2025-01-31"
)
```

### Advanced Usage with MetaAdCreativesProcessor

```python
from master_urls_manager import MetaAdCreativesProcessor

# Create a processor instance
processor = MetaAdCreativesProcessor(
    access_token="your_meta_access_token",
    ad_account_id="your_ad_account_id",
    page_id="optional_page_id"  # For enhanced permissions
)

# Process ads
processed_data = processor.process_ads(
    ad_ids=["123456789", "987654321"],
    ad_types={"123456789": "video", "987654321": "static"},
    date_from="2025-01-01",
    date_to="2025-01-31"
)
```

### Retrieving URLs

```python
from master_urls_manager import get_ad_url, detect_ad_type_from_name

# Detect ad type from name
ad_type = detect_ad_type_from_name("My Video Ad e:video")

# Get URLs for an ad
primary_url, thumbnail_url = get_ad_url("123456789", ad_type)
print(f"Primary URL: {primary_url}")
print(f"Thumbnail URL: {thumbnail_url}")
```

### Cache Management

```python
from master_urls_manager import (
    get_processed_data_cache,
    clear_processed_data_cache,
    set_use_cached_files
)

# Enable/disable cached files
set_use_cached_files(True)

# Get cached data
cached_data = get_processed_data_cache()

# Clear cache to force reload
clear_processed_data_cache()
```

## File Structure

The module manages the following file structure:

```
campaign-reporting/
├── raw/
│   └── meta_adcreatives/
│       └── meta_adcreatives_YYYYMMDD-YYYYMMDD.json
└── processed/
    └── master_urls/
        └── master_urls_YYYYMMDD.json
```

## Data Schema

Each ad in the processed data contains:

```json
{
  "ad_id": "123456789",
  "thumbnail_url": "basic_thumbnail_url",
  "video_thumbnail_url": "high_quality_video_thumbnail",
  "video_source_url": "video_source_url",
  "video_permalink_url": "video_permalink_url",
  "image_url": "image_url",
  "image_permalink_url": "image_permalink_url",
  "video_id": "video_id",
  "image_hash": "image_hash",
  "priority": 0,
  "max_priority_attempts": 3,
  "all_video_ids": ["vid1", "vid2"],
  "all_image_hashes": ["hash1", "hash2"]
}
```

## Configuration

### Environment Variables

- `AWS_ACCESS_KEY_ID`: AWS access key for S3 operations
- `AWS_SECRET_ACCESS_KEY`: AWS secret key for S3 operations
- `S3_BUCKET`: S3 bucket name for storing data
- `DOWNLOAD_REPORTS_LOCALLY`: Set to `True` to enable local file saving

### Constants

- `USE_CACHED_FILES`: Global flag to control cached file usage
- `_PROCESSED_DATA_CACHE`: Global cache for processed data

## Error Handling

The module includes comprehensive error handling:

- S3 access failures fall back to local storage
- API failures are logged and retried
- Invalid data is skipped with appropriate logging
- Network timeouts are handled gracefully

## Performance Features

- **Batch Processing**: Processes ads in batches of 50 for efficiency
- **Rate Limiting**: Built-in delays to respect API rate limits
- **Caching**: Reduces S3 calls and improves response times
- **Priority System**: Intelligent retry logic for failed URL fetches

## Troubleshooting

### Common Issues

1. **S3 Access Denied**: Check AWS credentials and bucket permissions
2. **API Rate Limits**: The module automatically handles rate limiting
3. **Cache Issues**: Use `clear_processed_data_cache()` to force reload
4. **Missing URLs**: Check if ads have exhausted all priority attempts

### Debug Mode

Enable debug logging by setting environment variables or modifying the print statements in the code.

## Dependencies

- `boto3`: AWS SDK for Python
- `requests`: HTTP library for API calls
- `python-dotenv`: Environment variable management
- Standard library: `json`, `os`, `time`, `datetime`, `typing`

## License

This module is part of the campaign reporting system and follows the same licensing terms.
