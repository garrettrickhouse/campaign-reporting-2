# Northbeam Export Configuration Guide

## Environment Variables

Add these to your `.env` file to configure Northbeam export behavior:

```bash
# Required API Configuration
NORTHBEAM_DATA_CLIENT_ID=your_client_id_here
NORTHBEAM_API_KEY=your_api_key_here
NORTHBEAM_PLATFORM_ACCOUNT_ID=your_platform_account_id_here

# Optional Export Configuration (defaults shown)
NORTHBEAM_EXPORT_TIMEOUT=300      # 5 minutes - how long to wait for export completion
NORTHBEAM_POLL_INTERVAL=10        # 10 seconds - how often to check export status
NORTHBEAM_MAX_RETRIES=3           # 3 attempts - how many times to retry failed requests
```

## Configuration Options

### NORTHBEAM_EXPORT_TIMEOUT
- **Default**: 300 seconds (5 minutes)
- **Purpose**: Maximum time to wait for an export to complete
- **Recommendation**: 
  - For small date ranges (1-7 days): 300 seconds
  - For medium date ranges (8-30 days): 600 seconds (10 minutes)
  - For large date ranges (30+ days): 900 seconds (15 minutes)

### NORTHBEAM_POLL_INTERVAL
- **Default**: 10 seconds
- **Purpose**: How frequently to check export status
- **Recommendation**: 
  - Keep at 10 seconds for most cases
  - Increase to 15-20 seconds if you hit rate limits frequently
  - Northbeam allows polling once per second, but 10s is more conservative

### NORTHBEAM_MAX_RETRIES
- **Default**: 3 attempts
- **Purpose**: How many times to retry failed API requests
- **Recommendation**: 
  - Keep at 3 for most cases
  - Increase to 5 if you experience intermittent network issues

## Troubleshooting Common Issues

### 1. Exports Timing Out
**Symptoms**: "Export polling timed out" messages
**Solutions**:
- Increase `NORTHBEAM_EXPORT_TIMEOUT` value
- Check if your date range is very large
- Verify your export payload isn't too complex

### 2. Rate Limit Errors
**Symptoms**: HTTP 429 errors during polling
**Solutions**:
- Increase `NORTHBEAM_POLL_INTERVAL` to 15-20 seconds
- The app automatically handles rate limits with consistent delays

### 3. Export Creation Fails
**Symptoms**: "Export creation failed" messages
**Solutions**:
- Check your API credentials
- Verify your export payload format
- Ensure your AWS S3 role is properly configured

### 4. S3 Fallback Issues
**Symptoms**: "Falling back to S3" but no files found
**Solutions**:
- Check your S3 bucket permissions
- Verify the AWS role ARN in your export payload
- Ensure the export actually completed successfully

## Testing Your Configuration

Use the included test script to verify your setup:

```bash
python test_northbeam_export.py
```

This will:
1. Test your API connection
2. Create a small test export
3. Monitor the export status
4. Report any issues found

## Best Practices

1. **Start Conservative**: Begin with default values and adjust based on your needs
2. **Monitor Logs**: Watch for timeout and rate limit messages
3. **Test Small Exports**: Verify functionality with 1-2 day ranges first
4. **Use Caching**: Enable `use_cached_files=True` to avoid unnecessary re-exports
5. **Check S3**: Verify exports are actually reaching your S3 bucket

## When to Adjust Settings

- **Increase timeout**: If exports consistently take longer than 5 minutes
- **Increase poll interval**: If you frequently hit rate limits
- **Increase retries**: If you have unstable network connections
- **Decrease poll interval**: If you need faster export completion (but be careful of rate limits)
