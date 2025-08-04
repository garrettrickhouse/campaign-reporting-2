# Campaign Reporting Dashboard

A comprehensive Streamlit dashboard for analyzing campaign performance metrics from Meta and Northbeam data.

## Features

- 📊 Real-time campaign metrics visualization
- 🏆 Top performing ads, products, creators, and agencies
- 📈 Performance analytics and insights
- 🔄 Dynamic report generation
- 📋 Configurable filtering and analysis
- 📄 Google Doc export functionality

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd campaign-reporting-2
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables:**
   Create a `.env` file with your API credentials:
   ```env
   NORTHBEAM_DATA_CLIENT_ID=your_client_id
   NORTHBEAM_API_KEY=your_api_key
   NORTHBEAM_PLATFORM_ACCOUNT_ID=your_platform_account_id
   AWS_ACCESS_KEY_ID=your_aws_access_key
   AWS_SECRET_ACCESS_KEY=your_aws_secret_key
   S3_BUCKET=your_s3_bucket
   META_SYSTEM_USER_ACCESS_TOKEN=your_meta_token
   GOOGLE_CREDENTIALS_FILE=credentials/your-google-credentials.json
   ```

4. **Set up Google Drive API:**
   - Create a Google Cloud Project
   - Enable Google Drive API
   - Create a service account and download the JSON credentials
   - Place the credentials file in the `credentials/` directory
   - Update the `GOOGLE_CREDENTIALS_FILE` environment variable

## Usage

### Running the Streamlit App

1. **Start the Streamlit app:**
   ```bash
   streamlit run app.py
   ```

2. **Open your browser:**
   Navigate to `http://localhost:8501`

3. **Generate a report:**
   - Review the configuration in the sidebar
   - Click "Generate Report" to create a new analysis
   - Explore the Summary and Details tabs

### Google Doc Export

After generating a report, you can export it as a Google Doc:

1. **Generate the report** using the steps above
2. **Click "Generate Google Doc"** in the Export Report section
3. **Wait for processing** - the system will:
   - Generate a comprehensive markdown report
   - Upload it to Google Drive
   - Convert it to a Google Doc
   - Make it shareable
4. **Access the document** via the provided shareable link
5. **Download the markdown** locally if needed

The exported report includes:
- **Executive Summary** with overall metrics
- **Top N tables** for ads, products, creators, and agencies
- **Campaign Analysis** with breakdowns by campaign type
- **Product Analysis** for each core product within campaigns
- **Creator Analysis** for each product-campaign combination

### Report Structure

The generated reports follow this comprehensive structure:

#### Executive Summary
- **Overall Performance Metrics:** Total ads, spend, revenue, ROAS, CTR, CPM, Thumbstop, AOV
- **Top N Ads by Spend:** Detailed table with campaign, product, ad type, creator, agency, and all metrics
- **Top N Products by Spend:** Aggregated product performance with ad counts and metrics
- **Top N Creators by Spend:** Creator performance with total ads and metrics
- **Top N Agencies by Spend:** Agency performance comparison

#### Campaign Analysis
For each campaign type (e.g., Prospecting, Retargeting):
- **Campaign Summary:** High-level metrics for the campaign
- **Product Breakdown:** For each core product within the campaign:
  - **Product Summary:** Metrics specific to the product-campaign combination
  - **Top Ads:** Best performing ads for that product-campaign combination
  - **Top Creators:** Best performing creators for that product-campaign combination

### Configuration

The app uses the following configuration from `main.py`:

- **Date Range:** `DATE_FROM` and `DATE_TO`
- **Top N:** Number of top performers to display
- **Agency Codes:** List of agency codes to filter by
- **Campaign Types:** Configured campaign types for analysis
- **Core Products:** Product groups for merging logic

## Dashboard Features

### Summary Tab
- **Overall Metrics:** Total ads, spend, ROAS, CTR, CPM, Thumbstop, AOV, Revenue
- **Top N Ads:** Best performing ads sorted by spend
- **Top N Products:** Aggregated product performance
- **Top N Creators:** Best performing creators
- **Top Agencies:** Agency performance comparison

### Details Tab
- Detailed analysis features (coming soon)
- Raw data exploration
- Advanced filtering options

## Data Sources

- **Meta Graph API:** Ad performance data
- **Northbeam API:** Attribution and revenue data
- **AWS S3:** Data storage and retrieval

## File Structure

```
campaign-reporting-2/
├── app.py                 # Streamlit dashboard
├── main.py               # Core reporting logic
├── requirements.txt      # Python dependencies
├── README.md            # This file
├── .env                 # Environment variables (create this)
└── reports/             # Generated reports
    ├── comprehensive_ads_*.json
    ├── meta_insights_*.json
    └── northbeam_*.csv
```

## Troubleshooting

### Common Issues

1. **Missing environment variables:**
   - Ensure all required API keys are set in `.env`
   - Check that the file is in the project root

2. **Data not loading:**
   - Verify API credentials are correct
   - Check network connectivity
   - Review console logs for errors

3. **Streamlit not starting:**
   - Ensure all dependencies are installed
   - Check Python version compatibility
   - Try `pip install --upgrade streamlit`

### Getting Help

- Check the console output for detailed error messages
- Verify API credentials and permissions
- Ensure all required services are accessible

## Development

### Adding New Features

1. **New Metrics:** Add calculation functions in `main.py`
2. **New Visualizations:** Extend the dashboard in `app.py`
3. **New Data Sources:** Implement new API integrations

### Testing

```bash
# Run the app in development mode
streamlit run app.py --server.port 8501

# Check for linting issues
pip install flake8
flake8 app.py main.py
```

## License

This project is licensed under the MIT License. 