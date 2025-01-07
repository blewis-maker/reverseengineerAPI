# KatapultPro Data Processing

This script processes data from the KatapultPro API, generating reports and geographic data files.

## Features

- Fetches job data from KatapultPro API
- Processes nodes, connections, and anchors
- Generates shapefiles and GeoPackage files
- Creates Excel reports with job status
- Uploads data to SharePoint
- Sends email notifications with reports
- Maintains temporal database of metrics and status changes
- Generates weekly progress reports

## Documentation

- [Database Structure and Relationships](DATABASE.md)
- [API Documentation](API.md) (TODO)
- [Weekly Reports Guide](REPORTS.md) (TODO)

## Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in your credentials:
   ```bash
   cp .env.example .env
   ```

## Environment Variables

The following environment variables are required:

- `EMAIL_USER`: Email address for notifications
- `EMAIL_PASSWORD`: Email password
- `AZURE_CLIENT_ID`: Azure AD client ID
- `AZURE_CLIENT_SECRET`: Azure AD client secret
- `AZURE_TENANT_ID`: Azure AD tenant ID
- `ARCGIS_USERNAME`: ArcGIS username
- `ARCGIS_PASSWORD`: ArcGIS password
- `KATAPULT_API_KEY`: KatapultPro API key
- `DB_NAME`: Database name (default: metrics_db)
- `DB_USER`: Database username
- `DB_PASS`: Database password
- `DB_HOST`: Database host
- `DB_PORT`: Database port (default: 5432)

## Database Setup

1. Create PostgreSQL database:
   ```bash
   createdb metrics_db
   ```

2. Apply database schema:
   ```bash
   psql -d metrics_db -f database/schema/schema.sql
   psql -d metrics_db -f database/schema/temporal_tracking.sql
   ```

3. Verify database setup:
   ```bash
   python3 database/verify_tables.py
   ```

## Local Usage

Run the script locally with:
```bash
python main.py
```

The script will:
1. Fetch all jobs from KatapultPro
2. Process the job data
3. Generate geographic data files
4. Create an Excel report
5. Upload data to SharePoint
6. Send email notifications
7. Update metrics database

## Weekly Reports

Generate weekly reports with:
```bash
python weekly_reporter.py
```

The weekly reporter:
1. Analyzes job status changes
2. Calculates user productivity
3. Generates burndown metrics
4. Creates detailed Excel reports
5. Uploads reports to SharePoint

See [DATABASE.md](DATABASE.md) for details on metrics tracking and data relationships.

## Cloud Run Deployment

The service is deployed on Google Cloud Run with the following configuration:

1. Build and deploy:
   ```bash
   gcloud builds submit --tag gcr.io/katapult-automation/katapult-updater
   gcloud run deploy katapult-updater --image gcr.io/katapult-automation/katapult-updater --platform managed --region us-central1 --project katapult-automation
   ```

2. Service Configuration:
   - Memory: 1GB
   - CPU: 1 core
   - Maximum instances: 100
   - Timeout: 3600s (1 hour)
   - Port: 8080
   - Region: us-central1

3. Scheduled Execution:
   - Runs daily at 8:00 PM
   - Managed by Cloud Scheduler
   - Updates ArcGIS layer and generates Aerial Status Report
   - Updates metrics database
   - Sends email notifications with report attachments

## Output Files

- `*.gpkg`: GeoPackage files containing geographic data
- `*.shp`, `*.shx`, `*.dbf`, `*.prj`: Shapefile components
- Excel reports with job status
- Weekly progress reports
- Log files with processing details

## Notes

- Sensitive files (`.env`, tokens, logs) are excluded from git
- SharePoint integration requires valid Azure AD credentials
- Email notifications require valid email credentials
- Database connections require valid PostgreSQL credentials 