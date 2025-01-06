# Status Change Tracking & Daily Updates System

## Overview
A system to track job status changes and metrics through daily snapshots using Cloud SQL and Cloud Run, integrating with the existing daily update process.

## Database Schema

### Job Metrics Table
```sql
CREATE TABLE job_metrics (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    status TEXT NOT NULL,
    utility TEXT,
    total_poles INTEGER,
    completed_poles INTEGER,
    field_complete INTEGER,
    back_office_complete INTEGER,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Status Changes Table
```sql
CREATE TABLE status_changes (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    previous_status TEXT,
    new_status TEXT NOT NULL,
    changed_at TIMESTAMP NOT NULL,
    week_number INTEGER,
    year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Weekly Metrics Table
```sql
CREATE TABLE weekly_metrics (
    id SERIAL PRIMARY KEY,
    week_number INTEGER,
    year INTEGER,
    utility TEXT,
    total_jobs INTEGER,
    total_poles INTEGER,
    completed_poles INTEGER,
    avg_completion_rate FLOAT,
    metrics_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Daily Update Process

1. **Data Collection (Twice Daily)**
   - Cloud Run job executes twice per day
   - Retrieves current job data from Katapult API
   - Captures metrics snapshot
   - Detects status changes by comparing with previous records

2. **Metrics Recording**
   ```python
   def record_job_metrics(job_data: Dict, timestamp: datetime):
       """Record job metrics during each run"""
       with db.connect() as conn:
           conn.execute(
               """INSERT INTO job_metrics 
                  (job_id, status, utility, total_poles, completed_poles, 
                   field_complete, back_office_complete, timestamp)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
               (job_data['id'], job_data['status'], ...)
           )
   ```

3. **Status Change Detection**
   ```python
   def detect_status_changes(job_id: str, current_status: str, timestamp: datetime):
       """Detect and record status changes"""
       with db.connect() as conn:
           # Get previous status
           prev_status = conn.execute(
               """SELECT status FROM job_metrics 
                  WHERE job_id = %s 
                  ORDER BY timestamp DESC LIMIT 1""",
               (job_id,)
           ).fetchone()
           
           if prev_status and prev_status[0] != current_status:
               # Record status change
               conn.execute(
                   """INSERT INTO status_changes 
                      (job_id, previous_status, new_status, changed_at, 
                       week_number, year)
                      VALUES (%s, %s, %s, %s, %s, %s)""",
                   (job_id, prev_status[0], current_status, timestamp, ...)
               )
   ```

## Weekly Report Integration

1. **Data Aggregation**
   ```python
   def aggregate_weekly_metrics():
       """Aggregate metrics for weekly report"""
       with db.connect() as conn:
           return conn.execute(
               """SELECT 
                    utility,
                    COUNT(DISTINCT job_id) as total_jobs,
                    SUM(total_poles) as total_poles,
                    SUM(completed_poles) as completed_poles,
                    AVG(completed_poles::float / NULLIF(total_poles, 0)) as completion_rate
                  FROM job_metrics
                  WHERE date_trunc('week', timestamp) = date_trunc('week', CURRENT_DATE)
                  GROUP BY utility"""
           )
   ```

2. **Status Change Analysis**
   ```python
   def analyze_status_changes():
       """Analyze status changes for the week"""
       with db.connect() as conn:
           return conn.execute(
               """SELECT 
                    previous_status,
                    new_status,
                    COUNT(*) as transition_count,
                    AVG(EXTRACT(EPOCH FROM (changed_at - lag(changed_at) 
                        OVER (PARTITION BY job_id ORDER BY changed_at)))/86400) 
                        as avg_days_in_status
                  FROM status_changes
                  WHERE date_trunc('week', changed_at) = date_trunc('week', CURRENT_DATE)
                  GROUP BY previous_status, new_status"""
           )
   ```

## Key Metrics Tracked

1. **Job Level Metrics**
   - Current status
   - Total poles
   - Completed poles
   - Field completion rate
   - Back office completion rate
   - Time in current status

2. **Status Change Metrics**
   - Status transition patterns
   - Average duration in each status
   - Weekly status change counts
   - Status flow analysis

3. **Utility Level Metrics**
   - Total jobs per utility
   - Completion rates by utility
   - Status distribution by utility
   - Weekly progress metrics

## Benefits

1. **Historical Tracking**
   - Persistent metric history
   - Status change patterns
   - Performance trends
   - Completion rate analysis

2. **Report Generation**
   - Data-driven weekly reports
   - Status change summaries
   - Utility progress tracking
   - Performance analytics

3. **Integration**
   - Works with existing Cloud Run setup
   - Minimal impact on current processes
   - Scalable data storage
   - Reliable metric tracking

## Implementation Notes

1. **Database Connection**
   ```python
   def init_db_connection():
       """Initialize connection to Cloud SQL"""
       return sqlalchemy.create_engine(
           sqlalchemy.engine.url.URL(
               drivername="postgresql+pg8000",
               username=DB_USER,
               password=DB_PASS,
               database=DB_NAME,
               query={
                   "unix_sock": f"/cloudsql/{CLOUD_SQL_CONNECTION_NAME}/.s.PGSQL.5432"
               }
           )
       )
   ```

2. **Error Handling**
   ```python
   def safe_record_metrics(job_data: Dict):
       """Safely record metrics with error handling"""
       try:
           record_job_metrics(job_data, datetime.now())
       except Exception as e:
           logging.error(f"Failed to record metrics: {str(e)}")
           # Implement retry logic or notification system
   ```

3. **Data Cleanup**
   ```python
   def cleanup_old_records():
       """Clean up old records to manage database size"""
       with db.connect() as conn:
           conn.execute(
               """DELETE FROM job_metrics 
                  WHERE timestamp < CURRENT_DATE - INTERVAL '90 days'"""
           )
   ```

## Integration with Existing Daily Run Functions

### Current `main.py` Integration Points

1. **Job Data Collection**
   ```python
   def run_job():
       """Existing run_job function in main.py"""
       try:
           # Initialize database connection
           db = init_db_connection()
           
           # Get current timestamp for consistent recording
           current_timestamp = datetime.now()
           
           # Existing job data collection
           jobs = getJobList()
           
           # Record metrics for each job
           for job in jobs:
               # Extract job data using existing extractNodes
               job_data = extractNodes(job)
               
               # Record metrics to database
               safe_record_metrics(job_data, current_timestamp)
               
               # Check for status changes
               detect_status_changes(
                   job_id=job_data['id'],
                   current_status=job_data['metadata'].get('job_status', 'Unknown'),
                   timestamp=current_timestamp
               )
           
           # Existing SharePoint update logic
           update_sharepoint_tracker(jobs)
           
           # Cleanup old records (optional)
           cleanup_old_records()
           
       except Exception as e:
           logging.error(f"Failed to run daily job: {str(e)}")
           raise
   ```

2. **SharePoint Integration**
   ```python
   def update_sharepoint_tracker(jobs: List[Dict]):
       """Modified SharePoint update function"""
       try:
           # Existing SharePoint clear/update logic
           clear_sharepoint_list()
           
           # Process jobs for SharePoint
           for job in jobs:
               # Get historical status data
               status_history = get_job_status_history(job['id'])
               
               # Enhance job data with historical metrics
               enhanced_job_data = enhance_job_data_with_history(job, status_history)
               
               # Update SharePoint with enhanced data
               update_sharepoint_item(enhanced_job_data)
               
       except Exception as e:
           logging.error(f"Failed to update SharePoint: {str(e)}")
           raise
   ```

3. **Historical Data Enhancement**
   ```python
   def get_job_status_history(job_id: str) -> Dict:
       """Get historical status data for a job"""
       with db.connect() as conn:
           # Get status changes for the past week
           status_changes = conn.execute(
               """SELECT 
                    previous_status,
                    new_status,
                    changed_at,
                    EXTRACT(EPOCH FROM (
                        changed_at - LAG(changed_at) OVER (
                            PARTITION BY job_id 
                            ORDER BY changed_at
                        )
                    ))/86400 as days_in_status
                  FROM status_changes
                  WHERE job_id = %s
                  AND changed_at >= CURRENT_DATE - INTERVAL '7 days'
                  ORDER BY changed_at DESC""",
               (job_id,)
           ).fetchall()
           
           # Get performance metrics
           metrics = conn.execute(
               """SELECT 
                    AVG(completed_poles::float / NULLIF(total_poles, 0)) as avg_completion_rate,
                    MAX(completed_poles) - MIN(completed_poles) as poles_completed_this_week
                  FROM job_metrics
                  WHERE job_id = %s
                  AND timestamp >= CURRENT_DATE - INTERVAL '7 days'""",
               (job_id,)
           ).fetchone()
           
           return {
               'status_changes': status_changes,
               'metrics': metrics
           }
   ```

### Deployment Changes

1. **Cloud Run Service Updates**
   - Update service configuration to include database credentials
   - Add Cloud SQL connection
   - Update memory allocation if needed
   ```yaml
   env_variables:
       DB_USER: "${DB_USER}"
       DB_PASS: "${DB_PASS}"
       DB_NAME: "${DB_NAME}"
       CLOUD_SQL_CONNECTION_NAME: "${CLOUD_SQL_CONNECTION_NAME}"
   
   cloudsql_instances:
   - "${CLOUD_SQL_CONNECTION_NAME}"
   ```

2. **Environment Variables**
   ```bash
   # Add to your .env file or Cloud Run configuration
   DB_USER=your_db_user
   DB_PASS=your_db_password
   DB_NAME=your_db_name
   CLOUD_SQL_CONNECTION_NAME=your-project:region:instance
   ```

3. **Dependencies**
   ```txt
   # Add to requirements.txt
   sqlalchemy==1.4.46
   pg8000==1.29.4  # Python PostgreSQL driver
   ```

### Testing Integration

1. **Local Testing**
   ```python
   def test_daily_run():
       """Test the daily run process locally"""
       # Set up test database
       test_db = init_test_db()
       
       # Run the job
       run_job()
       
       # Verify metrics were recorded
       verify_metrics_recorded()
       
       # Verify status changes detected
       verify_status_changes()
       
       # Verify SharePoint updates
       verify_sharepoint_updates()
   ```

2. **Monitoring**
   ```python
   def monitor_daily_run():
       """Monitor the daily run process"""
       metrics = {
           'jobs_processed': 0,
           'metrics_recorded': 0,
           'status_changes_detected': 0,
           'sharepoint_updates': 0
       }
       
       # Update metrics during processing
       def update_metrics(metric_name):
           metrics[metric_name] += 1
           
       # Log metrics after completion
       logging.info(f"Daily run metrics: {json.dumps(metrics, indent=2)}")
   ```

### Rollout Strategy

1. **Phase 1: Parallel Run**
   - Deploy database schema
   - Run new system alongside existing process
   - Verify data consistency
   - Monitor performance impact

2. **Phase 2: SharePoint Enhancement**
   - Integrate historical data into SharePoint updates
   - Validate enhanced data quality
   - Monitor user feedback

3. **Phase 3: Full Integration**
   - Switch to new system completely
   - Monitor for any issues
   - Implement additional features based on feedback

### Maintenance Considerations

1. **Database Maintenance**
   - Regular backups
   - Performance monitoring
   - Index optimization
   - Data retention policy enforcement

2. **Error Recovery**
   ```python
   def recover_failed_run():
       """Recover from a failed daily run"""
       # Get last successful run timestamp
       last_success = get_last_successful_run()
       
       # Reprocess jobs since last success
       reprocess_jobs(last_success)
       
       # Verify data consistency
       verify_data_consistency()
   ```

3. **Monitoring Alerts**
   - Set up alerts for:
     - Failed runs
     - Database connection issues
     - High latency
     - Storage capacity
     - Error rate thresholds
