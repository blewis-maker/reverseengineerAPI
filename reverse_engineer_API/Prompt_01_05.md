# Katapult API Integration Project

#Completed Tasks

6. Implemented Cloud Run System:
   - Configured `main.py` for automated daily updates
   - Set up Google Cloud Run infrastructure
   - Implemented job data extraction and processing
   - Configured Cloud Scheduler for daily runs
   - Set up SharePoint integration for daily reports
   - Implemented email notifications

## Current Status
- Full database extraction is working
- Daily update system is running on Google Cloud Run:
  - Scheduled execution at 8:00 PM
  - Successfully processing job data
  - Updating ArcGIS layers
  - Generating daily Aerial Status Report
- SharePoint integration is configured for:
  - Daily Aerial Status Tracker updates
- ArcGIS Enterprise integration:
  - Basic authentication implemented
  - Feature service endpoints configured
  - Field mappings established for poles, connections, and anchors
  - Initial testing shows successful shapefile generation

## Next Steps

### 1. Weekly Reporting System Implementation

#### Core Dependencies
1. Reuse Existing `main.py` Functions:
   ```python
   from main import (
       getJobData,           # Fetch detailed job data
       extractNodes,         # Extract and transform node data
       extractConnections,   # Extract and transform connection data
       extractAnchors,       # Extract and transform anchor data
       getUserList,         # Get user mapping information
       update_sharepoint_spreadsheet,  # SharePoint integration (we may need to make a new function that only accesses the new weekly reportworkbook with its necessary sheets so we dont confuse the 2 and mess up our current data)
       CONFIG               # Configuration settings
   )
   ```

2. Data Processing Flow:
   ```python
   def process_weekly_data():
       """Process weekly data using main.py functions"""
       # 1. Get weekly job list
       jobs = get_weekly_jobs()
       
       # 2. Use main.py functions to process each job
       for job_id in jobs:
           # Get detailed job data using existing function
           job_data = getJobData(job_id)
           
           # Extract features using existing transformations
           nodes = extractNodes(job_data, job_name, job_id)
           connections = extractConnections(job_data['connections'], nodes, job_data)
           anchors = extractAnchors(job_data, job_name, job_id)
           
           # New: Track status changes and user metrics
           track_status_changes(job_data)
           track_user_production(nodes)
   ```

#### Phase 1: Data Collection and Processing
1. Create Weekly Job Tracker:
   ```python
   def get_weekly_jobs():
       """Get jobs updated in the last 7 days"""
       # Leverage main.py's API handling and error retry logic
       # Add weekly time window to existing API call
       # Track status changes using main.py's data structure
   ```

2. User Production Tracking:
   ```python
   def track_user_production(nodes_data):
       """Track user productivity metrics using main.py's node structure"""
       # Use main.py's node structure for consistency
       # Field Collection: nodes_data['fld_complete']
       # Annotation: nodes_data['done']
       # Status transitions from main.py's status mapping
   ```

3. Status Change Tracking:
   ```python
   def track_status_changes(job_data):
       """Track job status transitions using main.py's data structure"""
       # Use main.py's job data structure
       # Leverage existing status mappings
       # Track transitions using main.py's metadata
   ```

#### Phase 2: Report Generation
1. Weekly Status Report:
   ```python
   def generate_weekly_status():
       """Generate weekly status report using main.py data structures"""
       # Use main.py's data transformation functions
       metrics = {
           'user_production': track_user_production(nodes),
           'status_changes': track_status_changes(job_data),
           'utilities': job_data.get('utility'),
           'dates': {
               'status_change': job_data.get('last_modified'),
               'field_complete': nodes.get('field_complete_date')
           }
       }
   ```

2. Burndown Analysis:
   ```python
   def calculate_burndown():
       """Calculate project burndown metrics"""
       # Use main.py's pole counting and status tracking
       # Leverage existing utility categorization
       # Use main.py's user assignment data
       backlog = {
           'field': count_status_poles('Pending Field Collection'),
           'backoffice': count_non_completed_poles() - field_backlog
       }
   ```

3. Schedule Generation:
   ```python
   def generate_schedule():
       """Generate project schedules using main.py data"""
       # Use main.py's job status tracking
       # Leverage existing utility categorization
       # Use main.py's pole counting functions
   ```

#### Phase 3: SharePoint Integration
1. Directory Structure (extending main.py's SharePoint integration):
   ```python
   def setup_weekly_sharepoint():
       """Set up SharePoint structure using main.py's connection"""
       # Use main.py's SharePoint authentication
       # Extend existing folder structure:
       folders = {
           'root': 'Weekly_Reports',
           'year': datetime.now().strftime('%Y'),
           'week': f"Week_{datetime.now().strftime('%V')}"
       }
   ```

2. Master Spreadsheet Integration:
   ```python
   def update_master_spreadsheet():
       """Update master spreadsheet using main.py's SharePoint functions"""
       # Use main.py's update_sharepoint_spreadsheet()
       # Extend with new sheets:
       sheets = {
           'weekly_status': generate_weekly_status(),
           'burndown': calculate_burndown(),
           'schedule': generate_schedule()
       }
   ```

#### Phase 4: Testing Plan
1. Test Data Setup (using main.py's data structures):
   ```python
   def setup_test_data():
       """Set up test data using main.py's functions"""
       test_jobs = [
           # Use main.py's getJobData structure
           {
               'job_id': 'test_1',
               'status': 'Pending Field Collection',
               'utility': 'Utility A',
               'nodes': [
                   # Use main.py's node structure
                   {'fld_complete': True, 'done': False},
                   {'fld_complete': False, 'done': False}
               ]
           },
           # Additional test jobs...
       ]
   ```

2. Validation Functions:
   ```python
   def validate_weekly_metrics():
       """Validate weekly metrics using main.py's data validation"""
       # Use main.py's data structure validation
       # Verify metrics against known test data
       # Validate SharePoint integration
   ```

3. Test Scenarios (integrated with main.py):
   ```python
   def run_test_scenarios():
       """Run test scenarios using main.py's functions"""
       # Test normal operation using main.py
       job_data = getJobData(test_job_id)
       nodes = extractNodes(job_data)
       
       # Test status changes
       track_status_changes(job_data)
       
       # Test SharePoint integration
       update_sharepoint_spreadsheet(test_report)
   ```

#### Phase 5: Automation
1. Scheduler Setup (extending main.py's functionality):
   ```python
   def weekly_report_job():
       """Weekly report generation integrated with main.py"""
       try:
           # Use main.py's core functions
           jobs = get_weekly_jobs()
           for job_id in jobs:
               job_data = getJobData(job_id)
               process_job_data(job_data)
           
           # Generate reports
           generate_weekly_status()
           calculate_burndown()
           generate_schedule()
           
           # Update SharePoint
           update_master_spreadsheet()
           
       except Exception as e:
           logger.error(f"Weekly report generation failed: {str(e)}")
   ```

2. Priority Management (using main.py's data structures):
   ```python
   def adjust_priorities(priority_updates):
       """Adjust priorities using main.py's job structure"""
       # Use main.py's job data structure
       # Update priorities in existing jobs
       # Recalculate schedules and burndown
   ```

### Implementation Notes
1. Data Structure (extending main.py):
   ```python
   WeeklyMetrics = {
       'user_production': {
           'field': {
               # Use main.py's user and pole structure
               'user_id': {
                   'completed_poles': [],
                   'utilities': set(),
                   'dates': []
               }
           },
           'backoffice': {
               # Use main.py's status tracking
               'annotation_complete': [],
               'sent_to_pe': [],
               'delivered': []
           }
       },
       'status_changes': [
           # Use main.py's status structure
           {
               'job_id': str,
               'old_status': str,
               'new_status': str,
               'date': datetime
           }
       ]
   }
   ```

2. Configuration (extending main.py's CONFIG):
   ```python
   WEEKLY_CONFIG = {
       # Extend main.py's CONFIG
       'run_rates': {
           'utility_a': {'poles_per_day': 50},
           'utility_b': {'poles_per_day': 40}
       },
       'priorities': {
           'high': 3,
           'medium': 2,
           'low': 1
       }
   }
   ```

### Next Actions
1. Create test jobs using main.py's data structure
2. Implement weekly job tracking extending main.py's functionality
3. Build reporting functions using main.py's data transformation
4. Set up SharePoint structure using main.py's integration
5. Create test scenarios using main.py's functions
6. Implement automation extending main.py's scheduling

### 1. ArcGIS Integration Completion
1. Token Management:
   - Implement token refresh mechanism
   - Add exponential backoff for login attempts
   - Handle "Too many login attempts" gracefully
   - Add detailed logging for authentication issues

2. Feature Service Updates:
   - Verify field mappings match exactly
   - Test incremental updates
   - Implement error recovery for failed updates
   - Add validation for updated features

3. Testing and Validation:
   - Verify pole updates are successful
   - Confirm connection geometries
   - Validate anchor specifications
   - Test job-based filtering

### 2. Automation Setup
1. Configure Scheduled Tasks:
   - Daily updates at 11:59 PM
   - Weekly updates on Saturday night (this is a very important step)
   - Error notification system
   - Retry mechanisms

2. ArcEnterprise Integration:
   - Implement feature service updates
   - Handle geometry modifications
   - Set up incremental updates
   - Preserve unique IDs

3. Testing and Validation:
   - Test daily update cycle
   - Validate weekly report generation
   - Verify SharePoint integration
   - Test ArcEnterprise updates

### 2. Monitoring and Maintenance
1. Setup Monitoring:
   - Job processing status
   - API rate limits
   - SharePoint sync status
   - Error tracking

2. Implement Recovery Procedures:
   - Failed update recovery
   - Data consistency checks
   - Manual override options
   - Backup procedures

### 3. Documentation
1. System Documentation:
   - Architecture overview
   - Configuration guide
   - Troubleshooting steps
   - Recovery procedures

2. User Documentation:
   - Report interpretation guide
   - Metrics definitions
   - Chart explanations
   - Common issues and solutions

## API Integration Notes
- Using KatapultPro API v2
- SharePoint integration via Microsoft Graph API
- Email notifications through Microsoft Graph API
- All credentials managed through environment variables

## Configuration
- Test mode toggle available for development
- Configurable email recipients
- Adjustable API retry parameters
- Customizable report formatting

## Known Issues
- Rate limiting requires careful management
- Large datasets require significant processing time
- Some jobs may lack certain optional fields

Grant the service account access to the secrets:

gcloud projects add-iam-policy-binding katapult-automation \
    --member="serviceAccount:897830478647-compute@developer.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"

2. After granting permissions, we can try deploying again. The service should be configured to run at 8:00 using Cloud Scheduler. We'll create a Cloud Scheduler job that triggers the Cloud Run service:

gcloud scheduler jobs create http katapult-updater-job \
    --schedule="0 8 * * *" \
    --uri="YOUR_CLOUD_RUN_URL" \
    --http-method=POST \
    --time-zone="America/Denver" \
    --project=katapult-automation
    "nodes": {
    "-Nvs8vxMPT4SJakAXwbC": {
      "_created": {
        "method": "desktop",
        "timestamp": 1713560338263,
        "uid": "sBIGRMMFIUYY9qaKgNlvAQIa61u2"
      },
     "nodes": {
    "-Nvs8vxMPT4SJakAXwbC": {
      "_created": {
        "method": "desktop",
        "timestamp": 1713560338263,
        "uid": "sBIGRMMFIUYY9qaKgNlvAQIa61u2"
      },
      "attributes": {
        "address": {
          "button_added": "211 S 15th St, Grand Junction, CO 81501, USA"
        },
        "done": {
          "button_added": true
        },
```

### 1. Weekly Reporting System Requirements

#### Core Requirements
1. Weekly Report Generation:
   - Generated every Sunday morning at 8:00 AM
   - Uses getUpdatedJobs API call to collect data from past 7 days (Sunday to Sunday)
   - Creates new sheet weekly, stored in Weekly Reports Directory on SharePoint

2. OSP Production Master Spreadsheet Structure:
   a) Weekly Status Report Sheet:
      - User-level pole completion metrics
      - Job status change tracking
      - Schedule adherence monitoring
      - Pole completion tracking based on:
        1. Field Collection:
           - Tracked by poles with fld_complete attribute = true
        2. Back Office Production:
           - Annotation: Poles with done attribute = true
             * Include associated jobs list
             * Original status date
           - Sent to PE: Poles in jobs changed from "Pending Photo Annotation" to "Sent to PE"
             * Include original status date
           - Delivery: Poles in jobs changed from "Sent to PE" to "Delivered/Pending EMR"
             * Based on previous week's reports or mid-week verification
             * Include original status date
        3. EMR Status:
           - Jobs currently in "Pending EMR" status
           - Include associated job list and original status date
        4. Approved for Construction:
           - Jobs moved to "Approved for Construction"
           - Include transition date
        * All metrics must include associated utility information

   b) Burndown Sheet:
      1. Configurable Parameters:
         - Administrator-defined run rates per utility
         - User assignment tracking
         - Project timeline predictions
      2. Burndown Chart Features:
         - Project-specific completion rates
         - Status-based progress tracking
         - Team/management progress monitoring
      3. Backlog Tracking:
         - All poles not in "Approved for Construction", "Hold", or "Junk" status
         - Separated into:
           * Field Backlog (Pending Field Collection status)
           * Back Office Backlog (all other active statuses)

   c) Schedule Sheet:
      1. Project Timeline Table:
         - Individual project completion estimates
         - Backlog totals excluding "Approved for Construction", "Hold", "Junk"
         - Separated field/back office backlog tracking
      2. Future Enhancement:
         - Priority-based schedule adjustments
         - Automatic burndown recalculation based on priority changes

#### Future Enhancements
1. Priority Management System:
   - Automated schedule adjustments
   - Dynamic burndown recalculation
   - Project priority configuration

2. Advanced Analytics:
   - Sophisticated prediction models
   - Workload planning tools
   - Backlog scaling analysis