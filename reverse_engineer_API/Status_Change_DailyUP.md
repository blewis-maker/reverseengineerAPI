# Status Change Tracking & Daily Updates System

## Implementation Status
- ✅ Database Schema Created
- ✅ Basic Database Connection
- ✅ Job Metrics Recording
- ✅ Pole Metrics Recording
- ❌ Status Change Detection
- ❌ User Metrics Recording
- ❌ Daily Summary Updates
- ❌ Burndown Metrics

## Database Schema

### Job Metrics Table (Implemented)
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
    assigned_users TEXT[],
    priority INTEGER,
    target_completion_date DATE,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Status Changes Table (Not Recording)
```sql
CREATE TABLE status_changes (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    previous_status TEXT,
    new_status TEXT NOT NULL,
    changed_at TIMESTAMP NOT NULL,
    changed_by TEXT,
    week_number INTEGER,
    year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Pole Metrics Table (Implemented)
```sql
CREATE TABLE pole_metrics (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    utility TEXT,
    field_completed BOOLEAN,
    field_completed_by TEXT,
    field_completed_at TIMESTAMP,
    back_office_completed BOOLEAN,
    annotated_by TEXT,
    annotation_completed_at TIMESTAMP,
    pole_height TEXT,
    pole_class TEXT,
    mr_status TEXT,
    poa_height TEXT,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### User Metrics Table (Not Recording)
```sql
CREATE TABLE user_metrics (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    utility TEXT,
    role TEXT,
    activity_type TEXT,
    poles_completed INTEGER,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### User Daily Summary Table (Not Recording)
```sql
CREATE TABLE user_daily_summary (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    date DATE NOT NULL,
    role TEXT,
    total_poles_completed INTEGER,
    utilities_worked TEXT[],
    jobs_worked TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Burndown Metrics Table (Not Recording)
```sql
CREATE TABLE burndown_metrics (
    id SERIAL PRIMARY KEY,
    utility TEXT,
    date DATE NOT NULL,
    total_poles INTEGER,
    completed_poles INTEGER,
    run_rate DOUBLE PRECISION,
    estimated_completion_date DATE,
    actual_resources INTEGER,
    required_resources INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Current Implementation Status

### Working Features
1. **Job Metrics Recording**
   - Successfully capturing job status
   - Recording pole counts
   - Tracking completion rates
   - Storing utility information

2. **Pole Metrics Recording**
   - Recording individual pole data
   - Tracking field completion status
   - Storing pole specifications
   - Capturing MR status

### Pending Implementation
1. **Status Change Detection**
   - Need to implement comparison logic
   - Add status change recording
   - Track change timestamps
   - Record change authors

2. **User Metrics**
   - Need to implement user activity tracking
   - Add role-based metrics
   - Track individual productivity
   - Record daily summaries

3. **Burndown Analytics**
   - Implement run rate calculations
   - Add resource tracking
   - Calculate completion estimates
   - Track utility-level progress

## Next Steps

1. **Fix Status Change Recording**
   - Implement status comparison logic
   - Add proper error handling
   - Include user tracking

2. **Implement User Metrics**
   - Extract editor information from photo data
   - Track field vs back office activities
   - Record daily summaries

3. **Add Burndown Calculations**
   - Implement run rate logic
   - Add resource calculations
   - Track completion estimates

## Integration Points

1. **Job Processing**
```python
def run_job():
    """Main job processing function"""
    try:
        # Initialize metrics recorder
        metrics_recorder = MetricsRecorder()
        
        # Process each job
        for job in jobs:
            # Record job metrics
            metrics_recorder.record_job_metrics(job)
            
            # Check for status changes
            metrics_recorder.check_status_changes(job)
            
            # Record pole metrics
            metrics_recorder.record_pole_metrics(job)
            
            # Update user metrics
            metrics_recorder.record_user_metrics(job)
            
    except Exception as e:
        logging.error(f"Failed to run job: {str(e)}")
```

2. **User Activity Tracking**
```python
def record_user_metrics(self, job_data):
    """Record user activity metrics"""
    try:
        # Extract editor information
        editors = self._get_editors_from_photos(job_data)
        
        # Record metrics for each editor
        for editor in editors:
            self._record_editor_activity(editor, job_data)
            
    except Exception as e:
        logging.error(f"Failed to record user metrics: {str(e)}")
```

## Testing Strategy

1. **Unit Tests**
   - Test each metrics recording function
   - Validate data integrity
   - Check error handling

2. **Integration Tests**
   - Test full job processing
   - Verify database updates
   - Check metric calculations

3. **Monitoring**
   - Track successful records
   - Monitor error rates
   - Check data consistency
