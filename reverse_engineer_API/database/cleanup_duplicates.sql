-- Disable triggers temporarily
ALTER TABLE job_metrics DISABLE TRIGGER ALL;
ALTER TABLE pole_metrics DISABLE TRIGGER ALL;

-- Create temporary tables to store unique records
CREATE TEMP TABLE temp_job_metrics AS
WITH ranked_jobs AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY job_id, timestamp 
                             ORDER BY id DESC) as rn
    FROM job_metrics
)
SELECT * FROM ranked_jobs WHERE rn = 1;

CREATE TEMP TABLE temp_pole_metrics AS
WITH ranked_poles AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY job_id, node_id, timestamp 
                             ORDER BY id DESC) as rn
    FROM pole_metrics
)
SELECT * FROM ranked_poles WHERE rn = 1;

-- Delete all records from original tables
TRUNCATE TABLE job_metrics;
TRUNCATE TABLE pole_metrics;

-- Reinsert unique records
INSERT INTO job_metrics
SELECT id, job_id, status, utility, total_poles, completed_poles,
       field_complete, back_office_complete, assigned_users,
       priority, target_completion_date, timestamp, created_at
FROM temp_job_metrics;

INSERT INTO pole_metrics
SELECT id, job_id, node_id, utility, field_completed, field_completed_by,
       field_completed_at, back_office_completed, annotated_by,
       annotation_completed_at, pole_height, pole_class, mr_status,
       poa_height, timestamp, created_at
FROM temp_pole_metrics;

-- Drop temporary tables
DROP TABLE temp_job_metrics;
DROP TABLE temp_pole_metrics;

-- Add constraints to prevent future duplicates
ALTER TABLE job_metrics DROP CONSTRAINT IF EXISTS unique_job_timestamp;
ALTER TABLE job_metrics ADD CONSTRAINT unique_job_timestamp 
    UNIQUE (job_id, timestamp);

ALTER TABLE pole_metrics DROP CONSTRAINT IF EXISTS unique_pole_node_timestamp;
ALTER TABLE pole_metrics ADD CONSTRAINT unique_pole_node_timestamp 
    UNIQUE (job_id, node_id, timestamp);

-- Create indices for better performance
CREATE INDEX IF NOT EXISTS idx_job_metrics_timestamp 
    ON job_metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_pole_metrics_timestamp 
    ON pole_metrics(timestamp);

-- Re-enable triggers
ALTER TABLE job_metrics ENABLE TRIGGER ALL;
ALTER TABLE pole_metrics ENABLE TRIGGER ALL; 