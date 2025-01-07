-- Create status changes table
CREATE TABLE IF NOT EXISTS status_changes (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    previous_status TEXT,
    new_status TEXT NOT NULL,
    changed_at TIMESTAMP NOT NULL,
    changed_by TEXT,
    duration_hours FLOAT,
    week_number INTEGER,
    year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (job_id, changed_at)
);

-- Create indices for status changes
CREATE INDEX IF NOT EXISTS idx_status_changes_job ON status_changes(job_id);
CREATE INDEX IF NOT EXISTS idx_status_changes_time ON status_changes(changed_at);

-- Add temporal tracking support

-- Create daily snapshots table for point-in-time analysis
CREATE TABLE IF NOT EXISTS daily_snapshots (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    status TEXT NOT NULL,
    utility TEXT NOT NULL,
    total_poles INTEGER NOT NULL,
    field_complete INTEGER NOT NULL,
    back_office_complete INTEGER NOT NULL,
    snapshot_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL,
    UNIQUE (job_id, snapshot_date)
);

-- Add indices for efficient temporal queries
CREATE INDEX IF NOT EXISTS idx_daily_snapshots_job_date ON daily_snapshots (job_id, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_daily_snapshots_utility_date ON daily_snapshots (utility, snapshot_date);

-- Add duration tracking to status changes
ALTER TABLE status_changes 
ADD COLUMN IF NOT EXISTS duration_hours FLOAT,
ADD COLUMN IF NOT EXISTS changed_by TEXT;

-- Rename pole_id to node_id in pole_metrics
DO $$ 
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pole_metrics' AND column_name = 'pole_id'
    ) THEN
        ALTER TABLE pole_metrics RENAME COLUMN pole_id TO node_id;
    END IF;
END $$;

-- Add unique constraints to prevent duplicates
ALTER TABLE pole_metrics 
DROP CONSTRAINT IF EXISTS unique_pole_timestamp,
ADD CONSTRAINT unique_pole_node_timestamp UNIQUE (job_id, node_id, timestamp);

ALTER TABLE job_metrics 
DROP CONSTRAINT IF EXISTS unique_job_timestamp,
ADD CONSTRAINT unique_job_timestamp UNIQUE (job_id, timestamp);

ALTER TABLE status_changes
DROP CONSTRAINT IF EXISTS unique_status_change,
ADD CONSTRAINT unique_status_change UNIQUE (job_id, changed_at);

-- Create status change log table
CREATE TABLE IF NOT EXISTS status_change_log (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    entity_type TEXT NOT NULL, -- 'job' or 'pole'
    entity_id TEXT NOT NULL,   -- job_id or node_id
    field_name TEXT NOT NULL,  -- 'status', 'field_completed', etc.
    old_value TEXT,
    new_value TEXT,
    changed_at TIMESTAMP NOT NULL,
    changed_by TEXT,
    change_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_type, entity_id, field_name, changed_at)
);

-- Create indices for status change log
CREATE INDEX IF NOT EXISTS idx_status_change_log_job ON status_change_log(job_id);
CREATE INDEX IF NOT EXISTS idx_status_change_log_entity ON status_change_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_status_change_log_time ON status_change_log(changed_at);

-- Create view for job status history
CREATE OR REPLACE VIEW job_status_history AS
SELECT 
    job_id,
    status,
    timestamp,
    LAG(status) OVER (PARTITION BY job_id ORDER BY timestamp) as previous_status,
    LAG(timestamp) OVER (PARTITION BY job_id ORDER BY timestamp) as previous_timestamp,
    EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (PARTITION BY job_id ORDER BY timestamp)))/3600 as duration_hours
FROM job_metrics
ORDER BY job_id, timestamp;

-- Create trigger function for status changes
CREATE OR REPLACE FUNCTION log_status_change()
RETURNS TRIGGER AS $$
DECLARE
    old_val TEXT;
    new_val TEXT;
    entity_id TEXT;
    entity_type TEXT;
    field_name TEXT;
    changed_by_val TEXT;
BEGIN
    -- Set entity type and field name based on table
    IF TG_TABLE_NAME = 'job_metrics' THEN
        entity_type := 'job';
        entity_id := NEW.job_id;
        field_name := 'status';
        old_val := OLD.status;
        new_val := NEW.status;
        changed_by_val := NULL;
    ELSE
        entity_type := 'pole';
        entity_id := NEW.node_id;
        field_name := 'field_completed';
        old_val := CASE WHEN OLD.field_completed THEN 'true' ELSE 'false' END;
        new_val := CASE WHEN NEW.field_completed THEN 'true' ELSE 'false' END;
        changed_by_val := NEW.field_completed_by;
    END IF;

    -- Only insert if there's a change
    IF old_val IS DISTINCT FROM new_val THEN
        INSERT INTO status_change_log (
            job_id,
            entity_type,
            entity_id,
            field_name,
            old_value,
            new_value,
            changed_at,
            changed_by
        ) VALUES (
            NEW.job_id,
            entity_type,
            entity_id,
            field_name,
            old_val,
            new_val,
            NEW.timestamp,
            changed_by_val
        );
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for status changes
DROP TRIGGER IF EXISTS job_status_change_trigger ON job_metrics;
CREATE TRIGGER job_status_change_trigger
    AFTER INSERT OR UPDATE OF status
    ON job_metrics
    FOR EACH ROW
    EXECUTE FUNCTION log_status_change();

DROP TRIGGER IF EXISTS pole_status_change_trigger ON pole_metrics;
CREATE TRIGGER pole_status_change_trigger
    AFTER INSERT OR UPDATE OF field_completed
    ON pole_metrics
    FOR EACH ROW
    EXECUTE FUNCTION log_status_change();

-- Create trigger function for back office completion changes
CREATE OR REPLACE FUNCTION log_back_office_completion_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Only insert if there's a change
    IF OLD.back_office_completed IS DISTINCT FROM NEW.back_office_completed THEN
        INSERT INTO status_change_log (
            job_id,
            entity_type,
            entity_id,
            field_name,
            old_value,
            new_value,
            changed_at,
            changed_by
        ) VALUES (
            NEW.job_id,
            'pole',
            NEW.node_id,
            'back_office_completed',
            CASE WHEN OLD.back_office_completed THEN 'true' ELSE 'false' END,
            CASE WHEN NEW.back_office_completed THEN 'true' ELSE 'false' END,
            NEW.timestamp,
            NEW.annotated_by
        );
        
        -- Update annotation tracking fields
        IF NEW.back_office_completed = true THEN
            NEW.annotation_completed_at = NEW.timestamp;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for back office completion changes
DROP TRIGGER IF EXISTS pole_back_office_completion_trigger ON pole_metrics;
CREATE TRIGGER pole_back_office_completion_trigger
    BEFORE INSERT OR UPDATE OF back_office_completed
    ON pole_metrics
    FOR EACH ROW
    EXECUTE FUNCTION log_back_office_completion_change(); 