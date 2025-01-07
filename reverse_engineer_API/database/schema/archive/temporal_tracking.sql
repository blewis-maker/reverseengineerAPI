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

-- Create project tracking tables
CREATE TABLE IF NOT EXISTS projects (
    id SERIAL PRIMARY KEY,
    project_id TEXT NOT NULL UNIQUE,  -- Zone Name from Design Job Tracking
    name TEXT,
    utility TEXT,
    total_poles INTEGER DEFAULT 0,
    target_date DATE,  -- Aerial Engineering due date
    status TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS project_dependencies (
    id SERIAL PRIMARY KEY,
    project_id TEXT REFERENCES projects(project_id),
    depends_on_project_id TEXT REFERENCES projects(project_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_dependency UNIQUE (project_id, depends_on_project_id)
);

CREATE TABLE IF NOT EXISTS project_resources (
    id SERIAL PRIMARY KEY,
    project_id TEXT REFERENCES projects(project_id),
    user_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('field', 'back_office')),
    capacity FLOAT DEFAULT 1.0,  -- Percentage of time allocated to project
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_project_resource UNIQUE (project_id, user_id, role)
);

-- Create burndown tracking tables
CREATE TABLE IF NOT EXISTS burndown_metrics (
    id SERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('utility', 'project', 'master')),
    entity_id TEXT NOT NULL,
    total_poles INTEGER,
    field_complete INTEGER,
    back_office_complete INTEGER,
    run_rate FLOAT,
    estimated_completion_date DATE,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_burndown_entry UNIQUE (entity_type, entity_id, timestamp)
);

CREATE TABLE IF NOT EXISTS burndown_history (
    id SERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('utility', 'project', 'master')),
    entity_id TEXT NOT NULL,
    total_poles INTEGER,
    completed_poles INTEGER,
    field_complete INTEGER,
    back_office_complete INTEGER,
    run_rate FLOAT,
    field_resources INTEGER,
    back_office_resources INTEGER,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_burndown_history UNIQUE (entity_type, entity_id, timestamp)
);

-- Add indices for performance
CREATE INDEX IF NOT EXISTS idx_burndown_metrics_timestamp ON burndown_metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_burndown_history_timestamp ON burndown_history(timestamp);
CREATE INDEX IF NOT EXISTS idx_project_resources_dates ON project_resources(start_date, end_date);

-- Add trigger to update project updated_at timestamp
CREATE OR REPLACE FUNCTION update_project_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_project_timestamp
    BEFORE UPDATE ON projects
    FOR EACH ROW
    EXECUTE FUNCTION update_project_timestamp();

-- Add trigger to record burndown history
CREATE OR REPLACE FUNCTION record_burndown_history()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO burndown_history (
        entity_type,
        entity_id,
        total_poles,
        completed_poles,
        field_complete,
        back_office_complete,
        run_rate,
        field_resources,
        back_office_resources,
        timestamp
    )
    SELECT
        NEW.entity_type,
        NEW.entity_id,
        NEW.total_poles,
        NEW.field_complete + NEW.back_office_complete,
        NEW.field_complete,
        NEW.back_office_complete,
        NEW.run_rate,
        (SELECT COUNT(*) FROM project_resources 
         WHERE project_id = NEW.entity_id AND role = 'field' 
         AND (end_date IS NULL OR end_date >= CURRENT_DATE)),
        (SELECT COUNT(*) FROM project_resources 
         WHERE project_id = NEW.entity_id AND role = 'back_office'
         AND (end_date IS NULL OR end_date >= CURRENT_DATE)),
        NEW.timestamp;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER record_burndown_history
    AFTER INSERT OR UPDATE ON burndown_metrics
    FOR EACH ROW
    EXECUTE FUNCTION record_burndown_history();

-- Add view for project status
CREATE OR REPLACE VIEW project_status AS
SELECT 
    p.project_id,
    p.name,
    p.utility,
    p.total_poles,
    p.target_date,
    COALESCE(bm.field_complete, 0) as field_complete,
    COALESCE(bm.back_office_complete, 0) as back_office_complete,
    COALESCE(bm.run_rate, 0) as run_rate,
    bm.estimated_completion_date,
    CASE 
        WHEN bm.estimated_completion_date IS NULL THEN 'Not Started'
        WHEN bm.estimated_completion_date > p.target_date THEN 'Behind'
        WHEN bm.estimated_completion_date > (p.target_date - INTERVAL '14 days') THEN 'At Risk'
        ELSE 'On Track'
    END as status,
    (SELECT COUNT(*) FROM project_resources pr 
     WHERE pr.project_id = p.project_id AND pr.role = 'field' 
     AND (pr.end_date IS NULL OR pr.end_date >= CURRENT_DATE)) as field_resources,
    (SELECT COUNT(*) FROM project_resources pr 
     WHERE pr.project_id = p.project_id AND pr.role = 'back_office' 
     AND (pr.end_date IS NULL OR pr.end_date >= CURRENT_DATE)) as back_office_resources,
    p.created_at,
    p.updated_at
FROM projects p
LEFT JOIN burndown_metrics bm ON bm.entity_id = p.project_id 
    AND bm.entity_type = 'project'
    AND bm.timestamp = (
        SELECT MAX(timestamp) 
        FROM burndown_metrics 
        WHERE entity_id = p.project_id 
        AND entity_type = 'project'
    ); 