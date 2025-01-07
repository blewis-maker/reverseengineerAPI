-- Create projects table for tracking project zones and schedules
CREATE TABLE IF NOT EXISTS projects (
    id SERIAL PRIMARY KEY,
    project_id TEXT NOT NULL,  -- Zone Name
    market TEXT NOT NULL,      -- Market (e.g., Grand Junction, Bayfield)
    name TEXT,                 -- Project name
    approval_status TEXT,      -- Under Consideration, Approved, etc.
    hld_target_date DATE,      -- HLD to be Completed
    hld_actual_date DATE,      -- HLD Actual
    designer TEXT,             -- Designer Assignment
    aerial_eng_target_date DATE, -- Aerial Eng to be Completed
    aerial_eng_actual_date DATE, -- Aerial Eng Actual
    utility TEXT,
    total_poles INTEGER,
    field_resources INTEGER,
    back_office_resources INTEGER,
    current_run_rate DOUBLE PRECISION,
    required_run_rate DOUBLE PRECISION,
    projected_end_date DATE,
    status TEXT,               -- Project status (On Track, At Risk, etc.)
    progress DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_project_id UNIQUE (project_id)
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_projects_utility ON projects(utility);
CREATE INDEX IF NOT EXISTS idx_projects_market ON projects(market);
CREATE INDEX IF NOT EXISTS idx_projects_designer ON projects(designer);
CREATE INDEX IF NOT EXISTS idx_projects_aerial_eng_target ON projects(aerial_eng_target_date);
CREATE INDEX IF NOT EXISTS idx_projects_approval_status ON projects(approval_status);

-- Create trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_projects_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_projects_timestamp
    BEFORE UPDATE ON projects
    FOR EACH ROW
    EXECUTE FUNCTION update_projects_timestamp();

-- Add project_id foreign key to job_metrics table for project association
ALTER TABLE job_metrics
ADD COLUMN project_id TEXT REFERENCES projects(project_id);

-- Add index for the foreign key
CREATE INDEX IF NOT EXISTS idx_job_metrics_project_id ON job_metrics(project_id); 