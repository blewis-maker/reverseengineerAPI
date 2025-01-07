-- Database schema for OSP job processing metrics tracking system

-- Users table for tracking user roles and capabilities
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    user_id TEXT UNIQUE NOT NULL,
    name TEXT,
    email TEXT,
    role TEXT,  -- 'field', 'back_office', 'admin', etc.
    specialties TEXT[],  -- Array of specialized skills
    max_daily_capacity INTEGER,  -- Maximum poles per day
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job metrics table for historical tracking of job progress
CREATE TABLE IF NOT EXISTS job_metrics (
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

-- Status changes table for tracking job status history
CREATE TABLE IF NOT EXISTS status_changes (
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

-- Pole metrics table for detailed pole status tracking
CREATE TABLE IF NOT EXISTS pole_metrics (
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

-- User metrics table for tracking productivity
CREATE TABLE IF NOT EXISTS user_metrics (
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

-- Daily user summary table for productivity rollup
CREATE TABLE IF NOT EXISTS user_daily_summary (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    date DATE NOT NULL,
    role TEXT,
    total_poles_completed INTEGER,
    utilities_worked TEXT[],
    jobs_worked TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (user_id, date, role)
);

-- Burndown metrics table for project progress tracking
CREATE TABLE IF NOT EXISTS burndown_metrics (
    id SERIAL PRIMARY KEY,
    utility TEXT,
    date DATE NOT NULL,
    total_poles INTEGER,
    completed_poles INTEGER,
    run_rate FLOAT,
    estimated_completion_date DATE,
    actual_resources INTEGER,
    required_resources INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Resource recommendations table for AI/ML recommendations
CREATE TABLE IF NOT EXISTS resource_recommendations (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    utility TEXT,
    job_id TEXT,
    recommended_users TEXT[],
    priority INTEGER,
    reason TEXT,
    accepted BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_job_metrics_job_id ON job_metrics(job_id);
CREATE INDEX IF NOT EXISTS idx_status_changes_job_id ON status_changes(job_id);
CREATE INDEX IF NOT EXISTS idx_pole_metrics_job_id ON pole_metrics(job_id);
CREATE INDEX IF NOT EXISTS idx_user_metrics_user_id ON user_metrics(user_id);
CREATE INDEX IF NOT EXISTS idx_burndown_metrics_utility ON burndown_metrics(utility); 