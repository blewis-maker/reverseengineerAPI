-- Create user_daily_summary table if it doesn't exist
CREATE TABLE IF NOT EXISTS user_daily_summary (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    date DATE NOT NULL,
    role TEXT NOT NULL,
    total_poles_completed INTEGER DEFAULT 0,
    utilities_worked TEXT[],
    jobs_worked TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, date, role)
);

-- Create burndown_metrics table if it doesn't exist
CREATE TABLE IF NOT EXISTS burndown_metrics (
    id SERIAL PRIMARY KEY,
    utility TEXT NOT NULL,
    date DATE NOT NULL,
    total_poles INTEGER,
    completed_poles INTEGER,
    run_rate DOUBLE PRECISION,
    estimated_completion_date DATE,
    actual_resources INTEGER,
    required_resources INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(utility, date)
);

-- Daily summary generation function
CREATE OR REPLACE FUNCTION generate_daily_summary(target_date DATE)
RETURNS void AS $$
BEGIN
    -- Get field work summary from status change log
    INSERT INTO user_daily_summary (
        user_id, date, role, total_poles_completed,
        utilities_worked, jobs_worked
    )
    SELECT 
        changed_by as user_id,
        DATE(changed_at) as date,
        'field' as role,
        COUNT(DISTINCT entity_id) as total_poles_completed,
        ARRAY_AGG(DISTINCT pm.utility) as utilities_worked,
        ARRAY_AGG(DISTINCT scl.job_id) as jobs_worked
    FROM status_change_log scl
    JOIN pole_metrics pm ON pm.node_id = scl.entity_id AND pm.job_id = scl.job_id
    WHERE scl.entity_type = 'pole' 
    AND scl.field_name = 'field_completed'
    AND scl.old_value = 'false'
    AND scl.new_value = 'true'
    AND DATE(scl.changed_at) = target_date
    GROUP BY changed_by, DATE(changed_at)
    ON CONFLICT (user_id, date, role) DO UPDATE
    SET total_poles_completed = EXCLUDED.total_poles_completed,
        utilities_worked = EXCLUDED.utilities_worked,
        jobs_worked = EXCLUDED.jobs_worked;

    -- Get back office work summary from status change log
    INSERT INTO user_daily_summary (
        user_id, date, role, total_poles_completed,
        utilities_worked, jobs_worked
    )
    SELECT 
        changed_by as user_id,
        DATE(changed_at) as date,
        'back_office' as role,
        COUNT(DISTINCT entity_id) as total_poles_completed,
        ARRAY_AGG(DISTINCT pm.utility) as utilities_worked,
        ARRAY_AGG(DISTINCT scl.job_id) as jobs_worked
    FROM status_change_log scl
    JOIN pole_metrics pm ON pm.node_id = scl.entity_id AND pm.job_id = scl.job_id
    WHERE scl.entity_type = 'pole' 
    AND scl.field_name = 'back_office_completed'
    AND scl.old_value = 'false'
    AND scl.new_value = 'true'
    AND DATE(scl.changed_at) = target_date
    GROUP BY changed_by, DATE(changed_at)
    ON CONFLICT (user_id, date, role) DO UPDATE
    SET total_poles_completed = EXCLUDED.total_poles_completed,
        utilities_worked = EXCLUDED.utilities_worked,
        jobs_worked = EXCLUDED.jobs_worked;
END;
$$ LANGUAGE plpgsql;

-- Burndown metrics calculation function
CREATE OR REPLACE FUNCTION calculate_burndown_metrics(target_date DATE)
RETURNS void AS $$
BEGIN
    INSERT INTO burndown_metrics (
        utility,
        date,
        total_poles,
        completed_poles,
        run_rate,
        estimated_completion_date,
        actual_resources,
        required_resources
    )
    SELECT 
        utility,
        target_date,
        SUM(total_poles) as total_poles,
        SUM(completed_poles) as completed_poles,
        CASE 
            WHEN SUM(total_poles) > 0 
            THEN SUM(completed_poles)::float / SUM(total_poles)
            ELSE 0
        END as run_rate,
        target_date + 
            ((SUM(total_poles) - SUM(completed_poles)) / 
            NULLIF(SUM(completed_poles)::float / 30, 0))::integer 
            as estimated_completion_date,
        COUNT(DISTINCT unnest(assigned_users)) as actual_resources,
        CEIL((SUM(total_poles) - SUM(completed_poles)) / 30.0)::integer 
            as required_resources
    FROM job_metrics
    WHERE DATE(timestamp) = target_date
    GROUP BY utility
    ON CONFLICT (utility, date) DO UPDATE
    SET total_poles = EXCLUDED.total_poles,
        completed_poles = EXCLUDED.completed_poles,
        run_rate = EXCLUDED.run_rate,
        estimated_completion_date = EXCLUDED.estimated_completion_date,
        actual_resources = EXCLUDED.actual_resources,
        required_resources = EXCLUDED.required_resources;
END;
$$ LANGUAGE plpgsql;

-- Function to backfill historical data
CREATE OR REPLACE FUNCTION backfill_metrics(start_date DATE, end_date DATE)
RETURNS void AS $$
DECLARE
    current_date DATE;
BEGIN
    current_date := start_date;
    WHILE current_date <= end_date LOOP
        PERFORM generate_daily_summary(current_date);
        PERFORM calculate_burndown_metrics(current_date);
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Create indices for better performance
CREATE INDEX IF NOT EXISTS idx_status_change_log_date ON status_change_log(changed_at);
CREATE INDEX IF NOT EXISTS idx_status_change_log_entity ON status_change_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_user_daily_summary_date ON user_daily_summary(date);
CREATE INDEX IF NOT EXISTS idx_burndown_metrics_date ON burndown_metrics(date); 