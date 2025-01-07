CREATE OR REPLACE FUNCTION log_status_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Handle job status changes
    IF TG_TABLE_NAME = 'job_metrics' AND OLD.status IS DISTINCT FROM NEW.status THEN
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
            'job',
            NEW.job_id,
            'status',
            OLD.status,
            NEW.status,
            NEW.timestamp,
            NULL
        )
        ON CONFLICT (entity_type, entity_id, field_name, changed_at)
        DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop and recreate the trigger only for job_metrics
DROP TRIGGER IF EXISTS job_status_change_trigger ON job_metrics;
CREATE TRIGGER job_status_change_trigger
    AFTER UPDATE OF status
    ON job_metrics
    FOR EACH ROW
    EXECUTE FUNCTION log_status_change(); 