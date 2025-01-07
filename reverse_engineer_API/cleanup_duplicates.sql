CREATE OR REPLACE FUNCTION log_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_TABLE_NAME = 'job_metrics' THEN
        IF OLD.status IS DISTINCT FROM NEW.status THEN
            INSERT INTO status_change_log (
                entity_id,
                entity_type,
                field_name,
                old_value,
                new_value,
                changed_at,
                job_id
            ) VALUES (
                NEW.job_id,
                'job',
                'status',
                OLD.status,
                NEW.status,
                NEW.timestamp,
                NEW.job_id
            );
        END IF;
    ELSIF TG_TABLE_NAME = 'pole_metrics' THEN
        IF OLD.back_office_completed IS DISTINCT FROM NEW.back_office_completed THEN
            INSERT INTO status_change_log (
                entity_id,
                entity_type,
                field_name,
                old_value,
                new_value,
                changed_at,
                job_id
            ) VALUES (
                NEW.node_id,
                'pole',
                'back_office_completed',
                OLD.back_office_completed::text,
                NEW.back_office_completed::text,
                NEW.timestamp,
                NEW.job_id
            );
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing triggers if they exist
DROP TRIGGER IF EXISTS log_status_change_trigger ON job_metrics;
DROP TRIGGER IF EXISTS log_status_change_trigger ON pole_metrics;

-- Create triggers for both tables
CREATE TRIGGER log_status_change_trigger
    BEFORE UPDATE ON job_metrics
    FOR EACH ROW
    EXECUTE FUNCTION log_status_change();

CREATE TRIGGER log_status_change_trigger
    BEFORE UPDATE ON pole_metrics
    FOR EACH ROW
    EXECUTE FUNCTION log_status_change(); 