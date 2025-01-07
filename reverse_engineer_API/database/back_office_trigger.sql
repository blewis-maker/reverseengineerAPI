-- Create trigger function for back office completion changes
CREATE OR REPLACE FUNCTION log_back_office_completion_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Only log changes for back office completion status changes
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
        )
        ON CONFLICT (entity_type, entity_id, field_name, changed_at)
        DO NOTHING;

        -- Update annotation tracking fields
        IF NEW.back_office_completed = true AND NEW.annotation_completed_at IS NULL THEN
            NEW.annotation_completed_at = NEW.timestamp;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop and recreate the trigger
DROP TRIGGER IF EXISTS pole_back_office_completion_trigger ON pole_metrics;
CREATE TRIGGER pole_back_office_completion_trigger
    BEFORE UPDATE OF back_office_completed
    ON pole_metrics
    FOR EACH ROW
    EXECUTE FUNCTION log_back_office_completion_change(); 