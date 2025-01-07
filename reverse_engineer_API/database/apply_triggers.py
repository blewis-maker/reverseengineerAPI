from database.db import DatabaseConnection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def apply_triggers():
    """Apply necessary triggers for status change tracking."""
    db = DatabaseConnection()
    
    with db.get_cursor() as cursor:
        # Create trigger function for status changes
        cursor.execute("""
            CREATE OR REPLACE FUNCTION log_status_change()
            RETURNS TRIGGER AS $$
            BEGIN
                -- Only insert if there's a change
                IF OLD.status IS DISTINCT FROM NEW.status THEN
                    INSERT INTO status_changes (
                        job_id,
                        previous_status,
                        new_status,
                        changed_at,
                        changed_by,
                        duration_hours,
                        week_number,
                        year
                    ) VALUES (
                        NEW.job_id,
                        OLD.status,
                        NEW.status,
                        NEW.timestamp,
                        (NEW.assigned_users)[1],  -- Use first assigned user if any
                        EXTRACT(EPOCH FROM (NEW.timestamp - OLD.timestamp))/3600,
                        EXTRACT(WEEK FROM NEW.timestamp)::INTEGER,
                        EXTRACT(YEAR FROM NEW.timestamp)::INTEGER
                    );
                    
                    -- Also log to status_change_log
                    INSERT INTO status_change_log (
                        job_id,
                        entity_type,
                        entity_id,
                        field_name,
                        old_value,
                        new_value,
                        changed_at,
                        changed_by,
                        change_reason
                    ) VALUES (
                        NEW.job_id,
                        'job',
                        NEW.job_id,
                        'status',
                        OLD.status,
                        NEW.status,
                        NEW.timestamp,
                        (NEW.assigned_users)[1],
                        CASE 
                            WHEN NEW.assigned_users IS NOT NULL AND NEW.assigned_users != '{}' 
                            THEN 'OSP Assignment: ' || (NEW.assigned_users)[1]
                            ELSE NULL
                        END
                    );
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        # Drop existing trigger if it exists
        cursor.execute("""
            DROP TRIGGER IF EXISTS job_status_change_trigger ON job_metrics;
        """)
        
        # Create trigger for job status changes
        cursor.execute("""
            CREATE TRIGGER job_status_change_trigger
            AFTER INSERT OR UPDATE OF status
            ON job_metrics
            FOR EACH ROW
            EXECUTE FUNCTION log_status_change();
        """)
        
        logger.info("Successfully applied status change triggers")

if __name__ == "__main__":
    apply_triggers() 