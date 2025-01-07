from database.db import DatabaseConnection

def cleanup_duplicates():
    db = DatabaseConnection()
    
    with db.get_cursor() as cursor:
        # Disable triggers temporarily
        cursor.execute("ALTER TABLE job_metrics DISABLE TRIGGER ALL;")
        cursor.execute("ALTER TABLE pole_metrics DISABLE TRIGGER ALL;")
        
        try:
            # Create temporary tables to store unique records
            cursor.execute("""
                -- Create temporary table for unique job metrics
                CREATE TEMP TABLE unique_job_metrics AS
                SELECT DISTINCT ON (job_id, timestamp) *
                FROM job_metrics
                ORDER BY job_id, timestamp, created_at DESC;
                
                -- Delete all records from job_metrics
                TRUNCATE job_metrics;
                
                -- Insert unique records back
                INSERT INTO job_metrics
                SELECT * FROM unique_job_metrics;
                
                -- Drop temporary table
                DROP TABLE unique_job_metrics;
            """)
            
            # Get the number of deleted records
            cursor.execute("SELECT COUNT(*) FROM job_metrics")
            current_job_count = cursor.fetchone()['count']
            print(f"Cleaned up job_metrics table. Current record count: {current_job_count}")
            
            # Repeat for pole_metrics
            cursor.execute("""
                -- Create temporary table for unique pole metrics
                CREATE TEMP TABLE unique_pole_metrics AS
                SELECT DISTINCT ON (job_id, node_id, timestamp) *
                FROM pole_metrics
                ORDER BY job_id, node_id, timestamp, created_at DESC;
                
                -- Delete all records from pole_metrics
                TRUNCATE pole_metrics;
                
                -- Insert unique records back
                INSERT INTO pole_metrics
                SELECT * FROM unique_pole_metrics;
                
                -- Drop temporary table
                DROP TABLE unique_pole_metrics;
            """)
            
            # Get the number of deleted records
            cursor.execute("SELECT COUNT(*) FROM pole_metrics")
            current_pole_count = cursor.fetchone()['count']
            print(f"Cleaned up pole_metrics table. Current record count: {current_pole_count}")
            
        finally:
            # Re-enable triggers
            cursor.execute("ALTER TABLE job_metrics ENABLE TRIGGER ALL;")
            cursor.execute("ALTER TABLE pole_metrics ENABLE TRIGGER ALL;")

if __name__ == "__main__":
    cleanup_duplicates() 