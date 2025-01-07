from database.db import DatabaseConnection

def apply_constraints():
    db = DatabaseConnection()
    
    with db.get_cursor() as cursor:
        # Add unique constraint to job_metrics
        cursor.execute("""
            ALTER TABLE job_metrics 
            DROP CONSTRAINT IF EXISTS unique_job_timestamp;
        """)
        cursor.execute("""
            ALTER TABLE job_metrics 
            ADD CONSTRAINT unique_job_timestamp UNIQUE (job_id, timestamp);
        """)
        
        # Add unique constraint to pole_metrics
        cursor.execute("""
            ALTER TABLE pole_metrics 
            DROP CONSTRAINT IF EXISTS unique_pole_node_timestamp;
        """)
        cursor.execute("""
            ALTER TABLE pole_metrics 
            ADD CONSTRAINT unique_pole_node_timestamp UNIQUE (job_id, node_id, timestamp);
        """)
        
        print("Successfully applied constraints")

if __name__ == "__main__":
    apply_constraints() 