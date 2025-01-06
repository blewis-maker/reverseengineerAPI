from database.db import DatabaseConnection
import logging
from datetime import datetime
import psycopg2

def verify_database():
    """Verify the contents of all database tables."""
    print("\nDatabase Verification Report")
    print("=" * 50)
    
    try:
        # Test database connection first
        db = DatabaseConnection()
        with db.get_connection() as conn:
            print("\nDatabase connection successful")
            print(f"Database name: {conn.info.dbname}")
            print(f"User: {conn.info.user}")
            print(f"Host: {conn.info.host}")
            print(f"Port: {conn.info.port}")
            
        # List of all tables to verify
        tables = [
            'job_metrics',
            'status_changes',
            'pole_metrics',
            'user_metrics',
            'user_daily_summary',
            'burndown_metrics'
        ]
        
        # Use standard cursor instead of RealDictCursor
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Verify tables exist
                print("\nVerifying tables exist:")
                print("-" * 20)
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                existing_tables = {row[0] for row in cursor.fetchall()}
                for table in tables:
                    if table in existing_tables:
                        print(f"✓ {table} exists")
                    else:
                        print(f"✗ {table} does not exist")
                
                # Check record counts for each table
                print("\nRecord Counts:")
                print("-" * 20)
                for table in tables:
                    if table in existing_tables:
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {table}")
                            count = cursor.fetchone()[0]
                            print(f"{table}: {count} records")
                        except psycopg2.Error as e:
                            print(f"Error counting records in {table}: {str(e)}")
                
                # Sample records from each table
                print("\nSample Records:")
                print("-" * 20)
                
                # Job Metrics
                print("\nJob Metrics:")
                try:
                    cursor.execute("""
                        SELECT job_id, status, utility, total_poles, field_complete, 
                               back_office_complete, timestamp
                        FROM job_metrics
                        LIMIT 3
                    """)
                    records = cursor.fetchall()
                    if records:
                        for record in records:
                            print(record)
                    else:
                        print("No records found")
                except psycopg2.Error as e:
                    print(f"Error querying job_metrics: {str(e)}")
                
                # Status Changes
                print("\nStatus Changes:")
                try:
                    cursor.execute("""
                        SELECT job_id, previous_status, new_status, changed_at
                        FROM status_changes
                        LIMIT 3
                    """)
                    records = cursor.fetchall()
                    if records:
                        for record in records:
                            print(record)
                    else:
                        print("No records found")
                except psycopg2.Error as e:
                    print(f"Error querying status_changes: {str(e)}")
                
                # Pole Metrics
                print("\nPole Metrics:")
                try:
                    cursor.execute("""
                        SELECT job_id, pole_id, utility, field_completed, 
                               field_completed_by, mr_status, timestamp
                        FROM pole_metrics
                        LIMIT 3
                    """)
                    records = cursor.fetchall()
                    if records:
                        for record in records:
                            print(record)
                    else:
                        print("No records found")
                except psycopg2.Error as e:
                    print(f"Error querying pole_metrics: {str(e)}")
                
                # Verify relationships
                print("\nVerifying Relationships:")
                print("-" * 20)
                
                try:
                    # Check job_id consistency
                    cursor.execute("""
                        SELECT DISTINCT j.job_id 
                        FROM job_metrics j
                        LEFT JOIN pole_metrics p ON j.job_id = p.job_id
                        WHERE p.job_id IS NULL
                    """)
                    orphaned_jobs = cursor.fetchall()
                    if orphaned_jobs:
                        print("\nWarning: Found jobs with no pole metrics:")
                        for job in orphaned_jobs:
                            print(f"Job ID: {job[0]}")
                    else:
                        print("\nAll jobs have corresponding pole metrics")
                except psycopg2.Error as e:
                    print(f"Error checking job relationships: {str(e)}")
                
                try:
                    # Check user metrics consistency
                    cursor.execute("""
                        SELECT DISTINCT user_id 
                        FROM user_metrics
                        WHERE user_id NOT IN (
                            SELECT user_id FROM user_daily_summary
                        )
                    """)
                    users_without_summary = cursor.fetchall()
                    if users_without_summary:
                        print("\nWarning: Found users with metrics but no daily summary:")
                        for user in users_without_summary:
                            print(f"User ID: {user[0]}")
                    else:
                        print("\nAll users have daily summaries")
                except psycopg2.Error as e:
                    print(f"Error checking user relationships: {str(e)}")
    
    except Exception as e:
        print(f"Error during verification: {str(e)}")
    
    print("\nVerification complete")

if __name__ == "__main__":
    verify_database() 