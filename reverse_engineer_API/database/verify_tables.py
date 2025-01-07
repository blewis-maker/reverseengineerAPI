from database.db import DatabaseConnection
import logging
from datetime import datetime
from tabulate import tabulate
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_tables():
    """Verify the contents of all database tables"""
    print("\nDetailed Database Verification Report")
    print("=" * 80)
    
    db = DatabaseConnection()
    
    with db.get_cursor() as cursor:
        # Verify job_metrics table
        print("\nJOB_METRICS")
        print("-" * 80)
        verify_table_schema(cursor, "job_metrics")
        verify_job_metrics(cursor)
        
        # Verify status_changes table
        print("\nSTATUS_CHANGES")
        print("-" * 80)
        verify_table_schema(cursor, "status_changes")
        verify_status_changes(cursor)
        
        # Verify pole_metrics table
        print("\nPOLE_METRICS")
        print("-" * 80)
        verify_table_schema(cursor, "pole_metrics")
        verify_pole_metrics(cursor)
        
        # Verify user_metrics table
        print("\nUSER_METRICS")
        print("-" * 80)
        verify_table_schema(cursor, "user_metrics")
        verify_user_metrics(cursor)
        
        # Verify user_daily_summary table
        print("\nUSER_DAILY_SUMMARY")
        print("-" * 80)
        verify_table_schema(cursor, "user_daily_summary")
        verify_user_daily_summary(cursor)
        
        # Verify burndown_metrics table
        print("\nBURNDOWN_METRICS")
        print("-" * 80)
        verify_table_schema(cursor, "burndown_metrics")
        verify_burndown_metrics(cursor)

def check_duplicates():
    """Check for duplicate records in job_metrics and pole_metrics tables"""
    print("\nDuplicate Records Analysis")
    print("=" * 80)
    
    db = DatabaseConnection()
    with db.get_cursor() as cursor:
        # Check for duplicate job records with same timestamp
        cursor.execute("""
            SELECT job_id, timestamp, COUNT(*) as count
            FROM job_metrics
            GROUP BY job_id, timestamp
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 5
        """)
        
        print("\nDuplicate Job Records (same timestamp):")
        print_table(cursor.fetchall(), ['job_id', 'timestamp', 'count'])
        
        # Check for duplicate pole records with same timestamp
        cursor.execute("""
            SELECT job_id, node_id, timestamp, COUNT(*) as count
            FROM pole_metrics
            GROUP BY job_id, node_id, timestamp
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 5
        """)
        
        print("\nDuplicate Pole Records (same timestamp):")
        print_table(cursor.fetchall(), ['job_id', 'node_id', 'timestamp', 'count'])

def analyze_status_history():
    """Analyze status changes over time for jobs and poles."""
    db = DatabaseConnection()
    
    print("\nStatus Change History Analysis")
    print("=" * 80)
    
    with db.get_cursor() as cursor:
        # Analyze job status changes over time
        cursor.execute("""
            WITH status_changes AS (
                SELECT 
                    job_id,
                    status,
                    timestamp,
                    LAG(status) OVER (PARTITION BY job_id ORDER BY timestamp) as previous_status,
                    LAG(timestamp) OVER (PARTITION BY job_id ORDER BY timestamp) as previous_timestamp
                FROM job_metrics
                ORDER BY job_id, timestamp
            )
            SELECT 
                job_id,
                previous_status,
                status as new_status,
                timestamp as changed_at,
                previous_timestamp,
                EXTRACT(EPOCH FROM (timestamp - previous_timestamp))/3600 as hours_in_previous_status
            FROM status_changes
            WHERE previous_status IS NOT NULL
                AND previous_status != status
            ORDER BY job_id, timestamp
            LIMIT 5;
        """)
        status_history = cursor.fetchall()
        
        print("\nJob Status Change Examples:")
        if status_history:
            print_table(status_history, ['job_id', 'previous_status', 'new_status', 'changed_at', 'previous_timestamp', 'hours_in_previous_status'])
        else:
            print("No status changes found yet - need more historical data")
            
        # Analyze pole completion status changes
        cursor.execute("""
            WITH pole_changes AS (
                SELECT 
                    job_id,
                    node_id,
                    field_completed,
                    timestamp,
                    LAG(field_completed) OVER (PARTITION BY job_id, node_id ORDER BY timestamp) as previous_status,
                    LAG(timestamp) OVER (PARTITION BY job_id, node_id ORDER BY timestamp) as previous_timestamp
                FROM pole_metrics
                ORDER BY job_id, node_id, timestamp
            )
            SELECT 
                job_id,
                node_id,
                previous_status as was_completed,
                field_completed as now_completed,
                timestamp as changed_at,
                previous_timestamp
            FROM pole_changes
            WHERE previous_status IS NOT NULL
                AND previous_status != field_completed
            ORDER BY job_id, node_id, timestamp
            LIMIT 5;
        """)
        pole_history = cursor.fetchall()
        
        print("\nPole Completion Status Change Examples:")
        if pole_history:
            print_table(pole_history, ['job_id', 'node_id', 'was_completed', 'now_completed', 'changed_at', 'previous_timestamp'])
        else:
            print("No pole status changes found yet - need more historical data")
        
        # Suggest schema improvements
        print("\nSuggested Schema Improvements:")
        print("-" * 80)
        print("""
CREATE OR REPLACE VIEW job_status_history AS
    WITH status_changes AS (
        SELECT 
            job_id,
            status,
            timestamp,
            LAG(status) OVER (PARTITION BY job_id ORDER BY timestamp) as previous_status,
            LAG(timestamp) OVER (PARTITION BY job_id ORDER BY timestamp) as previous_timestamp
        FROM job_metrics
        ORDER BY job_id, timestamp
    )
    SELECT 
        job_id,
        previous_status,
        status as new_status,
        timestamp as changed_at,
        previous_timestamp,
        EXTRACT(EPOCH FROM (timestamp - previous_timestamp))/3600 as hours_in_status
    FROM status_changes
    WHERE previous_status IS NOT NULL
        AND previous_status != status;

CREATE TABLE status_change_log (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    entity_type TEXT NOT NULL, -- 'job' or 'pole'
    entity_id TEXT NOT NULL,   -- job_id or node_id
    field_name TEXT NOT NULL,  -- 'status', 'field_completed', etc.
    old_value TEXT,
    new_value TEXT,
    changed_at TIMESTAMP NOT NULL,
    changed_by TEXT,
    change_reason TEXT,
    UNIQUE(entity_type, entity_id, field_name, changed_at)
);

-- Trigger function to log changes
CREATE OR REPLACE FUNCTION log_status_change()
RETURNS TRIGGER AS $$
BEGIN
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
        CASE 
            WHEN TG_TABLE_NAME = 'job_metrics' THEN 'job'
            ELSE 'pole'
        END,
        CASE 
            WHEN TG_TABLE_NAME = 'job_metrics' THEN NEW.job_id
            ELSE NEW.node_id
        END,
        CASE 
            WHEN TG_TABLE_NAME = 'job_metrics' THEN 'status'
            ELSE 'field_completed'
        END,
        OLD.status,
        NEW.status,
        NEW.timestamp,
        NEW.last_editor
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
        """)

def verify_table_schema(cursor, table_name):
    """Verify and display the schema of a table"""
    cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
    """, (table_name,))
    columns = cursor.fetchall()
    
    print("\nTable Schema:")
    schema_data = [[col['column_name'], col['data_type'], col['is_nullable']] for col in columns]
    print(tabulate(schema_data, headers=['Column', 'Type', 'Nullable'], tablefmt='grid'))

def verify_job_metrics(cursor):
    """Verify job_metrics table contents"""
    cursor.execute("SELECT COUNT(*) as count FROM job_metrics")
    count = cursor.fetchone()['count']
    print(f"\nTotal Records: {count}")
    
    if count > 0:
        cursor.execute("SELECT * FROM job_metrics LIMIT 3")
        records = cursor.fetchall()
        print("\nSample Records:")
        print_table(records, [desc[0] for desc in cursor.description])
        
        print("\nStatus Distribution:")
        cursor.execute("""
            SELECT status, COUNT(*) as count 
            FROM job_metrics 
            GROUP BY status
        """)
        print_table(cursor.fetchall(), ['status', 'count'])

def verify_status_changes(cursor):
    """Verify status_changes table contents"""
    cursor.execute("SELECT COUNT(*) as count FROM status_changes")
    count = cursor.fetchone()['count']
    print(f"\nTotal Records: {count}")

def verify_pole_metrics(cursor):
    """Verify pole_metrics table contents"""
    cursor.execute("SELECT COUNT(*) as count FROM pole_metrics")
    count = cursor.fetchone()['count']
    print(f"\nTotal Records: {count}")
    
    if count > 0:
        cursor.execute("SELECT * FROM pole_metrics LIMIT 3")
        records = cursor.fetchall()
        print("\nSample Records:")
        print_table(records, [desc[0] for desc in cursor.description])
        
        print("\nUtility Distribution:")
        cursor.execute("""
            SELECT utility, COUNT(*) as count 
            FROM pole_metrics 
            GROUP BY utility
        """)
        print_table(cursor.fetchall(), ['utility', 'count'])

def verify_user_metrics(cursor):
    """Verify user_metrics table contents"""
    cursor.execute("SELECT COUNT(*) as count FROM user_metrics")
    count = cursor.fetchone()['count']
    print(f"\nTotal Records: {count}")

def verify_user_daily_summary(cursor):
    """Verify user_daily_summary table contents"""
    cursor.execute("SELECT COUNT(*) as count FROM user_daily_summary")
    count = cursor.fetchone()['count']
    print(f"\nTotal Records: {count}")

def verify_burndown_metrics(cursor):
    """Verify burndown_metrics table contents"""
    cursor.execute("SELECT COUNT(*) as count FROM burndown_metrics")
    count = cursor.fetchone()['count']
    print(f"\nTotal Records: {count}")

def print_table(records, headers):
    """Print records in a tabulated format"""
    if not records:
        print("No records found")
        return
        
    rows = []
    for record in records:
        if isinstance(record, dict):
            row = [str(record.get(h, ''))[:50] + '...' if isinstance(record.get(h), str) 
                  and len(str(record.get(h))) > 50 else record.get(h) for h in headers]
        else:
            row = [str(val)[:50] + '...' if isinstance(val, str) 
                  and len(str(val)) > 50 else val for val in record]
        rows.append(row)
    
    print(tabulate(rows, headers=headers, tablefmt='grid'))

if __name__ == "__main__":
    verify_tables()
    check_duplicates()
    analyze_status_history() 