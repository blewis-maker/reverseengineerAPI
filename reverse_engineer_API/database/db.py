import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import logging
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Database connection manager"""
    
    def __init__(self):
        self.connection = None
    
    def connect(self):
        """Connect to the database"""
        logging.info("Attempting to connect to database...")
        try:
            if not self.connection or self.connection.closed:
                self.connection = psycopg2.connect(
                    dbname=os.getenv('DB_NAME', 'metrics_db'),
                    user=os.getenv('DB_USER', 'postgres'),
                    password=os.getenv('DB_PASS', 'metrics_db_password'),
                    host=os.getenv('DB_HOST', 'localhost'),
                    port=os.getenv('DB_PORT', '5432')
                )
                logging.info("Successfully connected to database")
                return self.connection
        except Exception as e:
            logging.error(f"Database connection error: {str(e)}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get a database connection using context manager"""
        if not self.connection or self.connection.closed:
            self.connect()
        try:
            yield self.connection
            self.connection.commit()
            logging.info("Successfully committed transaction")
        except Exception as e:
            self.connection.rollback()
            logging.error(f"Connection error: {str(e)}")
            raise
    
    @contextmanager
    def get_cursor(self, cursor_factory=RealDictCursor):
        """Get a database cursor using context manager"""
        if not self.connection or self.connection.closed:
            self.connect()
            
        logging.info("Creating database cursor...")
        try:
            cursor = self.connection.cursor(cursor_factory=cursor_factory)
            logging.info("Successfully created cursor")
            yield cursor
            self.connection.commit()
            logging.info("Successfully committed transaction")
        except Exception as e:
            self.connection.rollback()
            logging.error(f"Cursor error: {str(e)}")
            raise
        finally:
            cursor.close()
            logging.info("Cursor closed")
    
    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query, params=None):
        """Execute a query and return all results"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def execute_many(self, query, params_list):
        """Execute the same query with different parameters"""
        with self.get_cursor() as cursor:
            cursor.executemany(query, params_list)

    def execute_transaction(self, queries):
        """Execute multiple queries in a transaction"""
        with self.get_cursor() as cursor:
            for query, params in queries:
                cursor.execute(query, params)

# Create a singleton instance
db = DatabaseConnection()

# Helper functions for common operations
def insert_job_metrics(job_data):
    """Insert job metrics into the database"""
    query = """
    INSERT INTO job_metrics (
        job_id, status, utility, total_poles, completed_poles,
        field_complete, back_office_complete, assigned_users,
        priority, target_completion_date, timestamp
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
    )
    """
    with db.get_cursor() as cursor:
        cursor.execute(query, (
            job_data['job_id'],
            job_data['status'],
            job_data['utility'],
            job_data['total_poles'],
            job_data['completed_poles'],
            job_data['field_complete'],
            job_data['back_office_complete'],
            job_data['assigned_users'],
            job_data['priority'],
            job_data['target_completion_date']
        ))

def insert_status_change(job_id, previous_status, new_status, changed_by):
    """Record a status change"""
    query = """
    INSERT INTO status_changes (
        job_id, previous_status, new_status, changed_at,
        changed_by, week_number, year
    ) VALUES (
        %s, %s, %s, NOW(), %s,
        EXTRACT(WEEK FROM NOW())::INTEGER,
        EXTRACT(YEAR FROM NOW())::INTEGER
    )
    """
    with db.get_cursor() as cursor:
        cursor.execute(query, (
            job_id,
            previous_status,
            new_status,
            changed_by
        ))

def get_job_metrics(job_id):
    """Get metrics for a specific job"""
    query = """
    SELECT *
    FROM job_metrics
    WHERE job_id = %s
    ORDER BY timestamp DESC
    LIMIT 1
    """
    return db.execute_query(query, (job_id,))

def get_user_productivity(user_id, start_date, end_date):
    """Get user productivity metrics for a date range"""
    query = """
    SELECT 
        date,
        total_poles_completed,
        utilities_worked,
        jobs_worked
    FROM user_daily_summary
    WHERE user_id = %s
    AND date BETWEEN %s AND %s
    ORDER BY date
    """
    return db.execute_query(query, (user_id, start_date, end_date))

def update_burndown_metrics(utility, metrics_data):
    """Update burndown metrics for a utility"""
    query = """
    INSERT INTO burndown_metrics (
        utility, date, total_poles, completed_poles,
        run_rate, estimated_completion_date,
        actual_resources, required_resources
    ) VALUES (
        %s, NOW(), %s, %s, %s, %s, %s, %s
    )
    """
    with db.get_cursor() as cursor:
        cursor.execute(query, (
            utility,
            metrics_data['total_poles'],
            metrics_data['completed_poles'],
            metrics_data['run_rate'],
            metrics_data['estimated_completion_date'],
            metrics_data['actual_resources'],
            metrics_data['required_resources']
        )) 