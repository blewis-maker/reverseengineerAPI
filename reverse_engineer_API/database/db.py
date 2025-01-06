import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self):
        self.conn_params = {
            'dbname': os.getenv('DB_NAME', 'metrics_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASS'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        logger.info(f"Database connection parameters (excluding password):")
        for key, value in self.conn_params.items():
            if key != 'password':
                logger.info(f"{key}: {value}")
        self._conn = None

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        if self._conn is None:
            try:
                logger.info("Attempting to connect to database...")
                self._conn = psycopg2.connect(**self.conn_params)
                logger.info("Successfully connected to database")
            except Exception as e:
                logger.error(f"Error connecting to database: {str(e)}")
                raise

        try:
            yield self._conn
        except Exception as e:
            if self._conn:
                self._conn.rollback()
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    @contextmanager
    def get_cursor(self, cursor_factory=RealDictCursor):
        """Context manager for database cursors"""
        with self.get_connection() as conn:
            try:
                logger.info("Creating database cursor...")
                cursor = conn.cursor(cursor_factory=cursor_factory)
                logger.info("Successfully created cursor")
                yield cursor
                conn.commit()
                logger.info("Successfully committed transaction")
            except Exception as e:
                conn.rollback()
                logger.error(f"Cursor error: {str(e)}")
                raise
            finally:
                cursor.close()
                logger.info("Cursor closed")

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