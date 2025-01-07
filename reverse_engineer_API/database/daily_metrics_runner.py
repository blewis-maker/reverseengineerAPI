#!/usr/bin/env python3
import os
import sys
import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import DictCursor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Create a database connection."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'metrics_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', ''),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise

def run_daily_metrics(conn, target_date=None):
    """Run daily metrics generation for a specific date."""
    try:
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        with conn.cursor() as cur:
            # Generate daily summaries
            logger.info(f"Generating daily summaries for {target_date}")
            cur.execute("SELECT generate_daily_summary(%s)", (target_date,))
            
            # Calculate burndown metrics
            logger.info(f"Calculating burndown metrics for {target_date}")
            cur.execute("SELECT calculate_burndown_metrics(%s)", (target_date,))
            
            conn.commit()
            
            # Verify results
            cur.execute("""
                SELECT role, COUNT(*) as count, SUM(total_poles_completed) as total_poles
                FROM user_daily_summary
                WHERE date = %s
                GROUP BY role
            """, (target_date,))
            results = cur.fetchall()
            
            for role, count, total_poles in results:
                logger.info(f"Generated {count} summaries for {role} users, total poles: {total_poles}")
            
            # Check burndown metrics
            cur.execute("""
                SELECT COUNT(*) as count, 
                       SUM(total_poles) as total,
                       SUM(completed_poles) as completed
                FROM burndown_metrics
                WHERE date = %s
            """, (target_date,))
            count, total, completed = cur.fetchone()
            logger.info(f"Generated burndown metrics for {count} utilities, "
                       f"total poles: {total}, completed: {completed}")
            
    except Exception as e:
        logger.error(f"Failed to run daily metrics: {str(e)}")
        conn.rollback()
        raise

def backfill_metrics(conn, start_date, end_date):
    """Backfill metrics for a date range."""
    try:
        logger.info(f"Backfilling metrics from {start_date} to {end_date}")
        with conn.cursor() as cur:
            cur.execute("SELECT backfill_metrics(%s, %s)", (start_date, end_date))
            conn.commit()
        logger.info("Backfill completed successfully")
    except Exception as e:
        logger.error(f"Failed to backfill metrics: {str(e)}")
        conn.rollback()
        raise

def main():
    """Main function to run daily metrics."""
    conn = None
    try:
        conn = get_db_connection()
        
        if len(sys.argv) > 1:
            if sys.argv[1] == '--backfill':
                if len(sys.argv) != 4:
                    print("Usage: daily_metrics_runner.py --backfill START_DATE END_DATE")
                    sys.exit(1)
                start_date = datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
                end_date = datetime.strptime(sys.argv[3], '%Y-%m-%d').date()
                backfill_metrics(conn, start_date, end_date)
            else:
                target_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
                run_daily_metrics(conn, target_date)
        else:
            run_daily_metrics(conn)
            
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main() 