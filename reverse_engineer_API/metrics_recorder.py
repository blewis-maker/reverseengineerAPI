from datetime import datetime, timedelta
from typing import Dict, Any, Set, List
import logging
from database.db import DatabaseConnection

logger = logging.getLogger(__name__)

class MetricsRecorder:
    def __init__(self):
        self.db = DatabaseConnection()
    
    def _get_utility(self, job_data: Dict[str, Any]) -> str:
        """Extract utility from job data"""
        utility = job_data.get('metadata', {}).get('utility', 'Unknown')
        return utility if utility else 'Unknown'

    def record_job_metrics(self, job_data: Dict[str, Any]) -> None:
        """Record job metrics to database"""
        job_id = job_data.get('id')
        status = job_data.get('status')
        utility = self._get_utility(job_data)
        total_poles = len(job_data.get('nodes', {}))
        completed_poles = sum(1 for node in job_data.get('nodes', {}).values() 
                            if node.get('field_completed'))
        
        query = """
        INSERT INTO job_metrics (
            job_id, status, utility, total_poles, completed_poles,
            timestamp
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (
                job_id, status, utility, total_poles, completed_poles
            ))

    def record_pole_metrics(self, job_data: Dict[str, Any]) -> None:
        """Record pole metrics to database"""
        job_id = job_data.get('id')
        utility = self._get_utility(job_data)
        
        for node_id, node in job_data.get('nodes', {}).items():
            query = """
            INSERT INTO pole_metrics (
                job_id, node_id, utility, field_completed,
                back_office_completed, timestamp
            ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (
                    job_id, node_id, utility,
                    node.get('field_completed', False),
                    node.get('back_office_completed', False)
                ))

    def record_user_metrics(self, job_data: Dict[str, Any]) -> None:
        """Record user metrics to database"""
        job_id = job_data.get('id')
        utility = self._get_utility(job_data)
        
        # Record field work
        field_users = set()
        for node in job_data.get('nodes', {}).values():
            if node.get('field_completed_by'):
                field_users.add(node['field_completed_by'])
        
        for user_id in field_users:
            poles_completed = sum(1 for node in job_data.get('nodes', {}).values()
                                if node.get('field_completed_by') == user_id)
            query = """
            INSERT INTO user_metrics (
                user_id, job_id, utility, role, activity_type,
                poles_completed, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (
                    user_id, job_id, utility, 'field', 'field_collection',
                    poles_completed
                ))

    def process_job(self, job_data: Dict[str, Any]) -> None:
        """Process a job and record its metrics"""
        try:
            # Record job metrics
            self.record_job_metrics(job_data)
            
            # Record pole metrics
            self.record_pole_metrics(job_data)
            
            # Record user metrics
            self.record_user_metrics(job_data)
            
            # Update daily summary and burndown metrics
            timestamp = datetime.now()
            self.update_daily_summary(timestamp)
            
            # Update burndown metrics for the utility
            utility = self._get_utility(job_data)
            self.update_burndown_metrics(utility, timestamp)
            
            logger.info(f"Successfully processed job {job_data.get('id', 'Unknown')}")
        except Exception as e:
            logger.error(f"Error processing job: {str(e)}")
            raise 

    def backfill_metrics(self, start_date: datetime, end_date: datetime = None) -> None:
        """Backfill metrics for a date range"""
        if end_date is None:
            end_date = datetime.now()
        
        logger.info(f"Backfilling metrics from {start_date} to {end_date}")
        
        try:
            current_date = start_date
            while current_date <= end_date:
                # Update daily summary
                self.update_daily_summary(current_date)
                
                # Get all utilities from job_metrics for this date
                query = """
                SELECT DISTINCT utility 
                FROM job_metrics 
                WHERE DATE(timestamp) = DATE(%s)
                AND utility IS NOT NULL
                """
                with self.db.get_cursor() as cursor:
                    cursor.execute(query, (current_date,))
                    utilities = [row['utility'] for row in cursor.fetchall()]
                
                # Update burndown metrics for each utility
                for utility in utilities:
                    self.update_burndown_metrics(utility, current_date)
                
                current_date += timedelta(days=1)
                
            logger.info("Successfully backfilled metrics")
        except Exception as e:
            logger.error(f"Error backfilling metrics: {str(e)}")
            raise 

    def update_daily_summary(self, timestamp: datetime) -> None:
        """Update daily summary metrics for all users"""
        logger.info(f"Updating daily summary for {timestamp}")
        
        # Update field work summary
        query = """
        WITH field_metrics AS (
            SELECT 
                field_completed_by as user_id,
                job_id,
                utility,
                COUNT(*) as poles_completed
            FROM pole_metrics
            WHERE field_completed = true
            AND DATE(timestamp) = DATE(%s)
            GROUP BY field_completed_by, job_id, utility
        )
        INSERT INTO user_daily_summary (
            user_id, date, role, total_poles_completed,
            utilities_worked, jobs_worked
        )
        SELECT 
            user_id,
            DATE(%s) as date,
            'field' as role,
            SUM(poles_completed) as total_poles_completed,
            ARRAY_AGG(DISTINCT utility) as utilities_worked,
            ARRAY_AGG(DISTINCT job_id) as jobs_worked
        FROM field_metrics
        WHERE user_id IS NOT NULL
        GROUP BY user_id
        ON CONFLICT (user_id, date, role)
        DO UPDATE SET
            total_poles_completed = EXCLUDED.total_poles_completed,
            utilities_worked = EXCLUDED.utilities_worked,
            jobs_worked = EXCLUDED.jobs_worked
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (timestamp, timestamp))
        
        # Update back office work summary
        query = """
        WITH back_office_metrics AS (
            SELECT 
                annotated_by as user_id,
                job_id,
                utility,
                COUNT(*) as poles_completed
            FROM pole_metrics
            WHERE back_office_completed = true
            AND DATE(timestamp) = DATE(%s)
            GROUP BY annotated_by, job_id, utility
        )
        INSERT INTO user_daily_summary (
            user_id, date, role, total_poles_completed,
            utilities_worked, jobs_worked
        )
        SELECT 
            user_id,
            DATE(%s) as date,
            'back_office' as role,
            SUM(poles_completed) as total_poles_completed,
            ARRAY_AGG(DISTINCT utility) as utilities_worked,
            ARRAY_AGG(DISTINCT job_id) as jobs_worked
        FROM back_office_metrics
        WHERE user_id IS NOT NULL
        GROUP BY user_id
        ON CONFLICT (user_id, date, role)
        DO UPDATE SET
            total_poles_completed = EXCLUDED.total_poles_completed,
            utilities_worked = EXCLUDED.utilities_worked,
            jobs_worked = EXCLUDED.jobs_worked
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (timestamp, timestamp))
            
        logger.info(f"Daily summary updated for {timestamp}")

    def update_burndown_metrics(self, utility: str, timestamp: datetime) -> None:
        """Update burndown metrics for a utility"""
        logger.info(f"Updating burndown metrics for {utility} at {timestamp}")
        
        query = """
        WITH metrics AS (
            SELECT 
                utility,
                COUNT(*) as total_poles,
                COUNT(CASE WHEN field_completed THEN 1 END) as field_completed,
                COUNT(CASE WHEN back_office_completed THEN 1 END) as back_office_completed,
                COUNT(DISTINCT CASE WHEN field_completed_by IS NOT NULL 
                    THEN field_completed_by END) as field_resources,
                COUNT(DISTINCT CASE WHEN annotated_by IS NOT NULL 
                    THEN annotated_by END) as back_office_resources,
                CASE 
                    WHEN COUNT(*) > 0 AND 
                         EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) > 0 THEN
                        COUNT(CASE WHEN field_completed OR back_office_completed THEN 1 END)::float / 
                        NULLIF(EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp)))/86400.0, 0)
                    ELSE 0
                END as daily_rate
            FROM pole_metrics
            WHERE utility = %s
            AND DATE(timestamp) = DATE(%s)
            GROUP BY utility
        )
        INSERT INTO burndown_metrics (
            utility,
            date,
            total_poles,
            completed_poles,
            run_rate,
            estimated_completion_date,
            actual_resources,
            required_resources
        )
        SELECT 
            utility,
            DATE(%s) as date,
            total_poles,
            LEAST(field_completed, back_office_completed) as completed_poles,
            CASE 
                WHEN daily_rate > 0 THEN daily_rate * 7  -- Convert daily rate to weekly rate
                ELSE 0
            END as run_rate,
            CASE 
                WHEN daily_rate > 0 THEN
                    DATE(%s) + 
                    NULLIF(((total_poles - LEAST(field_completed, back_office_completed)) / 
                    NULLIF(daily_rate, 0))::integer, 0)
                ELSE NULL
            END as estimated_completion_date,
            field_resources + back_office_resources as actual_resources,
            CASE 
                WHEN total_poles > LEAST(field_completed, back_office_completed) THEN
                    CEIL((total_poles - LEAST(field_completed, back_office_completed)) / 30.0)::integer 
                ELSE 0
            END as required_resources
        FROM metrics
        ON CONFLICT (utility, date)
        DO UPDATE SET
            total_poles = EXCLUDED.total_poles,
            completed_poles = EXCLUDED.completed_poles,
            run_rate = EXCLUDED.run_rate,
            estimated_completion_date = EXCLUDED.estimated_completion_date,
            actual_resources = EXCLUDED.actual_resources,
            required_resources = EXCLUDED.required_resources
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (utility, timestamp, timestamp, timestamp))
            
        logger.info(f"Burndown metrics updated for {utility} at {timestamp}")