import logging
from datetime import datetime
from typing import Dict, List, Optional
from .db import db

logger = logging.getLogger(__name__)

class MetricsRecorder:
    """Handles recording of all metrics into the database"""
    
    def __init__(self):
        self.db = db
    
    def record_job_metrics(self, job_data: Dict, timestamp: datetime) -> None:
        """
        Record job metrics during each run.
        This is the primary entry point for job data collection.
        """
        try:
            # Extract basic job data
            job_id = job_data['id']
            metadata = job_data.get('metadata', {})
            nodes = job_data.get('nodes', [])
            
            # Calculate metrics
            total_poles = len(nodes)
            field_complete = len([n for n in nodes if n.get('fldcompl') == 'yes'])
            back_office_complete = len([n for n in nodes if n.get('backoffice_complete') == 'yes'])
            
            # Record job metrics
            self._insert_job_metrics(
                job_id=job_id,
                status=metadata.get('job_status', 'Unknown'),
                utility=metadata.get('utility', 'Unknown'),
                total_poles=total_poles,
                completed_poles=field_complete,
                field_complete=field_complete,
                back_office_complete=back_office_complete,
                assigned_users=metadata.get('assigned_users', []),
                priority=metadata.get('priority', 3),
                target_completion_date=metadata.get('target_completion_date'),
                timestamp=timestamp
            )
            
            # Check and record status changes
            self._check_status_changes(
                job_id=job_id,
                current_status=metadata.get('job_status', 'Unknown'),
                timestamp=timestamp
            )
            
            # Record pole metrics
            self._record_pole_metrics(job_id, nodes, timestamp)
            
            # Record user metrics if users are assigned
            if assigned_users := metadata.get('assigned_users'):
                self._record_user_metrics(
                    job_id=job_id,
                    users=assigned_users,
                    utility=metadata.get('utility', 'Unknown'),
                    timestamp=timestamp
                )
            
        except Exception as e:
            logger.error(f"Failed to record metrics for job {job_data.get('id')}: {str(e)}")
            raise

    def _insert_job_metrics(self, job_id: str, status: str, utility: str,
                          total_poles: int, completed_poles: int,
                          field_complete: int, back_office_complete: int,
                          assigned_users: List[str], priority: int,
                          target_completion_date: Optional[str],
                          timestamp: datetime) -> None:
        """Insert metrics into job_metrics table"""
        query = """
        INSERT INTO job_metrics (
            job_id, status, utility, total_poles, completed_poles,
            field_complete, back_office_complete, assigned_users,
            priority, target_completion_date, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (
                job_id, status, utility, total_poles, completed_poles,
                field_complete, back_office_complete, assigned_users,
                priority, target_completion_date, timestamp
            ))

    def _check_status_changes(self, job_id: str, current_status: str,
                            timestamp: datetime) -> None:
        """Check and record any status changes"""
        query = """
        SELECT status
        FROM job_metrics
        WHERE job_id = %s
        ORDER BY timestamp DESC
        LIMIT 1
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (job_id,))
            result = cursor.fetchone()
            
            if result and result['status'] != current_status:
                self._record_status_change(
                    job_id=job_id,
                    previous_status=result['status'],
                    new_status=current_status,
                    timestamp=timestamp
                )

    def _record_status_change(self, job_id: str, previous_status: str,
                            new_status: str, timestamp: datetime) -> None:
        """Record a status change"""
        query = """
        INSERT INTO status_changes (
            job_id, previous_status, new_status, changed_at,
            week_number, year
        ) VALUES (
            %s, %s, %s, %s,
            EXTRACT(WEEK FROM %s)::INTEGER,
            EXTRACT(YEAR FROM %s)::INTEGER
        )
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (
                job_id, previous_status, new_status, timestamp,
                timestamp, timestamp
            ))

    def _record_pole_metrics(self, job_id: str, nodes: List[Dict],
                           timestamp: datetime) -> None:
        """Record metrics for individual poles"""
        query = """
        INSERT INTO pole_metrics (
            job_id, pole_id, utility, field_completed, field_completed_by,
            field_completed_at, back_office_completed, annotated_by,
            annotation_completed_at, pole_height, pole_class,
            mr_status, poa_height, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        with self.db.get_cursor() as cursor:
            for node in nodes:
                cursor.execute(query, (
                    job_id,
                    node.get('id'),
                    node.get('utility'),
                    node.get('fldcompl') == 'yes',
                    node.get('field_completed_by'),
                    node.get('field_completed_at'),
                    node.get('backoffice_complete') == 'yes',
                    node.get('annotated_by'),
                    node.get('annotation_completed_at'),
                    node.get('pole_height'),
                    node.get('pole_class'),
                    node.get('mr_status'),
                    node.get('poa_height'),
                    timestamp
                ))

    def _record_user_metrics(self, job_id: str, users: List[str],
                           utility: str, timestamp: datetime) -> None:
        """Record metrics for user productivity"""
        query = """
        INSERT INTO user_metrics (
            user_id, job_id, utility, role, activity_type,
            poles_completed, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        with self.db.get_cursor() as cursor:
            for user in users:
                # TODO: Enhance with actual pole completion counts per user
                cursor.execute(query, (
                    user,
                    job_id,
                    utility,
                    'field',  # TODO: Get actual role
                    'assignment',
                    0,  # TODO: Calculate actual poles completed
                    timestamp
                ))

    def update_daily_summary(self, timestamp: datetime) -> None:
        """Update daily summary metrics for all users"""
        query = """
        INSERT INTO user_daily_summary (
            user_id, date, role, total_poles_completed,
            utilities_worked, jobs_worked
        )
        SELECT 
            user_id,
            DATE(%s) as date,
            role,
            SUM(poles_completed) as total_poles_completed,
            ARRAY_AGG(DISTINCT utility) as utilities_worked,
            ARRAY_AGG(DISTINCT job_id) as jobs_worked
        FROM user_metrics
        WHERE DATE(timestamp) = DATE(%s)
        GROUP BY user_id, role, DATE(timestamp)
        ON CONFLICT (user_id, date, role)
        DO UPDATE SET
            total_poles_completed = EXCLUDED.total_poles_completed,
            utilities_worked = EXCLUDED.utilities_worked,
            jobs_worked = EXCLUDED.jobs_worked
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (timestamp, timestamp))

    def update_burndown_metrics(self, utility: str, timestamp: datetime) -> None:
        """Update burndown metrics for a utility"""
        query = """
        WITH metrics AS (
            SELECT 
                SUM(total_poles) as total_poles,
                SUM(completed_poles) as completed_poles,
                COUNT(DISTINCT CASE WHEN assigned_users IS NOT NULL 
                    THEN assigned_users END) as actual_resources
            FROM job_metrics
            WHERE utility = %s
            AND DATE(timestamp) = DATE(%s)
        )
        INSERT INTO burndown_metrics (
            utility, date, total_poles, completed_poles,
            run_rate, estimated_completion_date,
            actual_resources, required_resources
        )
        SELECT 
            %s as utility,
            DATE(%s) as date,
            total_poles,
            completed_poles,
            completed_poles::float / NULLIF(total_poles, 0) as run_rate,
            DATE(%s) + 
                ((total_poles - completed_poles) / 
                NULLIF(completed_poles::float / 30, 0))::integer 
                as estimated_completion_date,
            actual_resources,
            CEIL((total_poles - completed_poles) / 30.0)::integer 
                as required_resources
        FROM metrics
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (
                utility, timestamp, utility, timestamp, timestamp
            ))

# Create a singleton instance
metrics_recorder = MetricsRecorder() 