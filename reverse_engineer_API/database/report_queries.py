import logging
from datetime import datetime, timedelta
from typing import Dict, List
from .db import db

logger = logging.getLogger(__name__)

class ReportQueries:
    """Handles data retrieval for weekly reports"""
    
    def __init__(self):
        self.db = db
    
    def get_weekly_job_metrics(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get job metrics for the weekly report"""
        query = """
        WITH daily_metrics AS (
            SELECT 
                job_id,
                utility,
                status,
                total_poles,
                completed_poles,
                field_complete,
                back_office_complete,
                DATE(timestamp) as date
            FROM job_metrics
            WHERE timestamp BETWEEN %s AND %s
        ),
        status_duration AS (
            SELECT 
                job_id,
                new_status,
                AVG(EXTRACT(EPOCH FROM (
                    LEAD(changed_at) OVER (PARTITION BY job_id ORDER BY changed_at)
                    - changed_at
                ))/86400) as avg_days_in_status
            FROM status_changes
            WHERE changed_at BETWEEN %s AND %s
            GROUP BY job_id, new_status
        )
        SELECT 
            m.job_id,
            m.utility,
            m.status as current_status,
            m.total_poles,
            m.completed_poles,
            m.field_complete,
            m.back_office_complete,
            COALESCE(sd.avg_days_in_status, 0) as avg_days_in_current_status,
            m.date
        FROM daily_metrics m
        LEFT JOIN status_duration sd ON 
            m.job_id = sd.job_id AND 
            m.status = sd.new_status
        ORDER BY m.utility, m.job_id, m.date
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (start_date, end_date, start_date, end_date))
            return cursor.fetchall()
    
    def get_utility_progress(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get utility-level progress metrics"""
        query = """
        WITH utility_metrics AS (
            SELECT 
                utility,
                DATE(timestamp) as date,
                COUNT(DISTINCT job_id) as active_jobs,
                SUM(total_poles) as total_poles,
                SUM(completed_poles) as completed_poles
            FROM job_metrics
            WHERE timestamp BETWEEN %s AND %s
            GROUP BY utility, DATE(timestamp)
        ),
        burndown_data AS (
            SELECT 
                utility,
                date,
                run_rate,
                estimated_completion_date,
                actual_resources,
                required_resources
            FROM burndown_metrics
            WHERE date BETWEEN %s AND %s
        )
        SELECT 
            um.*,
            bd.run_rate,
            bd.estimated_completion_date,
            bd.actual_resources,
            bd.required_resources
        FROM utility_metrics um
        LEFT JOIN burndown_data bd ON 
            um.utility = bd.utility AND 
            um.date = bd.date
        ORDER BY um.utility, um.date
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (start_date, end_date, start_date, end_date))
            return cursor.fetchall()
    
    def get_status_flow_analysis(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get status transition analysis"""
        query = """
        WITH status_flows AS (
            SELECT 
                previous_status,
                new_status,
                COUNT(*) as transition_count,
                AVG(EXTRACT(EPOCH FROM (
                    LEAD(changed_at) OVER (PARTITION BY job_id ORDER BY changed_at)
                    - changed_at
                ))/86400) as avg_days_between_changes
            FROM status_changes
            WHERE changed_at BETWEEN %s AND %s
            GROUP BY previous_status, new_status
        )
        SELECT 
            previous_status,
            new_status,
            transition_count,
            ROUND(avg_days_between_changes::numeric, 2) as avg_days_between_changes,
            ROUND(
                (transition_count::float / 
                SUM(transition_count) OVER ()) * 100,
                2
            ) as transition_percentage
        FROM status_flows
        ORDER BY transition_count DESC
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (start_date, end_date))
            return cursor.fetchall()
    
    def get_user_productivity(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get user productivity metrics"""
        query = """
        WITH user_daily AS (
            SELECT 
                user_id,
                date,
                role,
                total_poles_completed,
                utilities_worked,
                jobs_worked
            FROM user_daily_summary
            WHERE date BETWEEN %s AND %s
        ),
        user_averages AS (
            SELECT 
                user_id,
                role,
                AVG(total_poles_completed) as avg_daily_poles,
                COUNT(DISTINCT date) as days_worked,
                COUNT(DISTINCT UNNEST(utilities_worked)) as utilities_count,
                COUNT(DISTINCT UNNEST(jobs_worked)) as jobs_count
            FROM user_daily
            GROUP BY user_id, role
        )
        SELECT 
            ua.*,
            ROUND(
                avg_daily_poles / NULLIF(days_worked, 0)::float,
                2
            ) as productivity_rate,
            ROUND(
                jobs_count::float / NULLIF(days_worked, 0),
                2
            ) as jobs_per_day
        FROM user_averages ua
        ORDER BY avg_daily_poles DESC
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (start_date, end_date))
            return cursor.fetchall()
    
    def get_pole_completion_analysis(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get detailed pole completion analysis"""
        query = """
        WITH pole_stats AS (
            SELECT 
                utility,
                COUNT(*) as total_poles,
                SUM(CASE WHEN field_completed THEN 1 ELSE 0 END) as field_completed_count,
                SUM(CASE WHEN back_office_completed THEN 1 ELSE 0 END) as back_office_completed_count,
                AVG(CASE 
                    WHEN field_completed_at IS NOT NULL AND annotation_completed_at IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (annotation_completed_at - field_completed_at))/86400
                    ELSE NULL
                END) as avg_processing_days
            FROM pole_metrics
            WHERE timestamp BETWEEN %s AND %s
            GROUP BY utility
        )
        SELECT 
            utility,
            total_poles,
            field_completed_count,
            back_office_completed_count,
            ROUND(
                (field_completed_count::float / NULLIF(total_poles, 0)) * 100,
                2
            ) as field_completion_rate,
            ROUND(
                (back_office_completed_count::float / NULLIF(total_poles, 0)) * 100,
                2
            ) as back_office_completion_rate,
            ROUND(avg_processing_days::numeric, 2) as avg_processing_days
        FROM pole_stats
        ORDER BY total_poles DESC
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (start_date, end_date))
            return cursor.fetchall()

# Create a singleton instance
report_queries = ReportQueries() 