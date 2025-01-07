from datetime import datetime, date
from typing import Dict, List, Optional
from database.db import DatabaseConnection
import logging

logger = logging.getLogger(__name__)

class ProjectManager:
    def __init__(self):
        self.db = DatabaseConnection()
    
    def create_project(self, project_data: Dict) -> int:
        """Create a new project"""
        query = """
        INSERT INTO projects (
            project_id, name, utility, total_poles, 
            field_resources, back_office_resources,
            current_run_rate, required_run_rate,
            target_date, projected_end_date, status, progress
        ) VALUES (
            %(project_id)s, %(name)s, %(utility)s, %(total_poles)s,
            %(field_resources)s, %(back_office_resources)s,
            %(current_run_rate)s, %(required_run_rate)s,
            %(target_date)s, %(projected_end_date)s, %(status)s, %(progress)s
        ) RETURNING id
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, project_data)
            project_id = cursor.fetchone()['id']
            return project_id

    def update_project(self, project_id: str, update_data: Dict) -> bool:
        """Update project details"""
        set_clauses = []
        params = {'project_id': project_id}
        
        for key, value in update_data.items():
            set_clauses.append(f"{key} = %({key})s")
            params[key] = value
        
        query = f"""
        UPDATE projects 
        SET {', '.join(set_clauses)}
        WHERE project_id = %(project_id)s
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount > 0

    def get_project(self, project_id: str) -> Optional[Dict]:
        """Get project details by project_id"""
        query = "SELECT * FROM projects WHERE project_id = %s"
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (project_id,))
            return cursor.fetchone()

    def get_all_projects(self) -> List[Dict]:
        """Get all projects"""
        query = "SELECT * FROM projects ORDER BY target_date"
        with self.db.get_cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def update_project_metrics(self, project_id: str) -> None:
        """Update project metrics based on associated jobs"""
        query = """
        WITH project_metrics AS (
            SELECT 
                COUNT(DISTINCT pm.node_id) as total_poles,
                COUNT(DISTINCT CASE WHEN pm.field_completed THEN pm.node_id END) as field_complete,
                COUNT(DISTINCT CASE WHEN pm.back_office_completed THEN pm.node_id END) as back_office_complete,
                COUNT(DISTINCT pm.field_completed_by) as field_resources,
                COUNT(DISTINCT pm.annotated_by) as back_office_resources,
                CASE 
                    WHEN COUNT(*) > 0 AND 
                         EXTRACT(EPOCH FROM (MAX(pm.timestamp) - MIN(pm.timestamp))) > 0 THEN
                        COUNT(CASE WHEN pm.field_completed OR pm.back_office_completed THEN 1 END)::float / 
                        NULLIF(EXTRACT(EPOCH FROM (MAX(pm.timestamp) - MIN(pm.timestamp)))/86400.0, 0)
                    ELSE 0
                END as daily_rate
            FROM pole_metrics pm
            JOIN job_metrics jm ON pm.job_id = jm.job_id
            WHERE jm.project_id = %s
        )
        UPDATE projects p
        SET 
            total_poles = pm.total_poles,
            field_resources = pm.field_resources,
            back_office_resources = pm.back_office_resources,
            current_run_rate = CASE 
                WHEN pm.daily_rate > 0 THEN pm.daily_rate * 7  -- Convert to weekly rate
                ELSE 0
            END,
            progress = CASE 
                WHEN pm.total_poles > 0 THEN 
                    LEAST(pm.field_complete, pm.back_office_complete)::float / pm.total_poles
                ELSE 0
            END,
            projected_end_date = CASE 
                WHEN pm.daily_rate > 0 AND pm.total_poles > LEAST(pm.field_complete, pm.back_office_complete) THEN
                    CURRENT_DATE + 
                    ((pm.total_poles - LEAST(pm.field_complete, pm.back_office_complete)) / pm.daily_rate)::integer
                ELSE NULL
            END
        FROM project_metrics pm
        WHERE p.project_id = %s
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (project_id, project_id))

    def calculate_required_run_rate(self, project_id: str) -> None:
        """Calculate required run rate based on target date"""
        query = """
        UPDATE projects
        SET required_run_rate = CASE 
            WHEN target_date > CURRENT_DATE AND total_poles > 0 THEN
                (total_poles * (1 - progress)) / 
                NULLIF(EXTRACT(DAYS FROM (target_date - CURRENT_DATE)), 0) * 7  -- Weekly rate
            ELSE 0
        END
        WHERE project_id = %s
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (project_id,))

    def update_project_status(self, project_id: str) -> None:
        """Update project status based on progress and dates"""
        query = """
        UPDATE projects
        SET status = CASE
            WHEN progress >= 1 THEN 'Completed'
            WHEN target_date < CURRENT_DATE THEN 'Behind Schedule'
            WHEN current_run_rate >= required_run_rate THEN 'On Track'
            ELSE 'At Risk'
        END
        WHERE project_id = %s
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (project_id,))

    def associate_job_with_project(self, job_id: str, project_id: str) -> None:
        """Associate a job with a project"""
        query = """
        UPDATE job_metrics
        SET project_id = %s
        WHERE job_id = %s
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (project_id, job_id)) 