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
            metadata = job_data.get('metadata', {})
            nodes = job_data.get('nodes', {})  # Changed from [] to {} as nodes is a dictionary
            
            # Get job ID from metadata
            job_id = metadata.get('job_id')
            if not job_id:
                # Try to get it from the job data directly
                job_id = job_data.get('id')
            if not job_id:
                raise ValueError("Could not find job ID in job data")
            
            logging.info(f"Recording metrics for job {job_id}")
            
            # Calculate metrics
            total_poles = len([n for n in nodes.values() 
                             if any(n.get('attributes', {}).get(type_field, {}).get(source) == 'pole'
                                   for type_field in ['node_type', 'pole_type']
                                   for source in ['button_added', '-Imported', 'value', 'auto_calced'])])
            
            logging.info(f"Found {total_poles} total poles")
            
            field_complete = len([n for n in nodes.values() 
                                if n.get('attributes', {}).get('field_completed', {}).get('value') == True])
            
            logging.info(f"Found {field_complete} field completed poles")
            
            back_office_complete = len([n for n in nodes.values() 
                                      if n.get('attributes', {}).get('backoffice_complete', {}).get('value') == True])
            
            logging.info(f"Found {back_office_complete} back office completed poles")
            
            # Record job metrics
            logging.info("Inserting job metrics into database...")
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
            logging.info("Successfully inserted job metrics")
            
            # Check and record status changes
            logging.info("Checking for status changes...")
            self._check_status_changes(
                job_id=job_id,
                current_status=metadata.get('job_status', 'Unknown'),
                timestamp=timestamp
            )
            
            # Record pole metrics
            logging.info("Recording pole metrics...")
            self._record_pole_metrics(job_id, nodes, timestamp)
            logging.info("Successfully recorded all metrics for job")
            
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

    def _record_pole_metrics(self, job_id: str, nodes: Dict,
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
            for node_id, node_data in nodes.items():
                # Skip non-pole nodes
                attributes = node_data.get('attributes', {})
                is_pole = any(attributes.get(type_field, {}).get(source) == 'pole'
                             for type_field in ['node_type', 'pole_type']
                             for source in ['button_added', '-Imported', 'value', 'auto_calced'])
                if not is_pole:
                    continue
                    
                # Get pole tag info
                pole_tag = attributes.get('pole_tag', {}).get('-Imported', {})
                utility = pole_tag.get('company', 'Unknown')
                
                # Get field completion status
                field_completed = attributes.get('field_completed', {}).get('value', False)
                
                # Extract editor information from photos
                field_completed_by = None
                field_completed_at = None
                annotated_by = None
                annotation_completed_at = None
                
                # Check photos associated with the node for editor information
                node_photos = node_data.get('photos', {})
                for photo_id, photo_info in node_photos.items():
                    photo_editors = photo_info.get('photofirst_data', {}).get('_editors', {})
                    if photo_editors:
                        # Get the most recent editor
                        latest_edit = max(photo_editors.items(), key=lambda x: x[1])
                        editor_id, edit_time = latest_edit
                        
                        # Convert timestamp to datetime
                        edit_dt = datetime.fromtimestamp(edit_time/1000)
                        
                        # If this is a field photo and we haven't set field editor yet
                        if photo_info.get('association') == 'main' and not field_completed_by:
                            field_completed_by = editor_id
                            field_completed_at = edit_dt
                        # If this is an annotation photo and we haven't set annotator yet
                        elif photo_info.get('association') != 'main' and not annotated_by:
                            annotated_by = editor_id
                            annotation_completed_at = edit_dt
                
                # Get MR status
                mr_status = "Unknown"
                if 'proposed_pole_spec' in attributes:
                    mr_status = "PCO Required"
                else:
                    mr_state = attributes.get('mr_state', {}).get('auto_calced', "Unknown")
                    warning_present = 'warning' in attributes
                    if mr_state == "No MR" and not warning_present:
                        mr_status = "No MR"
                    elif mr_state == "MR Resolved" and not warning_present:
                        mr_status = "Comm MR"
                    elif mr_state == "MR Resolved" and warning_present:
                        mr_status = "Electric MR"
                
                # Get pole specifications
                pole_height = attributes.get('pole_height', {}).get('-Imported')
                pole_class = attributes.get('pole_class', {}).get('-Imported')
                
                # Get POA height from photos if available
                poa_height = None
                photos = node_data.get('photos', {})
                for photo_id, photo_info in photos.items():
                    if photo_info.get('association') == 'main':
                        # Extract POA height from wire data
                        photofirst_data = photo_info.get('photofirst_data', {}).get('wire', {})
                        for wire_info in photofirst_data.values():
                            if wire_info.get('_measured_height'):
                                poa_height = wire_info.get('_measured_height')
                                break
                
                cursor.execute(query, (
                    job_id,
                    node_id,
                    utility,
                    field_completed,
                    field_completed_by,
                    field_completed_at,
                    False,  # back_office_completed
                    annotated_by,
                    annotation_completed_at,
                    pole_height,
                    pole_class,
                    mr_status,
                    poa_height,
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
            # Track field users
            field_users = {}  # user_id -> pole_count
            # Track back office users
            back_office_users = {}  # user_id -> pole_count
            
            # Analyze nodes for user activity
            for node_id, node_data in nodes.items():
                # Skip non-pole nodes
                attributes = node_data.get('attributes', {})
                is_pole = any(attributes.get(type_field, {}).get(source) == 'pole'
                             for type_field in ['node_type', 'pole_type']
                             for source in ['button_added', '-Imported', 'value', 'auto_calced'])
                if not is_pole:
                    continue
                
                # Check photos for editor information
                node_photos = node_data.get('photos', {})
                for photo_id, photo_info in node_photos.items():
                    photo_editors = photo_info.get('photofirst_data', {}).get('_editors', {})
                    if photo_editors:
                        # Get the most recent editor
                        latest_edit = max(photo_editors.items(), key=lambda x: x[1])
                        editor_id, edit_time = latest_edit
                        
                        # If this is a field photo
                        if photo_info.get('association') == 'main':
                            if editor_id not in field_users:
                                field_users[editor_id] = 0
                            field_users[editor_id] += 1
                        # If this is an annotation photo
                        else:
                            if editor_id not in back_office_users:
                                back_office_users[editor_id] = 0
                            back_office_users[editor_id] += 1
            
            # Record field user metrics
            for user_id, pole_count in field_users.items():
                cursor.execute(query, (
                    user_id,
                    job_id,
                    utility,
                    'field',
                    'photo_collection',
                    pole_count,
                    timestamp
                ))
            
            # Record back office user metrics
            for user_id, pole_count in back_office_users.items():
                cursor.execute(query, (
                    user_id,
                    job_id,
                    utility,
                    'back_office',
                    'annotation',
                    pole_count,
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