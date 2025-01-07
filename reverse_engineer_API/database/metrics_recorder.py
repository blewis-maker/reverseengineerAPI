import logging
from datetime import datetime
from typing import Dict, List, Optional, Union
from .db import db
from psycopg2.extras import RealDictCursor

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
            # Validate input data
            if not isinstance(job_data, dict):
                raise ValueError(f"Expected job_data to be a dictionary, got {type(job_data)}")
            
            # Extract basic job data with list handling
            metadata = job_data.get('metadata', {})
            if isinstance(metadata, list):
                logging.warning(f"Expected metadata to be a dictionary, got list. Converting first item.")
                metadata = metadata[0] if metadata else {}
            elif not isinstance(metadata, dict):
                metadata = {}
                
            nodes = job_data.get('nodes', {})
            if isinstance(nodes, list):
                logging.warning(f"Expected nodes to be a dictionary, got list. Converting to dict by ID.")
                nodes = {str(i): node for i, node in enumerate(nodes) if isinstance(node, dict)}
            elif not isinstance(nodes, dict):
                nodes = {}
                
            photo_data = job_data.get('photos', {})
            if isinstance(photo_data, list):
                logging.warning(f"Expected photo_data to be a dictionary, got list. Converting to dict by ID.")
                photo_data = {str(i): photo for i, photo in enumerate(photo_data) if isinstance(photo, dict)}
            elif not isinstance(photo_data, dict):
                photo_data = {}
            
            # Get job ID with better error handling
            job_id = None
            if isinstance(metadata, dict):
                job_id = metadata.get('job_id')
            if not job_id and isinstance(job_data, dict):
                job_id = job_data.get('id')
            if not job_id:
                raise ValueError("Could not find job ID in job data")
            
            logging.info(f"Recording metrics for job {job_id}")
            
            # Extract utility from all possible sources
            utility = "Unknown"
            
            # First try metadata
            if isinstance(metadata, dict):
                metadata_utility = metadata.get('utility')
                if metadata_utility:
                    utility = metadata_utility
                    logging.info(f"Found utility in metadata: {utility}")
            
            # If not found in metadata, try pole tags and company attributes
            if utility == "Unknown":
                utility_counts = {}
                for node_data in nodes.values():
                    if isinstance(node_data, dict):
                        attributes = node_data.get('attributes', {})
                        if isinstance(attributes, dict):
                            # First try pole_tag
                            pole_tag = attributes.get('pole_tag', {})
                            company = pole_tag.get('-Imported', {}).get('company') or pole_tag.get('button_added', {}).get('company')
                            
                            # If not found in pole_tag, look for company in any direct child of pole_tag
                            if not company:
                                for key, value in pole_tag.items():
                                    if isinstance(value, dict) and 'company' in value:
                                        company = value['company']
                                        logging.debug(f"Found company in pole_tag.{key}: {company}")
                                        break
                            
                            # If still not found, check direct company attribute
                            if not company:
                                company_attr = attributes.get('company', {})
                                for source in ['-Imported', 'button_added', 'value', 'auto_calced']:
                                    company = company_attr.get(source)
                                    if company:
                                        logging.debug(f"Found company in attributes.company.{source}: {company}")
                                        break
                            
                            if company and company != "Unknown":
                                if company not in utility_counts:
                                    utility_counts[company] = 0
                                utility_counts[company] += 1
                
                # Use the most common utility if found
                if utility_counts:
                    utility = max(utility_counts.items(), key=lambda x: x[1])[0]
                    logging.info(f"Found utilities in nodes: {utility_counts}, using most common: {utility}")
            
            logging.info(f"Final utility selection: {utility}")
            
            # Calculate metrics with type checking
            total_poles = len([n for n in nodes.values() 
                             if isinstance(n, (dict, list)) and  # Handle both dict and list
                             self._is_pole_node(n)])
            
            field_complete = len([n for n in nodes.values() 
                                if isinstance(n, (dict, list)) and  # Handle both dict and list
                                self._get_field_completed(n)])
            
            back_office_complete = len([n for n in nodes.values() 
                                      if isinstance(n, (dict, list)) and  # Handle both dict and list
                                      self._get_back_office_completed(n)])
            
            # Get assigned user with type checking
            assigned_user = metadata.get('assigned_OSP') if isinstance(metadata, dict) else None
            assigned_users = [assigned_user] if assigned_user else []
            
            try:
                # Create daily snapshot first
                self._create_daily_snapshot(
                    job_id=job_id,
                    status=str(metadata.get('job_status', 'Unknown')),
                    utility=utility,
                    total_poles=int(total_poles),
                    field_complete=int(field_complete),
                    back_office_complete=int(back_office_complete),
                    timestamp=timestamp
                )
            except Exception as e:
                logging.error(f"Failed to create daily snapshot for job {job_id}: {str(e)}")
            
            try:
                # Record detailed job metrics
                self._insert_job_metrics(
                    job_id=job_id,
                    status=str(metadata.get('job_status', 'Unknown')),
                    utility=utility,
                    total_poles=int(total_poles),
                    completed_poles=int(field_complete),
                    field_complete=int(field_complete),
                    back_office_complete=int(back_office_complete),
                    assigned_users=assigned_users,
                    priority=int(metadata.get('priority', 3)),
                    target_completion_date=metadata.get('target_completion_date'),
                    timestamp=timestamp
                )
            except Exception as e:
                logging.error(f"Failed to insert job metrics for job {job_id}: {str(e)}")
            
            try:
                # Check for status changes
                self._check_status_changes(
                    job_id=job_id,
                    current_status=str(metadata.get('job_status', 'Unknown')),
                    timestamp=timestamp,
                    changed_by=assigned_user
                )
            except Exception as e:
                logging.error(f"Failed to check status changes for job {job_id}: {str(e)}")
                logging.error(f"Error details: {e.__class__.__name__}: {str(e)}")
            
            try:
                # Record pole metrics with temporal tracking
                if isinstance(nodes, dict):
                    self._record_pole_metrics(job_id, nodes, photo_data, timestamp)
            except Exception as e:
                logging.error(f"Failed to record pole metrics for job {job_id}: {str(e)}")
            
            # Record user metrics if users are assigned
            if assigned_users and isinstance(nodes, dict):
                try:
                    self._record_user_metrics(
                        job_id=job_id,
                        users=assigned_users,
                        utility=str(metadata.get('utility', 'Unknown')),
                        timestamp=timestamp,
                        nodes=nodes,
                        photo_data=photo_data
                    )
                except Exception as e:
                    logging.error(f"Failed to record user metrics for job {job_id}: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error recording metrics in database for job {job_data.get('id', 'unknown')}: {str(e)}")
            raise

    def _create_daily_snapshot(self, job_id: str, status: str, utility: str,
                             total_poles: int, field_complete: int,
                             back_office_complete: int, timestamp: datetime) -> None:
        """Create a daily snapshot of job metrics"""
        query = """
        INSERT INTO daily_snapshots (
            job_id, status, utility, total_poles,
            field_complete, back_office_complete,
            snapshot_date, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, DATE(%s), %s
        )
        ON CONFLICT (job_id, snapshot_date)
        DO UPDATE SET
            status = EXCLUDED.status,
            utility = EXCLUDED.utility,
            total_poles = EXCLUDED.total_poles,
            field_complete = EXCLUDED.field_complete,
            back_office_complete = EXCLUDED.back_office_complete,
            created_at = EXCLUDED.created_at
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (
                job_id, status, utility, total_poles,
                field_complete, back_office_complete,
                timestamp, timestamp
            ))

    def _check_status_changes(self, job_id: str, current_status: str,
                            timestamp: datetime, changed_by: Optional[str] = None) -> None:
        """Check and record any status changes"""
        try:
            with self.db.get_cursor() as cursor:
                try:
                    query = """
                        SELECT status, timestamp 
                        FROM job_metrics 
                        WHERE job_id = %s 
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    """
                    logging.info(f"Executing status query: {query} with job_id: {job_id}")
                    cursor.execute(query, (job_id,))
                    
                    result = cursor.fetchone()
                    logging.info(f"Status query result: {result}")
                    
                    if not result:
                        logging.info("No previous status found, recording initial status")
                        # Initial status - record without previous status
                        self._record_status_change(
                            job_id=job_id,
                            previous_status=None,
                            new_status=current_status,
                            timestamp=timestamp,
                            changed_by=changed_by
                        )
                        return
                    
                    # Get values from RealDictRow
                    previous_status = result.get('status')
                    previous_timestamp = result.get('timestamp')
                    logging.info(f"Previous status: {previous_status}, timestamp: {previous_timestamp}")
                    
                    # Status has changed
                    if previous_status != current_status:
                        duration_hours = None
                        if previous_timestamp and isinstance(previous_timestamp, datetime):
                            try:
                                duration_hours = (timestamp - previous_timestamp).total_seconds() / 3600
                                logging.info(f"Calculated duration: {duration_hours} hours")
                            except Exception as e:
                                logging.error(f"Failed to calculate duration: {str(e)}")
                        
                        self._record_status_change(
                            job_id=job_id,
                            previous_status=previous_status,
                            new_status=current_status,
                            timestamp=timestamp,
                            changed_by=changed_by,
                            duration_hours=duration_hours
                        )
                except Exception as e:
                    logging.error(f"Database query failed in _check_status_changes: {str(e)}")
                    logging.error(f"Query: {query}")
                    logging.error(f"Parameters: job_id={job_id}")
                    raise
                
        except Exception as e:
            logging.error(f"Error recording status change in database for job {job_id}: {str(e)}")
            raise

    def _record_status_change(self, job_id: str, previous_status: Optional[str],
                            new_status: str, timestamp: datetime,
                            changed_by: Optional[str] = None,
                            duration_hours: Optional[float] = None) -> None:
        """Record a status change with duration tracking"""
        try:
            # Record in status_changes table
            query = """
            INSERT INTO status_changes (
                job_id, previous_status, new_status, changed_at,
                changed_by, duration_hours, week_number, year
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                EXTRACT(WEEK FROM %s)::INTEGER,
                EXTRACT(YEAR FROM %s)::INTEGER
            )
            ON CONFLICT (job_id, changed_at)
            DO UPDATE SET
                previous_status = EXCLUDED.previous_status,
                new_status = EXCLUDED.new_status,
                changed_by = EXCLUDED.changed_by,
                duration_hours = EXCLUDED.duration_hours,
                week_number = EXCLUDED.week_number,
                year = EXCLUDED.year
            """
            
            # Also update the status_change_log for temporal history
            log_query = """
            INSERT INTO status_change_log (
                job_id, entity_type, entity_id, field_name,
                old_value, new_value, changed_at, changed_by,
                change_reason
            ) VALUES (
                %s, 'job', %s, 'status',
                %s, %s, %s, %s,
                CASE 
                    WHEN %s IS NOT NULL THEN 'OSP Assignment: ' || %s
                    ELSE NULL
                END
            )
            ON CONFLICT (entity_type, entity_id, field_name, changed_at)
            DO UPDATE SET
                old_value = EXCLUDED.old_value,
                new_value = EXCLUDED.new_value,
                changed_by = EXCLUDED.changed_by,
                change_reason = EXCLUDED.change_reason
            """
            
            with self.db.get_cursor() as cursor:
                # Record in status_changes
                cursor.execute(query, (
                    job_id, previous_status, new_status, timestamp,
                    changed_by, duration_hours, timestamp, timestamp
                ))
                
                # Record in status_change_log with OSP assignment tracking
                cursor.execute(log_query, (
                    job_id, job_id, previous_status,
                    new_status, timestamp, changed_by,
                    changed_by, changed_by
                ))
                
                # If this is an OSP assignment, record it separately
                if changed_by:
                    osp_query = """
                    INSERT INTO status_change_log (
                        job_id, entity_type, entity_id, field_name,
                        old_value, new_value, changed_at, changed_by
                    ) VALUES (
                        %s, 'job', %s, 'assigned_osp',
                        NULL, %s, %s, %s
                    )
                    ON CONFLICT (entity_type, entity_id, field_name, changed_at)
                    DO UPDATE SET
                        new_value = EXCLUDED.new_value,
                        changed_by = EXCLUDED.changed_by
                    """
                    cursor.execute(osp_query, (
                        job_id, job_id, changed_by,
                        timestamp, changed_by
                    ))
        except Exception as e:
            logging.error(f"Failed to record status change for job {job_id}: {str(e)}")
            raise

    def _record_pole_metrics(self, job_id: str, nodes: Dict,
                           photo_data: Dict, timestamp: datetime) -> None:
        """Record metrics for individual poles with temporal tracking"""
        logging.info(f"Recording pole metrics for job {job_id} with {len(nodes)} nodes")
        
        query = """
        INSERT INTO pole_metrics (
            job_id, node_id, utility, field_completed, field_completed_by,
            field_completed_at, back_office_completed, annotated_by,
            annotation_completed_at, pole_height, pole_class,
            mr_status, poa_height, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (job_id, node_id, timestamp)
        DO UPDATE SET
            utility = EXCLUDED.utility,
            field_completed = EXCLUDED.field_completed,
            field_completed_by = EXCLUDED.field_completed_by,
            field_completed_at = EXCLUDED.field_completed_at,
            back_office_completed = EXCLUDED.back_office_completed,
            annotated_by = EXCLUDED.annotated_by,
            annotation_completed_at = EXCLUDED.annotation_completed_at,
            pole_height = EXCLUDED.pole_height,
            pole_class = EXCLUDED.pole_class,
            mr_status = EXCLUDED.mr_status,
            poa_height = EXCLUDED.poa_height
        """
        
        # Debug log the query
        logging.info(f"Using SQL query for pole metrics: {query}")
        
        with self.db.get_cursor() as cursor:
            pole_count = 0
            for node_id, node_data in nodes.items():
                # Skip non-pole nodes
                if not self._is_pole_node(node_data):
                    continue
                    
                # Extract utility from pole tag company information
                attributes = node_data.get('attributes', {})
                utility = attributes.get('pole_tag', {}).get('-Imported', {}).get('company', 'Unknown')
                
                pole_data = self._extract_pole_data(node_data, photo_data)
                pole_data['utility'] = utility  # Override utility with the one from pole tag
                pole_count += 1
                
                try:
                    # Debug log the values
                    values = (
                        job_id,
                        node_id,
                        pole_data['utility'],
                        pole_data['field_completed'],
                        pole_data['field_completed_by'],
                        pole_data['field_completed_at'],
                        pole_data['back_office_completed'],
                        pole_data['annotated_by'],
                        pole_data['annotation_completed_at'],
                        pole_data['pole_height'],
                        pole_data['pole_class'],
                        pole_data['mr_status'],
                        pole_data['poa_height'],
                        timestamp
                    )
                    logging.info(f"Executing with values: {values}")
                    cursor.execute(query, values)
                    logging.info(f"Successfully recorded metrics for pole {node_id}")
                except Exception as e:
                    logging.error(f"Failed to record metrics for pole {node_id}: {str(e)}")
                    logging.error(f"Pole data: {pole_data}")
                    raise
            
            logging.info(f"Recorded metrics for {pole_count} poles out of {len(nodes)} nodes")

    def _is_pole_node(self, node_data: Union[Dict, List]) -> bool:
        """Check if node is a pole with support for both dict and list data types"""
        if isinstance(node_data, list):
            # If it's a list, check each item
            return any(self._is_pole_node(item) for item in node_data if isinstance(item, (dict, list)))
            
        if not isinstance(node_data, dict):
            return False
            
        attributes = node_data.get('attributes', {})
        if isinstance(attributes, list):
            attributes = attributes[0] if attributes else {}
            
        return any(attributes.get(type_field, {}).get(source) == 'pole'
                  for type_field in ['node_type', 'pole_type']
                  for source in ['button_added', '-Imported', 'value', 'auto_calced'])

    def _get_field_completed(self, node_data: Union[Dict, List]) -> bool:
        """Get field completed status with support for both dict and list data types"""
        if isinstance(node_data, list):
            return any(self._get_field_completed(item) for item in node_data if isinstance(item, (dict, list)))
            
        if not isinstance(node_data, dict):
            return False
            
        attributes = node_data.get('attributes', {})
        if isinstance(attributes, list):
            attributes = attributes[0] if attributes else {}
            
        return attributes.get('field_completed', {}).get('value', False)

    def _get_back_office_completed(self, node_data: Union[Dict, List]) -> bool:
        """Get back office completed status with support for both dict and list data types"""
        if isinstance(node_data, list):
            return any(self._get_back_office_completed(item) for item in node_data if isinstance(item, (dict, list)))
            
        if not isinstance(node_data, dict):
            return False
            
        attributes = node_data.get('attributes', {})
        if isinstance(attributes, list):
            attributes = attributes[0] if attributes else {}
            
        return attributes.get('backoffice_complete', {}).get('value', False)

    def _extract_pole_data(self, node_data: Dict, photo_data: Dict) -> Dict:
        """Extract pole data from node"""
        attributes = node_data.get('attributes', {})
        pole_tag = attributes.get('pole_tag', {}).get('-Imported', {})
        
        # Extract MR status
        mr_status = self._determine_mr_status(attributes)
        
        # Get editor information from photos
        editor_info = self._get_editor_info(node_data.get('photos', {}), photo_data)
        
        # Get back office completion status
        back_office_completed = attributes.get('backoffice_complete', {}).get('value', False)
        
        return {
            'utility': pole_tag.get('company', 'Unknown'),
            'field_completed': attributes.get('field_completed', {}).get('value', False),
            'field_completed_by': editor_info.get('field_completed_by'),
            'field_completed_at': editor_info.get('field_completed_at'),
            'back_office_completed': back_office_completed,
            'annotated_by': editor_info.get('annotated_by'),
            'annotation_completed_at': editor_info.get('annotation_completed_at'),
            'pole_height': attributes.get('pole_height', {}).get('-Imported'),
            'pole_class': attributes.get('pole_class', {}).get('-Imported'),
            'mr_status': mr_status,
            'poa_height': editor_info.get('poa_height')
        }

    def _determine_mr_status(self, attributes: Dict) -> str:
        """Determine MR status from attributes"""
        if 'proposed_pole_spec' in attributes:
            return "PCO Required"
        
        mr_state = attributes.get('mr_state', {}).get('auto_calced', "Unknown")
        warning_present = 'warning' in attributes
        
        if mr_state == "No MR" and not warning_present:
            return "No MR"
        elif mr_state == "MR Resolved" and not warning_present:
            return "Comm MR"
        elif mr_state == "MR Resolved" and warning_present:
            return "Electric MR"
        
        return "Unknown"

    def _get_editor_info(self, node_photos: Dict, photo_data: Dict) -> Dict:
        """Extract editor information from photos"""
        editor_info = {
            'field_completed_by': None,
            'field_completed_at': None,
            'annotated_by': None,
            'annotation_completed_at': None,
            'poa_height': None
        }
        
        try:
            for photo_id, photo_info in node_photos.items():
                if not isinstance(photo_info, dict):  # Type check for photo_info
                    continue
                    
                if photo_id not in photo_data:
                    continue
                    
                photo_editors = photo_data[photo_id].get('photofirst_data', {}).get('_editors', {})
                if not photo_editors:
                    continue
                    
                # Get the most recent editor
                latest_edit = max(photo_editors.items(), key=lambda x: x[1])
                editor_id, edit_time = latest_edit
                edit_dt = datetime.fromtimestamp(edit_time/1000)
                
                if photo_info.get('association') == 'main':
                    editor_info['field_completed_by'] = editor_id
                    editor_info['field_completed_at'] = edit_dt
                    
                    # Extract POA height
                    wire_data = photo_data[photo_id].get('photofirst_data', {}).get('wire', {})
                    for wire_info in wire_data.values():
                        if wire_info.get('_measured_height'):
                            editor_info['poa_height'] = wire_info.get('_measured_height')
                            break
                else:
                    editor_info['annotated_by'] = editor_id
                    editor_info['annotation_completed_at'] = edit_dt
        except Exception as e:
            logging.error(f"Error extracting editor info: {str(e)}")
            
        return editor_info

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
        ON CONFLICT (job_id, timestamp)
        DO UPDATE SET
            status = EXCLUDED.status,
            utility = EXCLUDED.utility,
            total_poles = EXCLUDED.total_poles,
            completed_poles = EXCLUDED.completed_poles,
            field_complete = EXCLUDED.field_complete,
            back_office_complete = EXCLUDED.back_office_complete,
            assigned_users = EXCLUDED.assigned_users,
            priority = EXCLUDED.priority,
            target_completion_date = EXCLUDED.target_completion_date
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (
                job_id, status, utility, total_poles, completed_poles,
                field_complete, back_office_complete, assigned_users,
                priority, target_completion_date, timestamp
            ))

    def _record_user_metrics(self, job_id: str, users: List[str],
                           utility: str, timestamp: datetime,
                           nodes: Dict, photo_data: Dict) -> None:
        """
        Record metrics for user productivity.
        
        Args:
            job_id: The ID of the job being processed
            users: List of assigned users
            utility: The utility company
            timestamp: Current processing timestamp
            nodes: Dictionary of nodes from the job data
            photo_data: Dictionary of photo data from the job data
        """
        logging.info(f"Recording user metrics for job {job_id} with {len(users)} assigned users")
        
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
                for photo_id in node_photos:
                    if photo_id in photo_data:
                        photo_editors = photo_data[photo_id].get('photofirst_data', {}).get('_editors', {})
                        if photo_editors:
                            # Get the most recent editor
                            latest_edit = max(photo_editors.items(), key=lambda x: x[1])
                            editor_id, edit_time = latest_edit
                            
                            # If this is a field photo
                            if photo_data[photo_id].get('association') == 'main':
                                if editor_id not in field_users:
                                    field_users[editor_id] = 0
                                field_users[editor_id] += 1
                            # If this is an annotation photo
                            else:
                                if editor_id not in back_office_users:
                                    back_office_users[editor_id] = 0
                                back_office_users[editor_id] += 1
            
            logging.info(f"Found {len(field_users)} field users and {len(back_office_users)} back office users")
            
            # Record field user metrics
            for user_id, pole_count in field_users.items():
                logging.info(f"Recording field user metrics - User: {user_id}, Poles: {pole_count}")
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
                logging.info(f"Recording back office user metrics - User: {user_id}, Poles: {pole_count}")
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