"""
Module for calculating burndown metrics for weekly reporting.
"""

import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class UtilityMetrics:
    total_poles: int = 0
    completed_poles: int = 0
    run_rate: float = 0.0
    estimated_completion: Optional[datetime.datetime] = None

@dataclass
class ProjectMetrics:
    total_poles: int = 0
    completed_poles: int = 0
    run_rate: float = 0.0
    estimated_completion: Optional[datetime.datetime] = None
    back_office_users: set = None
    field_users: set = None
    start_date: Optional[datetime.datetime] = None
    end_date: Optional[datetime.datetime] = None
    
    def __post_init__(self):
        if self.back_office_users is None:
            self.back_office_users = set()
        if self.field_users is None:
            self.field_users = set()

class BurndownCalculator:
    def __init__(self, start_date: datetime.datetime, end_date: datetime.datetime, total_nodes: int = 0):
        self.start_date = start_date
        self.end_date = end_date
        self.total_nodes = total_nodes
        self.days_elapsed = (end_date - start_date).days
        self.daily_target = total_nodes / self.days_elapsed if self.days_elapsed > 0 else 0
        
        # Standard run rates (poles per week)
        self.BACK_OFFICE_RATE = 100  # 100 poles per week per back office user
        self.FIELD_RATE = 80         # 80 poles per week per field user
        
        # Convert to daily rates
        self.back_office_capacity = self.BACK_OFFICE_RATE / 7  # Daily rate per back office user
        self.field_capacity = self.FIELD_RATE / 7              # Daily rate per field user
        
        self.utility_metrics: Dict[str, UtilityMetrics] = {}
        self.project_metrics: Dict[str, ProjectMetrics] = {}
        self.status_metrics: Dict[str, Dict[str, int]] = {}
        self.weekly_completion_rates: Dict[str, List[float]] = {}
        self.node_processing_stats = {
            'total_nodes': 0,
            'processed_nodes': 0,
            'skipped_nodes': 0,
            'error_nodes': 0,
            'pole_types': {},
            'completion_sources': {
                'button_added': 0,
                '-Imported': 0,
                'multi_added': 0
            }
        }
        
        logger.info(f"Initialized BurndownCalculator with dates: {start_date} to {end_date}")
        logger.info(f"Standard run rates - Back Office: {self.BACK_OFFICE_RATE} poles/week, Field: {self.FIELD_RATE} poles/week")
        
    def update_job_metrics(self, job_data: Dict[str, Any]) -> None:
        """Update metrics based on job data"""
        try:
            job_id = job_data.get('id', 'Unknown')
            logger.info(f"Processing job metrics for job: {job_id}")
            
            # Reset node processing stats for this job
            job_stats = {
                'total_nodes': 0,
                'processed_nodes': 0,
                'skipped_nodes': 0,
                'error_nodes': 0,
                'pole_types': {},
                'completion_sources': {
                    'button_added': 0,
                    '-Imported': 0,
                    'multi_added': 0
                }
            }
            
            # Validate job_data
            if not isinstance(job_data, dict):
                logger.error(f"Invalid job data type: {type(job_data)}")
                return

            # Get metadata with proper validation
            metadata = job_data.get('metadata', {})
            if not isinstance(metadata, dict):
                logger.error(f"Invalid metadata type: {type(metadata)}")
                return

            # Log metadata details
            logger.info(f"Job Metadata:")
            logger.info(f"  Project: {metadata.get('project', 'Unknown')}")
            logger.info(f"  Status: {job_data.get('status', 'Unknown')}")
            logger.info(f"  Assigned OSP: {metadata.get('assigned_OSP', 'None')}")
            logger.info(f"  Start Date: {metadata.get('start_date', 'None')}")
            logger.info(f"  End Date: {metadata.get('end_date', 'None')}")

            # Determine utility using same logic as main.py
            utility = 'Unknown'
            nodes = job_data.get('nodes', {})
            for node_data in nodes.values():
                attributes = node_data.get('attributes', {})
                # First check pole_tag
                pole_tag = attributes.get('pole_tag', {})
                company = pole_tag.get('-Imported', {}).get('company') or pole_tag.get('button_added', {}).get('company')
                
                # If not found, look for company in any direct child of pole_tag
                if not company:
                    for key, value in pole_tag.items():
                        if isinstance(value, dict) and 'company' in value:
                            company = value['company']
                            logger.debug(f"Found company in pole_tag.{key}: {company}")
                            break
                
                # If still not found, check direct company attribute
                if not company:
                    company_attr = attributes.get('company', {})
                    for source in ['-Imported', 'button_added', 'value', 'auto_calced']:
                        company = company_attr.get(source)
                        if company:
                            logger.debug(f"Found company in attributes.company.{source}: {company}")
                            break
                
                if company:
                    utility = company
                    logger.info(f"Using utility: {utility}")
                    break

            project = metadata.get('project', 'Unknown')
            status = job_data.get('status', 'Unknown')
            
            logger.info(f"Processing - Utility: {utility}, Project: {project}, Status: {status}")
            
            # Process nodes with validation
            nodes = job_data.get('nodes', {})
            if not isinstance(nodes, dict):
                logger.error(f"Invalid nodes type: {type(nodes)}")
                return

            # Count poles and completed poles
            pole_count = 0
            completed_poles = 0
            job_stats['total_nodes'] = len(nodes)
            
            for node_id, node_data in nodes.items():
                try:
                    if not isinstance(node_data, dict):
                        job_stats['skipped_nodes'] += 1
                        continue
                        
                    attributes = node_data.get('attributes', {})
                    if not isinstance(attributes, dict):
                        job_stats['skipped_nodes'] += 1
                        continue
                    
                    # Track node type
                    node_type = None
                    for type_field in ['node_type', 'pole_type']:
                        for source in ['button_added', '-Imported', 'value', 'auto_calced']:
                            type_value = attributes.get(type_field, {}).get(source)
                            if type_value:
                                node_type = type_value
                                job_stats['pole_types'][node_type] = job_stats['pole_types'].get(node_type, 0) + 1
                                break
                        if node_type:
                            break
                    
                    if node_type == 'pole':
                        pole_count += 1
                        # Check completion status and track source
                        done_attr = attributes.get('done', {})
                        for source in ['button_added', '-Imported', 'multi_added']:
                            if done_attr.get(source) == True:
                                completed_poles += 1
                                job_stats['completion_sources'][source] += 1
                                logger.debug(f"Pole {node_id} completed via {source}")
                                break
                    
                    job_stats['processed_nodes'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing node {node_id}: {str(e)}")
                    job_stats['error_nodes'] += 1
            
            # Log node processing statistics
            logger.info(f"Node Processing Statistics:")
            logger.info(f"  Total Nodes: {job_stats['total_nodes']}")
            logger.info(f"  Processed: {job_stats['processed_nodes']}")
            logger.info(f"  Skipped: {job_stats['skipped_nodes']}")
            logger.info(f"  Errors: {job_stats['error_nodes']}")
            logger.info(f"  Node Types: {job_stats['pole_types']}")
            logger.info(f"  Completion Sources: {job_stats['completion_sources']}")
            logger.info(f"Pole counts - Total: {pole_count}, Completed: {completed_poles}")
            
            # Update global stats
            for key in ['total_nodes', 'processed_nodes', 'skipped_nodes', 'error_nodes']:
                self.node_processing_stats[key] += job_stats[key]
            for node_type, count in job_stats['pole_types'].items():
                self.node_processing_stats['pole_types'][node_type] = self.node_processing_stats['pole_types'].get(node_type, 0) + count
            for source, count in job_stats['completion_sources'].items():
                self.node_processing_stats['completion_sources'][source] += count

            # Initialize utility metrics if not exists
            if utility not in self.utility_metrics:
                self.utility_metrics[utility] = UtilityMetrics()
                logger.info(f"Initialized metrics for utility: {utility}")
            
            # Initialize project metrics if not exists
            if project not in self.project_metrics:
                start_date = metadata.get('start_date')
                end_date = metadata.get('end_date')
                
                # Convert string dates to datetime objects
                if isinstance(start_date, str):
                    try:
                        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
                    except ValueError:
                        start_date = None
                if isinstance(end_date, str):
                    try:
                        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
                    except ValueError:
                        end_date = None
                    
                self.project_metrics[project] = ProjectMetrics(
                    start_date=start_date,
                    end_date=end_date
                )
                logger.info(f"Initialized metrics for project: {project}")
            
            # Update utility metrics
            self.utility_metrics[utility].total_poles += pole_count
            self.utility_metrics[utility].completed_poles += completed_poles
            
            # Update project metrics
            self.project_metrics[project].total_poles += pole_count
            self.project_metrics[project].completed_poles += completed_poles
            
            # Track users by category
            if status in ['Pending Photo Annotation', 'Photo Annotation In Progress', 'QA Review']:
                assigned_osp = metadata.get('assigned_OSP')
                if assigned_osp:
                    self.project_metrics[project].back_office_users.add(assigned_osp)
                    logger.info(f"Added back office user {assigned_osp} to project {project}")
            elif status in ['Field Collection In Progress', 'Pending Field Collection']:
                assigned_osp = metadata.get('assigned_OSP')
                if assigned_osp:
                    self.project_metrics[project].field_users.add(assigned_osp)
                    logger.info(f"Added field user {assigned_osp} to project {project}")
            
            # Calculate run rates and estimated completion
            if self.days_elapsed > 0:
                # Calculate utility run rate based on standard rates and user counts
                back_office_users = len(self.project_metrics[project].back_office_users)
                field_users = len(self.project_metrics[project].field_users)
                
                back_office_capacity = back_office_users * self.back_office_capacity
                field_capacity = field_users * self.field_capacity
                
                # Use the lower capacity as the bottleneck
                effective_daily_rate = min(back_office_capacity, field_capacity)
                
                logger.info(f"Calculated rates for {utility}/{project}:")
                logger.info(f"  Back Office: {back_office_users} users * {self.back_office_capacity:.1f} poles/day = {back_office_capacity:.1f}")
                logger.info(f"  Field: {field_users} users * {self.field_capacity:.1f} poles/day = {field_capacity:.1f}")
                logger.info(f"  Effective daily rate: {effective_daily_rate:.1f}")
                
                # Update metrics
                self.utility_metrics[utility].run_rate = effective_daily_rate
                self.project_metrics[project].run_rate = effective_daily_rate
                
                # Calculate estimated completion dates
                for metrics in [self.utility_metrics[utility], self.project_metrics[project]]:
                    remaining_poles = metrics.total_poles - metrics.completed_poles
                    if metrics.run_rate > 0:
                        days_to_completion = remaining_poles / metrics.run_rate
                        metrics.estimated_completion = datetime.datetime.now() + datetime.timedelta(days=days_to_completion)
                        logger.info(f"Estimated completion in {days_to_completion:.1f} days: {metrics.estimated_completion}")
            
            # Update status metrics
            if status not in self.status_metrics:
                self.status_metrics[status] = {'total_poles': 0, 'completed_poles': 0}
            self.status_metrics[status]['total_poles'] += pole_count
            self.status_metrics[status]['completed_poles'] += completed_poles
            
            # Track weekly completion rates
            for category in [utility, project]:
                if category not in self.weekly_completion_rates:
                    self.weekly_completion_rates[category] = []
                if self.days_elapsed >= 7:  # Only track if we have at least a week of data
                    weekly_rate = completed_poles / (self.days_elapsed / 7)
                    self.weekly_completion_rates[category].append(weekly_rate)
                    logger.info(f"Weekly completion rate for {category}: {weekly_rate:.1f} poles/week")
                    
        except Exception as e:
            logger.error(f"Error updating job metrics: {str(e)}")
            logger.error(f"Job data: {job_data}")
            
    def calculate_project_burndown(self, project: str, current_date: datetime.datetime) -> Dict[str, Any]:
        """Calculate burndown metrics for a specific project"""
        if project not in self.project_metrics:
            return {
                'project': project,
                'total_poles': 0,
                'completed_poles': 0,
                'progress': 0,
                'back_office_users': 0,
                'field_users': 0,
                'estimated_completion': None,
                'status': 'Not Started'
            }
            
        metrics = self.project_metrics[project]
        
        # Ensure we have valid datetime objects
        if not isinstance(metrics.start_date, datetime.datetime) or not isinstance(current_date, datetime.datetime):
            return {
                'project': project,
                'total_poles': metrics.total_poles,
                'completed_poles': metrics.completed_poles,
                'progress': (metrics.completed_poles / metrics.total_poles * 100) if metrics.total_poles > 0 else 0,
                'back_office_users': len(metrics.back_office_users),
                'field_users': len(metrics.field_users),
                'estimated_completion': None,
                'status': 'Unknown'
            }
            
        days_since_start = (current_date - metrics.start_date).days
        days_remaining = (metrics.end_date - current_date).days if metrics.end_date else 0
        
        if days_since_start <= 0:
            return {
                'project': project,
                'total_poles': metrics.total_poles,
                'completed_poles': 0,
                'progress': 0,
                'back_office_users': len(metrics.back_office_users),
                'field_users': len(metrics.field_users),
                'estimated_completion': None,
                'status': 'Not Started'
            }
            
        # Calculate completion rate based on back office capacity
        back_office_capacity = len(metrics.back_office_users) * self.back_office_capacity
        field_capacity = len(metrics.field_users) * self.field_capacity
        
        # Use the lower capacity as the bottleneck
        daily_capacity = min(back_office_capacity, field_capacity)
        if daily_capacity > 0:
            days_needed = (metrics.total_poles - metrics.completed_poles) / daily_capacity
            estimated_completion = current_date + datetime.timedelta(days=days_needed)
        else:
            estimated_completion = None
            
        progress = (metrics.completed_poles / metrics.total_poles * 100) if metrics.total_poles > 0 else 0
        
        return {
            'project': project,
            'total_poles': metrics.total_poles,
            'completed_poles': metrics.completed_poles,
            'progress': progress,
            'back_office_users': len(metrics.back_office_users),
            'field_users': len(metrics.field_users),
            'estimated_completion': estimated_completion,
            'status': self.get_project_status(metrics, current_date)
        }
        
    def get_project_status(self, metrics: ProjectMetrics, current_date: datetime.datetime) -> str:
        """Get the status of a project based on its metrics"""
        if not isinstance(metrics.start_date, datetime.datetime) or not isinstance(current_date, datetime.datetime):
            return 'Unknown'
            
        if metrics.completed_poles >= metrics.total_poles:
            return 'Completed'
            
        if current_date < metrics.start_date:
            return 'Not Started'
            
        if not metrics.end_date:
            return 'In Progress'
            
        if current_date > metrics.end_date:
            return 'Overdue'
            
        progress = (metrics.completed_poles / metrics.total_poles) if metrics.total_poles > 0 else 0
        expected_progress = (current_date - metrics.start_date).days / (metrics.end_date - metrics.start_date).days
        
        if progress >= expected_progress:
            return 'On Track'
        elif progress >= expected_progress * 0.8:
            return 'At Risk'
        else:
            return 'Behind Schedule'
            
    def calculate_burndown_metrics(self, completed_poles: int, current_date: datetime.datetime) -> Dict[str, Any]:
        """Calculate overall burndown metrics."""
        if not self.total_nodes:
            return {
                'total_poles': 0,
                'completed_poles': 0,
                'progress': 0,
                'run_rate': 0,
                'estimated_completion': None,
                'status': 'Not Started'
            }

        progress = (completed_poles / self.total_nodes * 100) if self.total_nodes > 0 else 0
        days_elapsed = (current_date - self.start_date).days if isinstance(self.start_date, datetime.datetime) else 0
        
        if days_elapsed > 0:
            run_rate = completed_poles / days_elapsed
            remaining_poles = self.total_nodes - completed_poles
            if run_rate > 0:
                days_to_completion = remaining_poles / run_rate
                estimated_completion = current_date + datetime.timedelta(days=days_to_completion)
            else:
                estimated_completion = None
        else:
            run_rate = 0
            estimated_completion = None

        return {
            'total_poles': self.total_nodes,
            'completed_poles': completed_poles,
            'progress': progress,
            'run_rate': run_rate,
            'estimated_completion': estimated_completion.strftime('%Y-%m-%d') if estimated_completion else None,
            'status': 'In Progress' if progress > 0 else 'Not Started'
        }

    def get_burndown_metrics(self) -> Dict[str, Any]:
        """Get all burndown metrics for reporting"""
        current_date = datetime.datetime.now()
        
        def format_date(date_obj):
            if isinstance(date_obj, datetime.datetime):
                return date_obj.strftime('%Y-%m-%d')
            elif isinstance(date_obj, str):
                try:
                    return datetime.datetime.strptime(date_obj, '%Y-%m-%d').strftime('%Y-%m-%d')
                except ValueError:
                    return date_obj
            return None
        
        return {
            'by_utility': {
                utility: {
                    'total_poles': metrics.total_poles,
                    'completed_poles': metrics.completed_poles,
                    'run_rate': metrics.run_rate,
                    'estimated_completion': format_date(metrics.estimated_completion),
                    'weekly_rates': self.weekly_completion_rates.get(utility, [])
                }
                for utility, metrics in self.utility_metrics.items()
            },
            'by_project': {
                project: self.calculate_project_burndown(project, current_date)
                for project in self.project_metrics
            },
            'by_status': self.status_metrics,
            'overall': self.calculate_burndown_metrics(
                sum(m.completed_poles for m in self.utility_metrics.values()),
                datetime.datetime.now()
            )
        } 