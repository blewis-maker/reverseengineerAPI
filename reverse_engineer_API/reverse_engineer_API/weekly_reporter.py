import logging
from datetime import datetime, timedelta
from typing import List, Dict, Set
from burndown_calculator import BurndownCalculator

logger = logging.getLogger(__name__)

def parse_date(date_str: str) -> datetime:
    """Parse a date string into a datetime object."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return datetime.now()

class WeeklyMetrics:
    def __init__(self):
        # User Production Metrics
        self.user_production = {
            'field': {},  # user_id -> {completed_poles: [], utilities: set(), dates: []}
            'back_office': {
                'annotation': {},  # user_id -> {completed_poles: [], jobs: [], dates: [], utilities: set()}
                'sent_to_pe': {
                    'jobs': [],  # [{job_id, pole_count, date, utility}]
                    'users': {}  # user_id -> {jobs: [], pole_count: int, dates: [], utilities: set()}
                },
                'delivery': {
                    'jobs': [],  # [{job_id, pole_count, date, utility}]
                    'users': {}  # user_id -> {jobs: [], pole_count: int, dates: [], utilities: set()}
                },
                'emr': {
                    'jobs': [],  # [{job_id, pole_count, date, utility}]
                    'users': {}  # user_id -> {jobs: [], pole_count: int, dates: [], utilities: set()}
                },
                'approved': {
                    'jobs': [],  # [{job_id, pole_count, date, utility}]
                    'users': {}  # user_id -> {jobs: [], pole_count: int, dates: [], utilities: set()}
                }
            }
        }
        
        # Status Change Tracking
        self.status_changes = {
            'field_collection': [],  # [{job_id, pole_count, date, utility}]
            'annotation': [],  # [{job_id, pole_count, date, utility}]
            'sent_to_pe': [],  # [{job_id, pole_count, date, utility}]
            'delivery': [],  # [{job_id, pole_count, date, utility}]
            'emr': [],  # [{job_id, pole_count, date, utility}]
            'approved': []  # [{job_id, pole_count, date, utility}]
        }
        
        # Backlog Metrics
        self.backlog = {
            'field': {
                'total_poles': 0,
                'jobs': set(),
                'utilities': set()
            },
            'back_office': {
                'total_poles': 0,
                'jobs': set(),
                'utilities': set()
            }
        }
        
        # Project Metrics
        self.projects = {}  # project_id -> {total_poles, completed_poles, back_office_users, field_users}
        
        # Initialize burndown metrics
        self.burndown = {
            'by_utility': {},  # utility -> {total_poles, completed_poles, run_rate, estimated_completion}
            'by_project': {}  # project -> {total_poles, completed_poles, run_rate, estimated_completion}
        }
        
        # Schedule tracking
        self.schedule = {
            'projects': []  # [{project_id, start_date, end_date, status, total_poles, completed_poles}]
        }

    def update_job_metrics(self, job_data: Dict, job_id: str, nodes: List[Dict]):
        """Update metrics based on job data."""
        try:
            metadata = job_data.get('metadata', {})
            utility = metadata.get('utility', 'Unknown')
            project_id = metadata.get('project_id', 'Unknown')
            job_status = job_data.get('status', 'Unknown')
            status_date = parse_date(job_data.get('status_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            old_status = job_data.get('old_status', 'Unknown')
            assigned_users = set(job_data.get('assigned_users', []))
            total_poles = len([n for n in nodes if n.get('type') == 'pole'])
            
            # Initialize project metrics if needed
            if project_id not in self.projects:
                self.projects[project_id] = {
                    'total_poles': 0,
                    'completed_poles': 0,
                    'back_office_users': set(),
                    'field_users': set()
                }
            
            # Update project metrics
            self.projects[project_id]['total_poles'] += total_poles
            if job_status in ['Delivered', 'Pending EMR']:
                self.projects[project_id]['completed_poles'] += total_poles
            
            # Update user assignments based on status
            if job_status in ['Field Collection', 'Pending Field Collection']:
                self.projects[project_id]['field_users'].update(assigned_users)
            elif job_status in ['Pending Photo Annotation', 'Sent to PE', 'Delivered', 'Pending EMR']:
                self.projects[project_id]['back_office_users'].update(assigned_users)
            
            # Update status changes
            if old_status != job_status:
                status_category = self._get_status_category(job_status)
                if status_category in self.status_changes:
                    self.status_changes[status_category].append({
                        'job_id': job_id,
                        'pole_count': total_poles,
                        'date': status_date.strftime('%Y-%m-%d') if isinstance(status_date, datetime) else status_date,
                        'utility': utility
                    })
            
            # Initialize backlog categories if not exists
            if 'backlog' not in self.burndown:
                self.burndown['backlog'] = {
                    'field': {'total_poles': 0, 'jobs': set(), 'utilities': set()},
                    'back_office': {'total_poles': 0, 'jobs': set(), 'utilities': set()},
                    'approve_construction': {'total_poles': 0, 'jobs': set(), 'utilities': set()}
                }
            
            # Process nodes for field completion status
            field_incomplete_count = sum(1 for node in nodes if not node.get('field_completed', {}).get('value') is True)
            
            # Update backlog metrics based on status and field completion
            if job_status == 'Pending Field Collection' or field_incomplete_count > 0:
                self.burndown['backlog']['field']['total_poles'] += field_incomplete_count
                self.burndown['backlog']['field']['jobs'].add(job_id)
                self.burndown['backlog']['field']['utilities'].add(utility)
            
            if job_status in ['Pending Photo Annotation', 'Sent to PE']:
                self.burndown['backlog']['back_office']['total_poles'] += len(nodes)
                self.burndown['backlog']['back_office']['jobs'].add(job_id)
                self.burndown['backlog']['back_office']['utilities'].add(utility)
                
            if job_status != 'Approved for Construction':
                self.burndown['backlog']['approve_construction']['total_poles'] += len(nodes)
                self.burndown['backlog']['approve_construction']['jobs'].add(job_id)
                self.burndown['backlog']['approve_construction']['utilities'].add(utility)
            
            # Update burndown metrics
            self._update_burndown_metrics(utility, project_id, total_poles, job_status)
            
            # Update user production metrics
            self._update_user_production(nodes, job_id, total_poles, utility, status_date, assigned_users, job_status)
            
        except Exception as e:
            logger.error(f"Error updating job metrics: {str(e)}")
            logger.error(f"Job data: {job_data}")

    def _get_status_category(self, status: str) -> str:
        """Map job status to status category."""
        status_mapping = {
            'Field Collection': 'field_collection',
            'Pending Field Collection': 'field_collection',
            'Photo Annotation In Progress': 'annotation',
            'Pending Photo Annotation': 'annotation',
            'Sent to PE': 'sent_to_pe',
            'Delivered': 'delivery',
            'Pending EMR': 'emr',
            'Approved for Construction': 'approved'
        }
        return status_mapping.get(status, '')

    def _update_burndown_metrics(self, utility: str, project_id: str, total_poles: int, job_status: str):
        """Update burndown metrics for utility and project."""
        # Initialize utility metrics if needed
        if utility not in self.burndown['by_utility']:
            self.burndown['by_utility'][utility] = {
                'total_poles': 0,
                'completed_poles': 0,
                'run_rate': 0.0,
                'estimated_completion': None
            }
        
        # Initialize project metrics if needed
        if project_id not in self.burndown['by_project']:
            self.burndown['by_project'][project_id] = {
                'total_poles': 0,
                'completed_poles': 0,
                'run_rate': 0.0,
                'estimated_completion': None
            }
        
        # Update pole counts
        self.burndown['by_utility'][utility]['total_poles'] += total_poles
        self.burndown['by_project'][project_id]['total_poles'] += total_poles
        
        if job_status in ['Delivered', 'Pending EMR']:
            self.burndown['by_utility'][utility]['completed_poles'] += total_poles
            self.burndown['by_project'][project_id]['completed_poles'] += total_poles
            
        # Calculate run rates and estimated completion
        for category, metrics in [
            ('by_utility', self.burndown['by_utility'][utility]),
            ('by_project', self.burndown['by_project'][project_id])
        ]:
            if metrics['total_poles'] > 0:
                metrics['run_rate'] = metrics['completed_poles'] / metrics['total_poles']
                if metrics['run_rate'] > 0:
                    days_to_completion = (metrics['total_poles'] - metrics['completed_poles']) / metrics['run_rate']
                    metrics['estimated_completion'] = (datetime.now() + timedelta(days=days_to_completion)).strftime('%Y-%m-%d')

    def _update_user_production(self, nodes: List[Dict], job_id: str, total_poles: int, 
                              utility: str, status_date: datetime, assigned_users: Set[str], job_status: str = None):
        """Update user production metrics based on the type of work performed."""
        # Ensure status_date is a datetime object
        if isinstance(status_date, str):
            status_date = parse_date(status_date)
        elif not isinstance(status_date, datetime):
            status_date = datetime.now()
            
        # Track field users (those who completed field work)
        field_completed_poles = 0
        for node in nodes:
            if node.get('field_completed', {}).get('value') is True:
                field_user = node.get('field_completed_by')
                if field_user:
                    if field_user not in self.user_production['field']:
                        self.user_production['field'][field_user] = {
                            'completed_poles': [],
                            'utilities': set(),
                            'dates': [],
                            'jobs': {}  # job_id -> pole_count
                        }
                    self.user_production['field'][field_user]['completed_poles'].append(node['id'])
                    self.user_production['field'][field_user]['utilities'].add(utility)
                    self.user_production['field'][field_user]['dates'].append(status_date)
                    field_completed_poles += 1
                
        # Update job information for field users if any poles were completed
        if field_completed_poles > 0:
            for field_user in self.user_production['field']:
                if job_id not in self.user_production['field'][field_user]['jobs']:
                    self.user_production['field'][field_user]['jobs'][job_id] = field_completed_poles
                
        # Track back office users (those who did annotation work)
        back_office_completed_poles = 0
        for node in nodes:
            if node.get('done', {}).get('button_added') or node.get('done', {}).get('-Imported') or node.get('done', {}).get('multi_added'):
                annotator = node.get('annotated_by')
                if annotator:
                    if annotator not in self.user_production['back_office']['annotation']:
                        self.user_production['back_office']['annotation'][annotator] = {
                            'completed_poles': [],
                            'jobs': [],
                            'dates': [],
                            'utilities': set()
                        }
                    self.user_production['back_office']['annotation'][annotator]['completed_poles'].append(node['id'])
                    self.user_production['back_office']['annotation'][annotator]['jobs'].append({
                        'job_id': job_id,
                        'pole_count': 1
                    })
                    self.user_production['back_office']['annotation'][annotator]['dates'].append(status_date)
                    self.user_production['back_office']['annotation'][annotator]['utilities'].add(utility)
                    back_office_completed_poles += 1
                    
        # Track other back office activities (sent to PE, delivery, EMR, approved)
        if job_status:
            status_category = self._get_status_category(job_status)
            if status_category in ['sent_to_pe', 'delivery', 'emr', 'approved']:
                for user in assigned_users:
                    if user not in self.user_production['back_office'][status_category]['users']:
                        self.user_production['back_office'][status_category]['users'][user] = {
                            'jobs': [],
                            'pole_count': 0,
                            'dates': [],
                            'utilities': set()
                        }
                    user_data = self.user_production['back_office'][status_category]['users'][user]
                    user_data['jobs'].append({
                        'job_id': job_id,
                        'pole_count': total_poles
                    })
                    user_data['pole_count'] += total_poles
                    user_data['dates'].append(status_date)
                    user_data['utilities'].add(utility)
                
        # Update utility metrics with completed poles
        if utility not in self.burndown['by_utility']:
            self.burndown['by_utility'][utility] = {
                'total_poles': 0,
                'field_completed': 0,
                'back_office_completed': 0,
                'run_rate': 0.0,
                'estimated_completion': None
            }
        self.burndown['by_utility'][utility]['field_completed'] += field_completed_poles
        self.burndown['by_utility'][utility]['back_office_completed'] += back_office_completed_poles
                
        logger.debug(f"Updated user production metrics for job {job_id}")
        logger.debug(f"Field users: {self.user_production['field'].keys()}")
        logger.debug(f"Back office users: {[u for c in self.user_production['back_office'].values() for u in c.get('users', {}).keys()]}")

    def get_weekly_status(self):
        """Get formatted weekly status metrics."""
        # Format utility metrics
        utility_metrics = {}
        for utility, data in self.burndown['by_utility'].items():
            if utility == 'Unknown':
                continue
            utility_metrics[utility] = {
                'total_poles': data['total_poles'],
                'field_completed': data['field_completed'],
                'back_office_completed': data['back_office_completed'],
                'run_rate': data['run_rate'],
                'estimated_completion': data['estimated_completion']
            }
            
        # Format user production metrics
        user_production = {
            'field': [],
            'annotation': [],
            'sent_to_pe': [],
            'delivery': [],
            'emr': [],
            'approved': []
        }
        
        # Process field users
        for user_id, data in self.user_production['field'].items():
            user_production['field'].append({
                'user': user_id,
                'completed_poles': len(data['completed_poles']),
                'utilities': list(data['utilities']),
                'jobs': [{'job_id': job_id, 'pole_count': pole_count} 
                        for job_id, pole_count in data['jobs'].items()]
            })
            
        # Process back office users
        for category in ['annotation', 'sent_to_pe', 'delivery', 'emr', 'approved']:
            if category == 'annotation':
                for user_id, data in self.user_production['back_office']['annotation'].items():
                    user_production[category].append({
                        'user': user_id,
                        'completed_poles': len(data['completed_poles']),
                        'utilities': list(data['utilities']),
                        'jobs': data['jobs']  # Already in correct format
                    })
            else:
                for user_id, data in self.user_production['back_office'][category]['users'].items():
                    user_production[category].append({
                        'user': user_id,
                        'completed_poles': data['pole_count'],
                        'utilities': list(data['utilities']),
                        'jobs': data['jobs']  # Already in correct format
                    })
                    
        # Format status changes
        status_changes = {}
        for status, changes in self.status_changes.items():
            total_poles = sum(change['pole_count'] for change in changes)
            total_jobs = len(changes)
            change_from_last_week = total_poles  # For now, just use total as change
            
            status_changes[status] = {
                'job_count': total_jobs,
                'pole_count': total_poles,
                'change_from_last_week': change_from_last_week
            }
            
        # Format schedule metrics
        schedule_metrics = {
            'projects': []
        }
        for project_id, data in self.projects.items():
            if project_id == 'Unknown':
                continue
            schedule_metrics['projects'].append({
                'project_id': project_id,
                'total_poles': data['total_poles'],
                'completed_poles': data['completed_poles'],
                'field_users': list(data['field_users']),
                'back_office_users': list(data['back_office_users']),
                'end_date': None  # TODO: Add end date calculation
            })
            
        return {
            'utility_metrics': utility_metrics,
            'user_production': user_production,
            'status_changes': status_changes,
            'burndown': self.burndown,
            'schedule': schedule_metrics
        }

def get_weekly_jobs(start_date: datetime, end_date: datetime, test_mode: bool = False) -> List[str]:
    """Get jobs updated within the specified date range."""
    from main import TEST_JOB_IDS
    if test_mode:
        return TEST_JOB_IDS
    else:
        # TODO: Implement actual job retrieval logic
        return []

def generate_weekly_report(end_date: datetime = None, test_mode: bool = False) -> bool:
    """Generate weekly report for the specified end date."""
    try:
        # Set up dates
        if end_date is None:
            end_date = datetime.now()
        
        # Adjust end_date to next Sunday if not already Sunday
        days_until_sunday = (6 - end_date.weekday()) % 7
        end_date = end_date + timedelta(days=days_until_sunday)
        end_date = end_date.replace(hour=8, minute=0, second=0, microsecond=0)  # 8:00 AM
        
        # Start date is previous Sunday
        start_date = end_date - timedelta(days=7)
        
        logger.info(f"Generating weekly report for period {start_date.strftime('%m/%d/%y')} to {end_date.strftime('%m/%d/%y')}")
        
        # Get jobs updated in the date range
        jobs = get_weekly_jobs(start_date, end_date, test_mode)
        if not jobs:
            logger.error("No jobs found for the specified date range")
            return False
            
        # Initialize metrics and burndown calculator
        metrics = WeeklyMetrics()
        burndown = BurndownCalculator(start_date=start_date, end_date=end_date)
        
        # Set standard run rates
        burndown.back_office_capacity = 100/7  # 100 poles per week per back office user
        burndown.field_capacity = 80/7         # 80 poles per week per field user
        
        # Get user list for mapping IDs to names
        from main import getUserList
        user_map = getUserList()
        
        # Process each job
        from main import getJobData, extractNodes
        for job_id in jobs:
            job_data = getJobData(job_id)
            if not job_data:
                continue
                
            # Extract nodes
            nodes = extractNodes(job_data, job_data.get('metadata', {}).get('name', ''), job_id, user_map)
            
            # Update metrics
            metrics.update_job_metrics(job_data, job_id, nodes)
            burndown.update_job_metrics(job_data)
            
        # Generate report
        from weekly_excel_generator import WeeklyReportGenerator
        report_date = end_date.strftime('%Y%m%d')
        output_path = f'reports/weekly/weekly_report_{report_date}.xlsx'
        
        # Create report generator
        generator = WeeklyReportGenerator(metrics, end_date)
        
        # Generate report
        success = generator.generate_report(output_path)
        if not success:
            logger.error("Failed to generate report")
            return False
            
        logger.info(f"Report generated at: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error generating weekly report: {str(e)}")
        return False 