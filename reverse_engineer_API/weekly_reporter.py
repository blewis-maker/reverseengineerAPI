from datetime import datetime, timedelta
import logging
from typing import Dict, List, Set
import os
import json
import http.client
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from main import (
    getJobData,
    extractNodes,
    extractConnections,
    extractAnchors,
    getUserList,
    update_sharepoint_spreadsheet,
    CONFIG,
    TEST_JOB_IDS
)

from weekly_excel_generator import WeeklyReportGenerator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test mode configuration
TEST_MODE = True  # Set to True for testing with test jobs

def parse_date(date_str: str) -> datetime:
    """Convert date string to datetime object."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except (ValueError, TypeError):
            return datetime.now()

def get_weekly_jobs(start_date: datetime, end_date: datetime, test_mode: bool = False) -> List[str]:
    """Get jobs updated within the specified date range."""
    try:
        if test_mode:
            return TEST_JOB_IDS
            
        # Set up API request
        url = f"{CONFIG['API_BASE_URL']}/jobs"
        params = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'status_changed': True
        }
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Make API request
        response = session.get(url, params=params)
        response.raise_for_status()
        
        # Parse response
        jobs_data = response.json()
        job_ids = [job['id'] for job in jobs_data]
        
        logging.info(f"Found {len(job_ids)} jobs updated between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}")
        return job_ids
        
    except Exception as e:
        logging.error(f"Error getting weekly jobs: {str(e)}")
        return []

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

    def update_job_metrics(self, job_data, job_id, nodes):
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
            field_incomplete_count = sum(1 for node in nodes if not node.get('field_completed', False))
            
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
        for node in nodes:
            if node.get('field_completed', {}).get('value') is True:
                field_user = node.get('field_completed_by')
                if field_user:
                    if field_user not in self.user_production['field']:
                        self.user_production['field'][field_user] = {
                            'completed_poles': [],
                            'utilities': set(),
                            'dates': []
                        }
                    self.user_production['field'][field_user]['completed_poles'].append(node['id'])
                    self.user_production['field'][field_user]['utilities'].add(utility)
                    self.user_production['field'][field_user]['dates'].append(status_date)
                
            # Track back office users (those who did annotation work)
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
                    self.user_production['back_office']['annotation'][annotator]['jobs'].append(job_id)
                    self.user_production['back_office']['annotation'][annotator]['dates'].append(status_date)
                    self.user_production['back_office']['annotation'][annotator]['utilities'].add(utility)
        
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
                    user_data['jobs'].append(job_id)
                    user_data['pole_count'] += total_poles
                    user_data['dates'].append(status_date)
                    user_data['utilities'].add(utility)
                
        logger.debug(f"Updated user production metrics for job {job_id}")
        logger.debug(f"Field users: {self.user_production['field'].keys()}")
        logger.debug(f"Back office users: {[u for c in self.user_production['back_office'].values() for u in c.get('users', {}).keys()]}")

    def get_weekly_status(self):
        """Get weekly status metrics in a format suitable for reporting."""
        status = {
            'user_production': {
                'field': [],
                'annotation': [],
                'sent_to_pe': [],
                'delivery': [],
                'emr': [],
                'approved': []
            },
            'status_changes': self.status_changes,
            'backlog': self.backlog,
            'projects': [],
            'utility_metrics': {},
            'burndown': self.burndown,
            'schedule': self.schedule
        }
        
        # Process field metrics
        for user_id, data in self.user_production['field'].items():
            status['user_production']['field'].append({
                'user': user_id,
                'completed_poles': len(data['completed_poles']),
                'utilities': list(data['utilities']),
                'dates': [d.strftime('%Y-%m-%d') if isinstance(d, datetime) else d for d in data['dates']]
            })
            
        # Process back office metrics for all categories
        for category in ['annotation', 'sent_to_pe', 'delivery', 'emr', 'approved']:
            if category == 'annotation':
                for user_id, data in self.user_production['back_office'][category].items():
                    status['user_production'][category].append({
                        'user': user_id,
                        'completed_poles': len(data['completed_poles']),
                        'jobs': data['jobs'],
                        'utilities': list(data['utilities']),
                        'dates': [d.strftime('%Y-%m-%d') if isinstance(d, datetime) else d for d in data['dates']]
                    })
            else:
                for user_id, data in self.user_production['back_office'][category]['users'].items():
                    status['user_production'][category].append({
                        'user': user_id,
                        'jobs': data['jobs'],
                        'pole_count': data['pole_count'],
                        'utilities': list(data['utilities']),
                        'dates': [d.strftime('%Y-%m-%d') if isinstance(d, datetime) else d for d in data['dates']]
                    })

        # Process project metrics
        for project_id, data in self.projects.items():
            status['projects'].append({
                'project_id': project_id,
                'total_poles': data['total_poles'],
                'completed_poles': data['completed_poles'],
                'back_office_users': len(data['back_office_users']),
                'field_users': len(data['field_users'])
            })
            
        # Process utility metrics
        for utility, metrics in self.burndown['by_utility'].items():
            status['utility_metrics'][utility] = {
                'total_poles': metrics['total_poles'],
                'completed_poles': metrics['completed_poles'],
                'run_rate': metrics['run_rate'],
                'estimated_completion': metrics['estimated_completion']
            }
            
        return status

def generate_weekly_report(start_date: datetime, end_date: datetime, output_path: str, test_mode: bool = False) -> bool:
    """Generate the weekly report."""
    try:
        # Initialize metrics
        metrics = WeeklyMetrics()
        
        # Get weekly jobs
        jobs = get_weekly_jobs(start_date, end_date, test_mode)
        if not jobs:
            logging.error("No jobs found for the week")
            return False
            
        # Get user list for mapping
        user_map = getUserList()
        if not user_map:
            logging.error("Failed to get user mapping")
            return False
            
        # Process each job
        for job_id in jobs:
            job_data = getJobData(job_id)
            if not job_data:
                continue
                
            # Extract nodes
            nodes = extractNodes(job_data, job_data.get('metadata', {}).get('name', ''), job_id, user_map)
            
            # Update metrics
            metrics.update_job_metrics(job_data, job_id, nodes)
            
        # Generate Excel report
        generator = WeeklyReportGenerator(metrics)
        generator.generate_report(output_path)
        
        logging.info(f"Weekly report generated successfully at {output_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error generating weekly report: {str(e)}")
        return False

def process_weekly_data(job_data: Dict, metrics: WeeklyMetrics, user_map: Dict) -> None:
    """Process weekly data for a single job."""
    try:
        # Extract metadata
        metadata = job_data.get('metadata', {})
        utility = metadata.get('utility', 'Unknown')
        job_id = job_data.get('id', 'Unknown')
        status = job_data.get('status')
        
        # Extract nodes
        nodes = extractNodes(job_data, metadata.get('name', ''), job_id, user_map)
        
        # Update metrics
        metrics.update_job_metrics(job_data)
        
        # Track various metrics
        track_field_collection(nodes, metrics, utility)
        track_back_office(nodes, metrics, utility, job_id, status)
        track_status_metrics(nodes, metrics, utility, job_id, status)
        
    except Exception as e:
        logging.error(f"Error processing weekly data: {str(e)}")

def get_sharepoint_path() -> str:
    """Generate SharePoint path for weekly report"""
    now = datetime.now()
    year = now.strftime('%Y')
    week = now.strftime('%V')
    return f"Weekly_Reports/{year}/Week_{week}"

def get_user_list():
    """Get list of users from the API."""
    try:
        response = requests.get(f"{API_BASE_URL}/getUserList")
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'users' in data:
                return data['users']
            else:
                logging.error(f"Unexpected user list format: {data}")
                return []
        else:
            logging.error(f"Failed to get user list. Status code: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"Error getting user list: {str(e)}")
        return []

def get_test_jobs() -> List[Dict]:
    """Get test jobs for testing the weekly report generation."""
    try:
        test_jobs = []
        for job_id in TEST_JOB_IDS:
            job_data = getJobData(job_id)
            if job_data:
                # Add required fields for metrics processing
                job_data['job_id'] = job_id
                job_data['utility'] = job_data.get('metadata', {}).get('utility', 'Unknown')
                job_data['status'] = job_data.get('status', 'Unknown')
                job_data['previous_status'] = job_data.get('previous_status')
                job_data['start_time'] = datetime.strptime(job_data.get('start_date', '2024-01-01'), '%Y-%m-%d')
                if job_data.get('completion_date'):
                    job_data['completion_time'] = datetime.strptime(job_data['completion_date'], '%Y-%m-%d')
                test_jobs.append(job_data)
        return test_jobs
    except Exception as e:
        logger.error(f"Error getting test jobs: {str(e)}")
        return []

if __name__ == "__main__":
    generate_weekly_report() 