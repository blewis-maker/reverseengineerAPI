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
import traceback

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

def get_weekly_jobs(start_date: datetime, end_date: datetime, test_mode: bool = False) -> List[Dict]:
    """Get jobs updated within the specified date range using the updatedjobslist endpoint."""
    try:
        if test_mode:
            return TEST_JOB_IDS
            
        all_updated_jobs = []
        current_from_date = start_date
        
        while True:
            # Set up API request
            conn = http.client.HTTPSConnection("katapultpro.com", timeout=30)
            
            # Format dates for API (MM/DD/YY format)
            from_date = current_from_date.strftime('%-m/%-d/%y')
            to_date = end_date.strftime('%-m/%-d/%y')
            
            # Construct URL with parameters
            url_path = f"/api/v2/updatedjobslist?fromDate={from_date}&toDate={to_date}&useToday=false&api_key={CONFIG['API_KEY']}"
            logging.info(f"Requesting updated jobs from {from_date} to {to_date}")
            
            try:
                # Add 2-second delay before each API call
                time.sleep(2)
                
                conn.request("GET", url_path)
                res = conn.getresponse()
                data = res.read().decode("utf-8")
                
                if res.status == 429:  # Rate limit exceeded
                    logging.warning("Rate limit exceeded, waiting 2 seconds before retry...")
                    time.sleep(2)
                    continue
                    
                if res.status != 200:
                    logging.error(f"API request failed with status {res.status}: {data}")
                    break
                
                # Parse response
                updated_jobs = json.loads(data)
                if not updated_jobs:
                    logging.info("No more jobs found in date range")
                    break
                    
                logging.info(f"Retrieved {len(updated_jobs)} jobs")
                all_updated_jobs.extend(updated_jobs)
                
                # Check if we need to paginate (got 200 results)
                if len(updated_jobs) == 200:
                    # Use the last job's timestamp as the new from_date
                    last_updated = datetime.fromisoformat(updated_jobs[-1]['last_updated'].replace('Z', '+00:00'))
                    current_from_date = last_updated
                    logging.info(f"Retrieved maximum results, continuing from {current_from_date}")
                else:
                    break
                    
            except Exception as e:
                logging.error(f"Error processing batch: {str(e)}")
                break
            finally:
                conn.close()
        
        logging.info(f"Total jobs found: {len(all_updated_jobs)}")
        
        # Convert the response to the format expected by the rest of the code
        formatted_jobs = []
        for job in all_updated_jobs:
            formatted_jobs.append({
                'id': job['jobId'],
                'name': f"Job {job['jobId']}",  # We'll get the actual name when we fetch full job data
                'last_updated': job['last_updated']
            })
        
        return formatted_jobs
        
    except Exception as e:
        logging.error(f"Error getting weekly jobs: {str(e)}")
        logging.error(f"Stack trace: {traceback.format_exc()}")
        return []

class WeeklyMetrics:
    def __init__(self):
        # Weekly metrics (updated weekly)
        self.weekly_metrics = {
            'user_production': {
                'field': {},  # user_id -> {completed_poles: [], utilities: set(), dates: []}
                'back_office': {
                    'annotation': {},
                    'sent_to_pe': {'jobs': [], 'users': {}},
                    'delivery': {'jobs': [], 'users': {}},
                    'emr': {'jobs': [], 'users': {}},
                    'approved': {'jobs': [], 'users': {}}
                }
            },
            'status_changes': {
                'field_collection': [],
                'annotation': [],
                'sent_to_pe': [],
                'delivery': [],
                'emr': [],
                'approved': []
            },
            'previous_week_metrics': None  # Will store last week's metrics for comparison
        }

        # Real-time metrics (updated with each run)
        self.realtime_metrics = {
            'burndown': {
                'by_utility': {},  # utility -> {total_poles, completed_poles, run_rate, trend}
                'by_project': {},  # project -> {total_poles, completed_poles, run_rate, trend}
                'backlog': {
                    'field': {'total_poles': 0, 'jobs': set(), 'utilities': set()},
                    'back_office': {'total_poles': 0, 'jobs': set(), 'utilities': set()},
                    'approve_construction': {'total_poles': 0, 'jobs': set(), 'utilities': set()}
                }
            },
            'schedule': {
                'projects': {},  # project_id -> {total_poles, completed_poles, resources, dependencies}
                'resources': {
                    'field': {},  # user_id -> {capacity, assigned_jobs}
                    'back_office': {}  # user_id -> {capacity, assigned_jobs}
                }
            }
        }

    def update_burndown_metrics(self, job_data, timestamp):
        """Update real-time burndown metrics"""
        utility = job_data.get('metadata', {}).get('utility', 'Unknown')
        project = job_data.get('metadata', {}).get('project', 'Unknown')
        total_poles = len([n for n in job_data.get('nodes', {}).values() if self._is_pole_node(n)])
        completed_poles = len([n for n in job_data.get('nodes', {}).values() if self._get_field_completed(n)])

        # Update utility metrics
        if utility not in self.realtime_metrics['burndown']['by_utility']:
            self.realtime_metrics['burndown']['by_utility'][utility] = {
                'total_poles': 0,
                'completed_poles': 0,
                'run_rate': 0,
                'trend': [],
                'history': []
            }

        utility_metrics = self.realtime_metrics['burndown']['by_utility'][utility]
        utility_metrics['total_poles'] += total_poles
        utility_metrics['completed_poles'] += completed_poles
        utility_metrics['history'].append({
            'timestamp': timestamp,
            'completed_poles': completed_poles
        })
        
        # Calculate run rate based on last 7 days
        recent_history = [h for h in utility_metrics['history'] 
                         if (timestamp - h['timestamp']).days <= 7]
        if len(recent_history) >= 2:
            poles_completed = recent_history[-1]['completed_poles'] - recent_history[0]['completed_poles']
            days = (recent_history[-1]['timestamp'] - recent_history[0]['timestamp']).days
            utility_metrics['run_rate'] = (poles_completed / days) * 7 if days > 0 else 0

        # Similar update for project metrics
        if project not in self.realtime_metrics['burndown']['by_project']:
            self.realtime_metrics['burndown']['by_project'][project] = {
                'total_poles': 0,
                'completed_poles': 0,
                'run_rate': 0,
                'trend': [],
                'resources': {'field': set(), 'back_office': set()},
                'dependencies': set(),
                'history': []
            }

        project_metrics = self.realtime_metrics['burndown']['by_project'][project]
        project_metrics['total_poles'] += total_poles
        project_metrics['completed_poles'] += completed_poles
        project_metrics['history'].append({
            'timestamp': timestamp,
            'completed_poles': completed_poles
        })

    def calculate_schedule(self):
        """Calculate project schedules based on current run rates and resources"""
        for project_id, project in self.realtime_metrics['schedule']['projects'].items():
            # Calculate resource capacity
            field_capacity = sum(
                self.realtime_metrics['schedule']['resources']['field'][user]['capacity']
                for user in project['resources']['field']
            )
            back_office_capacity = sum(
                self.realtime_metrics['schedule']['resources']['back_office'][user]['capacity']
                for user in project['resources']['back_office']
            )

            # Calculate remaining work
            remaining_poles = project['total_poles'] - project['completed_poles']
            
            # Calculate estimated completion time
            if field_capacity > 0 and back_office_capacity > 0:
                field_time = remaining_poles / field_capacity
                back_office_time = remaining_poles / back_office_capacity
                project['estimated_completion'] = max(field_time, back_office_time)
            else:
                project['estimated_completion'] = None

            # Update dependencies
            for dep_project in project['dependencies']:
                if dep_project in self.realtime_metrics['schedule']['projects']:
                    dep_completion = self.realtime_metrics['schedule']['projects'][dep_project]['estimated_completion']
                    if dep_completion:
                        project['estimated_completion'] = max(
                            project['estimated_completion'],
                            dep_completion
                        ) if project['estimated_completion'] else dep_completion

    def get_weekly_status(self):
        """Get formatted weekly status metrics."""
        # Format utility metrics
        utility_metrics = {}
        for utility, data in self.realtime_metrics['burndown']['by_utility'].items():
            if utility == 'Unknown':
                continue
            utility_metrics[utility] = {
                'total_poles': data['total_poles'],
                'field_complete': data['completed_poles'],
                'back_office_complete': data['completed_poles'],  # For now, using same value
                'run_rate': data['run_rate'],
                'estimated_completion': data['estimated_completion'] if 'estimated_completion' in data else None
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
        for user_id, data in self.weekly_metrics['user_production']['field'].items():
            user_production['field'].append({
                'user': user_id,
                'completed_poles': len(data['completed_poles']),
                'utilities': list(data['utilities']),
                'jobs': [{'job_id': job_id, 'pole_count': pole_count} 
                        for job_id, pole_count in data.get('jobs', {}).items()]
            })
            
        # Process back office users
        for category in ['annotation', 'sent_to_pe', 'delivery', 'emr', 'approved']:
            if category == 'annotation':
                for user_id, data in self.weekly_metrics['user_production']['back_office']['annotation'].items():
                    user_production[category].append({
                        'user': user_id,
                        'completed_poles': len(data['completed_poles']),
                        'utilities': list(data['utilities']),
                        'jobs': data.get('jobs', [])  # Already in correct format
                    })
            else:
                for user_id, data in self.weekly_metrics['user_production']['back_office'][category]['users'].items():
                    user_production[category].append({
                        'user': user_id,
                        'completed_poles': data['pole_count'],
                        'utilities': list(data['utilities']),
                        'jobs': data.get('jobs', [])  # Already in correct format
                    })
                    
        # Format status changes
        status_changes = {}
        for status, changes in self.weekly_metrics['status_changes'].items():
            total_poles = sum(change['pole_count'] for change in changes)
            total_jobs = len(changes)
            change_from_last_week = total_poles  # For now, just use total as change
            
            status_changes[status] = {
                'job_count': total_jobs,
                'pole_count': total_poles,
                'change_from_last_week': change_from_last_week
            }
            
        # Format burndown metrics
        burndown = {
            'master': {
                'total_poles': sum(data['total_poles'] for data in self.realtime_metrics['burndown']['by_utility'].values()),
                'field_complete': sum(data['completed_poles'] for data in self.realtime_metrics['burndown']['by_utility'].values()),
                'back_office_complete': sum(data['completed_poles'] for data in self.realtime_metrics['burndown']['by_utility'].values()),
                'run_rate': sum(data['run_rate'] for data in self.realtime_metrics['burndown']['by_utility'].values()),
                'estimated_completion_date': None  # Will be calculated based on overall progress
            },
            'by_utility': {
                utility: {
                    'total_poles': data['total_poles'],
                    'field_complete': data['completed_poles'],
                    'back_office_complete': data['completed_poles'],
                    'run_rate': data['run_rate'],
                    'estimated_completion': data.get('estimated_completion')
                }
                for utility, data in self.realtime_metrics['burndown']['by_utility'].items()
                if utility != 'Unknown'
            },
            'by_project': {
                project: {
                    'total_poles': data['total_poles'],
                    'field_complete': data['completed_poles'],
                    'back_office_complete': data['completed_poles'],
                    'run_rate': data['run_rate'],
                    'target_date': None,  # Will be set from project data
                    'estimated_completion': data.get('estimated_completion')
                }
                for project, data in self.realtime_metrics['burndown']['by_project'].items()
                if project != 'Unknown'
            }
        }
        
        # Calculate master burndown estimated completion date
        total_poles = burndown['master']['total_poles']
        completed_poles = burndown['master']['back_office_complete']
        run_rate = burndown['master']['run_rate']
        if total_poles > 0 and run_rate > 0:
            remaining_poles = total_poles - completed_poles
            days_to_completion = remaining_poles / (run_rate / 7)  # Convert weekly rate to daily
            burndown['master']['estimated_completion_date'] = datetime.now() + timedelta(days=days_to_completion)
            
        # Format schedule metrics
        schedule_metrics = {
            'projects': []
        }
        for project_id, data in self.realtime_metrics['schedule']['projects'].items():
            if project_id == 'Unknown':
                continue
            schedule_metrics['projects'].append({
                'project_id': project_id,
                'total_poles': data.get('total_poles', 0),
                'completed_poles': data.get('completed_poles', 0),
                'field_users': list(data.get('resources', {}).get('field', set())),
                'back_office_users': list(data.get('resources', {}).get('back_office', set())),
                'end_date': data.get('estimated_completion')
            })
            
        return {
            'utility_metrics': utility_metrics,
            'user_production': user_production,
            'status_changes': status_changes,
            'burndown': burndown,
            'schedule': schedule_metrics
        }

    def _load_previous_week_metrics(self, start_date, end_date):
        """Load metrics from previous week for comparison"""
        # TODO: Implement loading of previous week's metrics from database
        pass

    def update_job_metrics(self, job_data: Dict, job_id: str, nodes: List[Dict]):
        """Update metrics based on job data."""
        try:
            # Extract metadata and status information
            metadata = job_data.get('metadata', {})
            utility = metadata.get('utility', 'Unknown')
            project_id = metadata.get('project_id', 'Unknown')
            job_status = metadata.get('job_status', 'Unknown')
            status_date = parse_date(job_data.get('status_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            # For now, we don't have previous status tracking
            old_status = 'Unknown'  # We'll need to implement status change tracking
            
            assigned_users = set(job_data.get('assigned_users', []))
            total_poles = len([n for n in nodes if n.get('type') == 'pole'])
            
            # Debug logging
            logging.debug(f"Processing job {job_id}:")
            logging.debug(f"  Utility: {utility}")
            logging.debug(f"  Status: {job_status}")
            logging.debug(f"  Metadata: {json.dumps(metadata, indent=2)}")
            logging.debug(f"  Total Poles: {total_poles}")
            
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
            field_incomplete_count = sum(1 for node in nodes if not (
                node.get('attributes', {}).get('field_completed', {}).get('value') is True or
                node.get('fldcompl') == 'yes'
            ))
            
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
        """Update user production metrics based on node data."""
        # Ensure status_date is a datetime object
        if isinstance(status_date, str):
            status_date = parse_date(status_date)
        elif not isinstance(status_date, datetime):
            status_date = datetime.now()
        
        # Track field users (those who completed field work)
        field_completed_poles = 0
        field_users = set()  # Track unique field users for this job
        
        for node in nodes:
            # Use standardized fldcompl field
            if node.get('fldcompl') == 'yes':
                field_user = node.get('field_completed_by')
                if field_user:
                    field_users.add(field_user)
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
            for field_user in field_users:  # Only update users who worked on this job
                if job_id not in self.user_production['field'][field_user]['jobs']:
                    self.user_production['field'][field_user]['jobs'][job_id] = field_completed_poles

        # Track back office users (those who did annotation work)
        back_office_completed_poles = 0
        back_office_users = set()  # Track unique back office users for this job
        
        for node in nodes:
            if node.get('done', {}).get('button_added') or node.get('done', {}).get('-Imported') or node.get('done', {}).get('multi_added'):
                annotator = node.get('annotated_by')
                if annotator:
                    back_office_users.add(annotator)
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
                'estimated_completion': None,
                'utilities': set()  # Track actual utility names
            }
        self.burndown['by_utility'][utility]['field_completed'] += field_completed_poles
        self.burndown['by_utility'][utility]['back_office_completed'] += back_office_completed_poles
        self.burndown['by_utility'][utility]['utilities'].add(utility)  # Add utility to the set
            
        logger.debug(f"Updated user production metrics for job {job_id}")
        logger.debug(f"Field users: {self.user_production['field'].keys()}")
        logger.debug(f"Back office users: {[u for c in self.user_production['back_office'].values() for u in c.get('users', {}).keys()]}")

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

def run_weekly_report(test_mode: bool = False) -> bool:
    """
    Main entry point for weekly report generation.
    Can be used both for testing and production.
    """
    try:
        # Set up logging
        logging.info("\n=== Starting Weekly Report Generation ===\n")
        
        # Track statistics for final summary
        stats = {
            'total_jobs_found': 0,
            'jobs_processed': 0,
            'jobs_failed': 0,
            'total_nodes': 0,
            'field_complete': 0,
            'field_incomplete': 0,
            'utilities': set(),
            'status_counts': {},
            'processing_time': time.time()
        }
        
        # Calculate date range (Sunday to Sunday)
        end_date = datetime.now()
        days_until_sunday = (6 - end_date.weekday()) % 7
        end_date = end_date + timedelta(days=days_until_sunday)
        end_date = end_date.replace(hour=8, minute=0, second=0, microsecond=0)
        
        # Start date is previous Sunday
        start_date = end_date - timedelta(days=7)
        
        logging.info(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Get user mapping
        user_map = getUserList()
        if not user_map:
            logging.error("Failed to get user mapping")
            return False
        logging.info(f"Retrieved {len(user_map)} users for mapping")
        
        # Initialize metrics
        metrics = WeeklyMetrics()
        
        # Get jobs for the week
        jobs = get_weekly_jobs(start_date, end_date, test_mode)
        if not jobs:
            logging.error("No jobs found for the specified date range")
            return False
            
        stats['total_jobs_found'] = len(jobs)
        logging.info(f"Retrieved {len(jobs)} jobs to process")
        
        # Process each job
        for index, job in enumerate(jobs, 1):
            job_id = job['id']
            job_name = job['name']
            logging.info(f"\nProcessing job {index}/{len(jobs)}: {job_name} (ID: {job_id})")
            
            try:
                # Get full job data
                logging.debug(f"\nAttempting to retrieve data for job {job_id}")
                job_data = getJobData(job_id)
                if not job_data:
                    logging.error(f"Failed to get data for job {job_id}")
                    stats['jobs_failed'] += 1
                    continue
                
                # Log job metadata
                metadata = job_data.get('metadata', {})
                status = metadata.get('job_status', 'Unknown')
                utility = metadata.get('utility', 'Unknown')
                
                # Debug logging for job data
                logging.debug(f"Retrieved job data:")
                logging.debug(f"  Status: {status}")
                logging.debug(f"  Utility: {utility}")
                logging.debug(f"  Metadata: {json.dumps(metadata, indent=2)}")
                
                # Update statistics
                stats['status_counts'][status] = stats['status_counts'].get(status, 0) + 1
                stats['utilities'].add(utility)
                
                logging.info(f"Job Status: {status}")
                logging.info(f"Utility: {utility}")
                logging.info(f"Number of nodes: {len(job_data.get('nodes', {}))}")
                
                # Extract and process nodes
                nodes = extractNodes(job_data, metadata.get('name', ''), job_id, user_map)
                if not nodes:
                    logging.error(f"No nodes extracted for job {job_id}")
                    stats['jobs_failed'] += 1
                    continue
                
                # Update node statistics
                stats['total_nodes'] += len(nodes)
                field_complete = len([n for n in nodes if n.get('fldcompl') == 'yes'])
                field_incomplete = len([n for n in nodes if n.get('fldcompl') == 'no'])
                stats['field_complete'] += field_complete
                stats['field_incomplete'] += field_incomplete
                
                logging.info(f"Field Complete: {field_complete}")
                logging.info(f"Field Incomplete: {field_incomplete}")
                
                # Update metrics
                metrics.update_job_metrics(job_data, job_id, nodes)
                stats['jobs_processed'] += 1
                
            except Exception as e:
                logging.error(f"Error processing job {job_id}: {str(e)}")
                stats['jobs_failed'] += 1
                continue
        
        # Generate report
        output_dir = os.path.join('reports', 'weekly')
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"weekly_report_{end_date.strftime('%Y%m%d')}.xlsx")
        
        # Generate Excel report
        generator = WeeklyReportGenerator(metrics, end_date)
        success = generator.generate_report(output_path)
        
        # Calculate total processing time
        processing_time = time.time() - stats['processing_time']
        
        # Print detailed summary
        logging.info("\n=== Weekly Report Generation Summary ===")
        logging.info(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logging.info(f"\nJob Processing Statistics:")
        logging.info(f"Total Jobs Found: {stats['total_jobs_found']}")
        logging.info(f"Successfully Processed: {stats['jobs_processed']}")
        logging.info(f"Failed to Process: {stats['jobs_failed']}")
        
        logging.info(f"\nNode Statistics:")
        logging.info(f"Total Nodes Processed: {stats['total_nodes']}")
        logging.info(f"Field Complete: {stats['field_complete']}")
        logging.info(f"Field Incomplete: {stats['field_incomplete']}")
        field_complete_pct = (stats['field_complete'] / stats['total_nodes'] * 100) if stats['total_nodes'] > 0 else 0
        logging.info(f"Field Completion Rate: {field_complete_pct:.1f}%")
        
        logging.info(f"\nUtility Distribution:")
        for utility in sorted(stats['utilities']):
            logging.info(f"- {utility}")
            
        logging.info(f"\nJob Status Distribution:")
        for status, count in sorted(stats['status_counts'].items()):
            logging.info(f"- {status}: {count}")
            
        logging.info(f"\nProcessing Time: {processing_time:.2f} seconds")
        logging.info(f"Output File: {output_path}")
        logging.info("=====================================")
        
        if success:
            logging.info(f"\nWeekly report generated successfully")
            return True
        else:
            logging.error("Failed to generate report")
            return False
            
    except Exception as e:
        logging.error(f"Error generating weekly report: {str(e)}")
        logging.error(f"Stack trace: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    # When run directly, execute the weekly report
    success = run_weekly_report(test_mode=False)  # Set to True for testing
    if not success:
        logging.error("Weekly report generation failed")
        exit(1)
    logging.info("Weekly report generation completed successfully") 