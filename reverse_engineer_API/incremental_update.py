import os
import sys
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from openpyxl import Workbook
from openpyxl.chart import BarChart, LineChart, Reference
from openpyxl.drawing.image import Image
from openpyxl.utils.dataframe import dataframe_to_rows

# Import shared utilities from main.py
from main import (
    getJobData,
    extractNodes,
    extractConnections,
    extractAnchors,
    update_sharepoint_spreadsheet,
    send_email_notification
)

# Ensure required directories exist
os.makedirs('/app/logs', exist_ok=True)
os.makedirs('/app/metrics', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/incremental_update.log'),
        logging.StreamHandler(sys.stdout)  # Also log to console for Docker logs
    ]
)
logger = logging.getLogger(__name__)

class JobUpdateTracker:
    def __init__(self):
        self.tracker_file = "/app/metrics/job_update_tracker.json"
        self.load_tracker()

    def load_tracker(self):
        """Load the last update timestamp and metrics from tracker file"""
        if os.path.exists(self.tracker_file):
            with open(self.tracker_file, 'r') as f:
                self.tracker_data = json.load(f)
        else:
            self.tracker_data = {
                'last_daily_update': None,
                'last_weekly_update': None,
                'weekly_metrics': {}
            }

    def save_tracker(self):
        """Save current tracker state to file"""
        with open(self.tracker_file, 'w') as f:
            json.dump(self.tracker_data, f, indent=2)

    def get_last_update_time(self, update_type='daily'):
        """Get the timestamp of last update for given type"""
        return self.tracker_data.get(f'last_{update_type}_update')

    def update_timestamp(self, update_type='daily'):
        """Update the timestamp for given update type"""
        self.tracker_data[f'last_{update_type}_update'] = datetime.utcnow().isoformat()
        self.save_tracker()

def get_modified_jobs(since_timestamp=None):
    """Get list of jobs modified since the given timestamp"""
    try:
        # Get all jobs
        all_jobs = getJobList()
        
        if not since_timestamp:
            return all_jobs
            
        # Filter jobs modified since timestamp
        modified_jobs = []
        for job in all_jobs:
            job_data = getJobData(job['id'])
            if job_data and job_data.get('metadata', {}).get('last_modified'):
                last_modified = datetime.fromisoformat(job_data['metadata']['last_modified'])
                if last_modified > since_timestamp:
                    modified_jobs.append(job)
                    
        return modified_jobs
        
    except Exception as e:
        logger.error(f"Error getting modified jobs: {str(e)}")
        return []

def process_jobs(jobs):
    """Process a list of jobs and return metrics"""
    metrics = {
        'total_jobs': len(jobs),
        'total_nodes': 0,
        'total_connections': 0,
        'total_anchors': 0,
        'jobs_processed': []
    }
    
    all_nodes = []
    all_connections = []
    all_anchors = []
    jobs_summary = []
    
    for job in jobs:
        try:
            job_data = getJobData(job['id'])
            if not job_data:
                continue
                
            # Extract data
            nodes = extractNodes(job_data)
            connections = extractConnections(job_data)
            anchors = extractAnchors(job_data)
            
            # Update metrics
            metrics['total_nodes'] += len(nodes)
            metrics['total_connections'] += len(connections)
            metrics['total_anchors'] += len(anchors)
            
            # Add to collections
            all_nodes.extend(nodes)
            all_connections.extend(connections)
            all_anchors.extend(anchors)
            
            # Add job summary
            job_summary = {
                'job_id': job['id'],
                'job_name': job['name'],
                'nodes': len(nodes),
                'connections': len(connections),
                'anchors': len(anchors)
            }
            jobs_summary.append(job_summary)
            metrics['jobs_processed'].append(job_summary)
            
        except Exception as e:
            logger.error(f"Error processing job {job['id']}: {str(e)}")
            continue
            
    # Create and update report
    report_path = create_report(jobs_summary)
    if report_path:
        logger.info(f"Report created: {report_path}")
        
    return metrics

def daily_update():
    """Run daily update process"""
    logger.info("Starting daily update...")
    
    tracker = JobUpdateTracker()
    last_update = tracker.get_last_update_time('daily')
    
    if last_update:
        since_timestamp = datetime.fromisoformat(last_update)
    else:
        # If no previous update, process last 24 hours
        since_timestamp = datetime.utcnow() - timedelta(days=1)
        
    # Get modified jobs
    modified_jobs = get_modified_jobs(since_timestamp)
    logger.info(f"Found {len(modified_jobs)} modified jobs since {since_timestamp}")
    
    # Process jobs
    metrics = process_jobs(modified_jobs)
    logger.info(f"Processed {metrics['total_jobs']} jobs")
    
    # Update tracker
    tracker.update_timestamp('daily')
    
    return metrics

def weekly_update():
    """Run weekly update process"""
    logger.info("Starting weekly update...")
    
    tracker = JobUpdateTracker()
    last_update = tracker.get_last_update_time('weekly')
    
    if last_update:
        since_timestamp = datetime.fromisoformat(last_update)
    else:
        # If no previous update, process last 7 days
        since_timestamp = datetime.utcnow() - timedelta(days=7)
        
    # Get modified jobs
    modified_jobs = get_modified_jobs(since_timestamp)
    logger.info(f"Found {len(modified_jobs)} modified jobs since {since_timestamp}")
    
    # Process jobs
    metrics = process_jobs(modified_jobs)
    logger.info(f"Processed {metrics['total_jobs']} jobs")
    
    # Generate weekly report with charts
    try:
        generate_weekly_report(metrics)
    except Exception as e:
        logger.error(f"Error generating weekly report: {str(e)}")
    
    # Update tracker
    tracker.update_timestamp('weekly')
    
    return metrics

def generate_weekly_report(metrics):
    """Generate weekly report with charts and metrics"""
    logger.info("Generating weekly report...")
    
    # Create report directory if it doesn't exist
    report_dir = '/app/reports'
    os.makedirs(report_dir, exist_ok=True)
    
    # Generate timestamp for report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    report_path = os.path.join(report_dir, f'weekly_report_{timestamp}.xlsx')
    
    try:
        # Create Excel workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Weekly Metrics"
        
        # Add summary metrics
        ws['A1'] = "Weekly Status Report"
        ws['A2'] = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ws['A4'] = "Summary Metrics"
        ws['A5'] = "Total Jobs Processed"
        ws['B5'] = metrics['total_jobs']
        ws['A6'] = "Total Nodes"
        ws['B6'] = metrics['total_nodes']
        ws['A7'] = "Total Connections"
        ws['B7'] = metrics['total_connections']
        ws['A8'] = "Total Anchors"
        ws['B8'] = metrics['total_anchors']
        
        # Add job details
        ws['A10'] = "Job Details"
        headers = ['Job ID', 'Job Name', 'Nodes', 'Connections', 'Anchors']
        for col, header in enumerate(headers, 1):
            ws.cell(row=11, column=col, value=header)
            
        row = 12
        for job in metrics['jobs_processed']:
            ws.cell(row=row, column=1, value=job['job_id'])
            ws.cell(row=row, column=2, value=job['job_name'])
            ws.cell(row=row, column=3, value=job['nodes'])
            ws.cell(row=row, column=4, value=job['connections'])
            ws.cell(row=row, column=5, value=job['anchors'])
            row += 1
            
        # Create charts
        create_weekly_charts(wb, metrics)
        
        # Save workbook
        wb.save(report_path)
        logger.info(f"Weekly report saved to: {report_path}")
        
        # Send email notification
        send_email_notification(
            subject="Weekly Status Report Generated",
            body=f"The weekly status report has been generated and saved to: {report_path}",
            attachment_path=report_path
        )
        
    except Exception as e:
        logger.error(f"Error creating weekly report: {str(e)}")
        raise

def create_weekly_charts(wb, metrics):
    """Create charts for weekly report"""
    # Add a new worksheet for charts
    ws_charts = wb.create_sheet(title="Charts")
    
    # Create job metrics chart
    chart1 = BarChart()
    chart1.title = "Job Metrics Distribution"
    chart1.y_axis.title = "Count"
    chart1.x_axis.title = "Metric Type"
    
    # Add data for chart
    ws_charts['A1'] = "Metric"
    ws_charts['B1'] = "Count"
    metrics_data = [
        ("Nodes", metrics['total_nodes']),
        ("Connections", metrics['total_connections']),
        ("Anchors", metrics['total_anchors'])
    ]
    
    for row, (metric, count) in enumerate(metrics_data, 2):
        ws_charts.cell(row=row, column=1, value=metric)
        ws_charts.cell(row=row, column=2, value=count)
        
    data = Reference(ws_charts, min_col=2, min_row=1, max_row=4, max_col=2)
    cats = Reference(ws_charts, min_col=1, min_row=2, max_row=4)
    chart1.add_data(data, titles_from_data=True)
    chart1.set_categories(cats)
    
    # Add chart to worksheet
    ws_charts.add_chart(chart1, "A6") 