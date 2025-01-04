import os
import sys
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from pathlib import Path
import time
import random
from dotenv import load_dotenv

# Import functions from main.py
from main import (
    getJobData,
    extractNodes,
    extractConnections,
    extractAnchors,
    create_report,
    getUserList,
    update_sharepoint_spreadsheet,
    CONFIG,
    update_arcgis_features
)

# Load environment variables
load_dotenv()

# Ensure required directories exist in Docker environment
os.makedirs('/app/logs', exist_ok=True)

# Configure logging for Docker environment
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/incremental_update.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load API key from environment
API_KEY = os.getenv('KATAPULT_API_KEY')
if not API_KEY:
    raise ValueError("KATAPULT_API_KEY environment variable is not set")

def make_api_request(url, params, max_retries=3, base_delay=2):
    """Make API request with retry logic for rate limiting"""
    # Enforce rate limit between calls
    time.sleep(2)
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 429:  # Rate limited
                delay = base_delay * (2 ** attempt) + random.random()  # Exponential backoff with jitter
                logger.warning(f"Rate limited. Waiting {delay:.2f} seconds before retry {attempt + 1}/{max_retries}")
                time.sleep(delay)
                continue
                
            return response
            
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = base_delay * (2 ** attempt) + random.random()
            logger.warning(f"Request failed. Waiting {delay:.2f} seconds before retry {attempt + 1}/{max_retries}")
            time.sleep(delay)
    
    return None

def get_updated_jobs(hours=24):
    """Get list of jobs updated in the last specified hours using Katapult API"""
    try:
        # Calculate the date range
        to_date = datetime.now()
        from_date = to_date - timedelta(hours=hours)
        
        # Format dates for Katapult API
        to_date_str = to_date.strftime('%m/%d/%y')
        from_date_str = from_date.strftime('%m/%d/%y')
        
        # Katapult API endpoint for updated jobs
        url = "https://katapultpro.com/api/v2/updatedjobslist"
        params = {
            'fromDate': from_date_str,
            'toDate': to_date_str,
            'useToday': 'true',
            'api_key': API_KEY
        }
        
        logger.info(f"Requesting jobs updated between {from_date_str} and {to_date_str}")
        response = make_api_request(url, params)
        
        if not response:
            logger.error("Failed to get updated jobs after retries")
            return []
            
        if response.status_code == 401:
            logger.error("Authentication failed. Please check API key.")
            return []
        elif response.status_code != 200:
            logger.error(f"Failed to get updated jobs. Status code: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return []
            
        jobs = response.json()
        logger.info(f"Retrieved {len(jobs)} updated jobs")
        return jobs
        
    except Exception as e:
        logger.error(f"Error getting updated jobs: {str(e)}")
        return []

def process_daily_update():
    """Process daily updates for jobs"""
    try:
        # Get user list for mapping IDs to names
        user_map = getUserList()
        
        # Get list of jobs to process
        jobs_to_process = get_updated_jobs()
        
        if not jobs_to_process:
            logging.info("No jobs found for processing")
            return
        
        all_nodes = []
        all_connections = []
        all_anchors = []
        jobs_summary = []
        
        # Process each job
        for job_id in jobs_to_process:
            logging.info(f"Processing job {job_id}")
            
            # Get job data
            job_data = getJobData(job_id)
            if not job_data:
                logging.warning(f"No data found for job {job_id}")
                continue
            
            # Extract job name and metadata
            job_name = job_data.get('metadata', {}).get('name', f"Job {job_id}")
            
            # Extract nodes, connections, and anchors
            nodes_data = extractNodes(job_data, job_name, job_id)
            if nodes_data:
                all_nodes.extend(nodes_data)
                
                # Extract connections if we have valid nodes
                connections = job_data.get('connections', {})
                nodes = {node['node_id']: node for node in nodes_data}
                connections_data = extractConnections(connections, nodes, job_data)
                if connections_data:
                    all_connections.extend(connections_data)
                
                # Extract anchors
                anchors = extractAnchors(job_data, job_name, job_id)
                if anchors:
                    all_anchors.extend(anchors)
            
            logging.info(f"Completed processing job {job_id}")
        
        if all_nodes or all_connections or all_anchors:
            # Update ArcGIS feature services
            logging.info("Updating ArcGIS feature services...")
            arcgis_success = update_arcgis_features(all_nodes, all_connections, all_anchors)
            if arcgis_success:
                logging.info("ArcGIS feature services updated successfully")
            else:
                logging.error("Failed to update ArcGIS feature services")
            
            # Save to shapefiles and update SharePoint
            workspace_path = CONFIG['WORKSPACE_PATH']
            logging.info("Saving data to shapefiles...")
            saveToShapefiles(all_nodes, all_connections, all_anchors, workspace_path)
        else:
            logging.info("No data extracted for any job. Nothing to save.")
        
        return True
        
    except Exception as e:
        logging.error(f"Error in process_daily_update: {str(e)}")
        return False 