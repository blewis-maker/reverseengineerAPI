import os
import sys
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from pathlib import Path
import openpyxl
from io import BytesIO
from openpyxl.utils.dataframe import dataframe_to_rows
from dotenv import load_dotenv
import time
import random

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

def get_job_data(job_id):
    """Get detailed job data using Katapult API"""
    try:
        url = f"https://katapultpro.com/api/v2/jobs/{job_id}"
        params = {
            'api_key': API_KEY
        }
        
        response = make_api_request(url, params)
        if not response:
            logger.error(f"Failed to get job data for {job_id} after retries")
            return None
            
        if response.status_code != 200:
            logger.error(f"Failed to get job data for {job_id}. Status code: {response.status_code}")
            return None
            
        job_data = response.json()
        logger.info(f"Retrieved data for job {job_id}")
        return job_data
        
    except Exception as e:
        logger.error(f"Error getting job data for {job_id}: {str(e)}")
        return None

def process_job_data(job_data):
    """Process job data and extract relevant information"""
    try:
        if not job_data:
            return None
            
        # Extract basic job info
        job_info = {
            'Job ID': job_data.get('id'),
            'Job Name': job_data.get('name'),
            'Status': job_data.get('metadata', {}).get('status', 'Unknown'),
            'Utility': job_data.get('metadata', {}).get('utility', 'Unknown'),
            'Assigned OSP': job_data.get('metadata', {}).get('assigned_osp', 'Unassigned'),
            'Conversation': job_data.get('metadata', {}).get('conversation', ''),
            'Project': job_data.get('metadata', {}).get('project', 'Unknown'),
            'Comments': job_data.get('metadata', {}).get('comments', ''),
            'Last Edit': job_data.get('metadata', {}).get('last_modified', datetime.now().isoformat())
        }
        
        return job_info
        
    except Exception as e:
        logger.error(f"Error processing job data: {str(e)}")
        return None

def update_sharepoint_tracker(job_data_list):
    """Update SharePoint Aerial Status Tracker with new job data"""
    try:
        # SharePoint file details
        site_url = os.getenv('SHAREPOINT_SITE_URL', 'deeplydigital.sharepoint.com:/sites/OSPIntegrationTestingSite')
        drive_path = os.getenv('SHAREPOINT_DRIVE_PATH', 'Documents')
        file_name = os.getenv('SHAREPOINT_FILE_NAME', 'Aerial_Status_Tracker.xlsx')
        
        # For testing, use a local copy
        test_file = 'test_data/Aerial_Status_Tracker.xlsx'
        os.makedirs('test_data', exist_ok=True)
        
        # If file doesn't exist, create a new one with basic structure
        if not os.path.exists(test_file):
            df = pd.DataFrame(columns=[
                'Job ID', 'Job Name', 'Status', 'Utility', 'Assigned OSP',
                'Conversation', 'Project', 'Comments', 'Last Edit'
            ])
            df.to_excel(test_file, index=False)
            logger.info(f"Created new tracker file at {test_file}")
        
        # Read existing spreadsheet
        existing_df = pd.read_excel(test_file)
        
        # Process each job
        for job_data in job_data_list:
            if not job_data:
                continue
                
            job_name = job_data['Job Name']
            
            # Check if job exists
            if job_name in existing_df['Job Name'].values:
                # Update existing row
                idx = existing_df[existing_df['Job Name'] == job_name].index[0]
                for col in job_data.keys():
                    if col in existing_df.columns:
                        existing_df.at[idx, col] = job_data[col]
            else:
                # Add new row
                new_df = pd.DataFrame([job_data])
                existing_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Sort by Job Name
        existing_df.sort_values('Job Name', inplace=True)
        
        # Save back to file
        existing_df.to_excel(test_file, index=False)
        logger.info(f"Successfully updated tracker at {test_file}")
        
        # TODO: Implement actual SharePoint upload once credentials are configured
        logger.warning("SharePoint upload not implemented yet - using local file for testing")
        
    except Exception as e:
        logger.error(f"Error updating SharePoint tracker: {str(e)}")
        raise

def process_daily_update():
    """Process daily updates for jobs modified in the last 24 hours"""
    try:
        # Get updated jobs
        updated_jobs = get_updated_jobs(hours=24)
        if not updated_jobs:
            logger.info("No jobs updated in the last 24 hours")
            return
            
        # Process each job
        processed_jobs = []
        for job in updated_jobs:
            job_data = get_job_data(job.get('jobId'))  # make_api_request already handles the 2-second delay
            if job_data:
                processed_data = process_job_data(job_data)
                if processed_data:
                    processed_jobs.append(processed_data)
        
        if processed_jobs:
            # Update SharePoint tracker
            update_sharepoint_tracker(processed_jobs)
            logger.info(f"Successfully processed {len(processed_jobs)} jobs")
        else:
            logger.info("No jobs to update in SharePoint")
            
    except Exception as e:
        logger.error(f"Error in daily update process: {str(e)}")
        raise 