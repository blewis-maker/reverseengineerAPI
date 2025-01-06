import os
import logging
import sys
from datetime import datetime, timedelta
from weekly_reporter import (
    generate_weekly_report,
    WeeklyMetrics,
    get_weekly_jobs
)
from main import (
    TEST_JOB_IDS,
    getJobData,
    extractNodes,
    getUserList,
    CONFIG
)
from weekly_excel_generator import WeeklyReportGenerator
from burndown_calculator import BurndownCalculator
import traceback

def test_job_data_collection():
    """Test collection of weekly job data"""
    try:
        logging.info("Testing weekly job data collection...")
        
        # Get user list for mapping IDs to names
        user_map = getUserList()
        logging.info(f"Retrieved {len(user_map)} users")
        
        # Get test jobs
        logging.info(f"Using {len(TEST_JOB_IDS)} test jobs")
        
        # Test processing of each job
        for index, job_id in enumerate(TEST_JOB_IDS, 1):
            logging.info(f"\nProcessing job {index}/{len(TEST_JOB_IDS)}: {job_id}")
            
            # Get job data
            job_data = getJobData(job_id)
            if not job_data:
                logging.error(f"Failed to get data for job {job_id}")
                continue
                
            # Log key job information
            metadata = job_data.get('metadata', {})
            logging.info(f"Job Name: {metadata.get('name', 'Unknown')}")
            logging.info(f"Job Status: {job_data.get('status')}")
            logging.info(f"Utility: {metadata.get('utility', 'Unknown')}")
            logging.info(f"Number of nodes: {len(job_data.get('nodes', {}))}")
            
            # Test node extraction
            nodes = extractNodes(job_data, metadata.get('name', ''), job_id, user_map)
            logging.info(f"Extracted {len(nodes)} nodes")
            
            # Count important metrics using standardized field completion status
            field_complete = len([n for n in nodes if n.get('fldcompl') == 'yes'])
            annotation_complete = len([n for n in nodes if n.get('done')])
            
            logging.info(f"Field Complete: {field_complete}")
            logging.info(f"Annotation Complete: {annotation_complete}")
            
        return True
    except Exception as e:
        logging.error(f"Error testing job data collection: {str(e)}")
        return False

def test_metrics_collection():
    """Test collection and organization of metrics using real API data"""
    try:
        logging.info("\nTesting metrics collection with real API data...")
        
        # Set up date range - Sunday to Sunday
        end_date = datetime.now()
        # Adjust end_date to next Sunday if not already Sunday
        days_until_sunday = (6 - end_date.weekday()) % 7
        end_date = end_date + timedelta(days=days_until_sunday)
        end_date = end_date.replace(hour=8, minute=0, second=0, microsecond=0)  # 8:00 AM
        
        # Start date is previous Sunday
        start_date = end_date - timedelta(days=7)
        
        logging.info(f"Getting jobs updated between {start_date.strftime('%m/%d/%y')} and {end_date.strftime('%m/%d/%y')}")
        
        # Get real jobs from API
        logging.info("Making API call to get updated jobs...")
        jobs = getJobList()  # Use the real API call instead of test jobs
        
        if not jobs:
            logging.error("No jobs found from API")
            return False
            
        logging.info(f"Retrieved {len(jobs)} jobs from API")
        
        # Initialize metrics and burndown calculator
        metrics = WeeklyMetrics()
        burndown = BurndownCalculator(start_date=start_date, end_date=end_date)
        
        # Set standard run rates
        burndown.back_office_capacity = 100/7  # 100 poles per week per back office user
        burndown.field_capacity = 80/7         # 80 poles per week per field user
        
        # Get user list for mapping IDs to names
        user_map = getUserList()
        logging.info(f"Retrieved {len(user_map)} users for mapping")
        
        # Process jobs and collect metrics
        for index, job in enumerate(jobs, 1):
            job_id = job['id']
            logging.info(f"\nProcessing job {index}/{len(jobs)}: {job['name']} (ID: {job_id})")
            
            job_data = getJobData(job_id)
            if not job_data:
                logging.error(f"Failed to get data for job {job_id}")
                continue
            
            # Log job metadata for debugging
            metadata = job_data.get('metadata', {})
            logging.info(f"Job Status: {job_data.get('status')}")
            logging.info(f"Utility: {metadata.get('utility', 'Unknown')}")
            logging.info(f"Number of nodes: {len(job_data.get('nodes', {}))}")
            
            # Extract nodes with detailed logging
            nodes = extractNodes(job_data, metadata.get('name', ''), job_id, user_map)
            if not nodes:
                logging.error(f"No nodes extracted for job {job_id}")
                continue
                
            logging.info(f"Extracted {len(nodes)} nodes")
            
            # Log field completion status distribution
            field_complete_count = len([n for n in nodes if n.get('fldcompl') == 'yes'])
            field_incomplete_count = len([n for n in nodes if n.get('fldcompl') == 'no'])
            logging.info(f"Field Complete: {field_complete_count}")
            logging.info(f"Field Incomplete: {field_incomplete_count}")
            
            # Update metrics
            metrics.update_job_metrics(job_data, job_id, nodes)
            burndown.update_job_metrics(job_data)
            
        # Generate report in the reports/weekly directory
        output_path = os.path.join('reports', 'weekly', f"weekly_report_{end_date.strftime('%Y%m%d')}.xlsx")
        
        # Generate report using WeeklyReportGenerator
        report_generator = WeeklyReportGenerator(metrics, end_date)
        success = report_generator.generate_report(output_path)
        
        if success:
            logging.info(f"Weekly report generated successfully at {output_path}")
            return True
        else:
            logging.error("Failed to generate report")
            return False
            
    except Exception as e:
        logging.error(f"Error testing metrics collection: {str(e)}")
        logging.error(f"Stack trace: {traceback.format_exc()}")
        return False

def test_weekly_report():
    """Test the complete weekly report generation process"""
    try:
        # Set up test dates
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # Initialize metrics and burndown calculator
        metrics = WeeklyMetrics()
        burndown = BurndownCalculator(
            start_date=start_date,
            end_date=end_date,
            total_nodes=0
        )
        
        # Set capacity metrics
        burndown.back_office_capacity = 20  # Each back office user can process 20 poles per day
        burndown.field_capacity = 15  # Each field user can process 15 poles per day
        
        # Use real test jobs from main.py
        TEST_JOB_IDS = [
            "-O-nlOLQbPIYhHwJCPDN",
            "-Nvs8uA2MHZB5NdTK2_p",
            "-O4RHaixdJmN3lqi0Q3m",
            "-O7_Gr-6exIhw0vtfVga"
        ]
        
        # Get user list for mapping IDs to names
        user_map = getUserList()
        
        # Process each job
        for job_id in TEST_JOB_IDS:
            job_data = getJobData(job_id)
            if not job_data:
                continue
                
            # Extract nodes
            nodes = extractNodes(job_data, job_data.get('metadata', {}).get('name', ''), job_id, user_map)
            
            # Update metrics
            metrics.update_job_metrics(job_data, job_id, nodes)
            burndown.update_job_metrics(job_data)
            
        # Generate report
        report_date = end_date.strftime('%Y%m%d')
        output_path = f'reports/weekly/weekly_report_{report_date}.xlsx'
        
        # Create report generator
        generator = WeeklyReportGenerator(metrics, end_date)
        
        # Generate report
        success = generator.generate_report(output_path)
        if not success:
            logging.error("Failed to generate report")
            return False
            
        logging.info(f"Report generated at: {output_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error testing weekly report: {str(e)}")
        return False

def main():
    """Run all tests"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/test_weekly_report.log')
        ]
    )
    
    # Ensure required directories exist
    os.makedirs('logs', exist_ok=True)
    os.makedirs('reports/weekly', exist_ok=True)
    os.makedirs(CONFIG['WORKSPACE_PATH'], exist_ok=True)
    
    try:
        # Run tests
        logging.info("Starting weekly report tests...")
        
        # Test 1: Job Data Collection
        if not test_job_data_collection():
            logging.error("Job data collection test failed")
            return False
            
        # Test 2: Metrics Collection
        if not test_metrics_collection():
            logging.error("Metrics collection test failed")
            return False
            
        # Test 3: Weekly Report Generation
        if not test_weekly_report():
            logging.error("Weekly report generation test failed")
            return False
            
        logging.info("All tests completed successfully!")
        return True
        
    except Exception as e:
        logging.error(f"Error running tests: {str(e)}")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1) 