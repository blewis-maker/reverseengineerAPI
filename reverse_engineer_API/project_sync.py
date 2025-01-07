import os
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List
import requests
from database.db import db
from dotenv import load_dotenv
import msal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProjectSync:
    def __init__(self):
        load_dotenv()
        self.client_id = os.getenv('AZURE_CLIENT_ID')
        self.client_secret = os.getenv('AZURE_CLIENT_SECRET')
        self.tenant_id = os.getenv('AZURE_TENANT_ID')
        self.sharepoint_site = "GISEngineeringTeam"
        self.file_path = "Design Job Tracking.xlsx"

    def get_access_token(self) -> str:
        """Get Microsoft Graph API access token."""
        try:
            authority = f"https://login.microsoftonline.com/{self.tenant_id}"
            app = msal.ConfidentialClientApplication(
                self.client_id,
                authority=authority,
                client_credential=self.client_secret
            )

            scopes = ['https://graph.microsoft.com/.default']
            result = app.acquire_token_silent(scopes, account=None)
            if not result:
                result = app.acquire_token_for_client(scopes)

            if 'access_token' in result:
                return result['access_token']
            else:
                logger.error(f"Error getting access token: {result.get('error_description')}")
                return None
        except Exception as e:
            logger.error(f"Error in get_access_token: {str(e)}")
            return None

    def get_design_tracking_data(self) -> pd.DataFrame:
        """Fetch Design Job Tracking spreadsheet from SharePoint."""
        try:
            access_token = self.get_access_token()
            if not access_token:
                return None

            # Get site ID
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            }
            site_response = requests.get(
                f'https://graph.microsoft.com/v1.0/sites/deeplydigital.sharepoint.com:/sites/{self.sharepoint_site}',
                headers=headers
            )
            if site_response.status_code != 200:
                logger.error(f"Failed to get site. Status code: {site_response.status_code}")
                return None

            site_id = site_response.json()['id']

            # Get drive ID
            drives_response = requests.get(
                f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives',
                headers=headers
            )
            if drives_response.status_code != 200:
                logger.error("Failed to get drives")
                return None

            # Find Documents drive
            documents_drive = None
            for drive in drives_response.json()['value']:
                if drive['name'] == 'Documents':
                    documents_drive = drive
                    break

            if not documents_drive:
                logger.error("Could not find Documents drive")
                return None

            drive_id = documents_drive['id']

            # Get file content
            file_response = requests.get(
                f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{self.file_path}:/content',
                headers=headers
            )
            if file_response.status_code != 200:
                logger.error(f"Failed to get file. Status code: {file_response.status_code}")
                return None

            # Read Excel content
            df = pd.read_excel(file_response.content)
            return df

        except Exception as e:
            logger.error(f"Error getting Design Job Tracking data: {str(e)}")
            return None

    def sync_projects(self):
        """Sync projects from Design Job Tracking to database."""
        try:
            # Get Design Job Tracking data
            df = self.get_design_tracking_data()
            if df is None:
                return False

            # Process each row
            with db.get_cursor() as cursor:
                for _, row in df.iterrows():
                    project_id = row['Zone Name']  # This is our project_id
                    market = row['Market']
                    approval_phase = row['Approval Phase']
                    approval_status = row['Approval Status']
                    hld_date = row['HLD to be Completed']
                    designer = row['Designer Assignment']
                    aerial_eng_date = row['Aerial Eng to be Completed']
                    osp_assignment = row['OSP Assignment']

                    # Convert dates to proper format
                    if pd.notna(hld_date):
                        hld_date = pd.to_datetime(hld_date).date()
                    if pd.notna(aerial_eng_date):
                        aerial_eng_date = pd.to_datetime(aerial_eng_date).date()

                    # Update or insert project
                    cursor.execute("""
                        INSERT INTO projects (
                            project_id, name, utility, target_date, status
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (project_id) 
                        DO UPDATE SET
                            name = EXCLUDED.name,
                            utility = EXCLUDED.utility,
                            target_date = EXCLUDED.target_date,
                            status = EXCLUDED.status,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        project_id,
                        f"{market} - {project_id}",
                        market,
                        aerial_eng_date,
                        approval_status
                    ))

                    # Update project resources
                    if pd.notna(designer):
                        cursor.execute("""
                            INSERT INTO project_resources (project_id, user_id, role)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (project_id, user_id, role) DO NOTHING
                        """, (project_id, designer, 'back_office'))

                    if pd.notna(osp_assignment):
                        cursor.execute("""
                            INSERT INTO project_resources (project_id, user_id, role)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (project_id, user_id, role) DO NOTHING
                        """, (project_id, osp_assignment, 'field'))

            logger.info("Project sync completed successfully")
            return True

        except Exception as e:
            logger.error(f"Error syncing projects: {str(e)}")
            return False

    def update_burndown_metrics(self):
        """Update burndown metrics for all projects."""
        try:
            with db.get_cursor() as cursor:
                # Get all projects
                cursor.execute("SELECT project_id FROM projects")
                projects = cursor.fetchall()

                for project in projects:
                    project_id = project['project_id']

                    # Get job metrics for this project
                    cursor.execute("""
                        SELECT 
                            COUNT(DISTINCT node_id) as total_poles,
                            COUNT(DISTINCT CASE WHEN field_completed THEN node_id END) as field_complete,
                            COUNT(DISTINCT CASE WHEN back_office_completed THEN node_id END) as back_office_complete
                        FROM pole_metrics pm
                        JOIN job_metrics jm ON pm.job_id = jm.job_id
                        WHERE jm.metadata->>'project' = %s
                    """, (project_id,))
                    
                    metrics = cursor.fetchone()
                    
                    # Calculate run rate (poles completed per week over last 30 days)
                    cursor.execute("""
                        WITH daily_completion AS (
                            SELECT 
                                DATE_TRUNC('day', timestamp) as day,
                                COUNT(DISTINCT node_id) as completed_poles
                            FROM pole_metrics pm
                            JOIN job_metrics jm ON pm.job_id = jm.job_id
                            WHERE jm.metadata->>'project' = %s
                            AND timestamp >= CURRENT_DATE - INTERVAL '30 days'
                            AND (field_completed OR back_office_completed)
                            GROUP BY DATE_TRUNC('day', timestamp)
                        )
                        SELECT AVG(completed_poles) * 7 as weekly_rate
                        FROM daily_completion
                    """, (project_id,))
                    
                    run_rate = cursor.fetchone()['weekly_rate'] or 0

                    # Calculate estimated completion date
                    remaining_poles = metrics['total_poles'] - metrics['back_office_complete']
                    if run_rate > 0:
                        weeks_to_complete = remaining_poles / run_rate
                        estimated_completion = datetime.now() + pd.Timedelta(weeks=weeks_to_complete)
                    else:
                        estimated_completion = None

                    # Insert burndown metrics
                    cursor.execute("""
                        INSERT INTO burndown_metrics (
                            entity_type, entity_id, total_poles, 
                            field_complete, back_office_complete,
                            run_rate, estimated_completion_date, timestamp
                        ) VALUES (
                            'project', %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                        )
                    """, (
                        project_id,
                        metrics['total_poles'],
                        metrics['field_complete'],
                        metrics['back_office_complete'],
                        run_rate,
                        estimated_completion
                    ))

            logger.info("Burndown metrics updated successfully")
            return True

        except Exception as e:
            logger.error(f"Error updating burndown metrics: {str(e)}")
            return False

if __name__ == "__main__":
    sync = ProjectSync()
    if sync.sync_projects():
        sync.update_burndown_metrics() 