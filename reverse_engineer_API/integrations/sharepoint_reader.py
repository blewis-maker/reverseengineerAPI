from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
import io
import logging
import os
import json
import requests
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

def initialize_graph_client():
    """Initialize Microsoft Graph client"""
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    
    if not all([client_id, client_secret, tenant_id]):
        raise ValueError("Azure credentials not found in environment variables")
    
    # Initialize MSAL client
    app = ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret
    )
    
    # Get token
    result = app.acquire_token_silent(["https://graph.microsoft.com/.default"], account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    
    if "access_token" not in result:
        raise ValueError("Could not acquire token")
        
    # Create session with token
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Bearer {result["access_token"]}',
        'Content-Type': 'application/json'
    })
    return session

class SharePointReader:
    def __init__(self):
        # Format: hostname,spsite_guid,tenant_guid
        self.site_url = "deeplydigital.sharepoint.com/sites/GISEngineeringTeam"
        self.drive_path = "Shared Documents"
        self.file_name = "Design Tracker.xlsx"
        self.graph_client = initialize_graph_client()

    def read_design_tracker(self) -> List[Dict]:
        """Read the Design Tracker spreadsheet from SharePoint"""
        try:
            # Get site ID
            logger.info("Getting site ID...")
            site_response = self.graph_client.get(f"https://graph.microsoft.com/v1.0/sites/{self.site_url}")
            if site_response.status_code != 200:
                raise ValueError(f"Failed to get site. Status code: {site_response.status_code}")
            
            site_id = site_response.json()['id']
            logger.info(f"Successfully got site ID: {site_id}")
            
            # Get drive ID
            logger.info("Getting drive ID...")
            drives_response = self.graph_client.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives")
            if drives_response.status_code != 200:
                raise ValueError("Failed to get drives")
            
            # Find the Documents drive
            documents_drive = None
            for drive in drives_response.json()['value']:
                if drive['name'] == 'Documents':
                    documents_drive = drive
                    break
            
            if not documents_drive:
                raise ValueError("Could not find Documents drive")
            
            drive_id = documents_drive['id']
            logger.info(f"Successfully got drive ID: {drive_id}")
            
            # Get the file
            file_path = f"{self.drive_path}/{self.file_name}"
            logger.info(f"Getting file: {file_path}")
            
            file_response = self.graph_client.get(
                f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{file_path}"
            )
            
            if file_response.status_code != 200:
                raise ValueError(f"Failed to get file. Status code: {file_response.status_code}")
            
            file_id = file_response.json()['id']
            
            # Download file content
            content_response = self.graph_client.get(
                f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}/content",
                headers={'Accept': 'application/octet-stream'}
            )
            
            if content_response.status_code != 200:
                raise ValueError(f"Failed to download file. Status code: {content_response.status_code}")
            
            # Read Excel content
            content = io.BytesIO(content_response.content)
            df = pd.read_excel(content, sheet_name="Schedule", engine='openpyxl')
            
            # Convert DataFrame to list of dictionaries with standardized field names
            projects = []
            for _, row in df.iterrows():
                project = {
                    'project_id': str(row['Zone Name']).strip(),  # Ensure string and remove whitespace
                    'market': str(row['Market']).strip(),
                    'approval_status': str(row['Approval Status']).strip(),
                    'hld_target_date': self._parse_date(row['HLD to be Completed']),
                    'hld_actual_date': self._parse_date(row['HLD Actual']),
                    'designer': str(row['Designer Assignment']).strip() if pd.notna(row['Designer Assignment']) else None,
                    'aerial_eng_target_date': self._parse_date(row['Aerial Eng to be Completed']),
                    'aerial_eng_actual_date': self._parse_date(row['Aerial Eng Actual']),
                    'name': str(row['Zone Name']).strip(),
                    'status': 'Pending',  # Will be calculated based on dates and progress
                    'utility': self._map_market_to_utility(str(row['Market']).strip())
                }
                # Only add if we have a valid project_id
                if project['project_id'] and project['project_id'].lower() != 'nan':
                    projects.append(project)
            
            logger.info(f"Successfully read {len(projects)} projects from Design Tracker")
            return projects
        
        except Exception as e:
            logger.error(f"Error reading Design Tracker: {str(e)}")
            raise

    def _parse_date(self, date_str: str) -> Optional[datetime.date]:
        """Parse date string to datetime.date object"""
        if not date_str or pd.isna(date_str):
            return None
        try:
            return pd.to_datetime(date_str).date()
        except Exception as e:
            logger.warning(f"Could not parse date '{date_str}': {str(e)}")
            return None

    def _map_market_to_utility(self, market: str) -> str:
        """Map market names to utility names"""
        market_utility_map = {
            'Grand Junction': 'XCEL',
            'Bayfield': 'LPEA',
            'Cortez': 'EMPE',
            'Montrose DMEA': 'DMEA',
            'Pagosa': 'LPEA',
            'Telluride': 'SMPA',
            'Rifle': 'XCEL',
            'Silt': 'XCEL',
            'Battlement Mesa': 'XCEL',
            'Parachute': 'XCEL',
            'Mancos': 'EMPE',
            'Dolores': 'EMPE',
            'Marine Road Extension': 'XCEL',
            'Bloomfield': 'FEUS',
            'Montrose Downtown N': 'DMEA',
            'Montrose Downtown S': 'DMEA'
        }
        return market_utility_map.get(market, market) 