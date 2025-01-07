import os
import logging
import sys
from database.db import DatabaseConnection
from database.project_manager import ProjectManager
from integrations.sharepoint_reader import SharePointReader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def apply_migrations():
    """Apply database migrations"""
    try:
        db = DatabaseConnection()
        
        # Read and execute the migration SQL
        migration_path = os.path.join(os.path.dirname(__file__), 'create_projects_table.sql')
        with open(migration_path, 'r') as f:
            migration_sql = f.read()
            
        with db.get_cursor() as cursor:
            # Check if projects table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'projects'
                );
            """)
            table_exists = cursor.fetchone()['exists']
            
            if not table_exists:
                cursor.execute(migration_sql)
                logger.info("Successfully created projects table and applied migrations")
            else:
                logger.info("Projects table already exists, skipping creation")
            
    except Exception as e:
        logger.error(f"Error applying migrations: {str(e)}")
        raise

def sync_sharepoint_data():
    """Sync data from SharePoint"""
    try:
        # Initialize managers
        project_manager = ProjectManager()
        sharepoint = SharePointReader()
        
        # Read projects from SharePoint
        projects = sharepoint.read_design_tracker()
        logger.info(f"Found {len(projects)} projects in SharePoint")
        
        # Insert or update each project
        for project in projects:
            existing = project_manager.get_project(project['project_id'])
            
            if existing:
                logger.info(f"Updating existing project: {project['project_id']}")
                project_manager.update_project(project['project_id'], project)
            else:
                logger.info(f"Creating new project: {project['project_id']}")
                project_manager.create_project(project)
        
        logger.info("Successfully synced SharePoint data")
        
    except Exception as e:
        logger.error(f"Error syncing SharePoint data: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == '--sync-only':
            logger.info("Running SharePoint sync only")
            sync_sharepoint_data()
        else:
            logger.info("Starting database migration and data sync")
            apply_migrations()
            sync_sharepoint_data()
        logger.info("Successfully completed all operations")
    except Exception as e:
        logger.error(f"Failed to complete operations: {str(e)}")
        raise 