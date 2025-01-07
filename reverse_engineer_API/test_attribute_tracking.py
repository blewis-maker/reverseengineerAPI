from database.db import DatabaseConnection
import logging
from datetime import datetime, timedelta
from tabulate import tabulate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def print_table(data, headers):
    """Print data in a formatted table."""
    if not data:
        print("No data available")
        return
        
    # Convert list of dicts to list of lists
    rows = []
    for record in data:
        row = [record.get(h, '') for h in headers]
        rows.append(row)
    
    print(tabulate(rows, headers=headers, tablefmt='grid'))

def verify_field_tracking():
    """Verify field completion attribute changes are being tracked properly."""
    db = DatabaseConnection()
    
    with db.get_cursor() as cursor:
        # Check field completion changes over time
        cursor.execute("""
            WITH field_changes AS (
                SELECT 
                    job_id,
                    node_id,
                    field_completed,
                    field_completed_by,
                    field_completed_at,
                    timestamp,
                    LAG(field_completed) OVER (PARTITION BY job_id, node_id ORDER BY timestamp) as prev_field_completed,
                    LAG(field_completed_by) OVER (PARTITION BY job_id, node_id ORDER BY timestamp) as prev_completed_by,
                    LAG(field_completed_at) OVER (PARTITION BY job_id, node_id ORDER BY timestamp) as prev_completed_at
                FROM pole_metrics
                ORDER BY job_id, node_id, timestamp
            )
            SELECT 
                job_id,
                node_id,
                prev_field_completed as was_completed,
                field_completed as now_completed,
                prev_completed_by as old_user,
                field_completed_by as new_user,
                field_completed_at,
                timestamp,
                EXTRACT(EPOCH FROM (timestamp - field_completed_at))/3600 as hours_since_completion
            FROM field_changes
            WHERE field_completed IS DISTINCT FROM prev_field_completed
               OR field_completed_by IS DISTINCT FROM prev_completed_by
            ORDER BY timestamp DESC
            LIMIT 5;
        """)
        
        field_changes = cursor.fetchall()
        print("\nField Completion Changes:")
        print("=" * 80)
        if field_changes:
            headers = ['job_id', 'node_id', 'was_completed', 'now_completed',
                      'old_user', 'new_user', 'field_completed_at', 'timestamp',
                      'hours_since_completion']
            print_table(field_changes, headers)
        else:
            print("No field completion changes found")

def verify_back_office_tracking():
    """Verify back office completion attribute changes are being tracked properly."""
    db = DatabaseConnection()
    
    with db.get_cursor() as cursor:
        # Check back office completion changes over time
        cursor.execute("""
            WITH back_office_changes AS (
                SELECT 
                    job_id,
                    node_id,
                    back_office_completed,
                    annotated_by,
                    annotation_completed_at,
                    timestamp,
                    LAG(back_office_completed) OVER (PARTITION BY job_id, node_id ORDER BY timestamp) as prev_back_office,
                    LAG(annotated_by) OVER (PARTITION BY job_id, node_id ORDER BY timestamp) as prev_annotator,
                    LAG(annotation_completed_at) OVER (PARTITION BY job_id, node_id ORDER BY timestamp) as prev_annotation_time
                FROM pole_metrics
                ORDER BY job_id, node_id, timestamp
            )
            SELECT 
                job_id,
                node_id,
                prev_back_office as was_completed,
                back_office_completed as now_completed,
                prev_annotator as old_user,
                annotated_by as new_user,
                annotation_completed_at,
                timestamp,
                EXTRACT(EPOCH FROM (timestamp - annotation_completed_at))/3600 as hours_since_completion
            FROM back_office_changes
            WHERE back_office_completed IS DISTINCT FROM prev_back_office
               OR annotated_by IS DISTINCT FROM prev_annotator
            ORDER BY timestamp DESC
            LIMIT 5;
        """)
        
        back_office_changes = cursor.fetchall()
        print("\nBack Office Changes:")
        print("=" * 80)
        if back_office_changes:
            headers = ['job_id', 'node_id', 'was_completed', 'now_completed',
                      'old_user', 'new_user', 'annotation_completed_at', 'timestamp',
                      'hours_since_completion']
            print_table(back_office_changes, headers)
        else:
            print("No back office changes found")

def verify_weekly_metrics():
    """Verify weekly metrics calculations based on tracked changes."""
    db = DatabaseConnection()
    
    # Calculate date range for this week
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    with db.get_cursor() as cursor:
        # Field completion metrics for the week
        cursor.execute("""
            SELECT 
                field_completed_by as user_id,
                COUNT(DISTINCT node_id) as poles_completed,
                COUNT(DISTINCT job_id) as jobs_worked,
                array_agg(DISTINCT utility) as utilities,
                MIN(field_completed_at) as first_completion,
                MAX(field_completed_at) as last_completion
            FROM pole_metrics
            WHERE field_completed = true
              AND field_completed_at BETWEEN %s AND %s
            GROUP BY field_completed_by
            ORDER BY poles_completed DESC;
        """, (start_date, end_date))
        
        field_metrics = cursor.fetchall()
        print("\nWeekly Field Completion Metrics:")
        print("=" * 80)
        if field_metrics:
            headers = ['user_id', 'poles_completed', 'jobs_worked',
                      'utilities', 'first_completion', 'last_completion']
            print_table(field_metrics, headers)
        else:
            print("No field completion metrics for this week")
        
        # Back office metrics for the week
        cursor.execute("""
            SELECT 
                annotated_by as user_id,
                COUNT(DISTINCT node_id) as poles_completed,
                COUNT(DISTINCT job_id) as jobs_worked,
                array_agg(DISTINCT utility) as utilities,
                MIN(annotation_completed_at) as first_completion,
                MAX(annotation_completed_at) as last_completion
            FROM pole_metrics
            WHERE back_office_completed = true
              AND annotation_completed_at BETWEEN %s AND %s
            GROUP BY annotated_by
            ORDER BY poles_completed DESC;
        """, (start_date, end_date))
        
        back_office_metrics = cursor.fetchall()
        print("\nWeekly Back Office Metrics:")
        print("=" * 80)
        if back_office_metrics:
            headers = ['user_id', 'poles_completed', 'jobs_worked',
                      'utilities', 'first_completion', 'last_completion']
            print_table(back_office_metrics, headers)
        else:
            print("No back office metrics for this week")

def verify_user_tracking():
    """Verify that users are being tracked correctly for job statuses, field work, and back office work."""
    db = DatabaseConnection()
    
    with db.get_cursor() as cursor:
        # Check OSP assignments in status change log
        print("\nOSP Assignment Tracking:")
        print("=" * 80)
        cursor.execute("""
            SELECT 
                job_id,
                field_name,
                old_value,
                new_value,
                changed_at,
                changed_by
            FROM status_change_log
            WHERE entity_type = 'job'
              AND field_name = 'status'
            ORDER BY changed_at DESC
            LIMIT 5;
        """)
        
        osp_changes = cursor.fetchall()
        if osp_changes:
            headers = ['job_id', 'field_name', 'old_value', 'new_value', 'changed_at', 'changed_by']
            print_table(osp_changes, headers)
        else:
            print("No OSP assignment changes found")

        # Check field completion user tracking
        print("\nField Completion User Tracking:")
        print("=" * 80)
        cursor.execute("""
            SELECT DISTINCT
                pm.job_id,
                pm.node_id,
                pm.field_completed_by,
                pm.field_completed_at,
                j.utility
            FROM pole_metrics pm
            JOIN job_metrics j ON pm.job_id = j.job_id
            WHERE pm.field_completed = true
              AND pm.field_completed_by IS NOT NULL
            ORDER BY pm.field_completed_at DESC
            LIMIT 5;
        """)
        
        field_users = cursor.fetchall()
        if field_users:
            headers = ['job_id', 'node_id', 'field_completed_by', 'field_completed_at', 'utility']
            print_table(field_users, headers)
        else:
            print("No field completion user records found")

        # Check back office user tracking
        print("\nBack Office User Tracking:")
        print("=" * 80)
        cursor.execute("""
            SELECT DISTINCT
                pm.job_id,
                pm.node_id,
                pm.annotated_by,
                pm.annotation_completed_at,
                j.utility
            FROM pole_metrics pm
            JOIN job_metrics j ON pm.job_id = j.job_id
            WHERE pm.back_office_completed = true
              AND pm.annotated_by IS NOT NULL
            ORDER BY pm.annotation_completed_at DESC
            LIMIT 5;
        """)
        
        back_office_users = cursor.fetchall()
        if back_office_users:
            headers = ['job_id', 'node_id', 'annotated_by', 'annotation_completed_at', 'utility']
            print_table(back_office_users, headers)
        else:
            print("No back office user records found")

if __name__ == "__main__":
    print("\nVerifying Field and Back Office Attribute Tracking")
    print("=" * 80)
    
    verify_field_tracking()
    verify_back_office_tracking()
    verify_weekly_metrics()
    verify_user_tracking() 