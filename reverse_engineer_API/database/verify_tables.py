from database.db import DatabaseConnection
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from tabulate import tabulate

def verify_tables():
    """Perform detailed verification of all database tables."""
    db = DatabaseConnection()
    
    print("\nDetailed Database Verification Report")
    print("=" * 80)
    
    # List of tables to verify
    tables = [
        'job_metrics',
        'status_changes',
        'pole_metrics',
        'user_metrics',
        'user_daily_summary',
        'burndown_metrics'
    ]
    
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            for table in tables:
                print(f"\n{table.upper()}")
                print("-" * 80)
                
                # Get column information
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                """, (table,))
                columns = cursor.fetchall()
                
                print("\nTable Schema:")
                schema_data = [[col['column_name'], col['data_type'], col['is_nullable']] 
                             for col in columns]
                print(tabulate(schema_data, headers=['Column', 'Type', 'Nullable'], 
                             tablefmt='grid'))
                
                # Get record count
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cursor.fetchone()['count']
                print(f"\nTotal Records: {count}")
                
                if count > 0:
                    # Get sample records
                    cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                    records = cursor.fetchall()
                    
                    print("\nSample Records:")
                    # Convert records to list of lists for tabulate
                    headers = list(records[0].keys())
                    rows = [[str(record[col])[:50] + '...' if isinstance(record[col], str) 
                            and len(str(record[col])) > 50 else record[col] 
                            for col in headers] 
                           for record in records]
                    print(tabulate(rows, headers=headers, tablefmt='grid'))
                    
                    # Get value distributions for key columns
                    if table == 'job_metrics':
                        print("\nStatus Distribution:")
                        cursor.execute("""
                            SELECT status, COUNT(*) as count 
                            FROM job_metrics 
                            GROUP BY status
                        """)
                        status_dist = cursor.fetchall()
                        print(tabulate(status_dist, headers='keys', tablefmt='grid'))
                        
                    elif table == 'pole_metrics':
                        print("\nUtility Distribution:")
                        cursor.execute("""
                            SELECT utility, COUNT(*) as count 
                            FROM pole_metrics 
                            GROUP BY utility
                        """)
                        utility_dist = cursor.fetchall()
                        print(tabulate(utility_dist, headers='keys', tablefmt='grid'))
                        
                    elif table == 'user_metrics':
                        print("\nRole Distribution:")
                        cursor.execute("""
                            SELECT role, COUNT(*) as count 
                            FROM user_metrics 
                            GROUP BY role
                        """)
                        role_dist = cursor.fetchall()
                        print(tabulate(role_dist, headers='keys', tablefmt='grid'))
                
                print("\n" + "=" * 80)

if __name__ == "__main__":
    verify_tables() 