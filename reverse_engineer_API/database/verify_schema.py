from db import DatabaseConnection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_table_schemas():
    """Get all table schemas from the database"""
    db = DatabaseConnection()
    
    with db.get_cursor() as cursor:
        # Get all tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        
        schemas = {}
        for table in tables:
            table_name = table['table_name']
            
            # Get columns for each table
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            columns = cursor.fetchall()
            
            # Get constraints
            cursor.execute("""
                SELECT
                    c.conname as constraint_name,
                    c.contype as constraint_type,
                    pg_get_constraintdef(c.oid) as definition
                FROM pg_constraint c
                JOIN pg_namespace n ON n.oid = c.connamespace
                WHERE n.nspname = 'public'
                AND c.conrelid = %s::regclass
            """, (table_name,))
            constraints = cursor.fetchall()
            
            # Get indices
            cursor.execute("""
                SELECT
                    i.relname as index_name,
                    pg_get_indexdef(i.oid) as definition
                FROM pg_index x
                JOIN pg_class i ON i.oid = x.indexrelid
                JOIN pg_class t ON t.oid = x.indrelid
                WHERE t.relname = %s
            """, (table_name,))
            indices = cursor.fetchall()
            
            schemas[table_name] = {
                'columns': columns,
                'constraints': constraints,
                'indices': indices
            }
    
    return schemas

def print_schema_report():
    """Print a detailed report of the database schema"""
    schemas = get_table_schemas()
    
    print("\nDatabase Schema Report")
    print("=" * 80)
    
    for table_name, schema in schemas.items():
        print(f"\nTable: {table_name}")
        print("-" * 80)
        
        print("\nColumns:")
        print("{:<30} {:<15} {:<10} {:<20}".format(
            "Name", "Type", "Nullable", "Default"
        ))
        print("-" * 75)
        for col in schema['columns']:
            print("{:<30} {:<15} {:<10} {:<20}".format(
                col['column_name'],
                col['data_type'],
                col['is_nullable'],
                str(col['column_default'])[:20] if col['column_default'] else ''
            ))
        
        if schema['constraints']:
            print("\nConstraints:")
            for con in schema['constraints']:
                print(f"  {con['constraint_name']} ({con['constraint_type']})")
                print(f"    {con['definition']}")
        
        if schema['indices']:
            print("\nIndices:")
            for idx in schema['indices']:
                print(f"  {idx['index_name']}")
                print(f"    {idx['definition']}")
        
        print("\n" + "=" * 80)

if __name__ == '__main__':
    print_schema_report() 