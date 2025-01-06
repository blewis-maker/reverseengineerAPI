import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_database():
    """Create the database if it doesn't exist"""
    db_name = os.getenv('DB_NAME', 'metrics_db')
    
    # Connect to PostgreSQL server
    conn_params = {
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASS'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }

    try:
        # Connect to default database
        conn = psycopg2.connect(dbname='postgres', **conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Check if database exists
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))
        exists = cur.fetchone()
        
        if not exists:
            logger.info(f"Creating database {db_name}")
            cur.execute(f'CREATE DATABASE {db_name}')
            logger.info(f"Database {db_name} created successfully")
        else:
            logger.info(f"Database {db_name} already exists")

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error creating database: {str(e)}")
        raise

def run_schema():
    """Run the schema.sql file to create tables"""
    db_name = os.getenv('DB_NAME', 'metrics_db')
    conn_params = {
        'dbname': db_name,
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASS'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }

    try:
        # Read schema file
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        # Connect to the database and execute schema
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        logger.info("Executing schema.sql")
        cur.execute(schema_sql)
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info("Schema executed successfully")

    except Exception as e:
        logger.error(f"Error running schema: {str(e)}")
        raise

def main():
    """Initialize the database and create tables"""
    try:
        create_database()
        run_schema()
        logger.info("Database initialization completed successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        raise

if __name__ == '__main__':
    main() 