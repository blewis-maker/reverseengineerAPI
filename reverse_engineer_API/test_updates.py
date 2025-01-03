import os
import logging
import sys
from datetime import datetime
from incremental_update import process_daily_update

# Ensure required directories exist in Docker environment
os.makedirs('/app/logs', exist_ok=True)

# Configure logging for Docker environment
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/test_updates.log')
    ]
)
logger = logging.getLogger(__name__)

def test_daily_sync():
    """Test daily sync functionality in Docker environment"""
    logger.info("Starting daily sync test in Docker container...")
    
    try:
        # Ensure we're in the correct directory
        os.chdir('/app')
        
        # Run the daily update process
        process_daily_update()
        logger.info("Daily sync test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in daily sync test: {str(e)}")
        return False

if __name__ == '__main__':
    success = test_daily_sync()
    exit(0 if success else 1) 