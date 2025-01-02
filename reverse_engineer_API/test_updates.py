import logging
from incremental_update import daily_update, weekly_update

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_daily_update():
    """Test daily update functionality"""
    logger.info("Testing daily update...")
    try:
        metrics = daily_update()
        logger.info(f"Daily update test completed. Processed {metrics['total_jobs']} jobs")
        return True
    except Exception as e:
        logger.error(f"Error in daily update test: {str(e)}")
        return False

def test_weekly_update():
    """Test weekly update functionality"""
    logger.info("Testing weekly update...")
    try:
        metrics = weekly_update()
        logger.info(f"Weekly update test completed. Processed {metrics['total_jobs']} jobs")
        return True
    except Exception as e:
        logger.error(f"Error in weekly update test: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting update tests...")
    
    # Test daily update
    daily_success = test_daily_update()
    
    # Test weekly update
    weekly_success = test_weekly_update()
    
    # Report results
    logger.info("\nTest Results:")
    logger.info(f"Daily Update: {'SUCCESS' if daily_success else 'FAILED'}")
    logger.info(f"Weekly Update: {'SUCCESS' if weekly_success else 'FAILED'}") 