import logging
import asyncio

# Retry decorator to handle retries with exponential backoff
def retry(max_retries=3, delay=2, backoff_factor=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            _delay = delay
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.warning(f"Error in {func.__name__}: {e}, retrying {attempt + 1}/{max_retries} in {_delay} seconds...")
                    await asyncio.sleep(_delay)
                    _delay *= backoff_factor
            raise Exception(f"Failed to complete {func.__name__} after {max_retries} retries.")
        return wrapper
    return decorator

# General logging setup function, useful for setting custom logging formats
def setup_logging(log_level=logging.INFO):
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Logging is set up with level: %s", log_level)

# Helper to fetch and log API responses more effectively
async def log_api_response(func, *args, **kwargs):
    try:
        response = await func(*args, **kwargs)
        logging.info(f"API call to {func.__name__} succeeded.")
        return response
    except Exception as e:
        logging.error(f"API call to {func.__name__} failed with error: {e}")
        raise
