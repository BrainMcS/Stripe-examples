import time
import random
import logging
from functools import wraps
import requests

def retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2, jitter=0.1, 
                       retryable_exceptions=(Exception,)):
    """
    Retry decorator with exponential backoff.
    
    Args:
        max_retries: Maximum number of retries
        initial_delay: Initial delay in seconds
        backoff_factor: Multiplier for delay between retries
        jitter: Random factor to add to delay (0-1)
        retryable_exceptions: Tuple of exceptions that should trigger a retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay
            
            while True:
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        logging.error(f"Failed after {max_retries} retries: {str(e)}")
                        raise
                    
                    # Calculate delay with jitter
                    jitter_amount = random.uniform(0, jitter * delay)
                    sleep_time = delay + jitter_amount
                    
                    logging.warning(
                        f"Retry {retries}/{max_retries} after {sleep_time:.2f}s "
                        f"due to: {str(e)}"
                    )
                    
                    time.sleep(sleep_time)
                    delay *= backoff_factor
        
        return wrapper
    return decorator

# Example usage
@retry_with_backoff(
    max_retries=5, 
    initial_delay=0.5, 
    retryable_exceptions=(ConnectionError, TimeoutError)
)
def make_api_request(url, payload):
    # Simulated API request
    response = requests.post(url, json=payload)
    response.raise_for_status()  # Will raise HTTPError for 4xx/5xx responses
    return response.json()