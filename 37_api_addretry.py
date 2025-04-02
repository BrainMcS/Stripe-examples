import requests
import time
import random
import logging
from requests.exceptions import RequestException, ConnectionError, Timeout

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api_client")

class APIClient:
    def __init__(self, api_key, base_url="https://api.example.com/v1", max_retries=3):
        self.base_url = base_url
        self.api_key = api_key
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })
    
    def make_request(self, method, endpoint, **kwargs):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Initialize retry parameters
        retries = 0
        backoff = 0.5  # Start with 500ms
        
        while True:
            try:
                logger.info(f"Making {method} request to {url}")
                response = self.session.request(method, url, **kwargs)
                
                # Handle rate limiting (status code 429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', backoff))
                    logger.warning(f"Rate limited. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                    continue
                
                # Handle server errors (5xx)
                if 500 <= response.status_code < 600:
                    if retries >= self.max_retries:
                        logger.error(f"Max retries exceeded for {url}")
                        response.raise_for_status()
                    
                    retries += 1
                    sleep_time = backoff * (2 ** (retries - 1)) + random.uniform(0, 0.1)
                    logger.warning(f"Server error {response.status_code}. Retry {retries}/{self.max_retries} after {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    continue
                
                # Handle other errors
                response.raise_for_status()
                return response.json()
            
            except (ConnectionError, Timeout) as e:
                if retries >= self.max_retries:
                    logger.error(f"Max retries exceeded for {url}")
                    raise
                
                retries += 1
                sleep_time = backoff * (2 ** (retries - 1)) + random.uniform(0, 0.1)
                logger.warning(f"Connection error: {e}. Retry {retries}/{self.max_retries} after {sleep_time:.2f}s")
                time.sleep(sleep_time)
            
            except RequestException as e:
                logger.error(f"Request failed: {e}")
                raise
    
    def get(self, endpoint, **kwargs):
        return self.make_request("GET", endpoint, **kwargs)
    
    def post(self, endpoint, **kwargs):
        return self.make_request("POST", endpoint, **kwargs)
    
    def put(self, endpoint, **kwargs):
        return self.make_request("PUT", endpoint, **kwargs)
    
    def delete(self, endpoint, **kwargs):
        return self.make_request("DELETE", endpoint, **kwargs)

# Example usage
client = APIClient("sk_test_your_api_key", max_retries=5)

try:
    # This will automatically retry on connection errors or 5xx responses
    customer = client.get("customers/cus_123456789")
    print(f"Retrieved customer: {customer['name']}")
except Exception as e:
    print(f"Failed after multiple retries: {e}")