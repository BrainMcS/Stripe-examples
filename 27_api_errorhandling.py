import requests
import time
import logging
import random
from requests.exceptions import RequestException, ConnectionError, Timeout, TooManyRedirects, SSLError
from dotenv import load_dotenv
import os

# Configuration
load_dotenv()
API_KEY = os.getenv("STRIPE_API_KEY")
BASE_URL = "https://api.stripe.com/v1"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("robust_client")

class RobustAPIClient:
    def __init__(self, base_url, api_key=None, max_retries=5, initial_backoff=1):
        self.base_url = base_url
        self.api_key = api_key
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.session = requests.Session()
        
        if api_key:
            self.session.headers.update({"Authorization": f"Bearer {api_key}"})
    
    def request(self, method, endpoint, **kwargs):
        """Make a request with automatic retries and exponential backoff."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        retries = 0
        backoff = self.initial_backoff
        
        while retries <= self.max_retries:
            try:
                logger.info(f"Making {method} request to {url}")
                response = self.session.request(method, url, **kwargs)
                
                # Handle rate limiting (status code 429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', backoff))
                    logger.warning(f"Rate limited. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                    retries += 1
                    continue
                
                # Handle server errors (5xx)
                if 500 <= response.status_code < 600:
                    logger.warning(f"Server error {response.status_code}. Retrying in {backoff} seconds.")
                    time.sleep(backoff)
                    retries += 1
                    backoff = min(backoff * 2, 60)  # Exponential backoff with a cap
                    continue
                
                # Handle any other errors (4xx)
                response.raise_for_status()
                return response.json()
            
            except SSLError as e:
                logger.error(f"SSL/TLS Error: {e}")
                raise  # SSL errors are critical and shouldn't be retried
            
            except (ConnectionError, Timeout) as e:
                if retries >= self.max_retries:
                    logger.error(f"Maximum retries exceeded. Last error: {e}")
                    raise
                
                # Add a small random value to prevent thundering herd problem
                jitter = random.uniform(0, 0.5)
                sleep_time = backoff + jitter
                logger.warning(f"Connection error: {e}. Retrying in {sleep_time:.2f} seconds.")
                time.sleep(sleep_time)
                retries += 1
                backoff = min(backoff * 2, 60)
            
            except RequestException as e:
                logger.error(f"Request failed: {e}")
                raise
    
    def get(self, endpoint, **kwargs):
        return self.request("GET", endpoint, **kwargs)
    
    def post(self, endpoint, **kwargs):
        return self.request("POST", endpoint, **kwargs)
    
    def put(self, endpoint, **kwargs):
        return self.request("PUT", endpoint, **kwargs)
    
    def delete(self, endpoint, **kwargs):
        return self.request("DELETE", endpoint, **kwargs)
    
    def check_tls_version(self):
        """Check the TLS version being used by the client."""
        try:
            response = self.session.get("https://www.howsmyssl.com/a/check")
            data = response.json()
            tls_version = data.get("tls_version", "Unknown")
            logger.info(f"TLS Version: {tls_version}")
            return tls_version
        except Exception as e:
            logger.error(f"Failed to check TLS version: {e}")
            return "Error checking TLS version"

# Usage example
if __name__ == "__main__":
    client = RobustAPIClient(API_KEY, BASE_URL)
    
    # Check TLS version
    tls_version = client.check_tls_version()
    print(f"Using TLS version: {tls_version}")
    
    try:
        # Make a request with automatic retries
        user_data = client.get("users/123")
        print(f"Successfully retrieved user data: {user_data['name']}")
    except Exception as e:
        print(f"Failed after multiple retries: {e}")