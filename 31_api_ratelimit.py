import requests
import time
import logging
import queue
import threading
from typing import Dict, Any, List, Callable, Optional
from dotenv import load_dotenv
import os

# Configuration
load_dotenv()
API_KEY = os.getenv("STRIPE_API_KEY")
BASE_URL = "https://api.stripe.com/v1"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rate_limiter")

class RateLimitedClient:
    def __init__(
        self, 
        base_url: str, 
        api_key: str,
        default_rate_limit: int = 100,
        default_rate_window: int = 60,
        prioritized_endpoints: Optional[List[str]] = None
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })
        
        # Rate limiting configuration
        self.default_rate_limit = default_rate_limit  # requests per window
        self.default_rate_window = default_rate_window  # window size in seconds
        self.remaining_requests = default_rate_limit
        self.rate_limit_reset = time.time() + default_rate_window
        
        # Request queue and priority
        self.request_queue = queue.PriorityQueue()
        self.prioritized_endpoints = prioritized_endpoints or []
        
        # Start the queue worker
        self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.worker_thread.start()
    
    def _get_priority(self, endpoint: str) -> int:
        """Determine priority for an endpoint (lower value = higher priority)."""
        for i, pattern in enumerate(self.prioritized_endpoints):
            if pattern in endpoint:
                return i
        return len(self.prioritized_endpoints)
    
    def _process_queue(self):
        """Process queued requests while respecting rate limits."""
        while True:
            # Get the next request from the queue
            _, (method, endpoint, kwargs, callback) = self.request_queue.get()
            
            # Check if we need to wait for rate limit reset
            current_time = time.time()
            if current_time > self.rate_limit_reset:
                # Reset counter if the window has passed
                self.remaining_requests = self.default_rate_limit
                self.rate_limit_reset = current_time + self.default_rate_window
            
            # Wait if we've exhausted our request quota
            if self.remaining_requests <= 0:
                sleep_time = max(0, self.rate_limit_reset - current_time)
                logger.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                
                # Reset counter after waiting
                self.remaining_requests = self.default_rate_limit
                self.rate_limit_reset = time.time() + self.default_rate_window
            
            # Make the request
            try:
                response = self._make_request(method, endpoint, **kwargs)
                self._update_rate_limits(response)
                
                # Call the callback with the result
                if callback:
                    callback(response)
            
            except Exception as e:
                logger.error(f"Error processing request to {endpoint}: {e}")
                if callback:
                    callback({"error": str(e)})
            
            # Mark task as done
            self.request_queue.task_done()
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make an HTTP request to the API."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"Making {method} request to {url}")
        
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            # Handle specific status codes
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded according to server")
                
                # Update rate limits from headers if available
                self._update_rate_limits(e.response)
                
                # Re-queue the request (it will wait for the rate limit window)
                priority = self._get_priority(endpoint)
                self.request_queue.put((priority, (method, endpoint, kwargs, None)))
                
                return {"error": "Rate limited", "retry_scheduled": True}
            
            # Re-raise other HTTP errors
            raise
    
    def _update_rate_limits(self, response):
        """Update rate limit tracking based on response headers."""
        # Check for rate limit headers (varies by API)
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset = response.headers.get("X-RateLimit-Reset")
        
        if remaining is not None:
            self.remaining_requests = int(remaining)
        else:
            # Decrement our counter if the API doesn't provide headers
            self.remaining_requests -= 1
        
        if reset is not None:
            self.rate_limit_reset = int(reset)
    
    def enqueue_request(
        self, 
        method: str, 
        endpoint: str, 
        callback: Optional[Callable[[Dict[str, Any]], None]] = None, 
        **kwargs
    ):
        """
        Enqueue a request to be processed according to rate limits.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            callback: Function to call with the response
            **kwargs: Additional arguments to pass to requests
            
        Returns:
            Queue position
        """
        priority = self._get_priority(endpoint)
        self.request_queue.put((priority, (method, endpoint, kwargs, callback)))
        return self.request_queue.qsize()
    
    def get(self, endpoint: str, callback: Optional[Callable] = None, **kwargs):
        """Enqueue a GET request."""
        return self.enqueue_request("GET", endpoint, callback, **kwargs)
    
    def post(self, endpoint: str, callback: Optional[Callable] = None, **kwargs):
        """Enqueue a POST request."""
        return self.enqueue_request("POST", endpoint, callback, **kwargs)
    
    def put(self, endpoint: str, callback: Optional[Callable] = None, **kwargs):
        """Enqueue a PUT request."""
        return self.enqueue_request("PUT", endpoint, callback, **kwargs)
    
    def delete(self, endpoint: str, callback: Optional[Callable] = None, **kwargs):
        """Enqueue a DELETE request."""
        return self.enqueue_request("DELETE", endpoint, callback, **kwargs)
    
    def wait_until_complete(self):
        """Wait for all queued requests to complete."""
        self.request_queue.join()

# Usage example
if __name__ == "__main__":
    # Create a client with custom rate limits
    client = RateLimitedClient(
        base_url=BASE_URL,
        api_key=API_KEY,
        default_rate_limit=100,
        default_rate_window=60,
        prioritized_endpoints=["payments", "customers"]  # Prioritize these endpoints
    )
    
    # Define a callback to process responses
    def process_response(response):
        if "error" in response:
            print(f"Error: {response['error']}")
        else:
            print(f"Success: {response.get('id')}")
    
    # Queue up some requests
    for i in range(10):
        client.get(f"customers/{i}", callback=process_response)
    
    for i in range(5):
        client.get(f"payments/{i}", callback=process_response)  # These will be prioritized
    
    print("All requests queued. Waiting for completion...")
    client.wait_until_complete()
    print("All requests completed!")