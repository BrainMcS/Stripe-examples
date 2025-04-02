import time
import threading
from collections import defaultdict
from datetime import datetime

class RateLimiter:
    """
    A rate limiter that limits requests to 100 per minute per API key.
    Uses a rolling window approach to track requests.
    """
    
    def __init__(self, limit=100, window=60):
        """
        Initialize the rate limiter.
        
        Args:
            limit (int): Maximum number of requests allowed in the window
            window (int): Time window in seconds (default: 60 seconds = 1 minute)
        """
        self.limit = limit
        self.window = window
        self.requests = defaultdict(list)  # API key -> list of timestamps
        self.lock = threading.RLock()  # Use RLock for thread safety
        
    def is_allowed(self, api_key):
        """
        Check if a request with the given API key is allowed.
        
        Args:
            api_key (str): The API key to check
            
        Returns:
            bool: True if the request is allowed, False otherwise
        """
        with self.lock:
            current_time = time.time()
            
            # Remove timestamps that are outside the current window
            self.requests[api_key] = [
                timestamp for timestamp in self.requests[api_key]
                if current_time - timestamp < self.window
            ]
            
            # Check if the number of requests is below the limit
            if len(self.requests[api_key]) < self.limit:
                self.requests[api_key].append(current_time)
                return True
            
            return False
    
    def get_remaining(self, api_key):
        """
        Get the number of requests remaining for an API key.
        
        Args:
            api_key (str): The API key to check
            
        Returns:
            int: The number of requests remaining in the current window
        """
        with self.lock:
            current_time = time.time()
            
            # Clean up old requests first
            self.requests[api_key] = [
                timestamp for timestamp in self.requests[api_key]
                if current_time - timestamp < self.window
            ]
            
            return max(0, self.limit - len(self.requests[api_key]))
    
    def get_retry_after(self, api_key):
        """
        Get the number of seconds after which a request might be allowed.
        
        Args:
            api_key (str): The API key to check
            
        Returns:
            float: The estimated number of seconds to wait, or 0 if requests are allowed
        """
        with self.lock:
            if self.get_remaining(api_key) > 0:
                return 0
            
            # Find the oldest timestamp in the window
            oldest = min(self.requests[api_key])
            return max(0, self.window - (time.time() - oldest))


# Example usage with a mock Stripe API client
class MockStripeClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.rate_limiter = RateLimiter(limit=100, window=60)
    
    def make_request(self, endpoint, data=None):
        """
        Make a request to the Stripe API with rate limiting.
        
        Args:
            endpoint (str): The API endpoint to call
            data (dict): The data to send with the request
            
        Returns:
            dict: The response data, or an error message
        """
        if not self.rate_limiter.is_allowed(self.api_key):
            retry_after = self.rate_limiter.get_retry_after(self.api_key)
            return {
                'error': {
                    'type': 'rate_limit_error',
                    'message': f'Rate limit exceeded. Retry after {retry_after:.2f} seconds.',
                    'retry_after': retry_after
                }
            }
        
        # In a real implementation, this would make an actual API call
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Making request to {endpoint}")
        
        # Simulate API response
        return {
            'success': True,
            'endpoint': endpoint,
            'data': data,
            'remaining_requests': self.rate_limiter.get_remaining(self.api_key)
        }


# Demonstration
def demonstrate_rate_limiter():
    """Demonstrate the rate limiter with a sample API key"""
    import os
    from dotenv import load_dotenv

    load_dotenv()
    api_key = os.getenv('STRIPE_API_KEY')
    client = MockStripeClient(api_key)
    
    print("Making 10 initial requests...")
    for i in range(10):
        response = client.make_request(f"/v1/endpoint/{i}")
        print(f"Request {i+1}: Remaining = {response.get('remaining_requests', 'N/A')}")
    
    print("\nMaking 95 more requests to hit the limit...")
    for i in range(95):
        response = client.make_request("/v1/bulk-endpoint")
        # Only print every 10 requests to avoid cluttering output
        if i % 10 == 0:
            print(f"Bulk request {i+1}: Remaining = {response.get('remaining_requests', 'N/A')}")
    
    print("\nTrying to make requests after hitting the limit...")
    for i in range(5):
        response = client.make_request("/v1/another-endpoint")
        if 'error' in response:
            print(f"Request failed: {response['error']['message']}")
        else:
            print(f"Request succeeded: Remaining = {response['remaining_requests']}")
        
        # Wait a bit between requests to demonstrate retry behavior
        if i < 4:  # Don't wait after the last request
            time.sleep(2)


if __name__ == "__main__":
    demonstrate_rate_limiter()