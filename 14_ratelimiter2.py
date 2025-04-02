import time
import threading
import logging
from typing import Dict, Tuple, Optional
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('rate_limiter')

class TokenBucket:
    """
    Implements the Token Bucket algorithm for rate limiting.
    
    A token bucket has a capacity and refills at a certain rate. Tokens are consumed
    when a request is made, and if there aren't enough tokens, the request is denied.
    """
    
    def __init__(self, capacity: float, refill_rate: float):
        """
        Initialize a token bucket.
        
        Args:
            capacity: Maximum number of tokens the bucket can hold
            refill_rate: Rate at which tokens are added (tokens per second)
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self.lock = threading.RLock()
    
    def _refill(self) -> None:
        """Refill the bucket based on elapsed time"""
        now = time.time()
        elapsed = now - self.last_refill
        
        # Calculate new tokens to add
        new_tokens = elapsed * self.refill_rate
        
        # Add tokens up to capacity
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        
        # Update last refill time
        self.last_refill = now
    
    def consume(self, tokens: float = 1.0) -> bool:
        """
        Try to consume tokens from the bucket.
        
        Args:
            tokens: Number of tokens to consume
            
        Returns:
            True if tokens were consumed, False if not enough tokens
        """
        with self.lock:
            self._refill()
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            
            return False
    
    def get_wait_time(self, tokens: float = 1.0) -> float:
        """
        Calculate the wait time until enough tokens are available.
        
        Args:
            tokens: Number of tokens needed
            
        Returns:
            Time in seconds to wait, or 0 if tokens are already available
        """
        with self.lock:
            self._refill()
            
            if self.tokens >= tokens:
                return 0
            
            # Calculate how long to wait for enough tokens
            additional_tokens_needed = tokens - self.tokens
            wait_time = additional_tokens_needed / self.refill_rate
            
            return wait_time

class RateLimiter:
    """
    Rate limiter that manages multiple token buckets for different rate limit tiers.
    
    This class supports multiple buckets per key, allowing for complex rate limiting
    strategies like "100 requests per minute AND 1000 requests per hour".
    """
    
    def __init__(self):
        """Initialize the rate limiter"""
        self.buckets = defaultdict(dict)
        self.lock = threading.RLock()
    
    def add_limit(self, key: str, bucket_name: str, capacity: float, refill_rate: float) -> None:
        """
        Add a rate limit configuration for a key.
        
        Args:
            key: The key to apply the limit to (e.g., API key, IP address)
            bucket_name: Name of the bucket (e.g., "per_minute", "per_hour")
            capacity: Maximum number of tokens
            refill_rate: Tokens per second refill rate
        """
        with self.lock:
            self.buckets[key][bucket_name] = TokenBucket(capacity, refill_rate)
            logger.info(f"Added limit for {key}/{bucket_name}: {capacity} tokens at {refill_rate}/sec")
    
    def check_rate_limit(self, key: str, tokens: float = 1.0) -> Tuple[bool, Dict[str, float]]:
        """
        Check if a request should be rate limited.
        
        Args:
            key: The key to check limits for
            tokens: Number of tokens to consume
            
        Returns:
            Tuple of (allowed, wait_times) where wait_times is a dict of bucket_name -> wait_time
        """
        if key not in self.buckets:
            # No limits for this key
            return True, {}
        
        wait_times = {}
        allowed = True
        
        with self.lock:
            # Check all buckets for this key
            for bucket_name, bucket in self.buckets[key].items():
                if not bucket.consume(tokens):
                    # Rate limit exceeded for this bucket
                    allowed = False
                    wait_times[bucket_name] = bucket.get_wait_time(tokens)
        
        return allowed, wait_times
    
    def is_allowed(self, key: str, tokens: float = 1.0) -> bool:
        """
        Simple check if a request is allowed.
        
        Args:
            key: The key to check limits for
            tokens: Number of tokens to consume
            
        Returns:
            True if request is allowed, False otherwise
        """
        allowed, _ = self.check_rate_limit(key, tokens)
        return allowed
    
    def get_retry_after(self, key: str, tokens: float = 1.0) -> Optional[float]:
        """
        Get the recommended wait time before retrying.
        
        Args:
            key: The key to check limits for
            tokens: Number of tokens needed
            
        Returns:
            Seconds to wait before retrying, or None if no wait needed
        """
        allowed, wait_times = self.check_rate_limit(key, tokens)
        
        if allowed or not wait_times:
            return None
        
        # Return the maximum wait time across all buckets
        return max(wait_times.values())
    
    def get_remaining(self, key: str, bucket_name: Optional[str] = None) -> Dict[str, float]:
        """
        Get remaining tokens for a key.
        
        Args:
            key: The key to check
            bucket_name: Optional specific bucket to check
            
        Returns:
            Dict of bucket_name -> remaining tokens
        """
        if key not in self.buckets:
            return {}
        
        remaining = {}
        
        with self.lock:
            if bucket_name:
                # Check specific bucket
                if bucket_name in self.buckets[key]:
                    bucket = self.buckets[key][bucket_name]
                    bucket._refill()  # Refill before checking
                    remaining[bucket_name] = bucket.tokens
            else:
                # Check all buckets
                for bname, bucket in self.buckets[key].items():
                    bucket._refill()  # Refill before checking
                    remaining[bname] = bucket.tokens
        
        return remaining
    
    def remove_limits(self, key: str) -> None:
        """Remove all rate limits for a key"""
        with self.lock:
            if key in self.buckets:
                del self.buckets[key]
                logger.info(f"Removed all limits for {key}")

# Example usage
def test_rate_limiter():
    # Create a rate limiter
    limiter = RateLimiter()
    
    # Add limits for an API key
    api_key = "test_api_key"
    
    # 5 requests per second
    limiter.add_limit(api_key, "per_second", 5, 5)
    
    # 100 requests per minute
    limiter.add_limit(api_key, "per_minute", 100, 100/60)
    
    # Simulate requests
    print("\nSimulating 10 immediate requests:")
    for i in range(10):
        allowed = limiter.is_allowed(api_key)
        retry_after = limiter.get_retry_after(api_key)
        remaining = limiter.get_remaining(api_key)
        
        print(f"Request {i+1}: Allowed={allowed}, Retry After={retry_after}, Remaining={remaining}")
        
        if not allowed:
            # Wait if rate limited
            if retry_after:
                time.sleep(retry_after)
    
    # Show recovery
    print("\nWaiting for bucket to refill...")
    time.sleep(1)
    
    remaining = limiter.get_remaining(api_key)
    print(f"After waiting: Remaining={remaining}")
    
    # Test with different token costs
    print("\nTesting with different token costs:")
    
    # Expensive request (3 tokens)
    allowed = limiter.is_allowed(api_key, 3)
    remaining = limiter.get_remaining(api_key)
    print(f"Expensive request (3 tokens): Allowed={allowed}, Remaining={remaining}")

if __name__ == "__main__":
    test_rate_limiter()