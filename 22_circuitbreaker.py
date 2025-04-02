import time
import logging
from enum import Enum
import requests

class CircuitState(Enum):
    CLOSED = 'closed'     # Normal operation, requests go through
    OPEN = 'open'         # Failing state, short-circuits requests
    HALF_OPEN = 'half-open'  # Testing if service is back

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60, 
                 failure_window=120, expected_exceptions=(Exception,)):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_window = failure_window
        self.expected_exceptions = expected_exceptions
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.last_state_change_time = time.time()
    
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_state_change_time > self.recovery_timeout:
                    self._transition_to_half_open()
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker is OPEN until {self.last_state_change_time + self.recovery_timeout}")
            
            try:
                result = func(*args, **kwargs)
                self._handle_success()
                return result
            except self.expected_exceptions as e:
                self._handle_failure()
                raise
        
        return wrapper
    
    def _transition_to_open(self):
        self.state = CircuitState.OPEN
        self.last_state_change_time = time.time()
        logging.warning("Circuit breaker transitioned to OPEN")
    
    def _transition_to_half_open(self):
        self.state = CircuitState.HALF_OPEN
        self.last_state_change_time = time.time()
        logging.info("Circuit breaker transitioned to HALF-OPEN")
    
    def _transition_to_closed(self):
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.last_state_change_time = time.time()
        logging.info("Circuit breaker transitioned to CLOSED")
    
    def _handle_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self._transition_to_closed()
    
    def _handle_failure(self):
        current_time = time.time()
        
        # Reset failure count if outside failure window
        if self.last_failure_time and current_time - self.last_failure_time > self.failure_window:
            self.failure_count = 0
        
        self.failure_count += 1
        self.last_failure_time = current_time
        
        if self.state == CircuitState.HALF_OPEN or (
           self.state == CircuitState.CLOSED and self.failure_count >= self.failure_threshold):
            self._transition_to_open()

class CircuitBreakerOpenError(Exception):
    """Raised when a request is attempted while the circuit is open"""
    pass

# Example usage
payment_service = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=30,
    expected_exceptions=(ConnectionError, TimeoutError)
)

@payment_service
def process_payment(payment_data):
    # Payment processing logic
    response = requests.post('https://payment-api.example.com/v1/charge', json=payment_data)
    response.raise_for_status()
    return response.json()