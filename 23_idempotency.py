import uuid
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

class IdempotencyStore:
    """Simple in-memory idempotency key store"""
    
    def __init__(self, expiration_hours=24):
        self.store = {}  # key -> {result, created_at}
        self.expiration_hours = expiration_hours
    
    def get_result(self, key: str) -> Optional[Dict[str, Any]]:
        """Get stored result for idempotency key if it exists"""
        if key not in self.store:
            return None
        
        entry = self.store[key]
        created_at = entry['created_at']
        
        # Check if entry has expired
        if datetime.now() - created_at > timedelta(hours=self.expiration_hours):
            del self.store[key]
            return None
        
        return entry['result']
    
    def store_result(self, key: str, result: Dict[str, Any]) -> None:
        """Store result for an idempotency key"""
        self.store[key] = {
            'result': result,
            'created_at': datetime.now()
        }

class PaymentProcessor:
    def __init__(self):
        self.idempotency_store = IdempotencyStore()
        
    def generate_idempotency_key(self, payment_data: Dict[str, Any]) -> str:
        """Generate idempotency key from payment data"""
        # Include relevant fields that identify the unique transaction
        key_data = {
            'amount': payment_data['amount'],
            'currency': payment_data['currency'],
            'customer_id': payment_data['customer_id'],
            'payment_method_id': payment_data['payment_method_id'],
            # Use timestamp to hour precision to allow for retries within same hour
            'date': datetime.now().strftime('%Y-%m-%d-%H')
        }
        
        # Create deterministic JSON string (sort keys)
        key_json = json.dumps(key_data, sort_keys=True)
        
        # Hash the data to create the key
        key_hash = hashlib.sha256(key_json.encode()).hexdigest()
        return f"idempkey_{key_hash}"
        
    def process_payment(self, payment_data: Dict[str, Any], 
                        idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        """Process a payment with idempotency support"""
        # Generate an idempotency key if not provided
        if idempotency_key is None:
            idempotency_key = self.generate_idempotency_key(payment_data)
        
        # Check if we've already processed this request
        existing_result = self.idempotency_store.get_result(idempotency_key)
        if existing_result:
            return {**existing_result, 'idempotent': True}
        
        # Process the payment (simulated)
        result = self._do_process_payment(payment_data)
        
        # Store the result with the idempotency key
        self.idempotency_store.store_result(idempotency_key, result)
        
        return {**result, 'idempotent': False}
    
    def _do_process_payment(self, payment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Actual payment processing logic"""
        # In a real implementation, this would call the payment API
        return {
            'transaction_id': str(uuid.uuid4()),
            'status': 'succeeded',
            'amount': payment_data['amount'],
            'currency': payment_data['currency'],
            'processed_at': datetime.now().isoformat()
        }

# Example usage
processor = PaymentProcessor()

# First payment attempt
payment_data = {
    'amount': 100.00,
    'currency': 'USD',
    'customer_id': 'cus_123456',
    'payment_method_id': 'pm_789012',
    'description': 'Subscription payment'
}

# Use client-generated idempotency key
idempotency_key = 'idempkey_2023-04-15-cus_123456-100.00-USD'

# First attempt
result1 = processor.process_payment(payment_data, idempotency_key)
print(f"First attempt: {result1}")

# Second attempt (simulating a retry)
result2 = processor.process_payment(payment_data, idempotency_key)
print(f"Second attempt: {result2}")