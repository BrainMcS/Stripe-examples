from abc import ABC, abstractmethod
from typing import List, Dict, Any

# Observer Interface
class PaymentObserver(ABC):
    @abstractmethod
    def update(self, event_type: str, payment_data: Dict[str, Any]) -> None:
        pass

# Observable (Subject)
class PaymentProcessor:
    def __init__(self):
        self._observers: List[PaymentObserver] = []
    
    def register_observer(self, observer: PaymentObserver) -> None:
        if observer not in self._observers:
            self._observers.append(observer)
    
    def remove_observer(self, observer: PaymentObserver) -> None:
        self._observers.remove(observer)
    
    def notify_observers(self, event_type: str, payment_data: Dict[str, Any]) -> None:
        for observer in self._observers:
            observer.update(event_type, payment_data)
    
    def process_payment(self, payment_data: Dict[str, Any]) -> Dict[str, Any]:
        # Payment processing logic
        result = self._charge_payment(payment_data)
        
        # Notify observers about the payment event
        self.notify_observers('payment.processed', {
            **payment_data,
            'result': result
        })
        
        return result
    
    def _charge_payment(self, payment_data: Dict[str, Any]) -> Dict[str, Any]:
        # Simulate actual payment processing
        return {
            'transaction_id': 'tx_123456',
            'status': 'succeeded',
            'amount': payment_data['amount']
        }

# Concrete Observers
class EmailNotifier(PaymentObserver):
    def update(self, event_type: str, payment_data: Dict[str, Any]) -> None:
        if event_type == 'payment.processed' and payment_data['result']['status'] == 'succeeded':
            self._send_payment_confirmation_email(payment_data)
    
    def _send_payment_confirmation_email(self, payment_data: Dict[str, Any]) -> None:
        customer_email = payment_data.get('customer_email')
        amount = payment_data.get('amount')
        print(f"Email sent to {customer_email}: Payment of ${amount} was successful.")

class InventoryManager(PaymentObserver):
    def update(self, event_type: str, payment_data: Dict[str, Any]) -> None:
        if event_type == 'payment.processed' and payment_data['result']['status'] == 'succeeded':
            self._update_inventory(payment_data)
    
    def _update_inventory(self, payment_data: Dict[str, Any]) -> None:
        product_id = payment_data.get('product_id')
        print(f"Inventory updated for product {product_id}")

class FraudDetector(PaymentObserver):
    def update(self, event_type: str, payment_data: Dict[str, Any]) -> None:
        self._check_for_fraud(event_type, payment_data)
    
    def _check_for_fraud(self, event_type: str, payment_data: Dict[str, Any]) -> None:
        if event_type == 'payment.processed':
            # Fraud detection logic
            ip_address = payment_data.get('ip_address', 'unknown')
            amount = payment_data.get('amount', 0)
            
            if amount > 1000 and ip_address.startswith('192.'):
                print(f"FRAUD ALERT: Large payment of ${amount} from suspicious IP {ip_address}")

# Example usage
processor = PaymentProcessor()
processor.register_observer(EmailNotifier())
processor.register_observer(InventoryManager())
processor.register_observer(FraudDetector())

# Process a payment
payment_data = {
    'amount': 1500,
    'currency': 'USD',
    'customer_id': 'cus_123456',
    'customer_email': 'customer@example.com',
    'product_id': 'prod_789',
    'ip_address': '192.168.1.1'
}

result = processor.process_payment(payment_data)