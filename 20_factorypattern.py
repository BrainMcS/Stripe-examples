from abc import ABC, abstractmethod
from typing import Dict, Any, Type, Optional

# Abstract Product
class PaymentMethod(ABC):
    @abstractmethod
    def process_payment(self, amount: float) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        pass

# Concrete Products
class CreditCardPayment(PaymentMethod):
    def __init__(self, card_number: str, expiry: str, cvv: str, **kwargs):
        self.card_number = card_number
        self.expiry = expiry
        self.cvv = cvv
        self.kwargs = kwargs
    
    def process_payment(self, amount: float) -> Dict[str, Any]:
        # Credit card processing logic
        return {
            'method': 'credit_card',
            'amount': amount,
            'status': 'succeeded',
            'transaction_id': 'cc_tx_123456'
        }
    
    def validate(self) -> bool:
        # Validate credit card details
        return (len(self.card_number) >= 15 and 
                len(self.expiry) == 5 and 
                len(self.cvv) >= 3)

class PayPalPayment(PaymentMethod):
    def __init__(self, email: str, token: str, **kwargs):
        self.email = email
        self.token = token
        self.kwargs = kwargs
    
    def process_payment(self, amount: float) -> Dict[str, Any]:
        # PayPal processing logic
        return {
            'method': 'paypal',
            'amount': amount,
            'status': 'succeeded',
            'transaction_id': 'pp_tx_789012'
        }
    
    def validate(self) -> bool:
        # Validate PayPal details
        return '@' in self.email and len(self.token) > 10

class BankTransferPayment(PaymentMethod):
    def __init__(self, account_number: str, routing_number: str, **kwargs):
        self.account_number = account_number
        self.routing_number = routing_number
        self.kwargs = kwargs
    
    def process_payment(self, amount: float) -> Dict[str, Any]:
        # Bank transfer processing logic
        return {
            'method': 'bank_transfer',
            'amount': amount,
            'status': 'pending',  # Bank transfers typically start as pending
            'transaction_id': 'bt_tx_345678'
        }
    
    def validate(self) -> bool:
        # Validate bank details
        return (len(self.account_number) > 5 and 
                len(self.routing_number) == 9)

# Factory Class
class PaymentMethodFactory:
    _methods: Dict[str, Type[PaymentMethod]] = {
        'credit_card': CreditCardPayment,
        'paypal': PayPalPayment,
        'bank_transfer': BankTransferPayment
    }
    
    @classmethod
    def register_payment_method(cls, method_type: str, method_class: Type[PaymentMethod]) -> None:
        """Register a new payment method type"""
        cls._methods[method_type] = method_class
    
    @classmethod
    def create_payment_method(cls, method_type: str, **kwargs) -> Optional[PaymentMethod]:
        """Create a payment method instance of the specified type"""
        payment_method_class = cls._methods.get(method_type)
        
        if not payment_method_class:
            raise ValueError(f"Unsupported payment method: {method_type}")
        
        return payment_method_class(**kwargs)

# Example usage
def process_customer_payment(payment_type: str, payment_details: Dict[str, Any], amount: float) -> Dict[str, Any]:
    try:
        # Create payment method using factory
        payment_method = PaymentMethodFactory.create_payment_method(payment_type, **payment_details)
        
        # Validate payment details
        if not payment_method.validate():
            return {
                'status': 'failed',
                'error': 'Invalid payment details'
            }
        
        # Process the payment
        result = payment_method.process_payment(amount)
        return result
        
    except ValueError as e:
        return {
            'status': 'failed',
            'error': str(e)
        }
    except Exception as e:
        return {
            'status': 'failed',
            'error': f"Unexpected error: {str(e)}"
        }

# Process different payment types
credit_card_result = process_customer_payment(
    'credit_card',
    {
        'card_number': '4242424242424242',
        'expiry': '12/25',
        'cvv': '123',
        'cardholder_name': 'John Doe'
    },
    99.99
)
print(f"Credit Card Payment: {credit_card_result}")

paypal_result = process_customer_payment(
    'paypal',
    {
        'email': 'customer@example.com',
        'token': 'paypal_token_123456789'
    },
    49.99
)
print(f"PayPal Payment: {paypal_result}")

bank_transfer_result = process_customer_payment(
    'bank_transfer',
    {
        'account_number': '123456789',
        'routing_number': '987654321',
        'account_name': 'John Doe'
    },
    199.99
)
print(f"Bank Transfer Payment: {bank_transfer_result}")