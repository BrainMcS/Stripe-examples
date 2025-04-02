import requests

class PaymentProcessor:
    def __init__(self, api_key, base_url="https://api.example.com/v1"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })
    
    def create_payment(self, amount, currency, payment_method=None, card_details=None, 
                      bank_details=None, description=None):
        """
        Create a payment using various payment methods.
        
        Args:
            amount: Amount in cents
            currency: Three-letter currency code
            payment_method: Optional existing payment method ID
            card_details: Dict with card info (number, exp_month, exp_year, cvc)
            bank_details: Dict with bank info (account_number, routing_number, account_holder_name)
            description: Optional payment description
            
        Returns:
            Payment details
        """
        payload = {
            "amount": amount,
            "currency": currency,
        }
        
        # Handle different payment method types
        if payment_method:
            payload["payment_method"] = payment_method
        elif card_details:
            payload["source"] = {
                "type": "card",
                "card": card_details
            }
        elif bank_details:
            payload["source"] = {
                "type": "ach_debit",
                "ach_debit": bank_details
            }
        else:
            raise ValueError("Must provide either payment_method, card_details, or bank_details")
        
        if description:
            payload["description"] = description
        
        response = self.session.post(f"{self.base_url}/payments", json=payload)
        response.raise_for_status()
        return response.json()
    
    def create_card_payment(self, amount, currency, card_details, description=None):
        """Convenience method for card payments"""
        return self.create_payment(
            amount=amount,
            currency=currency,
            card_details=card_details,
            description=description
        )
    
    def create_ach_payment(self, amount, currency, bank_details, description=None):
        """
        Create a payment using ACH bank transfer.
        
        Args:
            amount: Amount in cents
            currency: Three-letter currency code (must be 'usd' for ACH)
            bank_details: Dict with bank info (account_number, routing_number, account_holder_name)
            description: Optional payment description
            
        Returns:
            Payment details
        """
        if currency.lower() != 'usd':
            raise ValueError("ACH payments only support USD currency")
        
        # Validate bank details
        required_fields = ['account_number', 'routing_number', 'account_holder_name']
        for field in required_fields:
            if field not in bank_details:
                raise ValueError(f"Missing required field for ACH payment: {field}")
        
        return self.create_payment(
            amount=amount,
            currency=currency,
            bank_details=bank_details,
            description=description
        )

# Example usage
processor = PaymentProcessor("sk_test_your_api_key")

# Create a card payment
card_payment = processor.create_card_payment(
    amount=2000,  # $20.00
    currency="usd",
    card_details={
        "number": "4242424242424242",
        "exp_month": 12,
        "exp_year": 2023,
        "cvc": "123"
    },
    description="Card payment test"
)

# Create an ACH payment
ach_payment = processor.create_ach_payment(
    amount=5000,  # $50.00
    currency="usd",
    bank_details={
        "account_number": "000123456789",
        "routing_number": "110000000",
        "account_holder_name": "John Doe",
        "account_type": "checking"
    },
    description="ACH payment test"
)