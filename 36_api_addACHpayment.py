import requests
import os

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
    
    def create_payment_intent(self, amount, currency, payment_method_types=None, description=None):
        """Create a payment intent for the specified amount and currency."""
        payload = {
            "amount": amount,
            "currency": currency,
        }
        
        if payment_method_types:
            payload["payment_method_types[]"] = payment_method_types
        else:
            payload["payment_method_types[]"] = ["card", "sepa_debit"]
        
        if description:
            payload["description"] = description
        
        response = self.session.post(f"{self.base_url}/payment_intents", data=payload)
        response.raise_for_status()
        return response.json()
    
    def create_sepa_payment_method(self, iban, name):
        """Create a SEPA Direct Debit PaymentMethod."""
        payload = {
            "type": "sepa_debit",
            "sepa_debit[iban]": iban,
            "billing_details[name]": name,
        }
        
        response = self.session.post(f"{self.base_url}/payment_methods", data=payload)
        response.raise_for_status()
        return response.json()

if __name__ == "__main__":
    API_KEY = os.getenv('STRIPE_API_KEY')
    BASE_URL = os.getenv('STRIPE_BASE_URL', 'https://api.stripe.com/v1')

    client = PaymentProcessor(API_KEY, BASE_URL)

    try:
        # Create a SEPA Direct Debit PaymentMethod
        sepa_payment_method = client.create_sepa_payment_method(
            iban="DE89370400440532013000",  # Test IBAN
            name="Jenny Rosen"
        )
        print(f"Created SEPA PaymentMethod: {sepa_payment_method['id']}")

        # Create a PaymentIntent for SEPA Direct Debit
        intent = client.create_payment_intent(
            amount=2000,  # Amount in cents (20.00 EUR)
            currency="eur",
            payment_method_types=["sepa_debit"],
            description="SEPA Direct Debit payment test"
        )
        print(f"Created PaymentIntent: {intent['id']}")

        # Confirm the PaymentIntent with the SEPA PaymentMethod
        confirm_payload = {
            "payment_method": sepa_payment_method['id']
        }
        confirmed_intent = client.session.post(f"{client.base_url}/payment_intents/{intent['id']}/confirm", data=confirm_payload)
        confirmed_intent.raise_for_status()
        print(f"Confirmed PaymentIntent: {confirmed_intent.json()['id']}")
        
        card_payment = client.create_card_payment(
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
        ach_payment = client.create_ach_payment(
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

    except requests.exceptions.RequestException as e:
        print(f"Error: {str(e)}")
        if e.response:
            print(f"Error details: {e.response.text}")