import requests
import json
import time
from datetime import datetime, timedelta
import stripe
from dotenv import load_dotenv
import os
from urllib.parse import urlencode

# Configuration
load_dotenv()
API_KEY = os.getenv("STRIPE_API_KEY")
BASE_URL = "https://api.stripe.com/v1"

class PaymentClient:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/x-www-form-urlencoded"
        })
    
    def create_payment_intent(self, amount, currency, description=None):
        """Create a payment intent for the specified amount and currency."""
        payload = {
            "amount": amount,
            "currency": currency,
            "payment_method_types[]": "card"
        }
        if description:
            payload["description"] = description
            
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        response = self.session.post(
            f"{self.base_url}/payment_intents",
            data=urlencode(payload),
            headers=headers
        )
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Error response content: {response.text}")
            raise
        return response.json()
    
    def get_payment_status(self, payment_id):
        """Retrieve the current status of a payment."""
        response = self.session.get(f"{self.base_url}/payment_intents/{payment_id}")
        response.raise_for_status()
        return response.json()
    
    def list_transactions(self, start_date, end_date, limit=100):
        """List transactions within a specified date range."""
        params = {
            "created[gte]": int(start_date.timestamp()),
            "created[lte]": int(end_date.timestamp()),
            "limit": limit
        }
        response = self.session.get(f"{self.base_url}/charges", params=params)
        response.raise_for_status()
        return response.json()
    
    def process_webhook(self, payload, signature_header, webhook_secret):
        """Verify and process a webhook notification."""
        # In a real implementation, you would verify the signature
        # using HMAC with the webhook secret
        
        event_type = payload.get("type")
        event_data = payload.get("data", {}).get("object", {})
        
        if event_type == "payment_intent.succeeded":
            self._handle_payment_success(event_data)
        elif event_type == "payment_intent.payment_failed":
            self._handle_payment_failure(event_data)
        
        return {"status": "processed", "event_type": event_type}
    
    def _handle_payment_success(self, payment_data):
        print(f"Payment succeeded: {payment_data['id']}")
        # Update database, send confirmation email, etc.
    
    def _handle_payment_failure(self, payment_data):
        print(f"Payment failed: {payment_data['id']}")
        # Log failure, notify user, etc.

# Usage example
if __name__ == "__main__":
    client = PaymentClient(API_KEY, BASE_URL)
    
    # Create a payment intent
    intent = client.create_payment_intent(2000, "usd", "Subscription payment")
    print(f"Created payment intent: {intent['id']}")
    
    # Check payment status
    status = client.get_payment_status(intent['id'])
    print(f"Payment status: {status['status']}")
    
    # List recent transactions
    now = datetime.now()
    week_ago = now - timedelta(days=7)
    transactions = client.list_transactions(week_ago, now)
    print(f"Found {len(transactions['data'])} transactions in the last week")