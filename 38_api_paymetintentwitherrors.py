import requests
import json
import time
from datetime import datetime, timedelta
import stripe
from dotenv import load_dotenv
import os
import logging
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout
from urllib.parse import urlencode

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("payment_client")

# Configuration
load_dotenv()
API_KEY = os.getenv("STRIPE_API_KEY")
BASE_URL = "https://api.stripe.com/v1"

class StripeError(Exception):
    """Base exception for Stripe-related errors"""
    def __init__(self, message, error_code=None, http_status=None, raw_error=None):
        self.message = message
        self.error_code = error_code
        self.http_status = http_status
        self.raw_error = raw_error
        super().__init__(self.message)
        
class PaymentClient:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        })
    
    def _make_request(self, method, endpoint, **kwargs):
        """Make a request with error handling."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            logger.info(f"Making {method} request to {url}")
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            # Handle Stripe API errors with proper context
            if e.response.headers.get('content-type') == 'application/json':
                error_data = e.response.json()
                error_type = error_data.get('error', {}).get('type')
                error_message = error_data.get('error', {}).get('message', 'Unknown error')
                error_code = error_data.get('error', {}).get('code')
                
                logger.error(f"Stripe API error: {error_type} - {error_message}")
                
                raise StripeError(
                    message=error_message,
                    error_code=error_code,
                    http_status=e.response.status_code,
                    raw_error=error_data
                )
            else:
                logger.error(f"HTTP error: {e}")
                raise StripeError(f"HTTP error: {e}", http_status=e.response.status_code)
        except ConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise StripeError(f"Connection error: Failed to connect to Stripe API")
        except Timeout as e:
            logger.error(f"Timeout error: {e}")
            raise StripeError(f"Timeout error: Stripe API request timed out")
        except RequestException as e:
            logger.error(f"Request error: {e}")
            raise StripeError(f"Request error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise StripeError(f"Unexpected error: {e}")
    
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
        try:
            return self._make_request("GET", f"payment_intents/{payment_id}")
        except StripeError as e:
            logger.error(f"Failed to get payment status for {payment_id}: {e}")
            # Handle specific error cases
            if e.http_status == 404:
                logger.error(f"Payment intent {payment_id} not found")
            raise
    
    def list_transactions(self, start_date, end_date, limit=100):
        """List transactions within a specified date range."""
        params = {
            "created[gte]": int(start_date.timestamp()),
            "created[lte]": int(end_date.timestamp()),
            "limit": limit
        }
        try:
            return self._make_request("GET", "charges", params=params)
        except StripeError as e:
            logger.error(f"Failed to list transactions: {e}")
            raise
    
    def process_webhook(self, payload, signature_header, webhook_secret):
        """Verify and process a webhook notification."""
        try:
            # First, verify the webhook signature
            try:
                # If using stripe library
                event = stripe.Webhook.construct_event(
                    payload, signature_header, webhook_secret
                )
            except stripe.error.SignatureVerificationError as e:
                logger.error(f"Invalid signature: {e}")
                raise StripeError(f"Invalid webhook signature: {e}")
            
            # Process the event
            event_type = event.get("type")
            event_data = event.get("data", {}).get("object", {})
            
            logger.info(f"Processing webhook event: {event_type}")
            
            if event_type == "payment_intent.succeeded":
                self._handle_payment_success(event_data)
            elif event_type == "payment_intent.payment_failed":
                self._handle_payment_failure(event_data)
            else:
                logger.info(f"Unhandled event type: {event_type}")
            
            return {"status": "processed", "event_type": event_type}
            
        except StripeError as e:
            logger.error(f"Webhook processing error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected webhook error: {e}")
            raise StripeError(f"Unexpected webhook error: {e}")
    
    def _handle_payment_success(self, payment_data):
        try:
            payment_id = payment_data.get('id', 'unknown')
            logger.info(f"Payment succeeded: {payment_id}")
            # Update database, send confirmation email, etc.
        except Exception as e:
            logger.error(f"Error handling payment success: {e}")
            # Don't raise - we don't want to fail the webhook processing
    
    def _handle_payment_failure(self, payment_data):
        try:
            payment_id = payment_data.get('id', 'unknown')
            logger.info(f"Payment failed: {payment_id}")
            failure_message = payment_data.get('last_payment_error', {}).get('message', 'Unknown reason')
            logger.info(f"Failure reason: {failure_message}")
            # Log failure, notify user, etc.
        except Exception as e:
            logger.error(f"Error handling payment failure: {e}")
            # Don't raise - we don't want to fail the webhook processing

# Usage example
if __name__ == "__main__":
    client = PaymentClient(API_KEY, BASE_URL)
    
    try:
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
        
    except StripeError as e:
        print(f"Stripe error occurred: {e.message}")
        if e.error_code:
            print(f"Error code: {e.error_code}")
        if e.http_status:
            print(f"HTTP status: {e.http_status}")
    except Exception as e:
        print(f"Unexpected error: {e}")