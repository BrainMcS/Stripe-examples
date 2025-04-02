from flask import Flask, request, jsonify
import stripe
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)


# Load environment variables from .env file
load_dotenv()

# Stripe configuration
stripe.api_key = os.getenv('STRIPE_API_KEY')
endpoint_secret = "whsec_your_signing_secret"

# Mock database function - in a real app, this would use a database
def update_database(event_type, event_data):
    """
    Update the database with event information
    
    Args:
        event_type (str): The type of event
        event_data (dict): The data associated with the event
    """
    logger.info(f"Updating database for {event_type}")
    
    # In a real application, you would use an actual database
    # db.execute(
    #     "INSERT INTO stripe_events (event_id, event_type, customer_id, data, created_at) VALUES (?, ?, ?, ?, ?)",
    #     event_data['id'], event_type, event_data.get('customer', 'none'), json.dumps(event_data), datetime.now()
    # )
    
    logger.info(f"Database updated for event: {event_data['id']}")

# Mock email sending function - in a real app, use a proper email service
def send_email(email, subject, message):
    """
    Send an email to a customer
    
    Args:
        email (str): Recipient email address
        subject (str): Email subject
        message (str): Email message body
    """
    logger.info(f"Sending email to {email}")
    logger.info(f"Subject: {subject}")
    logger.info(f"Message: {message}")
    
    # In a real application, you would use an email service like SendGrid
    # sendgrid.send_email(
    #     to=email,
    #     subject=subject,
    #     body=message
    # )
    
    logger.info(f"Email sent to {email}")


@app.route('/webhook', methods=['POST'])
def webhook():
    payload = request.data.decode('utf-8')
    sig_header = request.headers.get('Stripe-Signature')
    event = None
    
    try:
        # Verify webhook signature
        event = stripe.Webhook.construct_event(
            payload, sig_header, endpoint_secret
        )
    except ValueError as e:
        # Invalid payload
        logger.error(f"Invalid payload: {e}")
        return jsonify({'error': 'Invalid payload'}), 400
    except stripe.error.SignatureVerificationError as e:
        # Invalid signature
        logger.error(f"Invalid signature: {e}")
        return jsonify({'error': 'Invalid signature'}), 400
    
    # Handle the event
    logger.info(f"Received event: {event['type']}")
    
    if event['type'] == 'charge.succeeded':
        handle_successful_charge(event['data']['object'])
    elif event['type'] == 'invoice.payment_failed':
        handle_failed_payment(event['data']['object'])
    else:
        logger.info(f"Unhandled event type: {event['type']}")
    
    # Always update the database for auditing
    update_database(event['type'], event['data']['object'])
    
    return jsonify({'status': 'success'}), 200


def handle_successful_charge(charge):
    """
    Handle a successful charge event
    
    Args:
        charge (dict): The charge object from the event
    """
    logger.info(f"Processing successful charge: {charge['id']}")
    
    # Get customer information
    customer_id = charge.get('customer')
    if not customer_id:
        logger.warning(f"No customer ID in charge {charge['id']}")
        return
    
    try:
        customer = stripe.Customer.retrieve(customer_id)
        email = customer.email
        
        # Format amount with proper decimal places
        amount = charge['amount'] / 100.0  # Convert cents to dollars
        currency = charge['currency'].upper()
        
        # Send confirmation email
        subject = f"Payment Confirmation - {currency} {amount:.2f}"
        message = f"""
Hello {customer.name},

Thank you for your payment of {currency} {amount:.2f}.

Payment details:
- Transaction ID: {charge['id']}
- Date: {datetime.fromtimestamp(charge['created']).strftime('%Y-%m-%d %H:%M:%S')}
- Payment method: {charge['payment_method_details']['type']}
- Last 4 digits: {charge['payment_method_details']['card']['last4']}

Thank you for your business!

Best regards,
The Example Company Team
        """
        
        send_email(email, subject, message)
        logger.info(f"Sent confirmation email to {email}")
        
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error: {e}")
    except Exception as e:
        logger.error(f"Error handling successful charge: {e}")


def handle_failed_payment(invoice):
    """
    Handle a failed payment event
    
    Args:
        invoice (dict): The invoice object from the event
    """
    logger.info(f"Processing failed payment for invoice: {invoice['id']}")
    
    # Get customer information
    customer_id = invoice.get('customer')
    if not customer_id:
        logger.warning(f"No customer ID in invoice {invoice['id']}")
        return
    
    try:
        customer = stripe.Customer.retrieve(customer_id)
        email = customer.email
        
        # Get failure details
        payment_intent = None
        if invoice.get('payment_intent'):
            payment_intent = stripe.PaymentIntent.retrieve(invoice['payment_intent'])
        
        # Get error message
        error_message = "Your payment method didn't work"
        if payment_intent and payment_intent.get('last_payment_error'):
            error_message = payment_intent['last_payment_error'].get('message', error_message)
        
        # Format amount with proper decimal places
        amount = invoice['amount_due'] / 100.0  # Convert cents to dollars
        currency = invoice['currency'].upper()
        
        # Determine next steps for customer
        next_attempt = "We'll automatically try again in a few days."
        if invoice.get('next_payment_attempt'):
            attempt_date = datetime.fromtimestamp(invoice['next_payment_attempt'])
            next_attempt = f"We'll automatically try again on {attempt_date.strftime('%B %d, %Y')}."
        
        # Send alert email
        subject = f"Payment Failed - Action Required"
        message = f"""
Hello {customer.name},

We couldn't process your payment of {currency} {amount:.2f}.

Reason: {error_message}

Invoice ID: {invoice['id']}
Date: {datetime.fromtimestamp(invoice['created']).strftime('%Y-%m-%d %H:%M:%S')}

{next_attempt}

To update your payment information, please visit:
https://example.com/billing/update

If you have any questions, please reply to this email.

Best regards,
The Example Company Team
        """
        
        send_email(email, subject, message)
        logger.info(f"Sent payment failure email to {email}")
        
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error: {e}")
    except Exception as e:
        logger.error(f"Error handling failed payment: {e}")


if __name__ == '__main__':
    app.run(port=5000, debug=True)