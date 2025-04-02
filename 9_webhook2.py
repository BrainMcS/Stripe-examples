from flask import Flask, request, jsonify
import stripe
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('stripe_webhooks')

app = Flask(__name__)

# Stripe configuration
stripe.api_key = os.environ.get('STRIPE_API_KEY')
webhook_secret = os.environ.get('STRIPE_WEBHOOK_SECRET')

# Mock database functions
def update_payment_status(payment_id, status, metadata=None):
    """Update payment status in the database"""
    logger.info(f"Updating payment {payment_id} to status: {status}")
    # In a real application, this would update a database record
    return True

def record_invoice_failure(invoice_id, customer_id, payment_intent_id, failure_reason):
    """Record a failed invoice payment"""
    logger.info(f"Recording invoice failure: {invoice_id} for customer {customer_id}")
    # In a real application, this would create a database record
    return True

def update_subscription_status(subscription_id, status, metadata=None):
    """Update subscription status in the database"""
    logger.info(f"Updating subscription {subscription_id} to status: {status}")
    # In a real application, this would update a database record
    return True

def send_email_notification(email, subject, body):
    """Send an email notification"""
    logger.info(f"Sending email to {email}: {subject}")
    # In a real application, this would send an actual email
    return True

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle Stripe webhook events"""
    payload = request.data
    sig_header = request.headers.get('Stripe-Signature')
    event = None

    try:
        # Verify the event with the signature
        event = stripe.Webhook.construct_event(
            payload, sig_header, webhook_secret
        )
    except ValueError as e:
        # Invalid payload
        logger.error(f"Invalid payload: {e}")
        return jsonify({'status': 'error', 'message': 'Invalid payload'}), 400
    except stripe.error.SignatureVerificationError as e:
        # Invalid signature
        logger.error(f"Invalid signature: {e}")
        return jsonify({'status': 'error', 'message': 'Invalid signature'}), 400

    # Handle the event based on its type
    event_type = event['type']
    event_data = event['data']['object']
    
    logger.info(f"Processing {event_type} event: {event['id']}")

    try:
        if event_type == 'charge.succeeded':
            handle_charge_succeeded(event_data)
        elif event_type == 'invoice.payment_failed':
            handle_invoice_payment_failed(event_data)
        elif event_type == 'customer.subscription.updated':
            handle_subscription_updated(event_data)
        elif event_type == 'customer.subscription.deleted':
            handle_subscription_deleted(event_data)
        else:
            logger.info(f"Unhandled event type: {event_type}")
        
        # Always store the entire event for audit purposes
        store_event(event)
        
        return jsonify({'status': 'success'}), 200
    
    except Exception as e:
        logger.error(f"Error processing event {event['id']}: {str(e)}")
        # In a production environment, you might want to 
        # return a 200 status even on error to prevent Stripe from retrying
        return jsonify({'status': 'error', 'message': str(e)}), 200

def handle_charge_succeeded(charge):
    """Process a successful charge event"""
    payment_id = charge['id']
    amount = charge['amount'] / 100.0  # Convert cents to dollars
    currency = charge['currency'].upper()
    customer_id = charge.get('customer')
    payment_intent = charge.get('payment_intent')
    
    # Update the payment status in our database
    update_payment_status(payment_id, 'succeeded', {
        'amount': amount,
        'currency': currency,
        'customer_id': customer_id,
        'payment_intent_id': payment_intent,
        'processed_at': datetime.now().isoformat()
    })
    
    # If this is associated with a customer, send a confirmation email
    if customer_id:
        try:
            customer = stripe.Customer.retrieve(customer_id)
            if customer.get('email'):
                subject = f"Payment Confirmation - {currency} {amount:.2f}"
                body = f"""
                Dear {customer.get('name', 'Valued Customer')},
                
                We've received your payment of {currency} {amount:.2f}.
                
                Thank you for your business!
                
                Best regards,
                Your Company
                """
                send_email_notification(customer['email'], subject, body)
        except Exception as e:
            logger.error(f"Error sending confirmation email: {str(e)}")
    
    logger.info(f"Successfully processed charge: {payment_id}")

def handle_invoice_payment_failed(invoice):
    """Process a failed invoice payment event"""
    invoice_id = invoice['id']
    customer_id = invoice.get('customer')
    payment_intent_id = invoice.get('payment_intent')
    
    # Get failure details
    failure_reason = "Payment method declined"
    if payment_intent_id:
        try:
            payment_intent = stripe.PaymentIntent.retrieve(payment_intent_id)
            last_payment_error = payment_intent.get('last_payment_error')
            if last_payment_error:
                failure_reason = last_payment_error.get('message', failure_reason)
        except Exception as e:
            logger.error(f"Error retrieving payment intent details: {str(e)}")
    
    # Record the failure in our database
    record_invoice_failure(invoice_id, customer_id, payment_intent_id, failure_reason)
    
    # Notify the customer
    if customer_id:
        try:
            customer = stripe.Customer.retrieve(customer_id)
            if customer.get('email'):
                amount = invoice['amount_due'] / 100.0
                currency = invoice['currency'].upper()
                
                subject = "Action Required: Payment Failed"
                body = f"""
                Dear {customer.get('name', 'Valued Customer')},
                
                We were unable to process your payment of {currency} {amount:.2f}.
                
                Reason: {failure_reason}
                
                Please update your payment method as soon as possible to avoid service interruption.
                
                Need help? Reply to this email or contact our support team.
                
                Best regards,
                Your Company
                """
                send_email_notification(customer['email'], subject, body)
        except Exception as e:
            logger.error(f"Error sending payment failure notification: {str(e)}")
    
    logger.info(f"Processed invoice payment failure: {invoice_id}")

def handle_subscription_updated(subscription):
    """Process a subscription update event"""
    subscription_id = subscription['id']
    customer_id = subscription.get('customer')
    status = subscription.get('status')
    
    # Get relevant subscription details
    current_period_end = datetime.fromtimestamp(subscription.get('current_period_end', 0))
    items = subscription.get('items', {}).get('data', [])
    product_ids = []
    
    for item in items:
        if item.get('price') and item['price'].get('product'):
            product_ids.append(item['price']['product'])
    
    # Update subscription in our database
    update_subscription_status(subscription_id, status, {
        'current_period_end': current_period_end.isoformat(),
        'product_ids': product_ids,
        'updated_at': datetime.now().isoformat()
    })
    
    # Notify customer about important status changes
    if customer_id and status in ['past_due', 'unpaid', 'canceled']:
        try:
            customer = stripe.Customer.retrieve(customer_id)
            if customer.get('email'):
                subject_map = {
                    'past_due': "Action Required: Subscription Payment Past Due",
                    'unpaid': "Urgent: Subscription Payment Failed",
                    'canceled': "Subscription Canceled"
                }
                
                body_map = {
                    'past_due': "Your subscription payment is past due. Please update your payment method.",
                    'unpaid': "We've been unable to collect payment for your subscription after multiple attempts.",
                    'canceled': "Your subscription has been canceled."
                }
                
                subject = subject_map.get(status, f"Subscription Status Change: {status}")
                body = f"""
                Dear {customer.get('name', 'Valued Customer')},
                
                {body_map.get(status, f"Your subscription status has changed to: {status}")}
                
                Subscription ID: {subscription_id}
                
                If you have any questions, please contact our support team.
                
                Best regards,
                Your Company
                """
                send_email_notification(customer['email'], subject, body)
        except Exception as e:
            logger.error(f"Error sending subscription update notification: {str(e)}")
    
    logger.info(f"Processed subscription update: {subscription_id}")

def handle_subscription_deleted(subscription):
    """Process a subscription deletion event"""
    subscription_id = subscription['id']
    customer_id = subscription.get('customer')
    canceled_at = datetime.fromtimestamp(subscription.get('canceled_at', 0))
    
    # Update our records
    update_subscription_status(subscription_id, 'canceled', {
        'canceled_at': canceled_at.isoformat(),
        'updated_at': datetime.now().isoformat()
    })
    
    # Optional: Trigger any cleanup tasks or final emails here
    
    logger.info(f"Processed subscription deletion: {subscription_id}")

def store_event(event):
    """Store the full event for audit purposes"""
    event_id = event['id']
    event_type = event['type']
    created = datetime.fromtimestamp(event['created'])
    
    # In a real app, store this in a database
    logger.info(f"Storing event {event_id} of type {event_type} created at {created}")

if __name__ == '__main__':
    app.run(port=5000)