import stripe
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set your Stripe API key
stripe.api_key = os.getenv("STRIPE_API_KEY")

def create_stripe_customer_and_subscription(email, name, payment_method_id, price_id):
    """
    Create a customer in Stripe, attach a payment method, and subscribe them to a product/price.
    
    Args:
        email (str): Customer's email address
        name (str): Customer's name
        payment_method_id (str): ID of the payment method to attach
        price_id (str): Stripe Price ID for the subscription
        
    Returns:
        dict: Information about the created customer and subscription
    """
    try:
        # 1. Create a customer
        print(f"Creating customer for {email}...")
        customer = stripe.Customer.create(
            email=email,
            name=name,
            metadata={
                'created_by': 'technical_interview',
                'created_at': datetime.now().isoformat()
            }
        )
        customer_id = customer.id
        print(f"Customer created with ID: {customer_id}")
        
        # 2. Attach the payment method to the customer
        print("Attaching payment method...")
        stripe.PaymentMethod.attach(
            payment_method_id,
            customer=customer_id
        )
        
        # 3. Set this payment method as the default for the customer
        stripe.Customer.modify(
            customer_id,
            invoice_settings={
                'default_payment_method': payment_method_id
            }
        )
        print("Payment method attached and set as default")
        
        # 4. Create the subscription
        print(f"Creating subscription to price: {price_id}...")
        subscription = stripe.Subscription.create(
            customer=customer_id,
            items=[
                {'price': price_id}
            ],
            expand=['latest_invoice.payment_intent'],
            metadata={
                'created_by': 'technical_interview',
                'created_at': datetime.now().isoformat()
            }
        )
        
        subscription_status = subscription.status
        print(f"Subscription created with status: {subscription_status}")
        
        # 5. Return the results
        return {
            'customer_id': customer_id,
            'subscription_id': subscription.id,
            'subscription_status': subscription_status,
            'current_period_end': datetime.fromtimestamp(subscription.current_period_end).isoformat()
        }
        
    except stripe.error.CardError as e:
        # Since it's a decline, stripe.error.CardError will be caught
        print(f"Card error: {e.error.message}")
        return {'error': 'payment_failed', 'message': e.error.message}
        
    except stripe.error.RateLimitError as e:
        # Too many requests made to the API too quickly
        print(f"Rate limit error: {e}")
        return {'error': 'rate_limit', 'message': 'Please try again later'}
        
    except stripe.error.InvalidRequestError as e:
        # Invalid parameters were supplied to Stripe's API
        print(f"Invalid request error: {e}")
        return {'error': 'invalid_request', 'message': str(e)}
        
    except stripe.error.AuthenticationError as e:
        # Authentication with Stripe's API failed
        print(f"Authentication error: {e}")
        return {'error': 'authentication', 'message': 'API key issue'}
        
    except stripe.error.APIConnectionError as e:
        # Network communication with Stripe failed
        print(f"API connection error: {e}")
        return {'error': 'connectivity', 'message': 'Network issue, please try again'}
        
    except stripe.error.StripeError as e:
        # Display a very generic error to the user, and maybe send
        # yourself an email
        print(f"Stripe error: {e}")
        return {'error': 'stripe_error', 'message': 'Something went wrong with the payment'}
        
    except Exception as e:
        # Something else happened, completely unrelated to Stripe
        print(f"Unexpected error: {e}")
        return {'error': 'unknown', 'message': 'An unexpected error occurred'}


# Example usage
if __name__ == "__main__":
    # In a real scenario, these would come from a form or other input
    customer_email = "test@example.com"
    customer_name = "Test Customer"
    payment_method = "pm_card_visa"  # This would be collected from Elements in production
    price = "price_1234567890"  # This would be the actual price ID from your Stripe account
    
    result = create_stripe_customer_and_subscription(
        customer_email, 
        customer_name, 
        payment_method, 
        price
    )
    
    print("\nResult:")
    for key, value in result.items():
        print(f"{key}: {value}")