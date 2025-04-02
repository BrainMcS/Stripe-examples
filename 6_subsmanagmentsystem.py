"""
Subscription Management System Design

This module outlines the key components and classes for a subscription management
system that integrates with Stripe. The design focuses on managing payment methods,
subscriptions, and sending notifications for upcoming renewals.

Note: This is a design document with sample class implementations, not a complete
working system.
"""

import stripe
import datetime
import logging
from enum import Enum
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

# Configuration and Setup
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "whsec_your_signing_secret")
RENEWAL_NOTIFICATION_DAYS = [30, 7, 1]  # Days before renewal to send notifications
DEFAULT_CURRENCY = "usd"
ENABLE_AUTOMATIC_TAX = True
ENABLE_PRORATIONS = True
LOG_LEVEL = logging.INFO

# Initialize logging

# Initialize logging
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("subscription_system")


# Enums for various status types
class SubscriptionStatus(Enum):
    ACTIVE = "active"
    PAST_DUE = "past_due"
    UNPAID = "unpaid"
    CANCELED = "canceled"
    TRIALING = "trialing"
    ENDED = "ended"


class PaymentMethodStatus(Enum):
    VALID = "valid"
    EXPIRED = "expired"
    FAILED = "failed"
    CANCELED = "canceled"


class NotificationType(Enum):
    RENEWAL_REMINDER = "renewal_reminder"
    PAYMENT_FAILED = "payment_failed"
    SUBSCRIPTION_CREATED = "subscription_created"
    SUBSCRIPTION_CANCELED = "subscription_canceled"
    PAYMENT_METHOD_EXPIRING = "payment_method_expiring"
    PAYMENT_METHOD_UPDATED = "payment_method_updated"


# Data Models
@dataclass
class Customer:
    """Customer information"""
    id: str
    stripe_customer_id: str
    email: str
    name: str
    company_name: Optional[str] = None
    created_at: datetime.datetime = datetime.datetime.now()
    updated_at: datetime.datetime = datetime.datetime.now()
    metadata: Dict[str, Any] = None


@dataclass
class PaymentMethod:
    """Payment method information"""
    id: str
    customer_id: str
    stripe_payment_method_id: str
    type: str  # card, sepa_debit, etc.
    status: PaymentMethodStatus
    last_four: str
    expiry_month: Optional[int] = None
    expiry_year: Optional[int] = None
    created_at: datetime.datetime = datetime.datetime.now()
    updated_at: datetime.datetime = datetime.datetime.now()
    is_default: bool = False
    metadata: Dict[str, Any] = None


@dataclass
class Subscription:
    """Subscription information"""
    id: str
    customer_id: str
    stripe_subscription_id: str
    status: SubscriptionStatus
    current_period_start: datetime.datetime
    current_period_end: datetime.datetime
    items: List[Dict[str, Any]]  # List of subscription items with prices
    cancel_at_period_end: bool = False
    created_at: datetime.datetime = datetime.datetime.now()
    updated_at: datetime.datetime = datetime.datetime.now()
    payment_method_id: Optional[str] = None
    metadata: Dict[str, Any] = None
    tax_ids: List[str] = None


@dataclass
class SubscriptionPlan:
    """Plan/product information"""
    id: str
    stripe_product_id: str
    name: str
    description: str
    active: bool = True
    prices: List[Dict[str, Any]] = None
    created_at: datetime.datetime = datetime.datetime.now()
    updated_at: datetime.datetime = datetime.datetime.now()
    metadata: Dict[str, Any] = None


@dataclass
class Notification:
    """Notification record"""
    id: str
    customer_id: str
    subscription_id: Optional[str]
    notification_type: NotificationType
    sent_at: datetime.datetime
    content: str
    delivery_status: str  # sent, delivered, failed, etc.
    metadata: Dict[str, Any] = None


# Service Layer
class CustomerService:
    """Service for managing customers"""
    
    def create_customer(self, email: str, name: str, company_name: Optional[str] = None) -> Customer:
        """Create a new customer in both local database and Stripe"""
        logger.info(f"Creating customer: {email}")
        
        # 1. Create customer in Stripe
        stripe_customer = stripe.Customer.create(
            email=email,
            name=name,
            metadata={
                "company_name": company_name,
                "source": "subscription_system"
            }
        )
        
        # 2. Store customer in our database
        customer = Customer(
            id=generate_id(),
            stripe_customer_id=stripe_customer.id,
            email=email,
            name=name,
            company_name=company_name
        )
        
        # 3. In a real system, we'd save to database here
        # db.customers.insert(customer)
        
        return customer
    
    def update_customer(self, customer_id: str, **kwargs) -> Customer:
        """Update customer information"""
        # Implementation details...
        pass
    
    def get_customer(self, customer_id: str) -> Customer:
        """Retrieve customer by ID"""
        # Implementation details...
        pass
    
    def get_customer_by_email(self, email: str) -> Customer:
        """Retrieve customer by email"""
        # Implementation details...
        pass
    
    def delete_customer(self, customer_id: str) -> bool:
        """Delete a customer (mark as inactive)"""
        # Implementation details...
        pass


class PaymentMethodService:
    """Service for managing payment methods"""
    
    def add_payment_method(self, customer_id: str, payment_method_id: str, set_default: bool = True) -> PaymentMethod:
        """Add a payment method to a customer"""
        logger.info(f"Adding payment method for customer: {customer_id}")
        
        # 1. Get customer
        customer = customer_service.get_customer(customer_id)
        
        # 2. Attach the payment method to the Stripe customer
        stripe.PaymentMethod.attach(
            payment_method_id,
            customer=customer.stripe_customer_id
        )
        
        # 3. If set as default, update customer's default payment method
        if set_default:
            stripe.Customer.modify(
                customer.stripe_customer_id,
                invoice_settings={
                    "default_payment_method": payment_method_id
                }
            )
        
        # 4. Retrieve payment method details from Stripe
        stripe_payment_method = stripe.PaymentMethod.retrieve(payment_method_id)
        
        # 5. Create payment method record
        payment_method = PaymentMethod(
            id=generate_id(),
            customer_id=customer_id,
            stripe_payment_method_id=payment_method_id,
            type=stripe_payment_method.type,
            status=PaymentMethodStatus.VALID,
            last_four=stripe_payment_method.card.last4,
            expiry_month=stripe_payment_method.card.exp_month,
            expiry_year=stripe_payment_method.card.exp_year,
            is_default=set_default
        )
        
        # 6. In a real system, save to database
        # db.payment_methods.insert(payment_method)
        
        return payment_method
    
    def update_payment_method(self, payment_method_id: str, **kwargs) -> PaymentMethod:
        """Update payment method information"""
        # Implementation details...
        pass
    
    def delete_payment_method(self, payment_method_id: str) -> bool:
        """Delete a payment method"""
        # Implementation details...
        pass
    
    def get_customer_payment_methods(self, customer_id: str) -> List[PaymentMethod]:
        """Get all payment methods for a customer"""
        # Implementation details...
        pass
    
    def check_for_expiring_cards(self) -> List[PaymentMethod]:
        """Check for payment methods expiring in the next 30 days"""
        # Implementation details...
        pass


class SubscriptionService:
    """Service for managing subscriptions"""
    
    def create_subscription(
        self, 
        customer_id: str, 
        plan_items: List[Dict[str, Any]],
        payment_method_id: Optional[str] = None,
        trial_days: Optional[int] = None
    ) -> Subscription:
        """Create a new subscription"""
        logger.info(f"Creating subscription for customer: {customer_id}")
        
        # 1. Get customer
        customer = customer_service.get_customer(customer_id)
        
        # 2. Prepare subscription items
        items = []
        for item in plan_items:
            items.append({
                "price": item["price_id"],
                "quantity": item.get("quantity", 1)
            })
        
        # 3. Set up subscription parameters
        subscription_params = {
            "customer": customer.stripe_customer_id,
            "items": items,
            "expand": ["latest_invoice.payment_intent"]
        }
        
        # 4. Add trial period if specified
        if trial_days:
            trial_end = datetime.datetime.now() + datetime.timedelta(days=trial_days)
            subscription_params["trial_end"] = int(trial_end.timestamp())
        
        # 5. Add payment method if specified
        if payment_method_id:
            payment_method = payment_method_service.get_payment_method(payment_method_id)
            subscription_params["default_payment_method"] = payment_method.stripe_payment_method_id
        
        # 6. Add automatic tax if enabled
        if ENABLE_AUTOMATIC_TAX:
            subscription_params["automatic_tax"] = {"enabled": True}
        
        # 7. Create the subscription in Stripe
        stripe_subscription = stripe.Subscription.create(**subscription_params)
        
        # 8. Create subscription in our database
        subscription = Subscription(
            id=generate_id(),
            customer_id=customer_id,
            stripe_subscription_id=stripe_subscription.id,
            status=SubscriptionStatus(stripe_subscription.status),
            current_period_start=datetime.datetime.fromtimestamp(stripe_subscription.current_period_start),
            current_period_end=datetime.datetime.fromtimestamp(stripe_subscription.current_period_end),
            items=stripe_subscription.items.data,
            payment_method_id=payment_method_id
        )
        
        # 9. In a real system, save to database
        # db.subscriptions.insert(subscription)
        
        # 10. Send confirmation notification
        notification_service.send_notification(
            customer_id=customer_id,
            subscription_id=subscription.id,
            notification_type=NotificationType.SUBSCRIPTION_CREATED
        )
        
        return subscription
    
    def update_subscription(self, subscription_id: str, **kwargs) -> Subscription:
        """Update subscription details"""
        # Implementation details...
        pass
    
    def cancel_subscription(self, subscription_id: str, cancel_immediately: bool = False) -> Subscription:
        """Cancel a subscription"""
        # Implementation details...
        pass
    
    def change_payment_method(self, subscription_id: str, payment_method_id: str) -> Subscription:
        """Change the payment method for a subscription"""
        # Implementation details...
        pass
    
    def check_upcoming_renewals(self) -> List[Subscription]:
        """Check for subscriptions with upcoming renewals"""
        # Implementation details...
        pass


class NotificationService:
    """Service for managing customer notifications"""
    
    def send_notification(
        self, 
        customer_id: str, 
        notification_type: NotificationType,
        subscription_id: Optional[str] = None,
        custom_message: Optional[str] = None
    ) -> Notification:
        """Send a notification to a customer"""
        logger.info(f"Sending {notification_type.value} notification to customer: {customer_id}")
        
        # 1. Get customer
        customer = customer_service.get_customer(customer_id)
        
        # 2. Generate notification content
        content = self._generate_notification_content(
            notification_type, 
            customer, 
            subscription_id,
            custom_message
        )
        
        # 3. In a real system, send via email service
        # email_result = email_service.send_email(customer.email, subject, content)
        
        # 4. Create notification record
        notification = Notification(
            id=generate_id(),
            customer_id=customer_id,
            subscription_id=subscription_id,
            notification_type=notification_type,
            sent_at=datetime.datetime.now(),
            content=content,
            delivery_status="sent"  # In a real system, this would be updated based on email service response
        )
        
        # 5. In a real system, save to database
        # db.notifications.insert(notification)
        
        return notification
    
    def _generate_notification_content(
        self, 
        notification_type: NotificationType,
        customer: Customer, 
        subscription_id: Optional[str] = None,
        custom_message: Optional[str] = None
    ) -> str:
        """Generate notification content based on type"""
        # In a real system, this would use templates
        if custom_message:
            return custom_message
            
        # Simple template-like content generation
        if notification_type == NotificationType.RENEWAL_REMINDER:
            subscription = subscription_service.get_subscription(subscription_id)
            days_until_renewal = (subscription.current_period_end - datetime.datetime.now()).days
            return f"Hello {customer.name}, your subscription will renew in {days_until_renewal} days."
        
        elif notification_type == NotificationType.PAYMENT_FAILED:
            return f"Hello {customer.name}, we were unable to process your payment."
        
        # Additional notification types would be handled here
        
        return f"Hello {customer.name}, notification about your subscription."


class WebhookHandler:
    """Handler for Stripe webhook events"""
    
    def handle_webhook(self, payload: str, signature: str) -> bool:
        """Process a Stripe webhook event"""
        try:
            event = stripe.Webhook.construct_event(
                payload, signature, STRIPE_WEBHOOK_SECRET
            )
        except ValueError as e:
            logger.error(f"Invalid payload: {e}")
            return False
        except stripe.error.SignatureVerificationError as e:
            logger.error(f"Invalid signature: {e}")
            return False
        
        logger.info(f"Received event: {event['type']}")
        
        # Handle the event based on its type
        event_type = event['type']
        event_data = event['data']['object']
        
        if event_type == 'customer.subscription.created':
            return self._handle_subscription_created(event_data)
        elif event_type == 'customer.subscription.updated':
            return self._handle_subscription_updated(event_data)
        elif event_type == 'customer.subscription.deleted':
            return self._handle_subscription_deleted(event_data)
        elif event_type == 'invoice.payment_succeeded':
            return self._handle_payment_succeeded(event_data)
        elif event_type == 'invoice.payment_failed':
            return self._handle_payment_failed(event_data)
        elif event_type == 'payment_method.attached':
            return self._handle_payment_method_attached(event_data)
        elif event_type == 'payment_method.detached':
            return self._handle_payment_method_detached(event_data)
        else:
            logger.info(f"Unhandled event type: {event_type}")
            return True
    
    def _handle_subscription_created(self, event_data: Dict[str, Any]) -> bool:
        """Handle subscription.created event"""
        # Implementation to sync the new subscription
        return True
    
    def _handle_subscription_updated(self, event_data: Dict[str, Any]) -> bool:
        """Handle subscription.updated event"""
        # Implementation to update local subscription data
        return True
    
    def _handle_subscription_deleted(self, event_data: Dict[str, Any]) -> bool:
        """Handle subscription.deleted event"""
        # Implementation to update local subscription status
        return True
    
    def _handle_payment_succeeded(self, event_data: Dict[str, Any]) -> bool:
        """Handle invoice.payment_succeeded event"""
        # Implementation to record successful payment
        return True
    
    def _handle_payment_failed(self, event_data: Dict[str, Any]) -> bool:
        """Handle invoice.payment_failed event"""
        # Implementation to handle failed payment (send notification, update status)
        return True
    
    def _handle_payment_method_attached(self, event_data: Dict[str, Any]) -> bool:
        """Handle payment_method.attached event"""
        # Implementation to record new payment method
        return True
    
    def _handle_payment_method_detached(self, event_data: Dict[str, Any]) -> bool:
        """Handle payment_method.detached event"""
        # Implementation to update payment method status
        return True


# Helper functions
def generate_id() -> str:
    """Generate a unique ID for database records"""
    import uuid
    return str(uuid.uuid4())


# Initialize services
customer_service = CustomerService()
payment_method_service = PaymentMethodService()
subscription_service = SubscriptionService()
notification_service = NotificationService()
webhook_handler = WebhookHandler()


# Example usage
def example_workflow():
    """Example workflow of creating a customer with subscription"""
    # 1. Create a customer
    customer = customer_service.create_customer(
        email="test@example.com",
        name="Test Customer",
        company_name="Example Corp"
    )
    
    # 2. Add a payment method (in a real app, this would come from Elements)
    payment_method = payment_method_service.add_payment_method(
        customer_id=customer.id,
        payment_method_id="pm_card_visa",  # This would be a real PM ID in production
        set_default=True
    )
    
    # 3. Create a subscription
    subscription = subscription_service.create_subscription(
        customer_id=customer.id,
        plan_items=[
            {"price_id": "price_monthly_standard", "quantity": 1}
        ],
        payment_method_id=payment_method.id
    )
    
    print(f"Created subscription: {subscription.id}")
    print(f"Status: {subscription.status.value}")
    print(f"Current period ends: {subscription.current_period_end}")


if __name__ == "__main__":
    # Set up Stripe API key
    stripe.api_key = STRIPE_API_KEY
    
    # Run example workflow
    example_workflow()