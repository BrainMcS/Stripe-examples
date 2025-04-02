import stripe
from dotenv import load_dotenv
import os

load_dotenv()
stripe.api_key = os.getenv("STRIPE_API_KEY")

# Create a resource (POST)
payment_intent = stripe.PaymentIntent.create(
    amount=2000,  # $20.00 in cents
    currency="usd",
    payment_method_types=["card"],
    customer="cus_123ABC",
    metadata={"order_id": "6735"}
)

# Retrieve the resource (GET)
payment_intent = stripe.PaymentIntent.retrieve(payment_intent.id)

# Update the resource (POST in Stripe, would be PUT/PATCH in pure REST)
payment_intent = stripe.PaymentIntent.modify(
    payment_intent.id,
    amount=2500,  # Update to $25.00
    metadata={"order_id": "6735", "promotion_code": "SPRING2023"}
)

# List resources with filtering (GET with query parameters)
all_intents = stripe.PaymentIntent.list(
    customer="cus_123ABC",
    limit=5
)