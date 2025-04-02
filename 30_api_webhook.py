import hmac
import hashlib
import time
import json
import logging
from flask import Flask, request, jsonify
from typing import Dict, Any, Callable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webhook_processor")

app = Flask(__name__)

class WebhookProcessor:
    def __init__(self, webhook_secret: str):
        self.webhook_secret = webhook_secret.encode('utf-8')
        self.event_handlers = {}
        self.event_log = []
    
    def register_handler(self, event_type: str, handler: Callable[[Dict], Any]):
        """Register a handler function for a specific event type."""
        self.event_handlers[event_type] = handler
        logger.info(f"Registered handler for event type: {event_type}")
    
    def verify_signature(self, payload: bytes, signature: str, timestamp: str) -> bool:
        """
        Verify the webhook signature using HMAC.
        
        Args:
            payload: Raw request body as bytes
            signature: Signature from the request header
            timestamp: Timestamp from the request header
            
        Returns:
            True if signature is valid, False otherwise
        """
        if not signature or not timestamp:
            logger.warning("Missing signature or timestamp")
            return False
        
        # Compute expected signature
        signed_payload = f"{timestamp}.{payload.decode('utf-8')}"
        expected_signature = hmac.new(
            self.webhook_secret,
            signed_payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        # Check if signatures match using constant-time comparison
        return hmac.compare_digest(expected_signature, signature)
    
    def process_event(self, event_data: Dict) -> Dict:
        """
        Process a webhook event.
        
        Args:
            event_data: Event data from the webhook
            
        Returns:
            Processing result
        """
        event_type = event_data.get("type")
        event_id = event_data.get("id")
        
        logger.info(f"Processing event {event_id} of type {event_type}")
        
        # Store event in log
        event_log_entry = {
            "id": event_id,
            "type": event_type,
            "timestamp": time.time(),
            "processed": False,
            "status": "pending"
        }
        self.event_log.append(event_log_entry)
        
        # Check if we have a handler for this event type
        handler = self.event_handlers.get(event_type)
        if not handler:
            logger.warning(f"No handler registered for event type: {event_type}")
            event_log_entry["status"] = "ignored"
            return {"status": "ignored", "message": f"No handler for event type: {event_type}"}
        
        try:
            # Call the appropriate handler
            result = handler(event_data)
            
            # Update event log
            event_log_entry["processed"] = True
            event_log_entry["status"] = "success"
            event_log_entry["result"] = result
            
            return {"status": "success", "result": result}
        
        except Exception as e:
            logger.error(f"Error processing event {event_id}: {e}")
            
            # Update event log
            event_log_entry["processed"] = True
            event_log_entry["status"] = "error"
            event_log_entry["error"] = str(e)
            
            return {"status": "error", "message": str(e)}

# Create a webhook processor instance
processor = WebhookProcessor(webhook_secret="your_webhook_secret")

# Define event handlers
def handle_payment_succeeded(event_data):
    payment_intent = event_data.get("data", {}).get("object", {})
    payment_id = payment_intent.get("id")
    amount = payment_intent.get("amount")
    currency = payment_intent.get("currency")
    
    logger.info(f"Payment succeeded: {payment_id} for {amount/100} {currency}")
    
    # In a real implementation, you would:
    # - Update your database
    # - Send confirmation to the customer
    # - Trigger fulfillment process
    
    return {"message": "Payment recorded successfully"}

def handle_payment_failed(event_data):
    payment_intent = event_data.get("data", {}).get("object", {})
    payment_id = payment_intent.get("id")
    error = payment_intent.get("last_payment_error", {})
    
    logger.warning(f"Payment failed: {payment_id}. Error: {error}")
    
    # In a real implementation, you would:
    # - Update your database
    # - Notify the customer
    # - Flag for review if suspicious
    
    return {"message": "Payment failure recorded"}

# Register handlers
processor.register_handler("payment_intent.succeeded", handle_payment_succeeded)
processor.register_handler("payment_intent.payment_failed", handle_payment_failed)

@app.route('/webhook', methods=['POST'])
def webhook():
    payload = request.get_data()
    signature = request.headers.get('Stripe-Signature')
    timestamp = request.headers.get('Stripe-Timestamp')
    
    # Verify webhook signature
    if not processor.verify_signature(payload, signature, timestamp):
        logger.warning("Invalid webhook signature")
        return jsonify({"error": "Invalid signature"}), 401
    
    # Parse the event data
    try:
        event_data = json.loads(payload)
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON payload"}), 400
    
    # Process the event
    result = processor.process_event(event_data)
    
    return jsonify(result)

if __name__ == "__main__":
    app.run(port=5000)