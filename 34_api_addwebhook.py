import hmac
import hashlib
import json
import time
from flask import Flask, request, jsonify

app = Flask(__name__)

# Configuration - in production, store this securely
WEBHOOK_SECRET = "whsec_your_webhook_signing_secret"

@app.route('/webhooks', methods=['POST'])
def handle_webhook():
    payload = request.data.decode('utf-8')
    signature_header = request.headers.get('Stripe-Signature')
    
    try:
        event_data = verify_webhook_signature(payload, signature_header)
        
        event_type = event_data.get('type')
        
        if event_type == 'payment_intent.succeeded':
            handle_payment_success(event_data)
        elif event_type == 'payment_intent.payment_failed':
            handle_payment_failure(event_data)
        
        return jsonify({'status': 'success'})
    
    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

def verify_webhook_signature(payload, signature_header):
    if not signature_header:
        raise ValueError("No signature header found")
    
    # Parse the signature header
    timestamp = None
    signatures = []
    
    for item in signature_header.split(','):
        key, value = item.split('=', 1)
        if key == 't':
            timestamp = value
        elif key == 'v1':
            signatures.append(value)
    
    if not timestamp or not signatures:
        raise ValueError("Invalid signature header format")
    
    # Check timestamp freshness (optional but recommended)
    if abs(time.time() - int(timestamp)) > 300:  # Within 5 minutes
        raise ValueError("Timestamp too old")
    
    # Compute expected signature
    signed_payload = f"{timestamp}.{payload}"
    expected_signature = hmac.new(
        WEBHOOK_SECRET.encode('utf-8'),
        signed_payload.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    # Verify signature using constant-time comparison
    if not any(hmac.compare_digest(expected_signature, sig) for sig in signatures):
        raise ValueError("Signature verification failed")
    
    return json.loads(payload)

def handle_payment_success(event_data):
    payment_intent = event_data.get('data', {}).get('object', {})
    print(f"Payment succeeded: {payment_intent.get('id')}")
    # Update database, send confirmation email, etc.

def handle_payment_failure(event_data):
    payment_intent = event_data.get('data', {}).get('object', {})
    print(f"Payment failed: {payment_intent.get('id')}")
    # Log failure, notify user, etc.

if __name__ == "__main__":
    app.run(port=5000)