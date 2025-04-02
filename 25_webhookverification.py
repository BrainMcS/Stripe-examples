import hmac
import hashlib
import json
import logging
import secrets
import asyncio
from typing import Dict, Any, Optional, Callable, List, Tuple
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('webhook_verification')

class WebhookVerifier:
    def __init__(self, 
                secret: str, 
                signature_header: str = 'X-Signature',
                timestamp_header: str = 'X-Timestamp',
                max_timestamp_diff: int = 300,  # 5 minutes
                replay_protection: bool = True):
        """
        Initialize the webhook verifier.
        
        Args:
            secret: The webhook secret shared with the sender
            signature_header: HTTP header containing the signature
            timestamp_header: HTTP header containing the timestamp
            max_timestamp_diff: Maximum allowed timestamp difference in seconds
            replay_protection: Whether to enable replay protection
        """
        self.secret = secret
        self.signature_header = signature_header
        self.timestamp_header = timestamp_header
        self.max_timestamp_diff = max_timestamp_diff
        self.replay_protection = replay_protection
        
        # For replay protection
        self._seen_signatures: Dict[str, datetime] = {}
        self._cleanup_interval = 3600  # 1 hour
        self._last_cleanup = datetime.now()
    
    def compute_signature(self, payload: bytes, timestamp: str) -> str:
        """
        Compute HMAC signature for a webhook payload.
        
        Args:
            payload: Raw webhook payload bytes
            timestamp: Timestamp string
            
        Returns:
            Hex-encoded HMAC signature
        """
        # Combine timestamp and payload
        message = timestamp.encode() + b'.' + payload
        
        # Compute HMAC using SHA-256
        signature = hmac.new(
            key=self.secret.encode(),
            msg=message,
            digestmod=hashlib.sha256
        ).hexdigest()
        
        return signature
    
    def is_valid_timestamp(self, timestamp_str: str) -> bool:
        """
        Check if the timestamp is within the allowed time window.
        
        Args:
            timestamp_str: Timestamp as string
            
        Returns:
            True if timestamp is valid, False otherwise
        """
        try:
            # Parse timestamp
            webhook_time = datetime.fromtimestamp(int(timestamp_str))
            current_time = datetime.now()
            time_diff = abs((current_time - webhook_time).total_seconds())
            
            # Check if timestamp is within allowed window
            return time_diff <= self.max_timestamp_diff
        except (ValueError, TypeError):
            logger.warning(f"Invalid timestamp format: {timestamp_str}")
            return False
    
    def is_replay(self, signature: str) -> bool:
        """
        Check if this signature has been seen before (replay attack).
        
        Args:
            signature: The request signature
            
        Returns:
            True if this is a replay, False otherwise
        """
        if not self.replay_protection:
            return False
        
        # Perform cleanup of old signatures if needed
        self._cleanup_seen_signatures()
        
        # Check if signature exists in our records
        if signature in self._seen_signatures:
            logger.warning(f"Detected replay attack: signature {signature} already processed")
            return True
        
        # Record this signature
        self._seen_signatures[signature] = datetime.now()
        return False
    
    def _cleanup_seen_signatures(self) -> None:
        """Remove old signatures from the replay protection cache"""
        now = datetime.now()
        if (now - self._last_cleanup).total_seconds() < self._cleanup_interval:
            return
        
        # Cleanup signatures older than 2x the max timestamp difference
        expiration = now - timedelta(seconds=2 * self.max_timestamp_diff)
        expired_signatures = [
            sig for sig, timestamp in self._seen_signatures.items()
            if timestamp < expiration
        ]
        
        for sig in expired_signatures:
            self._seen_signatures.pop(sig, None)
        
        self._last_cleanup = now
        logger.info(f"Cleaned up {len(expired_signatures)} expired signatures")
    
    def verify(self, 
              payload: bytes, 
              headers: Dict[str, str]) -> Tuple[bool, Optional[str]]:
        """
        Verify a webhook request.
        
        Args:
            payload: Raw webhook payload bytes
            headers: HTTP headers
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Extract signature from headers
        received_signature = headers.get(self.signature_header)
        if not received_signature:
            return False, f"Missing signature header: {self.signature_header}"
        
        # Extract timestamp from headers
        timestamp = headers.get(self.timestamp_header)
        if not timestamp:
            return False, f"Missing timestamp header: {self.timestamp_header}"
        
        # Validate timestamp
        if not self.is_valid_timestamp(timestamp):
            return False, f"Invalid timestamp: {timestamp}"
        
        # Check for replay attacks
        if self.is_replay(received_signature):
            return False, "Duplicate webhook detected (replay attack)"
        
        # Compute expected signature
        expected_signature = self.compute_signature(payload, timestamp)
        
        # Use constant-time comparison to prevent timing attacks
        if not hmac.compare_digest(received_signature, expected_signature):
            return False, "Invalid signature"
        
        return True, None


class WebhookProcessor:
    def __init__(self, verifier: WebhookVerifier):
        """
        Initialize the webhook processor.
        
        Args:
            verifier: WebhookVerifier instance for signature verification
        """
        self.verifier = verifier
        self.event_handlers: Dict[str, List[Callable]] = {}
        self.processing_queue = asyncio.Queue()
        self.is_running = False
    
    def register_handler(self, event_type: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """
        Register a handler for a specific event type.
        
        Args:
            event_type: Type of webhook event to handle
            handler: Function to call when event is received
        """
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        
        self.event_handlers[event_type].append(handler)
        logger.info(f"Registered handler for event type: {event_type}")
    
    async def _process_webhook_async(self, 
                                    event_type: str, 
                                    payload: Dict[str, Any], 
                                    webhook_id: str) -> None:
        """
        Process a webhook event asynchronously.
        
        Args:
            event_type: Type of webhook event
            payload: Webhook payload
            webhook_id: Unique identifier for this webhook
        """
        handlers = self.event_handlers.get(event_type, [])
        
        if not handlers:
            logger.warning(f"No handlers registered for event type: {event_type}")
            return
        
        try:
            # Process with each registered handler
            for handler in handlers:
                try:
                    # Add webhook_id to payload for idempotency tracking
                    augmented_payload = {**payload, '_webhook_id': webhook_id}
                    handler(augmented_payload)
                except Exception as e:
                    logger.error(f"Error in handler for {event_type}: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing webhook {webhook_id}: {str(e)}")
    
    async def _worker(self) -> None:
        """Background worker to process webhooks from the queue"""
        self.is_running = True
        
        while self.is_running:
            try:
                # Get next webhook from queue
                event_type, payload, webhook_id = await self.processing_queue.get()
                
                # Process the webhook
                await self._process_webhook_async(event_type, payload, webhook_id)
                
                # Mark task as done
                self.processing_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in webhook worker: {str(e)}")
    
    async def start_processing(self) -> None:
        """Start the asynchronous webhook processor"""
        logger.info("Starting webhook processor")
        asyncio.create_task(self._worker())
    
    async def stop_processing(self) -> None:
        """Stop the webhook processor"""
        logger.info("Stopping webhook processor")
        self.is_running = False
        
        # Wait for queue to empty
        if not self.processing_queue.empty():
            await self.processing_queue.join()
    
    def process_webhook(self, 
                       payload_bytes: bytes, 
                       headers: Dict[str, str]) -> Dict[str, Any]:
        """
        Verify and queue a webhook for processing.
        
        Args:
            payload_bytes: Raw webhook payload
            headers: HTTP headers
            
        Returns:
            Response with verification status
        """
        # Verify the webhook signature
        is_valid, error_message = self.verifier.verify(payload_bytes, headers)
        
        if not is_valid:
            logger.warning(f"Webhook verification failed: {error_message}")
            return {
                'success': False,
                'error': error_message
            }
        
        try:
            # Parse JSON payload
            payload = json.loads(payload_bytes.decode('utf-8'))
            
            # Extract event type
            event_type = payload.get('type', 'unknown')
            
            # Generate a unique ID for idempotency if not provided
            webhook_id = payload.get('id', f"whk_{secrets.token_hex(16)}")
            
            # Queue webhook for asynchronous processing
            asyncio.create_task(
                self.processing_queue.put((event_type, payload, webhook_id))
            )
            
            logger.info(f"Queued webhook {webhook_id} of type {event_type} for processing")
            
            return {
                'success': True,
                'webhook_id': webhook_id
            }
            
        except json.JSONDecodeError:
            return {
                'success': False,
                'error': 'Invalid JSON payload'
            }
        except Exception as e:
            logger.error(f"Error queueing webhook: {str(e)}")
            return {
                'success': False,
                'error': f"Internal error: {str(e)}"
            }


# Example usage with a web framework (FastAPI in this case)
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

# Initialize webhook components
webhook_secret = "your_webhook_secret_here"
webhook_verifier = WebhookVerifier(
    secret=webhook_secret,
    signature_header="X-Webhook-Signature",
    timestamp_header="X-Webhook-Timestamp",
    replay_protection=True
)

webhook_processor = WebhookProcessor(webhook_verifier)

# Register webhook handlers
def handle_payment_succeeded(payload: Dict[str, Any]) -> None:
    # Check for idempotency
    webhook_id = payload.get('_webhook_id')
    payment_id = payload.get('data', {}).get('id')
    
    logger.info(f"Processing payment.succeeded for payment {payment_id} (webhook {webhook_id})")
    
    # Your processing logic here
    # In a real implementation, you would first check if this webhook has
    # already been processed by looking up the webhook_id in your database

def handle_payment_failed(payload: Dict[str, Any]) -> None:
    webhook_id = payload.get('_webhook_id')
    payment_id = payload.get('data', {}).get('id')
    
    logger.info(f"Processing payment.failed for payment {payment_id} (webhook {webhook_id})")
    
    # Your processing logic here

# Register handlers
webhook_processor.register_handler("payment.succeeded", handle_payment_succeeded)
webhook_processor.register_handler("payment.failed", handle_payment_failed)

# Start the webhook processor
@app.on_event("startup")
async def startup_event():
    await webhook_processor.start_processing()

# Stop the webhook processor
@app.on_event("shutdown")
async def shutdown_event():
    await webhook_processor.stop_processing()

# Webhook endpoint
@app.post("/webhook")
async def webhook_endpoint(request: Request):
    # Get raw request body
    payload_bytes = await request.body()
    
    # Process the webhook
    result = webhook_processor.process_webhook(
        payload_bytes=payload_bytes,
        headers=dict(request.headers)
    )
    
    # Always return 200 OK to acknowledge receipt
    # This follows best practice of responding quickly
    if not result['success']:
        logger.warning(f"Webhook verification failed: {result.get('error')}")
        # Still return 200 OK to avoid revealing verification details
        return JSONResponse(
            status_code=200,
            content={"message": "Webhook received"}
        )
    
    return JSONResponse(
        status_code=200,
        content={"message": "Webhook received and queued for processing"}
    )