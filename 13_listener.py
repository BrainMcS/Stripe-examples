import os
import time
import logging
import requests
import threading
import queue
import signal
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime
import sqlite3
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("event_integration.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('event_integration')

class EventDatabase:
    """Database for storing processed events and integration state"""
    
    def __init__(self, db_path: str = "event_integration.db"):
        self.db_path = db_path
        self._initialize_db()
    
    def _initialize_db(self) -> None:
        """Initialize the database schema"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            # Table for storing processed events
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                source_system TEXT NOT NULL,
                event_type TEXT NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL,
                error_message TEXT
            )
            ''')
            
            # Table for storing integration actions
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS integration_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                target_system TEXT NOT NULL,
                action_type TEXT NOT NULL,
                action_id TEXT,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                FOREIGN KEY (event_id) REFERENCES processed_events(event_id)
            )
            ''')
            
            # Table for storing system state like last poll time
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            conn.commit()
            logger.info("Database initialized successfully")
            
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {str(e)}")
            raise
        finally:
            conn.close()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        return conn
    
    def mark_event_processed(self, event_id: str, source_system: str, event_type: str, 
                            status: str = "success", error_message: str = None) -> int:
        """Mark an event as processed"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO processed_events 
            (event_id, source_system, event_type, processed_at, status, error_message)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            ''', (event_id, source_system, event_type, status, error_message))
            
            conn.commit()
            return cursor.lastrowid
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"Error marking event {event_id} as processed: {str(e)}")
            raise
        finally:
            conn.close()
    
    def is_event_processed(self, event_id: str) -> bool:
        """Check if an event has already been processed"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT id FROM processed_events WHERE event_id = ?
            ''', (event_id,))
            
            result = cursor.fetchone()
            return result is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if event {event_id} was processed: {str(e)}")
            return False
        finally:
            conn.close()
    
    def record_action(self, event_id: str, target_system: str, action_type: str, 
                    status: str = "pending", action_id: str = None, error_message: str = None) -> int:
        """Record an integration action"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT INTO integration_actions 
            (event_id, target_system, action_type, action_id, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (event_id, target_system, action_type, action_id, status, error_message))
            
            conn.commit()
            return cursor.lastrowid
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"Error recording action for event {event_id}: {str(e)}")
            raise
        finally:
            conn.close()
    
    def update_action_status(self, action_id: int, status: str, 
                           action_result_id: str = None, error_message: str = None) -> bool:
        """Update the status of an integration action"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
            UPDATE integration_actions 
            SET status = ?, action_id = ?, error_message = ?, 
                completed_at = CASE WHEN ? IN ('success', 'failed') THEN CURRENT_TIMESTAMP ELSE NULL END
            WHERE id = ?
            ''', (status, action_result_id, error_message, status, action_id))
            
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"Error updating action {action_id} status: {str(e)}")
            return False
        finally:
            conn.close()
    
    def get_action(self, action_id: int) -> Optional[Dict[str, Any]]:
        """Get an integration action by ID"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT * FROM integration_actions WHERE id = ?
            ''', (action_id,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            logger.error(f"Error getting action {action_id}: {str(e)}")
            return None
        finally:
            conn.close()
    
    def get_actions_for_event(self, event_id: str) -> List[Dict[str, Any]]:
        """Get all actions for an event"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT * FROM integration_actions WHERE event_id = ?
            ''', (event_id,))
            
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error getting actions for event {event_id}: {str(e)}")
            return []
        finally:
            conn.close()
    
    def set_state(self, key: str, value: str) -> bool:
        """Set a system state value"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO system_state (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (key, value))
            
            conn.commit()
            return True
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"Error setting state {key}: {str(e)}")
            return False
        finally:
            conn.close()
    
    def get_state(self, key: str, default: str = None) -> str:
        """Get a system state value"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT value FROM system_state WHERE key = ?
            ''', (key,))
            
            row = cursor.fetchone()
            return row['value'] if row else default
        except sqlite3.Error as e:
            logger.error(f"Error getting state {key}: {str(e)}")
            return default
        finally:
            conn.close()

class APIClient:
    """Base class for API clients"""
    
    def __init__(self, base_url: str, api_key: str = None):
        self.base_url = base_url
        self.api_key = api_key
        self.session = requests.Session()
        
        # Set up default headers
        if api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {api_key}'
            })
        
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make an API request with retries and error handling"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                response = self.session.request(method, url, **kwargs)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', retry_delay * (2 ** attempt)))
                    logger.warning(f"Rate limited. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                # Handle server errors (5xx)
                if 500 <= response.status_code < 600:
                    logger.warning(f"Server error: {response.status_code}. Retrying...")
                    time.sleep(retry_delay * (2 ** attempt))
                    continue
                
                return response
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay * (2 ** attempt))
                else:
                    raise
        
        raise requests.exceptions.RequestException("Max retries exceeded")

class ShoppingCartAPI(APIClient):
    """Client for the shopping cart API"""
    
    def get_recent_orders(self, since_timestamp: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get recent orders from the shopping cart"""
        params = {}
        if since_timestamp:
            params['created_after'] = since_timestamp
        
        response = self._make_request('GET', '/orders', params=params)
        
        if response.status_code == 200:
            return response.json().get('orders', [])
        else:
            logger.error(f"Failed to get orders: {response.status_code} - {response.text}")
            return []
    
    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific order by ID"""
        response = self._make_request('GET', f'/orders/{order_id}')
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            logger.warning(f"Order {order_id} not found")
            return None
        else:
            logger.error(f"Failed to get order {order_id}: {response.status_code} - {response.text}")
            return None
    
    def update_order_status(self, order_id: str, status: str, tracking_number: str = None) -> bool:
        """Update an order's status"""
        data = {
            'status': status
        }
        
        if tracking_number:
            data['tracking_number'] = tracking_number
        
        response = self._make_request('PATCH', f'/orders/{order_id}', json=data)
        
        if response.status_code in (200, 204):
            return True
        else:
            logger.error(f"Failed to update order {order_id}: {response.status_code} - {response.text}")
            return False

class LogisticsAPI(APIClient):
    """Client for the logistics/shipping API"""
    
    def create_shipping_label(self, order_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a shipping label for an order"""
        # Extract shipping information from the order
        try:
            shipping_address = order_data.get('shipping_address', {})
            items = order_data.get('items', [])
            
            # Calculate package dimensions and weight based on items
            weight = sum(item.get('weight', 0) for item in items)
            
            # Create label request
            label_request = {
                'reference_id': order_data.get('id'),
                'ship_to': {
                    'name': shipping_address.get('name', ''),
                    'company': shipping_address.get('company', ''),
                    'street1': shipping_address.get('street1', ''),
                    'street2': shipping_address.get('street2', ''),
                    'city': shipping_address.get('city', ''),
                    'state': shipping_address.get('state', ''),
                    'zip': shipping_address.get('zip', ''),
                    'country': shipping_address.get('country', 'US'),
                    'phone': shipping_address.get('phone', ''),
                    'email': order_data.get('customer', {}).get('email', '')
                },
                'packages': [{
                    'weight': weight or 1.0,  # Default to 1.0 if no weight
                    'dimensions': {
                        'length': 12,
                        'width': 12,
                        'height': 12
                    }
                }],
                'service': 'ground',  # Default shipping service
                'is_return': False
            }
            
            # Send request to create shipping label
            response = self._make_request('POST', '/shipping/labels', json=label_request)
            
            if response.status_code == 201:
                return response.json()
            else:
                logger.error(f"Failed to create shipping label: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating shipping label: {str(e)}")
            return None
    
    def get_label_status(self, label_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a shipping label"""
        response = self._make_request('GET', f'/shipping/labels/{label_id}')
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get label {label_id}: {response.status_code} - {response.text}")
            return None

class EventProcessor:
    """Processes events and triggers appropriate actions"""
    
    def __init__(self, event_db: EventDatabase):
        self.event_db = event_db
        self.action_handlers = {}
    
    def register_action_handler(self, event_type: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Register a handler function for a specific event type"""
        self.action_handlers[event_type] = handler
    
    def process_event(self, event: Dict[str, Any]) -> bool:
        """Process an event by executing the appropriate action handler"""
        event_id = event.get('id')
        event_type = event.get('type')
        source_system = event.get('source', 'unknown')
        
        if not event_id or not event_type:
            logger.error(f"Invalid event format: {event}")
            return False
        
        # Check if we've already processed this event
        if self.event_db.is_event_processed(event_id):
            logger.info(f"Event {event_id} already processed, skipping")
            return True
        
        logger.info(f"Processing event {event_id} of type {event_type}")
        
        # Find the appropriate handler
        handler = self.action_handlers.get(event_type)
        if not handler:
            logger.warning(f"No handler registered for event type: {event_type}")
            self.event_db.mark_event_processed(
                event_id, 
                source_system, 
                event_type, 
                status="skipped", 
                error_message="No handler registered"
            )
            return False
        
        try:
            # Execute the handler
            handler(event)
            
            # Mark event as processed
            self.event_db.mark_event_processed(event_id, source_system, event_type)
            return True
            
        except Exception as e:
            logger.error(f"Error processing event {event_id}: {str(e)}")
            
            # Mark event as failed
            self.event_db.mark_event_processed(
                event_id, 
                source_system, 
                event_type, 
                status="failed", 
                error_message=str(e)
            )
            return False

class IntegrationService:
    """Main service that coordinates event processing and API interactions"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.running = False
        self.event_db = EventDatabase(config.get('db_path', 'event_integration.db'))
        self.event_processor = EventProcessor(self.event_db)
        
        # Initialize API clients
        self.cart_api = ShoppingCartAPI(
            config['shopping_cart']['base_url'],
            config['shopping_cart']['api_key']
        )
        
        self.logistics_api = LogisticsAPI(
            config['logistics']['base_url'],
            config['logistics']['api_key']
        )
        
        # Set up action handlers
        self.setup_action_handlers()
        
        # Worker queue for async processing
        self.work_queue = queue.Queue()
        self.worker_threads = []
    
    def setup_action_handlers(self) -> None:
        """Set up action handlers for different event types"""
        # Register handlers for specific event types
        self.event_processor.register_action_handler(
            'order.created', 
            self.handle_new_order
        )
        
        self.event_processor.register_action_handler(
            'order.paid',
            self.handle_paid_order
        )
        
        self.event_processor.register_action_handler(
            'order.canceled',
            self.handle_canceled_order
        )
    
    def handle_new_order(self, event: Dict[str, Any]) -> None:
        """Handle a new order event"""
        order_id = event.get('data', {}).get('id')
        
        if not order_id:
            logger.error(f"Invalid order event: {event}")
            raise ValueError("Invalid order event, missing ID")
        
        logger.info(f"Handling new order: {order_id}")
        
        # Get full order details
        order = self.cart_api.get_order(order_id)
        if not order:
            raise ValueError(f"Order {order_id} not found")
        
        # Check if order is paid
        if order.get('status') != 'paid':
            logger.info(f"Order {order_id} is not paid yet, waiting for payment")
            return
        
        # Create shipping label
        action_id = self.event_db.record_action(
            event_id=event['id'],
            target_system='logistics',
            action_type='create_shipping_label'
        )
        
        try:
            # Create shipping label
            shipping_result = self.logistics_api.create_shipping_label(order)
            
            if shipping_result:
                # Update action status
                self.event_db.update_action_status(
                    action_id=action_id,
                    status='success',
                    action_result_id=shipping_result.get('id')
                )
                
                # Update order with tracking information
                tracking_number = shipping_result.get('tracking_number')
                if tracking_number:
                    self.cart_api.update_order_status(
                        order_id=order_id,
                        status='processing',
                        tracking_number=tracking_number
                    )
                
                logger.info(f"Created shipping label for order {order_id}: {shipping_result.get('id')}")
            else:
                # Update action status
                self.event_db.update_action_status(
                    action_id=action_id,
                    status='failed',
                    error_message='Failed to create shipping label'
                )
                logger.error(f"Failed to create shipping label for order {order_id}")
        
        except Exception as e:
            # Update action status
            self.event_db.update_action_status(
                action_id=action_id,
                status='failed',
                error_message=str(e)
            )
            logger.error(f"Error creating shipping label for order {order_id}: {str(e)}")
            raise
    
    def handle_paid_order(self, event: Dict[str, Any]) -> None:
        """Handle a paid order event"""
        order_id = event.get('data', {}).get('id')
        
        if not order_id:
            logger.error(f"Invalid order event: {event}")
            raise ValueError("Invalid order event, missing ID")
        
        logger.info(f"Handling paid order: {order_id}")
        
        # Process similarly to new order but knowing it's already paid
        # Get full order details
        order = self.cart_api.get_order(order_id)
        if not order:
            raise ValueError(f"Order {order_id} not found")
        
        # Create shipping label
        action_id = self.event_db.record_action(
            event_id=event['id'],
            target_system='logistics',
            action_type='create_shipping_label'
        )
        
        try:
            # Create shipping label
            shipping_result = self.logistics_api.create_shipping_label(order)
            
            if shipping_result:
                # Update action status
                self.event_db.update_action_status(
                    action_id=action_id,
                    status='success',
                    action_result_id=shipping_result.get('id')
                )
                
                # Update order with tracking information
                tracking_number = shipping_result.get('tracking_number')
                if tracking_number:
                    self.cart_api.update_order_status(
                        order_id=order_id,
                        status='processing',
                        tracking_number=tracking_number
                    )
                
                logger.info(f"Created shipping label for paid order {order_id}: {shipping_result.get('id')}")
            else:
                # Update action status
                self.event_db.update_action_status(
                    action_id=action_id,
                    status='failed',
                    error_message='Failed to create shipping label'
                )
                logger.error(f"Failed to create shipping label for paid order {order_id}")
        
        except Exception as e:
            # Update action status
            self.event_db.update_action_status(
                action_id=action_id,
                status='failed',
                error_message=str(e)
            )
            logger.error(f"Error creating shipping label for paid order {order_id}: {str(e)}")
            raise
    
    def handle_canceled_order(self, event: Dict[str, Any]) -> None:
        """Handle a canceled order event"""
        order_id = event.get('data', {}).get('id')
        
        if not order_id:
            logger.error(f"Invalid order event: {event}")
            raise ValueError("Invalid order event, missing ID")
        
        logger.info(f"Handling canceled order: {order_id}")
        
        # Get actions for this order to see if we need to cancel anything
        actions = self.event_db.get_actions_for_event(event['id'])
        
        # Check if we already created a shipping label that needs to be voided
        shipping_actions = [a for a in actions if a['action_type'] == 'create_shipping_label' and a['status'] == 'success']
        
        for action in shipping_actions:
            label_id = action.get('action_result_id')
            if label_id:
                # Record void action
                void_action_id = self.event_db.record_action(
                    event_id=event['id'],
                    target_system='logistics',
                    action_type='void_shipping_label'
                )
                
                try:
                    # Void the shipping label (implementation would depend on logistics API)
                    # This is a placeholder for the actual API call
                    # response = self.logistics_api.void_label(label_id)
                    
                    # For this example, we'll just pretend it worked
                    logger.info(f"Voided shipping label {label_id} for canceled order {order_id}")
                    
                    # Update action status
                    self.event_db.update_action_status(
                        action_id=void_action_id,
                        status='success'
                    )
                    
                except Exception as e:
                    # Update action status
                    self.event_db.update_action_status(
                        action_id=void_action_id,
                        status='failed',
                        error_message=str(e)
                    )
                    logger.error(f"Error voiding shipping label {label_id}: {str(e)}")
        
        # Update order status in the shopping cart
        self.cart_api.update_order_status(
            order_id=order_id,
            status='canceled'
        )
    
    def poll_events(self) -> None:
        """Poll for new events from the shopping cart API"""
        # Get the last poll timestamp
        last_poll_str = self.event_db.get_state('last_poll_timestamp')
        
        if last_poll_str:
            try:
                last_poll = int(last_poll_str)
            except ValueError:
                last_poll = int(time.time()) - 3600  # Default to 1 hour ago
        else:
            last_poll = int(time.time()) - 3600  # Default to 1 hour ago
        
        logger.info(f"Polling for new orders since {datetime.fromtimestamp(last_poll).isoformat()}")
        
        try:
            # Get recent orders
            orders = self.cart_api.get_recent_orders(last_poll)
            logger.info(f"Found {len(orders)} new orders")
            
            # Process each order as an event
            for order in orders:
                # Create event structure
                event = {
                    'id': f"order-{order['id']}",
                    'type': 'order.created',
                    'source': 'shopping_cart',
                    'created_at': order.get('created_at'),
                    'data': order
                }
                
                # Add to work queue for processing
                self.work_queue.put(event)
            
            # Update last poll timestamp
            now = int(time.time())
            self.event_db.set_state('last_poll_timestamp', str(now))
            
        except Exception as e:
            logger.error(f"Error polling for new orders: {str(e)}")
    
    def worker(self) -> None:
        """Worker thread to process events from the queue"""
        while self.running:
            try:
                # Get event from queue, wait up to 1 second
                try:
                    event = self.work_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                # Process the event
                self.event_processor.process_event(event)
                
                # Mark task as done
                self.work_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in worker thread: {str(e)}")
    
    def start(self) -> None:
        """Start the integration service"""
        logger.info("Starting integration service")
        self.running = True
        
        # Start worker threads
        num_workers = self.config.get('num_workers', 2)
        for i in range(num_workers):
            thread = threading.Thread(target=self.worker, daemon=True)
            thread.start()
            self.worker_threads.append(thread)
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
        # Main loop
        try:
            while self.running:
                # Poll for new events
                self.poll_events()
                
                # Sleep between polls
                poll_interval = self.config.get('poll_interval', 60)
                time.sleep(poll_interval)
                
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            self.stop()
    
    def handle_shutdown(self, signum, frame) -> None:
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down")
        self.stop()
    
    def stop(self) -> None:
        """Stop the integration service"""
        logger.info("Stopping integration service")
        self.running = False
        
        # Wait for worker threads to finish
        for thread in self.worker_threads:
            if thread.is_alive():
                thread.join(timeout=5)
        
        logger.info("Integration service stopped")
# Load environment variables from .env file
load_dotenv()

# Example configuration
config = {
    'db_path': 'event_integration.db',
    'num_workers': 2,
    'poll_interval': 60,  # seconds
    'shopping_cart': {
        'base_url': os.getenv('SHOPPING_CART_BASE_URL'),
        'api_key': os.getenv('SHOPPING_CART_API_KEY')
    },
    'logistics': {
        'base_url': os.getenv('LOGISTICS_BASE_URL'),
        'api_key': os.getenv('LOGISTICS_API_KEY')
    }
}

# Example usage
if __name__ == "__main__":
    # Create and start the integration service
    service = IntegrationService(config)
    service.start()