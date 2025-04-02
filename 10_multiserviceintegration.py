import requests
import os
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('user_sync')

# API Configuration
class Config:
    # CRM Configuration (e.g., Salesforce)
    CRM_API_URL = os.environ.get('CRM_API_URL', 'https://api.crm-example.com/v1')
    CRM_API_KEY = os.environ.get('CRM_API_KEY')
    
    # Email Marketing Configuration (e.g., Mailchimp)
    EMAIL_API_URL = os.environ.get('EMAIL_API_URL', 'https://api.email-example.com/v3')
    EMAIL_API_KEY = os.environ.get('EMAIL_API_KEY')
    EMAIL_LIST_ID = os.environ.get('EMAIL_LIST_ID')
    
    # Payment Processor Configuration (e.g., Stripe)
    PAYMENT_API_URL = os.environ.get('PAYMENT_API_URL', 'https://api.stripe.com/v1')
    PAYMENT_API_KEY = os.environ.get('PAYMENT_API_KEY')
    
    # Sync Settings
    SYNC_INTERVAL = int(os.environ.get('SYNC_INTERVAL', 900))  # 15 minutes by default
    RETRY_MAX_ATTEMPTS = int(os.environ.get('RETRY_MAX_ATTEMPTS', 3))
    RETRY_DELAY = int(os.environ.get('RETRY_DELAY', 5))  # seconds

class CRMService:
    """Service for interacting with the CRM API"""
    
    def __init__(self):
        self.base_url = Config.CRM_API_URL
        self.headers = {
            'Authorization': f'Bearer {Config.CRM_API_KEY}',
            'Content-Type': 'application/json'
        }
    
    def get_recent_contacts(self, since: datetime) -> List[Dict[str, Any]]:
        """Get contacts created or updated since the given time"""
        since_str = since.isoformat()
        url = f"{self.base_url}/contacts"
        params = {
            'modified_since': since_str,
            'limit': 100
        }
        
        all_contacts = []
        page = 1
        
        while True:
            params['page'] = page
            response = self._make_request('GET', url, params=params)
            
            if not response or 'contacts' not in response:
                break
                
            contacts = response['contacts']
            all_contacts.extend(contacts)
            
            if len(contacts) < 100:  # No more pages
                break
                
            page += 1
        
        return all_contacts
    
    def create_contact(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new contact in the CRM"""
        url = f"{self.base_url}/contacts"
        
        # Transform user data to CRM format
        crm_data = {
            'first_name': user_data.get('first_name', ''),
            'last_name': user_data.get('last_name', ''),
            'email': user_data.get('email', ''),
            'phone': user_data.get('phone', ''),
            'address': {
                'line1': user_data.get('address_line1', ''),
                'line2': user_data.get('address_line2', ''),
                'city': user_data.get('city', ''),
                'state': user_data.get('state', ''),
                'postal_code': user_data.get('postal_code', ''),
                'country': user_data.get('country', '')
            },
            'custom_fields': {
                'source': user_data.get('source', 'sync_script'),
                'signup_date': user_data.get('created_at', datetime.now().isoformat())
            }
        }
        
        response = self._make_request('POST', url, json=crm_data)
        return response
    
    def update_contact(self, contact_id: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing contact in the CRM"""
        url = f"{self.base_url}/contacts/{contact_id}"
        
        # Transform user data to CRM format (similar to create but might be different)
        crm_data = {
            'first_name': user_data.get('first_name'),
            'last_name': user_data.get('last_name'),
            'phone': user_data.get('phone'),
            'address': {
                'line1': user_data.get('address_line1'),
                'line2': user_data.get('address_line2'),
                'city': user_data.get('city'),
                'state': user_data.get('state'),
                'postal_code': user_data.get('postal_code'),
                'country': user_data.get('country')
            },
            'custom_fields': {
                'updated_at': datetime.now().isoformat()
            }
        }
        
        # Remove None values
        crm_data = {k: v for k, v in crm_data.items() if v is not None}
        
        response = self._make_request('PATCH', url, json=crm_data)
        return response
    
    def find_contact_by_email(self, email: str) -> Dict[str, Any]:
        """Find a contact by email address"""
        url = f"{self.base_url}/contacts"
        params = {
            'email': email
        }
        
        response = self._make_request('GET', url, params=params)
        
        if response and 'contacts' in response and response['contacts']:
            return response['contacts'][0]
        
        return None
    
    def _make_request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make an API request with retries and error handling"""
        for attempt in range(Config.RETRY_MAX_ATTEMPTS):
            try:
                response = requests.request(
                    method,
                    url,
                    headers=self.headers,
                    timeout=30,
                    **kwargs
                )
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"CRM API request failed: {str(e)}")
                if attempt < Config.RETRY_MAX_ATTEMPTS - 1:
                    wait_time = Config.RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retry attempts reached. Giving up.")
                    return None
        
        return None

class EmailService:
    """Service for interacting with the Email Marketing API"""
    
    def __init__(self):
        self.base_url = Config.EMAIL_API_URL
        self.headers = {
            'Authorization': f'Bearer {Config.EMAIL_API_KEY}',
            'Content-Type': 'application/json'
        }
        self.list_id = Config.EMAIL_LIST_ID
    
    def get_recent_subscribers(self, since: datetime) -> List[Dict[str, Any]]:
        """Get subscribers added or updated since the given time"""
        since_str = since.isoformat()
        url = f"{self.base_url}/lists/{self.list_id}/members"
        params = {
            'since': since_str,
            'count': 100
        }
        
        all_subscribers = []
        offset = 0
        
        while True:
            params['offset'] = offset
            response = self._make_request('GET', url, params=params)
            
            if not response or 'members' not in response:
                break
                
            members = response['members']
            all_subscribers.extend(members)
            
            if len(members) < 100:  # No more pages
                break
                
            offset += 100
        
        return all_subscribers
    
    def add_subscriber(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Add a subscriber to the email list"""
        url = f"{self.base_url}/lists/{self.list_id}/members"
        
        # Generate MD5 hash of lowercase email for Mailchimp-style APIs
        import hashlib
        email_hash = hashlib.md5(user_data.get('email', '').lower().encode()).hexdigest()
        
        # Transform user data to Email Marketing format
        email_data = {
            'email_address': user_data.get('email', ''),
            'status': 'subscribed',  # Or 'pending' if double opt-in required
            'merge_fields': {
                'FNAME': user_data.get('first_name', ''),
                'LNAME': user_data.get('last_name', '')
            },
            'tags': [user_data.get('source', 'sync_script')]
        }
        
        # Add address if available
        if user_data.get('address_line1'):
            email_data['merge_fields']['ADDRESS'] = {
                'addr1': user_data.get('address_line1', ''),
                'addr2': user_data.get('address_line2', ''),
                'city': user_data.get('city', ''),
                'state': user_data.get('state', ''),
                'zip': user_data.get('postal_code', ''),
                'country': user_data.get('country', '')
            }
        
        response = self._make_request('POST', url, json=email_data)
        return response
    
    def update_subscriber(self, email: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing subscriber in the email list"""
        import hashlib
        email_hash = hashlib.md5(email.lower().encode()).hexdigest()
        
        url = f"{self.base_url}/lists/{self.list_id}/members/{email_hash}"
        
        # Transform user data to Email Marketing format
        email_data = {
            'merge_fields': {}
        }
        
        if user_data.get('first_name'):
            email_data['merge_fields']['FNAME'] = user_data.get('first_name')
        
        if user_data.get('last_name'):
            email_data['merge_fields']['LNAME'] = user_data.get('last_name')
        
        # Add address if available
        if any(user_data.get(f) for f in ['address_line1', 'city', 'state', 'postal_code', 'country']):
            email_data['merge_fields']['ADDRESS'] = {}
            
            for src, dest in [
                ('address_line1', 'addr1'),
                ('address_line2', 'addr2'),
                ('city', 'city'),
                ('state', 'state'),
                ('postal_code', 'zip'),
                ('country', 'country')
            ]:
                if user_data.get(src):
                    email_data['merge_fields']['ADDRESS'][dest] = user_data.get(src)
        
        response = self._make_request('PATCH', url, json=email_data)
        return response
    
    def find_subscriber_by_email(self, email: str) -> Dict[str, Any]:
        """Find a subscriber by email address"""
        import hashlib
        email_hash = hashlib.md5(email.lower().encode()).hexdigest()
        
        url = f"{self.base_url}/lists/{self.list_id}/members/{email_hash}"
        
        response = self._make_request('GET', url)
        return response
    
    def _make_request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make an API request with retries and error handling"""
        for attempt in range(Config.RETRY_MAX_ATTEMPTS):
            try:
                response = requests.request(
                    method,
                    url,
                    headers=self.headers,
                    timeout=30,
                    **kwargs
                )
                
                # Some APIs like Mailchimp return 404 for non-existent members, handle accordingly
                if method == 'GET' and response.status_code == 404:
                    return None
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Email API request failed: {str(e)}")
                if attempt < Config.RETRY_MAX_ATTEMPTS - 1:
                    wait_time = Config.RETRY_DELAY * (2 ** attempt)
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retry attempts reached. Giving up.")
                    return None
        
        return None

class PaymentService:
    """Service for interacting with the Payment Processor API"""
    
    def __init__(self):
        self.base_url = Config.PAYMENT_API_URL
        self.headers = {
            'Authorization': f'Bearer {Config.PAYMENT_API_KEY}',
            'Content-Type': 'application/json'
        }
    
    def get_recent_customers(self, since: datetime) -> List[Dict[str, Any]]:
        """Get customers created or updated since the given time"""
        # Convert datetime to Unix timestamp for Stripe-like APIs
        since_timestamp = int(since.timestamp())
        
        url = f"{self.base_url}/customers"
        params = {
            'created': {
                'gte': since_timestamp
            },
            'limit': 100
        }
        
        all_customers = []
        has_more = True
        starting_after = None
        
        while has_more:
            if starting_after:
                params['starting_after'] = starting_after
                
            response = self._make_request('GET', url, params=params)
            
            if not response or 'data' not in response:
                break
                
            customers = response['data']
            all_customers.extend(customers)
            
            has_more = response.get('has_more', False)
            if has_more and customers:
                starting_after = customers[-1]['id']
            else:
                break
        
        return all_customers
    
    def create_customer(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new customer in the payment processor"""
        url = f"{self.base_url}/customers"
        
        # Transform user data to Payment Processor format
        payment_data = {
            'email': user_data.get('email', ''),
            'name': f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip(),
            'phone': user_data.get('phone', ''),
            'metadata': {
                'source': user_data.get('source', 'sync_script'),
                'created_by': 'integration',
                'sync_date': datetime.now().isoformat()
            }
        }
        
        # Add address if available
        if user_data.get('address_line1'):
            payment_data['address'] = {
                'line1': user_data.get('address_line1', ''),
                'line2': user_data.get('address_line2', ''),
                'city': user_data.get('city', ''),
                'state': user_data.get('state', ''),
                'postal_code': user_data.get('postal_code', ''),
                'country': user_data.get('country', '')
            }
        
        response = self._make_request('POST', url, json=payment_data)
        return response
    
    def update_customer(self, customer_id: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing customer in the payment processor"""
        url = f"{self.base_url}/customers/{customer_id}"
        
        # Transform user data to Payment Processor format
        payment_data = {
            'metadata': {
                'updated_by': 'integration',
                'sync_date': datetime.now().isoformat()
            }
        }
        
        # Only add fields that are present in user_data
        if user_data.get('email'):
            payment_data['email'] = user_data.get('email')
        
        if user_data.get('first_name') or user_data.get('last_name'):
            name_parts = []
            if user_data.get('first_name'):
                name_parts.append(user_data.get('first_name'))
            if user_data.get('last_name'):
                name_parts.append(user_data.get('last_name'))
            
            if name_parts:
                payment_data['name'] = ' '.join(name_parts)
        
        if user_data.get('phone'):
            payment_data['phone'] = user_data.get('phone')
        
        # Add address if any part is available
        if any(user_data.get(f) for f in ['address_line1', 'city', 'state', 'postal_code', 'country']):
            payment_data['address'] = {}
            
            for field in ['address_line1', 'address_line2', 'city', 'state', 'postal_code', 'country']:
                stripe_field = field.replace('address_', '') if field.startswith('address_') else field
                if user_data.get(field):
                    payment_data['address'][stripe_field] = user_data.get(field)
        
        response = self._make_request('POST', url, json=payment_data)  # Stripe uses POST for updates
        return response
    
    def find_customer_by_email(self, email: str) -> Dict[str, Any]:
        """Find a customer by email address"""
        url = f"{self.base_url}/customers"
        params = {
            'email': email,
            'limit': 1
        }
        
        response = self._make_request('GET', url, params=params)
        
        if response and 'data' in response and response['data']:
            return response['data'][0]
        
        return None
    
    def _make_request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make an API request with retries and error handling"""
        for attempt in range(Config.RETRY_MAX_ATTEMPTS):
            try:
                response = requests.request(
                    method,
                    url,
                    headers=self.headers,
                    timeout=30,
                    **kwargs
                )
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Payment API request failed: {str(e)}")
                if attempt < Config.RETRY_MAX_ATTEMPTS - 1:
                    wait_time = Config.RETRY_DELAY * (2 ** attempt)
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retry attempts reached. Giving up.")
                    return None
        
        return None

class UserSyncManager:
    """Manages the synchronization of users between different systems"""
    
    def __init__(self):
        self.crm_service = CRMService()
        self.email_service = EmailService()
        self.payment_service = PaymentService()
        self.last_sync_time = self._get_last_sync_time()
    
    def sync_users(self) -> None:
        """Perform the user synchronization"""
        current_time = datetime.now()
        
        try:
            # 1. Get new/updated users from each system
            crm_users = self._get_crm_users()
            email_users = self._get_email_users()
            payment_users = self._get_payment_users()
            
            # 2. Sync CRM users to other systems
            for user in crm_users:
                self._sync_crm_user_to_other_systems(user)
            
            # 3. Sync Email users to other systems
            for user in email_users:
                self._sync_email_user_to_other_systems(user)
            
            # 4. Sync Payment users to other systems
            for user in payment_users:
                self._sync_payment_user_to_other_systems(user)
            
            # 5. Update the last sync time
            self._update_last_sync_time(current_time)
            
            logger.info(f"Sync completed successfully. Processed {len(crm_users)} CRM users, "
                       f"{len(email_users)} Email users, and {len(payment_users)} Payment users.")
            
        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")
    
    def _get_crm_users(self) -> List[Dict[str, Any]]:
        """Get recent users from the CRM system"""
        logger.info(f"Fetching recent users from CRM since {self.last_sync_time}")
        contacts = self.crm_service.get_recent_contacts(self.last_sync_time)
        
        # Transform CRM contacts to standard format
        users = []
        for contact in contacts:
            user_data = {
                'email': contact.get('email'),
                'first_name': contact.get('first_name'),
                'last_name': contact.get('last_name'),
                'phone': contact.get('phone'),
                'source': 'crm',
                'crm_id': contact.get('id')
            }
            
            # Add address if available
            address = contact.get('address', {})
            if address:
                user_data.update({
                    'address_line1': address.get('line1'),
                    'address_line2': address.get('line2'),
                    'city': address.get('city'),
                    'state': address.get('state'),
                    'postal_code': address.get('postal_code'),
                    'country': address.get('country')
                })
            
            users.append(user_data)
        
        logger.info(f"Found {len(users)} recent users in CRM")
        return users
    
    def _get_email_users(self) -> List[Dict[str, Any]]:
        """Get recent users from the Email Marketing system"""
        logger.info(f"Fetching recent users from Email Marketing since {self.last_sync_time}")
        subscribers = self.email_service.get_recent_subscribers(self.last_sync_time)
        
        # Transform Email subscribers to standard format
        users = []
        for subscriber in subscribers:
            merge_fields = subscriber.get('merge_fields', {})
            address = merge_fields.get('ADDRESS', {})
            
            user_data = {
                'email': subscriber.get('email_address'),
                'first_name': merge_fields.get('FNAME'),
                'last_name': merge_fields.get('LNAME'),
                'source': 'email',
                'email_id': subscriber.get('id')
            }
            
            # Add address if available
            if address:
                user_data.update({
                    'address_line1': address.get('addr1'),
                    'address_line2': address.get('addr2'),
                    'city': address.get('city'),
                    'state': address.get('state'),
                    'postal_code': address.get('zip'),
                    'country': address.get('country')
                })
            
            users.append(user_data)
        
        logger.info(f"Found {len(users)} recent users in Email Marketing")
        return users
    
    def _get_payment_users(self) -> List[Dict[str, Any]]:
        """Get recent users from the Payment Processor system"""
        logger.info(f"Fetching recent users from Payment Processor since {self.last_sync_time}")
        customers = self.payment_service.get_recent_customers(self.last_sync_time)
        
        # Transform Payment customers to standard format
        users = []
        for customer in customers:
            # Split name into first and last name if possible
            name_parts = customer.get('name', '').split(' ', 1)
            first_name = name_parts[0] if name_parts else ''
            last_name = name_parts[1] if len(name_parts) > 1 else ''
            
            user_data = {
                'email': customer.get('email'),
                'first_name': first_name,
                'last_name': last_name,
                'phone': customer.get('phone'),
                'source': 'payment',
                'payment_id': customer.get('id')
            }
            
            # Add address if available
            address = customer.get('address', {})
            if address:
                user_data.update({
                    'address_line1': address.get('line1'),
                    'address_line2': address.get('line2'),
                    'city': address.get('city'),
                    'state': address.get('state'),
                    'postal_code': address.get('postal_code'),
                    'country': address.get('country')
                })
            
            users.append(user_data)
        
        logger.info(f"Found {len(users)} recent users in Payment Processor")
        return users
    
    def _sync_crm_user_to_other_systems(self, user_data: Dict[str, Any]) -> None:
        """Sync a CRM user to other systems"""
        email = user_data.get('email')
        if not email:
            logger.warning(f"Cannot sync CRM user without email: {user_data.get('crm_id')}")
            return
        
        # Sync to Email Marketing
        email_subscriber = self.email_service.find_subscriber_by_email(email)
        if email_subscriber:
            logger.info(f"Updating existing email subscriber for {email}")
            self.email_service.update_subscriber(email, user_data)
        else:
            logger.info(f"Adding new email subscriber for {email}")
            self.email_service.add_subscriber(user_data)
        
        # Sync to Payment Processor
        payment_customer = self.payment_service.find_customer_by_email(email)
        if payment_customer:
            logger.info(f"Updating existing payment customer for {email}")
            self.payment_service.update_customer(payment_customer['id'], user_data)
        else:
            logger.info(f"Adding new payment customer for {email}")
            self.payment_service.create_customer(user_data)
    
    def _sync_email_user_to_other_systems(self, user_data: Dict[str, Any]) -> None:
        """Sync an Email Marketing user to other systems"""
        email = user_data.get('email')
        if not email:
            logger.warning(f"Cannot sync Email user without email: {user_data.get('email_id')}")
            return
        
        # Sync to CRM
        crm_contact = self.crm_service.find_contact_by_email(email)
        if crm_contact:
            logger.info(f"Updating existing CRM contact for {email}")
            self.crm_service.update_contact(crm_contact['id'], user_data)
        else:
            logger.info(f"Adding new CRM contact for {email}")
            self.crm_service.create_contact(user_data)
        
        # Sync to Payment Processor
        payment_customer = self.payment_service.find_customer_by_email(email)
        if payment_customer:
            logger.info(f"Updating existing payment customer for {email}")
            self.payment_service.update_customer(payment_customer['id'], user_data)
        else:
            logger.info(f"Adding new payment customer for {email}")
            self.payment_service.create_customer(user_data)
    
    def _sync_payment_user_to_other_systems(self, user_data: Dict[str, Any]) -> None:
        """Sync a Payment Processor user to other systems"""
        email = user_data.get('email')
        if not email:
            logger.warning(f"Cannot sync Payment user without email: {user_data.get('payment_id')}")
            return
        
        # Sync to CRM
        crm_contact = self.crm_service.find_contact_by_email(email)
        if crm_contact:
            logger.info(f"Updating existing CRM contact for {email}")
            self.crm_service.update_contact(crm_contact['id'], user_data)
        else:
            logger.info(f"Adding new CRM contact for {email}")
            self.crm_service.create_contact(user_data)
        
        # Sync to Email Marketing
        email_subscriber = self.email_service.find_subscriber_by_email(email)
        if email_subscriber:
            logger.info(f"Updating existing email subscriber for {email}")
            self.email_service.update_subscriber(email, user_data)
        else:
            logger.info(f"Adding new email subscriber for {email}")
            self.email_service.add_subscriber(user_data)
    
    def _get_last_sync_time(self) -> datetime:
        """Get the last synchronization time from persistent storage"""
        try:
            # In a real implementation, this would read from a database or file
            # For this example, we'll look back 24 hours by default
            return datetime.now() - timedelta(hours=24)
        except Exception as e:
            logger.error(f"Error getting last sync time: {str(e)}")
            return datetime.now() - timedelta(hours=24)
    
    def _update_last_sync_time(self, sync_time: datetime) -> None:
        """Update the last synchronization time in persistent storage"""
        try:
            # In a real implementation, this would save to a database or file
            logger.info(f"Updated last sync time to {sync_time}")
        except Exception as e:
            logger.error(f"Error updating last sync time: {str(e)}")

def main():
    """Main entry point for the user sync script"""
    logger.info("Starting user synchronization")
    
    sync_manager = UserSyncManager()
    sync_manager.sync_users()
    
    logger.info("User synchronization completed")

if __name__ == "__main__":
    main()