import os
import requests
import logging
import sqlite3
import base64
import secrets
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('token_manager')

class CredentialEncryption:
    """Handles encryption and decryption of sensitive credential data"""
    
    def __init__(self, master_key: str = None):
        """
        Initialize the encryption engine with a master key
        
        Args:
            master_key: Master encryption key. If None, will try to load from environment or generate one.
        """
        self.master_key = master_key or os.environ.get('TOKEN_MASTER_KEY')
        
        if not self.master_key:
            # Generate a new master key if none exists
            logger.warning("No master key provided. Generating a new one.")
            self.master_key = secrets.token_hex(32)
            logger.info(f"Generated new master key: {self.master_key}")
            logger.info("IMPORTANT: Store this key securely for future use")
        
        # Create a key derivation function
        salt = b'token_manager_salt'  # In production, this should be securely stored
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        
        # Derive the key from the master key
        key = base64.urlsafe_b64encode(kdf.derive(self.master_key.encode()))
        self.cipher = Fernet(key)
    
    def encrypt(self, data: str) -> str:
        """Encrypt a string"""
        return self.cipher.encrypt(data.encode()).decode()
    
    def decrypt(self, encrypted_data: str) -> str:
        """Decrypt an encrypted string"""
        return self.cipher.decrypt(encrypted_data.encode()).decode()

class TokenDatabase:
    """Manages storage and retrieval of authentication tokens"""
    
    def __init__(self, db_path: str = "tokens.db", encryption: CredentialEncryption = None):
        """
        Initialize the token database
        
        Args:
            db_path: Path to the SQLite database file
            encryption: Encryption engine for sensitive data
        """
        self.db_path = db_path
        self.encryption = encryption or CredentialEncryption()
        self._initialize_db()
    
    def _initialize_db(self) -> None:
        """Initialize the database schema if it doesn't exist"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            # Create services table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS services (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                auth_type TEXT NOT NULL,
                base_url TEXT NOT NULL,
                client_id TEXT,
                client_secret TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            # Create tokens table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_id INTEGER NOT NULL,
                user_id TEXT,
                access_token TEXT NOT NULL,
                refresh_token TEXT,
                token_type TEXT,
                expires_at TIMESTAMP,
                scope TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (service_id) REFERENCES services(id),
                UNIQUE (service_id, user_id)
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
        return sqlite3.connect(self.db_path)
    
    def add_service(self, name: str, auth_type: str, base_url: str, 
                   client_id: str = None, client_secret: str = None) -> int:
        """
        Add a new service configuration
        
        Args:
            name: Service name (e.g., 'github', 'google')
            auth_type: Authentication type ('oauth2', 'api_key', etc.)
            base_url: Base URL for API requests
            client_id: OAuth client ID
            client_secret: OAuth client secret
            
        Returns:
            The ID of the created or updated service
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            # Encrypt sensitive data
            encrypted_client_secret = None
            if client_secret:
                encrypted_client_secret = self.encryption.encrypt(client_secret)
            
            # Check if service already exists
            cursor.execute("SELECT id FROM services WHERE name = ?", (name,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing service
                service_id = existing[0]
                cursor.execute('''
                UPDATE services 
                SET auth_type = ?, base_url = ?, client_id = ?, client_secret = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                ''', (auth_type, base_url, client_id, encrypted_client_secret, service_id))
                logger.info(f"Updated service configuration: {name}")
            else:
                # Create new service
                cursor.execute('''
                INSERT INTO services (name, auth_type, base_url, client_id, client_secret)
                VALUES (?, ?, ?, ?, ?)
                ''', (name, auth_type, base_url, client_id, encrypted_client_secret))
                service_id = cursor.lastrowid
                logger.info(f"Added new service configuration: {name}")
            
            conn.commit()
            return service_id
            
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"Error adding service {name}: {str(e)}")
            raise
        finally:
            conn.close()
    
    def get_service(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a service configuration by name"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT id, name, auth_type, base_url, client_id, client_secret
            FROM services WHERE name = ?
            ''', (name,))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            service = {
                'id': row[0],
                'name': row[1],
                'auth_type': row[2],
                'base_url': row[3],
                'client_id': row[4],
                'client_secret': row[5]
            }
            
            # Decrypt client secret if it exists
            if service['client_secret']:
                service['client_secret'] = self.encryption.decrypt(service['client_secret'])
            
            return service
            
        except sqlite3.Error as e:
            logger.error(f"Error getting service {name}: {str(e)}")
            return None
        finally:
            conn.close()
    
    def store_token(self, service_name: str, token_data: Dict[str, Any], user_id: str = None) -> bool:
        """
        Store an authentication token
        
        Args:
            service_name: Name of the service
            token_data: Token data including access_token, refresh_token, etc.
            user_id: Optional user identifier for multi-user systems
            
        Returns:
            True if successful, False otherwise
        """
        conn = self._get_connection()
        try:
            # Get service ID
            service = self.get_service(service_name)
            if not service:
                logger.error(f"Service not found: {service_name}")
                return False
            
            service_id = service['id']
            
            # Calculate expiration time if provided with expires_in
            expires_at = None
            if 'expires_in' in token_data:
                expires_at = datetime.now() + timedelta(seconds=token_data['expires_in'])
            elif 'expires_at' in token_data:
                expires_at = datetime.fromtimestamp(token_data['expires_at'])
            
            # Encrypt tokens
            encrypted_access_token = self.encryption.encrypt(token_data['access_token'])
            encrypted_refresh_token = None
            if token_data.get('refresh_token'):
                encrypted_refresh_token = self.encryption.encrypt(token_data['refresh_token'])
            
            cursor = conn.cursor()
            
            # Check if token already exists for this service and user
            cursor.execute('''
            SELECT id FROM tokens WHERE service_id = ? AND (user_id = ? OR (user_id IS NULL AND ? IS NULL))
            ''', (service_id, user_id, user_id))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing token
                token_id = existing[0]
                cursor.execute('''
                UPDATE tokens 
                SET access_token = ?, refresh_token = ?, token_type = ?, 
                    expires_at = ?, scope = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                ''', (
                    encrypted_access_token, 
                    encrypted_refresh_token,
                    token_data.get('token_type'),
                    expires_at.isoformat() if expires_at else None,
                    token_data.get('scope'),
                    token_id
                ))
                logger.info(f"Updated token for service {service_name}" + (f" and user {user_id}" if user_id else ""))
            else:
                # Create new token
                cursor.execute('''
                INSERT INTO tokens 
                (service_id, user_id, access_token, refresh_token, token_type, expires_at, scope)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    service_id,
                    user_id,
                    encrypted_access_token,
                    encrypted_refresh_token,
                    token_data.get('token_type'),
                    expires_at.isoformat() if expires_at else None,
                    token_data.get('scope')
                ))
                logger.info(f"Stored new token for service {service_name}" + (f" and user {user_id}" if user_id else ""))
            
            conn.commit()
            return True
            
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"Error storing token for {service_name}: {str(e)}")
            return False
        except Exception as e:
            conn.rollback()
            logger.error(f"Unexpected error storing token: {str(e)}")
            return False
        finally:
            conn.close()
    
    def get_token(self, service_name: str, user_id: str = None) -> Optional[Dict[str, Any]]:
        """
        Get a stored token
        
        Args:
            service_name: Name of the service
            user_id: Optional user identifier
            
        Returns:
            Token data if found, None otherwise
        """
        conn = self._get_connection()
        try:
            # Get service ID
            service = self.get_service(service_name)
            if not service:
                logger.error(f"Service not found: {service_name}")
                return None
            
            service_id = service['id']
            
            cursor = conn.cursor()
            cursor.execute('''
            SELECT access_token, refresh_token, token_type, expires_at, scope, created_at, updated_at
            FROM tokens 
            WHERE service_id = ? AND (user_id = ? OR (user_id IS NULL AND ? IS NULL))
            ''', (service_id, user_id, user_id))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            # Decrypt tokens
            access_token = self.encryption.decrypt(row[0])
            refresh_token = row[1]
            if refresh_token:
                refresh_token = self.encryption.decrypt(refresh_token)
            
            # Parse expires_at to determine if token is expired
            expires_at = row[3]
            is_expired = False
            if expires_at:
                expires_at_dt = datetime.fromisoformat(expires_at)
                is_expired = expires_at_dt <= datetime.now()
            
            token = {
                'access_token': access_token,
                'refresh_token': refresh_token,
                'token_type': row[2],
                'expires_at': expires_at,
                'scope': row[4],
                'created_at': row[5],
                'updated_at': row[6],
                'is_expired': is_expired
            }
            
            return token
            
        except sqlite3.Error as e:
            logger.error(f"Error getting token for {service_name}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting token: {str(e)}")
            return None
        finally:
            conn.close()
    
    def delete_token(self, service_name: str, user_id: str = None) -> bool:
        """Delete a stored token"""
        conn = self._get_connection()
        try:
            # Get service ID
            service = self.get_service(service_name)
            if not service:
                logger.error(f"Service not found: {service_name}")
                return False
            
            service_id = service['id']
            
            cursor = conn.cursor()
            cursor.execute('''
            DELETE FROM tokens 
            WHERE service_id = ? AND (user_id = ? OR (user_id IS NULL AND ? IS NULL))
            ''', (service_id, user_id, user_id))
            
            conn.commit()
            
            if cursor.rowcount > 0:
                logger.info(f"Deleted token for service {service_name}" + (f" and user {user_id}" if user_id else ""))
                return True
            else:
                logger.warning(f"No token found to delete for service {service_name}" + (f" and user {user_id}" if user_id else ""))
                return False
            
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"Error deleting token for {service_name}: {str(e)}")
            return False
        finally:
            conn.close()

class OAuth2Handler:
    """Handles OAuth2 authentication flow and token management"""
    
    def __init__(self, token_db: TokenDatabase):
        """
        Initialize the OAuth2 handler
        
        Args:
            token_db: Token database instance
        """
        self.token_db = token_db
    
    def get_authorization_url(self, service_name: str, redirect_uri: str, 
                             scope: str = None, state: str = None) -> Tuple[str, str]:
        """
        Get the authorization URL for the OAuth2 flow
        
        Args:
            service_name: Name of the service
            redirect_uri: Callback URL after authorization
            scope: Space-separated list of requested scopes
            state: Optional state parameter to prevent CSRF
            
        Returns:
            Tuple of (authorization_url, state)
        """
        service = self.token_db.get_service(service_name)
        if not service:
            raise ValueError(f"Service not found: {service_name}")
        
        if service['auth_type'] != 'oauth2':
            raise ValueError(f"Service {service_name} does not use OAuth2")
        
        # Generate state if not provided
        if not state:
            state = secrets.token_hex(16)
        
        # Build authorization URL
        params = {
            'client_id': service['client_id'],
            'redirect_uri': redirect_uri,
            'response_type': 'code',
            'state': state
        }
        
        if scope:
            params['scope'] = scope
        
        auth_url = f"{service['base_url']}/authorize?{self._build_query_string(params)}"
        
        return auth_url, state
    
    def exchange_code_for_token(self, service_name: str, code: str, redirect_uri: str, user_id: str = None) -> Dict[str, Any]:
        """
        Exchange an authorization code for an access token
        
        Args:
            service_name: Name of the service
            code: Authorization code from the callback
            redirect_uri: Callback URL, must match the one used in authorization
            user_id: Optional user identifier
            
        Returns:
            Token data
        """
        service = self.token_db.get_service(service_name)
        if not service:
            raise ValueError(f"Service not found: {service_name}")
        
        if service['auth_type'] != 'oauth2':
            raise ValueError(f"Service {service_name} does not use OAuth2")
        
        # Build token request
        token_url = f"{service['base_url']}/token"
        
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': redirect_uri,
            'client_id': service['client_id'],
            'client_secret': service['client_secret']
        }
        
        # Send token request
        response = requests.post(token_url, data=data, timeout=30)
        response.raise_for_status()
        token_data = response.json()
        
        # Store the token
        self.token_db.store_token(service_name, token_data, user_id)
        
        return token_data
    
    def refresh_token(self, service_name: str, user_id: str = None) -> Optional[Dict[str, Any]]:
        """
        Refresh an expired token
        
        Args:
            service_name: Name of the service
            user_id: Optional user identifier
            
        Returns:
            New token data if successful, None otherwise
        """
        service = self.token_db.get_service(service_name)
        if not service:
            logger.error(f"Service not found: {service_name}")
            return None
        
        if service['auth_type'] != 'oauth2':
            logger.error(f"Service {service_name} does not use OAuth2")
            return None
        
        # Get current token
        current_token = self.token_db.get_token(service_name, user_id)
        if not current_token:
            logger.error(f"No token found for service {service_name}" + (f" and user {user_id}" if user_id else ""))
            return None
        
        if not current_token.get('refresh_token'):
            logger.error(f"No refresh token available for service {service_name}")
            return None
        
        # Build refresh request
        token_url = f"{service['base_url']}/token"
        
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': current_token['refresh_token'],
            'client_id': service['client_id'],
            'client_secret': service['client_secret']
        }
        
        try:
            # Send refresh request
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()
            
            # If the response doesn't include a refresh token, keep the old one
            if 'refresh_token' not in token_data and current_token.get('refresh_token'):
                token_data['refresh_token'] = current_token['refresh_token']
            
            # Store the new token
            self.token_db.store_token(service_name, token_data, user_id)
            
            logger.info(f"Successfully refreshed token for service {service_name}" + (f" and user {user_id}" if user_id else ""))
            return token_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error refreshing token: {str(e)}")
            return None
    
    def get_token(self, service_name: str, user_id: str = None, auto_refresh: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get a valid token, refreshing if necessary
        
        Args:
            service_name: Name of the service
            user_id: Optional user identifier
            auto_refresh: Whether to automatically refresh expired tokens
            
        Returns:
            Valid token data if available, None otherwise
        """
        token = self.token_db.get_token(service_name, user_id)
        
        if not token:
            logger.warning(f"No token found for service {service_name}" + (f" and user {user_id}" if user_id else ""))
            return None
        
        # Check if token is expired and refresh if needed
        if token.get('is_expired') and auto_refresh:
            logger.info(f"Token expired for service {service_name}. Attempting to refresh...")
            token = self.refresh_token(service_name, user_id)
            
            if not token:
                logger.error(f"Failed to refresh token for service {service_name}")
                return None
        
        return token
    
    def revoke_token(self, service_name: str, user_id: str = None) -> bool:
        """
        Revoke a token (if supported by the service)
        
        Args:
            service_name: Name of the service
            user_id: Optional user identifier
            
        Returns:
            True if successful, False otherwise
        """
        service = self.token_db.get_service(service_name)
        if not service:
            logger.error(f"Service not found: {service_name}")
            return False
        
        # Get current token
        current_token = self.token_db.get_token(service_name, user_id)
        if not current_token:
            logger.error(f"No token found for service {service_name}" + (f" and user {user_id}" if user_id else ""))
            return False
        
        # Not all services support token revocation
        # This is a generic implementation and might need customization for specific services
        revoke_url = f"{service['base_url']}/revoke"
        
        data = {
            'token': current_token['access_token'],
            'client_id': service['client_id'],
            'client_secret': service['client_secret']
        }
        
        try:
            # Send revocation request
            response = requests.post(revoke_url, data=data, timeout=30)
            response.raise_for_status()
            
            # Delete the token from our database
            self.token_db.delete_token(service_name, user_id)
            
            logger.info(f"Successfully revoked token for service {service_name}" + (f" and user {user_id}" if user_id else ""))
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error revoking token: {str(e)}")
            
            # Even if the revocation request fails, we might still want to delete the token locally
            self.token_db.delete_token(service_name, user_id)
            
            return False
    
    def _build_query_string(self, params: Dict[str, str]) -> str:
        """Build a URL query string from a dictionary"""
        return '&'.join([f"{k}={requests.utils.quote(str(v))}" for k, v in params.items()])

class TokenManager:
    """Main class for managing authentication tokens across services"""
    
    def __init__(self, db_path: str = "tokens.db", master_key: str = None):
        """
        Initialize the token manager
        
        Args:
            db_path: Path to the SQLite database file
            master_key: Master encryption key
        """
        self.encryption = CredentialEncryption(master_key)
        self.token_db = TokenDatabase(db_path, self.encryption)
        self.oauth_handler = OAuth2Handler(self.token_db)
    
    def register_oauth2_service(self, name: str, base_url: str, client_id: str, client_secret: str) -> bool:
        """
        Register an OAuth2 service
        
        Args:
            name: Service name
            base_url: Base URL for API requests
            client_id: OAuth client ID
            client_secret: OAuth client secret
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.token_db.add_service(name, 'oauth2', base_url, client_id, client_secret)
            return True
        except Exception as e:
            logger.error(f"Error registering OAuth2 service {name}: {str(e)}")
            return False
    
    def register_api_key_service(self, name: str, base_url: str) -> bool:
        """
        Register a service that uses API keys
        
        Args:
            name: Service name
            base_url: Base URL for API requests
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.token_db.add_service(name, 'api_key', base_url)
            return True
        except Exception as e:
            logger.error(f"Error registering API key service {name}: {str(e)}")
            return False
    
    def start_oauth2_flow(self, service_name: str, redirect_uri: str, scope: str = None) -> Tuple[str, str]:
        """
        Start the OAuth2 authorization flow
        
        Args:
            service_name: Name of the service
            redirect_uri: Callback URL after authorization
            scope: Space-separated list of requested scopes
            
        Returns:
            Tuple of (authorization_url, state)
        """
        return self.oauth_handler.get_authorization_url(service_name, redirect_uri, scope)
    
    def complete_oauth2_flow(self, service_name: str, code: str, redirect_uri: str, 
                           state: str = None, expected_state: str = None, user_id: str = None) -> Dict[str, Any]:
        """
        Complete the OAuth2 authorization flow
        
        Args:
            service_name: Name of the service
            code: Authorization code from the callback
            redirect_uri: Callback URL
            state: State parameter from the callback
            expected_state: Expected state parameter to validate
            user_id: Optional user identifier
            
        Returns:
            Token data
        """
        # Verify state if provided
        if expected_state and state and expected_state != state:
            raise ValueError("State mismatch, possible CSRF attack")
        
        return self.oauth_handler.exchange_code_for_token(service_name, code, redirect_uri, user_id)
    
    def get_valid_token(self, service_name: str, user_id: str = None) -> Optional[Dict[str, Any]]:
        """
        Get a valid token for a service
        
        Args:
            service_name: Name of the service
            user_id: Optional user identifier
            
        Returns:
            Valid token data if available, None otherwise
        """
        # For OAuth2 services
        service = self.token_db.get_service(service_name)
        if not service:
            logger.error(f"Service not found: {service_name}")
            return None
        
        if service['auth_type'] == 'oauth2':
            return self.oauth_handler.get_token(service_name, user_id)
        else:
            # For API key services, just return the stored token
            return self.token_db.get_token(service_name, user_id)
    
    def store_api_key(self, service_name: str, api_key: str, user_id: str = None) -> bool:
        """
        Store an API key
        
        Args:
            service_name: Name of the service
            api_key: The API key
            user_id: Optional user identifier
            
        Returns:
            True if successful, False otherwise
        """
        token_data = {
            'access_token': api_key,
            'token_type': 'api_key'
        }
        
        return self.token_db.store_token(service_name, token_data, user_id)
    
    def revoke_token(self, service_name: str, user_id: str = None) -> bool:
        """
        Revoke a token for a service
        
        Args:
            service_name: Name of the service
            user_id: Optional user identifier
            
        Returns:
            True if successful, False otherwise
        """
        service = self.token_db.get_service(service_name)
        if not service:
            logger.error(f"Service not found: {service_name}")
            return False
        
        if service['auth_type'] == 'oauth2':
            return self.oauth_handler.revoke_token(service_name, user_id)
        else:
            # For API key services, just delete the token
            return self.token_db.delete_token(service_name, user_id)

# Example usage
def example_oauth2_flow():
    """Example of using the TokenManager for OAuth2 flow"""
    # Initialize token manager
    manager = TokenManager(db_path="example_tokens.db")
    
    # Register a service (e.g., GitHub)
    manager.register_oauth2_service(
        name="github",
        base_url="https://github.com",
        client_id="your_client_id",
        client_secret="your_client_secret"
    )
    
    # Start OAuth2 flow - in a real app, redirect the user to this URL
    auth_url, state = manager.start_oauth2_flow(
        service_name="github",
        redirect_uri="http://localhost:5000/callback",
        scope="repo user"
    )
    
    print(f"Authorization URL: {auth_url}")
    print(f"State: {state}")
    
    # After user authorizes and is redirected to your callback URL,
    # you would receive a code and state parameter
    # For demo purposes, we'll simulate this
    code = input("Enter the code from the callback URL: ")
    
    # Complete the OAuth2 flow
    token = manager.complete_oauth2_flow(
        service_name="github",
        code=code,
        redirect_uri="http://localhost:5000/callback",
        expected_state=state
    )
    
    print(f"Access token: {token['access_token']}")
    
    # Later, when you need to make API calls
    token = manager.get_valid_token("github")
    if token:
        headers = {
            'Authorization': f"{token['token_type']} {token['access_token']}"
        }
        # Make your API call with these headers

def example_api_key_flow():
    """Example of using the TokenManager for API key authentication"""
    # Initialize token manager
    manager = TokenManager(db_path="example_tokens.db")
    
    # Register a service (e.g., Stripe)
    manager.register_api_key_service(
        name="stripe",
        base_url="https://api.stripe.com/v1"
    )
    
    # Store an API key
    api_key = os.environ.get("STRIPE_API_KEY")
    if api_key:
        manager.store_api_key("stripe", api_key)
    else:
        raise ValueError("Stripe API key not found. Please set the STRIPE_API_KEY environment variable.")
    
    # Later, when you need to make API calls
    token = manager.get_valid_token("stripe")
    if token:
        headers = {
            'Authorization': f"Bearer {token['access_token']}"
        }
        # Make your API call with these headers

if __name__ == "__main__":
    print("OAuth2 Token Manager Example")
    print("1. OAuth2 Flow Example")
    print("2. API Key Example")
    
    choice = input("Choose an example (1 or 2): ")
    
    if choice == "1":
        example_oauth2_flow()
    elif choice == "2":
        example_api_key_flow()
    else:
        print("Invalid choice")