import requests
import urllib3
import logging
import ssl
import json
from requests.exceptions import SSLError, ConnectionError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api_debugger")

class APIDebugger:
    def __init__(self):
        self.session = requests.Session()
    
    def diagnose_and_fix(self, url, headers=None, payload=None, method="GET"):
        """Diagnose and fix issues with API connectivity."""
        logger.info(f"Diagnosing connection to {url}")
        
        # Step 1: Check TLS version
        self._check_tls_version()
        
        # Step 2: Verify the URL is correctly formatted
        if not (url.startswith("http://") or url.startswith("https://")):
            logger.warning(f"URL {url} doesn't have proper protocol. Adding https://")
            url = f"https://{url}"
        
        # Step 3: Test basic connectivity
        try:
            logger.info("Testing basic connectivity...")
            response = requests.get("https://www.google.com", timeout=5)
            response.raise_for_status()
            logger.info("Internet connection is working")
        except Exception as e:
            logger.error(f"Internet connection issue: {e}")
            return {"success": False, "error": "Internet connectivity issue", "details": str(e)}
        
        # Step 4: Check headers for authentication issues
        if headers and "Authorization" in headers:
            auth_header = headers["Authorization"]
            if auth_header.startswith("Bearer"):
                if len(auth_header.split(" ")) != 2 or len(auth_header.split(" ")[1]) < 10:
                    logger.warning("Authorization header may be malformed")
            logger.info("Authorization header present")
        else:
            logger.warning("No Authorization header found. API might require authentication.")
        
        # Step 5: Attempt connection with different SSL configurations
        try:
            logger.info("Attempting connection with standard configuration...")
            return self._make_request(url, headers, payload, method)
        except SSLError as e:
            logger.warning(f"SSL error encountered: {e}")
            logger.info("Trying with custom SSL context...")
            return self._make_request_with_custom_ssl(url, headers, payload, method)
        except ConnectionError as e:
            logger.error(f"Connection error: {e}")
            return {"success": False, "error": "Connection error", "details": str(e)}
        except Exception as e:
            logger.error(f"Unknown error: {e}")
            return {"success": False, "error": "Unknown error", "details": str(e)}
    
    def _check_tls_version(self):
        """Check and report on the TLS version being used."""
        try:
            response = requests.get("https://www.howsmyssl.com/a/check", timeout=5)
            data = response.json()
            tls_version = data.get("tls_version", "Unknown")
            logger.info(f"TLS Version: {tls_version}")
            
            # Check if TLS is adequate (1.2 or higher)
            if tls_version in ["TLS 1.0", "TLS 1.1"]:
                logger.warning(f"TLS version {tls_version} is outdated. TLS 1.2 or higher is recommended.")
            return tls_version
        except Exception as e:
            logger.error(f"Failed to check TLS version: {e}")
            return "Unknown"
    
    def _make_request(self, url, headers=None, payload=None, method="GET"):
        """Make an HTTP request and return the response."""
        try:
            if method.upper() == "GET":
                response = self.session.get(url, headers=headers, timeout=10)
            elif method.upper() == "POST":
                response = self.session.post(url, headers=headers, json=payload, timeout=10)
            elif method.upper() == "PUT":
                response = self.session.put(url, headers=headers, json=payload, timeout=10)
            elif method.upper() == "DELETE":
                response = self.session.delete(url, headers=headers, timeout=10)
            else:
                return {"success": False, "error": f"Unsupported method: {method}"}
            
            logger.info(f"Response status code: {response.status_code}")
            
            # Check for common status code issues
            if response.status_code == 401:
                return {"success": False, "error": "Authentication failed. Check your API key."}
            elif response.status_code == 403:
                return {"success": False, "error": "Authorization failed. Check your permissions."}
            elif response.status_code == 404:
                return {"success": False, "error": "Resource not found. Check the URL."}
            elif response.status_code == 429:
                return {"success": False, "error": "Rate limit exceeded. Try again later."}
            
            try:
                data = response.json()
                return {"success": True, "status_code": response.status_code, "data": data}
            except ValueError:
                return {"success": True, "status_code": response.status_code, "data": response.text}
        
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def _make_request_with_custom_ssl(self, url, headers=None, payload=None, method="GET"):
        """Attempt a request with a custom SSL context."""
        try:
            # Create a custom SSL context
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
            # Create a custom HTTPAdapter with the SSL context
            adapter = urllib3.poolmanager.PoolManager(ssl_context=context)
            self.session.mount("https://", adapter)
            
            logger.warning("Using custom SSL context with certificate verification disabled!")
            
            return self._make_request(url, headers, payload, method)
        except Exception as e:
            logger.error(f"Request with custom SSL failed: {e}")
            return {"success": False, "error": "SSL configuration failed", "details": str(e)}

# Usage example
if __name__ == "__main__":
    debugger = APIDebugger()
    
    # Example of debugging a problematic API call
    url = "https://api.stripe.com/v1/users"
    headers = {
        "Authorization": "Bearer invalid_token",
        "Content-Type": "application/json"
    }
    
    result = debugger.diagnose_and_fix(url, headers)
    print(json.dumps(result, indent=2))