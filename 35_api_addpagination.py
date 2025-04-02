import requests

class APIClient:
    def __init__(self, api_key, base_url="https://api.example.com/v1"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })
    
    def get_customers(self, limit=None):
        """
        Get customers with optional pagination.
        
        Args:
            limit: Maximum number of customers to return, or None for all
            
        Returns:
            List of customer objects
        """
        if limit is not None and limit <= 100:
            # Simple case: we can get all customers in one request
            return self._get_customers_page(limit=limit)
        
        # We need to handle pagination
        all_customers = []
        starting_after = None
        page_size = 100  # Maximum page size
        
        while True:
            # Get a page of customers
            page_params = {"limit": page_size}
            if starting_after:
                page_params["starting_after"] = starting_after
            
            page = self._get_customers_page(**page_params)
            all_customers.extend(page)
            
            # Check if we've reached our limit
            if limit and len(all_customers) >= limit:
                return all_customers[:limit]
            
            # Check if there are more pages
            if len(page) < page_size:
                # We got fewer results than requested, so this is the last page
                break
            
            # Get ID for pagination
            if page:
                starting_after = page[-1]["id"]
            else:
                break
        
        return all_customers
    
    def _get_customers_page(self, **params):
        """
        Get a single page of customers with the given parameters.
        """
        response = self.session.get(f"{self.base_url}/customers", params=params)
        response.raise_for_status()
        return response.json().get('data', [])
    
    def get_customer(self, customer_id):
        response = self.session.get(f"{self.base_url}/customers/{customer_id}")
        response.raise_for_status()
        return response.json()

# Example usage
client = APIClient("sk_test_your_api_key")

# Get first 200 customers
first_200 = client.get_customers(limit=200)
print(f"Retrieved {len(first_200)} customers")

# Get all customers (potentially hundreds or thousands)
all_customers = client.get_customers()
print(f"Retrieved {len(all_customers)} customers total")