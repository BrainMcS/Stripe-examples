import requests

class PaymentClient:
    def __init__(self, api_key, base_url="https://api.example.com"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })
    
    def create_charge(self, amount, currency, source):
        response = self.session.post(
            f"{self.base_url}/charges",
            json={
                "amount": amount,
                "currency": currency,
                "source": source
            }
        )
        response.raise_for_status()
        return response.json()
    
    # Example extension: Add refund capability
    def refund_charge(self, charge_id, amount=None):
        payload = {"charge": charge_id}
        if amount:
            payload["amount"] = amount
            
        response = self.session.post(
            f"{self.base_url}/refunds",
            json=payload
        )
        response.raise_for_status()
        return response.json()