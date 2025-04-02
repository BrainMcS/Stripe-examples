import requests
import json
import logging
import time
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("customer_sync")

class CustomerSynchronizer:
    def __init__(
        self,
        source_api_url: str,
        source_api_key: str,
        target_api_url: str,
        target_api_key: str
    ):
        self.source_api_url = source_api_url
        self.source_headers = {"Authorization": f"Bearer {source_api_key}"}
        
        self.target_api_url = target_api_url
        self.target_headers = {"Authorization": f"Bearer {target_api_key}"}
        
        # Statistics for the sync report
        self.stats = {
            "total_customers": 0,
            "synced": 0,
            "created": 0,
            "updated": 0,
            "failed": 0,
            "conflicts": 0,
            "skipped": 0
        }
    
    def fetch_customers_from_source(self, modified_since: datetime = None) -> List[Dict]:
        """
        Fetch customers from the source API.
        
        Args:
            modified_since: Only fetch customers modified after this datetime
            
        Returns:
            List of customer records
        """
        logger.info("Fetching customers from source API")
        
        params = {}
        if modified_since:
            # Format datetime as ISO string or timestamp based on API requirements
            params["modified_since"] = modified_since.isoformat()
        
        customers = []
        page = 1
        per_page = 100
        
        while True:
            params["page"] = page
            params["per_page"] = per_page
            
            response = requests.get(
                f"{self.source_api_url}/customers",
                headers=self.source_headers,
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            batch = data.get("customers", [])
            customers.extend(batch)
            
            # Check if we've received all customers
            if len(batch) < per_page or not data.get("has_more", False):
                break
            
            page += 1
        
        logger.info(f"Fetched {len(customers)} customers from source API")
        self.stats["total_customers"] = len(customers)
        return customers
    
    def fetch_customer_from_target(self, customer_id: str) -> Dict:
        """
        Fetch a single customer from the target API by ID.
        
        Args:
            customer_id: Customer ID to fetch
            
        Returns:
            Customer record if found, empty dict if not found
        """
        try:
            response = requests.get(
                f"{self.target_api_url}/customers/{customer_id}",
                headers=self.target_headers
            )
            
            if response.status_code == 404:
                return {}
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching customer {customer_id} from target: {e}")
            return {}
    
    def compare_customers(self, source_customer: Dict, target_customer: Dict) -> Tuple[bool, Dict]:
        """
        Compare source and target customer records to detect changes.
        
        Args:
            source_customer: Customer record from source
            target_customer: Customer record from target
            
        Returns:
            Tuple of (has_changes, merged_data)
        """
        # If the target customer doesn't exist, we need to create it
        if not target_customer:
            return True, source_customer
        
        # Fields to compare (adjust based on your customer data structure)
        fields_to_compare = [
            "email", "name", "phone", "address", "metadata"
        ]
        
        has_changes = False
        merged_data = target_customer.copy()
        
        for field in fields_to_compare:
            if field in source_customer and source_customer.get(field) != target_customer.get(field):
                has_changes = True
                merged_data[field] = source_customer[field]
        
        return has_changes, merged_data
    
    def sync_customer(self, customer: Dict) -> Dict:
        """
        Synchronize a single customer between source and target.
        
        Args:
            customer: Customer record to synchronize
            
        Returns:
            Result of the sync operation
        """
        customer_id = customer.get("id")
        logger.info(f"Syncing customer {customer_id}")
        
        # Check if customer exists in target system
        target_customer = self.fetch_customer_from_target(customer_id)
        
        # Compare and determine changes
        has_changes, merged_data = self.compare_customers(customer, target_customer)
        
        if not has_changes:
            logger.info(f"No changes for customer {customer_id}, skipping")
            self.stats["skipped"] += 1
            return {"status": "skipped", "customer_id": customer_id}
        
        # If customer doesn't exist in target, create it
        if not target_customer:
            try:
                response = requests.post(
                    f"{self.target_api_url}/customers",
                    headers=self.target_headers,
                    json=merged_data
                )
                response.raise_for_status()
                
                logger.info(f"Created customer {customer_id} in target system")
                self.stats["created"] += 1
                return {"status": "created", "customer_id": customer_id}
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to create customer {customer_id}: {e}")
                self.stats["failed"] += 1
                return {"status": "failed", "customer_id": customer_id, "error": str(e)}
        
        # Update existing customer
        try:
            # Check for conflicts if target has a modified timestamp
            if self._has_conflict(customer, target_customer):
                logger.warning(f"Conflict detected for customer {customer_id}")
                self.stats["conflicts"] += 1
                return self._resolve_conflict(customer, target_customer)
            
            # Perform update
            response = requests.put(
                f"{self.target_api_url}/customers/{customer_id}",
                headers=self.target_headers,
                json=merged_data
            )
            response.raise_for_status()
            
            logger.info(f"Updated customer {customer_id} in target system")
            self.stats["updated"] += 1
            return {"status": "updated", "customer_id": customer_id}
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update customer {customer_id}: {e}")
            self.stats["failed"] += 1
            return {"status": "failed", "customer_id": customer_id, "error": str(e)}
    
    def _has_conflict(self, source_customer: Dict, target_customer: Dict) -> bool:
        """
        Check if there's a conflict between source and target records.
        
        A conflict occurs when the target record has been modified more recently
        than the source record's last sync timestamp.
        """
        # If either record doesn't have timestamps, we can't detect conflicts
        if not (source_customer.get("last_synced_at") and target_customer.get("updated_at")):
            return False
        
        source_last_synced = datetime.fromisoformat(source_customer["last_synced_at"])
        target_last_updated = datetime.fromisoformat(target_customer["updated_at"])
        
        return target_last_updated > source_last_synced
    
    def _resolve_conflict(self, source_customer: Dict, target_customer: Dict) -> Dict:
        """
        Resolve a conflict between source and target records.
        
        This is a simple implementation that prioritizes the most recently updated record.
        In a real-world scenario, you might want a more sophisticated strategy or manual review.
        """
        source_updated_at = datetime.fromisoformat(source_customer.get("updated_at", "1970-01-01T00:00:00"))
        target_updated_at = datetime.fromisoformat(target_customer.get("updated_at", "1970-01-01T00:00:00"))
        
        # If target is more recent, keep it
        if target_updated_at > source_updated_at:
            logger.info(f"Conflict resolved: keeping target version for {source_customer.get('id')}")
            return {"status": "conflict_resolved", "resolution": "kept_target", "customer_id": source_customer.get("id")}
        
        # If source is more recent, update target
        logger.info(f"Conflict resolved: updating with source version for {source_customer.get('id')}")
        try:
            response = requests.put(
                f"{self.target_api_url}/customers/{source_customer.get('id')}",
                headers=self.target_headers,
                json=source_customer
            )
            response.raise_for_status()
            return {"status": "conflict_resolved", "resolution": "used_source", "customer_id": source_customer.get("id")}
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to resolve conflict for customer {source_customer.get('id')}: {e}")
            self.stats["failed"] += 1
            return {"status": "failed", "customer_id": source_customer.get("id"), "error": str(e)}
    
    def synchronize_all(self, modified_since: datetime = None) -> Dict:
        """
        Synchronize all customers from source to target.
        
        Args:
            modified_since: Only sync customers modified after this datetime
            
        Returns:
            Synchronization report
        """
        start_time = time.time()
        
        # Fetch customers from source
        customers = self.fetch_customers_from_source(modified_since)
        
        # Sync each customer
        results = []
        for customer in customers:
            result = self.sync_customer(customer)
            results.append(result)
            
            # Update overall sync count
            if result["status"] not in ["failed", "skipped"]:
                self.stats["synced"] += 1
        
        # Calculate sync duration
        duration = time.time() - start_time
        
        # Generate report
        report = {
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": duration,
            "stats": self.stats,
            "details": results
        }
        
        logger.info(f"Synchronization completed in {duration:.2f} seconds")
        logger.info(f"Stats: {json.dumps(self.stats)}")
        
        return report

# Usage example
if __name__ == "__main__":
    # Create synchronizer
    synchronizer = CustomerSynchronizer(
        source_api_url="https://api.source-system.com/v1",
        source_api_key="source_api_key",
        target_api_url="https://api.target-system.com/v1",
        target_api_key="target_api_key"
    )
    
    # Only sync customers modified in the last day
    yesterday = datetime.now() - timedelta(days=1)
    
    # Run synchronization
    report = synchronizer.synchronize_all(modified_since=yesterday)
    
    # Save report to file
    with open(f"sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
        json.dump(report, f, indent=2)
    
    print(f"Synchronization completed. {report['stats']['synced']} customers synced.")