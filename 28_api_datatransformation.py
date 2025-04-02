import requests
import pandas as pd
import json
import csv
from datetime import datetime
from typing import Dict, List, Any
from dotenv import load_dotenv
import os

# Configuration
load_dotenv()
API_KEY = os.getenv("STRIPE_API_KEY")
BASE_URL = "https://api.stripe.com/v1"

class TransactionAnalyzer:
    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.headers = {"Authorization": f"Bearer {api_key}"}
    
    def fetch_all_transactions(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Fetch all transactions within a date range, handling pagination.
        
        Args:
            start_date: ISO format date string (YYYY-MM-DD)
            end_date: ISO format date string (YYYY-MM-DD)
            
        Returns:
            List of transaction dictionaries
        """
        transactions = []
        page = 1
        has_more = True
        
        # Convert dates to timestamps if the API requires it
        start_timestamp = int(datetime.fromisoformat(start_date).timestamp())
        end_timestamp = int(datetime.fromisoformat(end_date).timestamp())
        
        while has_more:
            params = {
                "start_date": start_timestamp,
                "end_date": end_timestamp,
                "page": page,
                "limit": 100
            }
            
            response = requests.get(f"{self.api_url}/transactions", headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Add this page of results to our list
            transactions.extend(data["transactions"])
            
            # Check if there are more pages
            has_more = data["has_more"]
            page += 1
        
        return transactions
    
    def group_by_category(self, transactions: List[Dict]) -> Dict[str, List[Dict]]:
        """Group transactions by their category."""
        categories = {}
        
        for transaction in transactions:
            category = transaction.get("category", "Uncategorized")
            if category not in categories:
                categories[category] = []
            categories[category].append(transaction)
        
        return categories
    
    def calculate_summary(self, transactions: List[Dict]) -> Dict[str, Any]:
        """Calculate summary statistics for a list of transactions."""
        if not transactions:
            return {
                "count": 0,
                "total_amount": 0,
                "average_amount": 0,
                "min_amount": 0,
                "max_amount": 0
            }
        
        amounts = [t["amount"] for t in transactions]
        
        return {
            "count": len(transactions),
            "total_amount": sum(amounts),
            "average_amount": sum(amounts) / len(amounts),
            "min_amount": min(amounts),
            "max_amount": max(amounts)
        }
    
    def generate_report(self, start_date: str, end_date: str, output_format: str = "json") -> str:
        """
        Generate a transaction report for the specified date range.
        
        Args:
            start_date: ISO format date string (YYYY-MM-DD)
            end_date: ISO format date string (YYYY-MM-DD)
            output_format: Format for the report ("json" or "csv")
            
        Returns:
            Report as a string in the specified format
        """
        # Fetch all transactions
        transactions = self.fetch_all_transactions(start_date, end_date)
        
        # Group by category
        categories = self.group_by_category(transactions)
        
        # Calculate overall summary
        overall_summary = self.calculate_summary(transactions)
        
        # Calculate summary for each category
        report_data = {
            "report_period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "overall_summary": overall_summary,
            "categories": {}
        }
        
        for category, txns in categories.items():
            report_data["categories"][category] = {
                "summary": self.calculate_summary(txns),
                "transactions": txns
            }
        
        # Generate output in the requested format
        if output_format.lower() == "json":
            return json.dumps(report_data, indent=2)
        
        elif output_format.lower() == "csv":
            # Flatten the data for CSV format
            rows = []
            for category, data in report_data["categories"].items():
                for txn in data["transactions"]:
                    txn["category"] = category
                    rows.append(txn)
            
            # Convert to CSV
            if not rows:
                return "No transactions found for the specified period."
            
            output = []
            headers = rows[0].keys()
            output.append(",".join(headers))
            
            for row in rows:
                csv_row = [str(row.get(header, "")) for header in headers]
                output.append(",".join(csv_row))
            
            return "\n".join(output)
        
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

# Usage example
if __name__ == "__main__":
    analyzer = TransactionAnalyzer(BASE_URL, API_KEY)
    
    # Generate a JSON report for January 2023
    json_report = analyzer.generate_report("2023-01-01", "2023-01-31", "json")
    with open("january_report.json", "w") as f:
        f.write(json_report)
    
    # Generate a CSV report for February 2023
    csv_report = analyzer.generate_report("2023-02-01", "2023-02-28", "csv")
    with open("february_report.csv", "w") as f:
        f.write(csv_report)