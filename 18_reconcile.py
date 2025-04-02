import csv
import json
import argparse
import logging
import os
from datetime import datetime
from typing import Dict, List, Any
import sqlite3
import requests
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("reconciliation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('reconciliation')

class DataSource:
    """Base class for data sources"""
    
    def __init__(self, name: str):
        self.name = name
    
    def get_transactions(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """
        Get transactions for the specified date range
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            List of transaction dictionaries
        """
        raise NotImplementedError("Subclasses must implement get_transactions")
    
    def get_transaction_fields(self) -> List[str]:
        """
        Get the list of transaction fields that this source provides
        
        Returns:
            List of field names
        """
        raise NotImplementedError("Subclasses must implement get_transaction_fields")

class CSVDataSource(DataSource):
    """Data source for CSV files"""
    
    def __init__(self, name: str, file_path: str, date_field: str, date_format: str = '%Y-%m-%d'):
        """
        Initialize a CSV data source
        
        Args:
            name: Name of the data source
            file_path: Path to the CSV file
            date_field: Field name for the transaction date
            date_format: Format string for parsing dates
        """
        super().__init__(name)
        self.file_path = file_path
        self.date_field = date_field
        self.date_format = date_format
    
    def get_transactions(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get transactions from a CSV file"""
        if not os.path.exists(self.file_path):
            logger.error(f"CSV file not found: {self.file_path}")
            return []
        
        transactions = []
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row in reader:
                    # Parse the date
                    try:
                        date_str = row.get(self.date_field, '')
                        tx_date = datetime.strptime(date_str, self.date_format).date()
                        
                        # Check if within date range
                        if start_date.date() <= tx_date <= end_date.date():
                            transactions.append(row)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error parsing date '{date_str}': {str(e)}")
            
            logger.info(f"Loaded {len(transactions)} transactions from {self.file_path}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error loading transactions from CSV: {str(e)}")
            return []
    
    def get_transaction_fields(self) -> List[str]:
        """Get fields from CSV headers"""
        if not os.path.exists(self.file_path):
            logger.error(f"CSV file not found: {self.file_path}")
            return []
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                return reader.fieldnames or []
        except Exception as e:
            logger.error(f"Error reading CSV headers: {str(e)}")
            return []

class JSONDataSource(DataSource):
    """Data source for JSON files"""
    
    def __init__(self, name: str, file_path: str, date_field: str, 
                date_format: str = '%Y-%m-%d', records_path: str = None):
        """
        Initialize a JSON data source
        
        Args:
            name: Name of the data source
            file_path: Path to the JSON file
            date_field: Field name for the transaction date
            date_format: Format string for parsing dates
            records_path: JSON path to the records array (e.g. 'data.transactions')
        """
        super().__init__(name)
        self.file_path = file_path
        self.date_field = date_field
        self.date_format = date_format
        self.records_path = records_path
    
    def get_transactions(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get transactions from a JSON file"""
        if not os.path.exists(self.file_path):
            logger.error(f"JSON file not found: {self.file_path}")
            return []
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Extract records from the specified path
            records = data
            if self.records_path:
                for key in self.records_path.split('.'):
                    if key in records:
                        records = records[key]
                    else:
                        logger.error(f"Invalid records path: {self.records_path}")
                        return []
            
            # Ensure records is a list
            if not isinstance(records, list):
                logger.error(f"Records is not a list: {type(records)}")
                return []
            
            # Filter by date range
            transactions = []
            for record in records:
                # Parse the date
                try:
                    date_str = record.get(self.date_field, '')
                    tx_date = datetime.strptime(date_str, self.date_format).date()
                    
                    # Check if within date range
                    if start_date.date() <= tx_date <= end_date.date():
                        transactions.append(record)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing date '{date_str}': {str(e)}")
            
            logger.info(f"Loaded {len(transactions)} transactions from {self.file_path}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error loading transactions from JSON: {str(e)}")
            return []
    
    def get_transaction_fields(self) -> List[str]:
        """Get fields from the first JSON record"""
        if not os.path.exists(self.file_path):
            logger.error(f"JSON file not found: {self.file_path}")
            return []
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Extract records from the specified path
            records = data
            if self.records_path:
                for key in self.records_path.split('.'):
                    if key in records:
                        records = records[key]
                    else:
                        logger.error(f"Invalid records path: {self.records_path}")
                        return []
            
            # Get fields from the first record
            if isinstance(records, list) and records:
                return list(records[0].keys())
            
            return []
            
        except Exception as e:
            logger.error(f"Error reading JSON fields: {str(e)}")
            return []

class DatabaseDataSource(DataSource):
    """Data source for SQL databases"""
    
    def __init__(self, name: str, connection_string: str, query: str, 
                params: Dict[str, Any] = None, date_field: str = None):
        """
        Initialize a database data source
        
        Args:
            name: Name of the data source
            connection_string: Database connection string or path
            query: SQL query to execute
            params: Query parameters
            date_field: Field name for the transaction date
        """
        super().__init__(name)
        self.connection_string = connection_string
        self.query = query
        self.params = params or {}
        self.date_field = date_field
    
    def get_transactions(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get transactions from a database"""
        try:
            # Create connection based on the connection string
            if self.connection_string.endswith(('.db', '.sqlite', '.sqlite3')):
                conn = sqlite3.connect(self.connection_string)
                conn.row_factory = sqlite3.Row
            else:
                # In a real implementation, support more database types
                # For now, we just support SQLite
                raise ValueError(f"Unsupported database type: {self.connection_string}")
            
            # Update parameters with date range
            params = self.params.copy()
            if 'start_date' in self.query and '{start_date}' not in self.query:
                params['start_date'] = start_date.strftime('%Y-%m-%d')
            if 'end_date' in self.query and '{end_date}' not in self.query:
                params['end_date'] = end_date.strftime('%Y-%m-%d')
            
            # Format query with parameters if needed
            query = self.query
            if '{start_date}' in query:
                query = query.replace('{start_date}', f"'{start_date.strftime('%Y-%m-%d')}'")
            if '{end_date}' in query:
                query = query.replace('{end_date}', f"'{end_date.strftime('%Y-%m-%d')}'")
            
            # Execute query
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            # Convert rows to dictionaries
            transactions = []
            for row in cursor.fetchall():
                if isinstance(row, sqlite3.Row):
                    transaction = {key: row[key] for key in row.keys()}
                else:
                    # Fallback for other types of cursors
                    transaction = {col[0]: value for col, value in zip(cursor.description, row)}
                
                transactions.append(transaction)
            
            cursor.close()
            conn.close()
            
            logger.info(f"Loaded {len(transactions)} transactions from database")
            return transactions
            
        except Exception as e:
            logger.error(f"Error loading transactions from database: {str(e)}")
            return []
    
    def get_transaction_fields(self) -> List[str]:
        """Get fields from the query columns"""
        try:
            # Create connection based on the connection string
            if self.connection_string.endswith(('.db', '.sqlite', '.sqlite3')):
                conn = sqlite3.connect(self.connection_string)
            else:
                # In a real implementation, support more database types
                raise ValueError(f"Unsupported database type: {self.connection_string}")
            
            # Execute query with a LIMIT 0 to get column names without data
            query = f"{self.query.rstrip(';')} LIMIT 0;"
            cursor = conn.cursor()
            cursor.execute(query, self.params)
            
            # Get column names
            fields = [col[0] for col in cursor.description]
            
            cursor.close()
            conn.close()
            
            return fields
            
        except Exception as e:
            logger.error(f"Error getting database fields: {str(e)}")
            return []

class APIDataSource(DataSource):
    """Data source for API endpoints"""
    
    def __init__(self, name: str, base_url: str, endpoint: str, 
                auth_token: str = None, date_field: str = None,
                date_format: str = '%Y-%m-%d', records_path: str = None):
        """
        Initialize an API data source
        
        Args:
            name: Name of the data source
            base_url: Base URL for API requests
            endpoint: API endpoint to call
            auth_token: Authentication token
            date_field: Field name for the transaction date
            date_format: Format string for parsing dates
            records_path: JSON path to the records array (e.g. 'data.transactions')
        """
        super().__init__(name)
        self.base_url = base_url.rstrip('/')
        self.endpoint = endpoint
        self.auth_token = auth_token
        self.date_field = date_field
        self.date_format = date_format
        self.records_path = records_path
    
    def get_transactions(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get transactions from an API"""
        try:
            url = f"{self.base_url}/{self.endpoint.lstrip('/')}"
            
            # Set up headers
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            if self.auth_token:
                headers['Authorization'] = f"Bearer {self.auth_token}"
            
            # Set up parameters
            params = {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
            }
            
            # Make the request
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract records from the specified path
            records = data
            if self.records_path:
                for key in self.records_path.split('.'):
                    if key in records:
                        records = records[key]
                    else:
                        logger.error(f"Invalid records path: {self.records_path}")
                        return []
            
            # Ensure records is a list
            if not isinstance(records, list):
                logger.error(f"Records is not a list: {type(records)}")
                return []
            
            # Filter by date if needed (if API doesn't filter by date)
            if self.date_field:
                transactions = []
                for record in records:
                    # Parse the date
                    try:
                        date_str = record.get(self.date_field, '')
                        tx_date = datetime.strptime(date_str, self.date_format).date()
                        
                        # Check if within date range
                        if start_date.date() <= tx_date <= end_date.date():
                            transactions.append(record)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error parsing date '{date_str}': {str(e)}")
                
                logger.info(f"Loaded {len(transactions)} transactions from API")
                return transactions
            else:
                # Trust that the API filtered correctly
                logger.info(f"Loaded {len(records)} transactions from API")
                return records
            
        except Exception as e:
            logger.error(f"Error loading transactions from API: {str(e)}")
            return []
    
    def get_transaction_fields(self) -> List[str]:
        """Get fields by fetching a sample record"""
        try:
            url = f"{self.base_url}/{self.endpoint.lstrip('/')}"
            
            # Set up headers
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            if self.auth_token:
                headers['Authorization'] = f"Bearer {self.auth_token}"
            
            # Set up parameters to fetch a single record
            params = {
                'limit': 1
            }
            
            # Make the request
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract records from the specified path
            records = data
            if self.records_path:
                for key in self.records_path.split('.'):
                    if key in records:
                        records = records[key]
                    else:
                        logger.error(f"Invalid records path: {self.records_path}")
                        return []
            
            # Get fields from the first record
            if isinstance(records, list) and records:
                return list(records[0].keys())
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting API fields: {str(e)}")
            return []

class ReconciliationSystem:
    """System for reconciling transaction data between two sources"""
    
    def __init__(self, source_a: DataSource, source_b: DataSource, key_fields: List[str]):
        """
        Initialize the reconciliation system
        
        Args:
            source_a: First data source
            source_b: Second data source
            key_fields: Fields to use as the unique key for matching transactions
        """
        self.source_a = source_a
        self.source_b = source_b
        self.key_fields = key_fields
    
    def generate_key(self, transaction: Dict[str, Any]) -> str:
        """
        Generate a unique key for a transaction based on key fields
        
        Args:
            transaction: Transaction data
            
        Returns:
            Unique key string
        """
        key_parts = []
        
        for field in self.key_fields:
            value = transaction.get(field, '')
            key_parts.append(str(value))
        
        return '|'.join(key_parts)
    
    def reconcile(self, start_date: datetime, end_date: datetime, 
                comparison_fields: List[str] = None) -> Dict[str, Any]:
        """
        Reconcile transactions between two sources
        
        Args:
            start_date: Start date for reconciliation
            end_date: End date for reconciliation
            comparison_fields: Fields to compare (defaults to all common fields)
            
        Returns:
            Reconciliation results
        """
        logger.info(f"Starting reconciliation for {start_date.date()} to {end_date.date()}")
        
        # Get transactions from both sources
        transactions_a = self.source_a.get_transactions(start_date, end_date)
        transactions_b = self.source_b.get_transactions(start_date, end_date)
        
        # Create dictionaries keyed by the unique key
        transactions_a_dict = {self.generate_key(tx): tx for tx in transactions_a}
        transactions_b_dict = {self.generate_key(tx): tx for tx in transactions_b}
        
        # Find keys in both sources
        keys_a = set(transactions_a_dict.keys())
        keys_b = set(transactions_b_dict.keys())
        
        keys_common = keys_a.intersection(keys_b)
        keys_only_a = keys_a - keys_b
        keys_only_b = keys_b - keys_a
        
        logger.info(f"Found {len(keys_common)} matching transactions")
        logger.info(f"Found {len(keys_only_a)} transactions only in {self.source_a.name}")
        logger.info(f"Found {len(keys_only_b)} transactions only in {self.source_b.name}")
        
        # If no comparison fields specified, use all common fields
        if not comparison_fields:
            fields_a = set(transactions_a[0].keys()) if transactions_a else set()
            fields_b = set(transactions_b[0].keys()) if transactions_b else set()
            comparison_fields = list(fields_a.intersection(fields_b) - set(self.key_fields))
        
        # Compare matching transactions
        matches = []
        mismatches = []
        
        for key in keys_common:
            tx_a = transactions_a_dict[key]
            tx_b = transactions_b_dict[key]
            
            mismatch_fields = []
            
            for field in comparison_fields:
                value_a = tx_a.get(field)
                value_b = tx_b.get(field)
                
                # Compare values (handle different types)
                if self._normalize_value(value_a) != self._normalize_value(value_b):
                    mismatch_fields.append({
                        'field': field,
                        'value_a': value_a,
                        'value_b': value_b
                    })
            
            if mismatch_fields:
                mismatches.append({
                    'key': key,
                    'transaction_a': tx_a,
                    'transaction_b': tx_b,
                    'mismatches': mismatch_fields
                })
            else:
                matches.append({
                    'key': key,
                    'transaction_a': tx_a,
                    'transaction_b': tx_b
                })
        
        # Prepare transactions that only exist in one source
        only_in_a = [transactions_a_dict[key] for key in keys_only_a]
        only_in_b = [transactions_b_dict[key] for key in keys_only_b]
        
        # Prepare results
        results = {
            'start_date': start_date.date().isoformat(),
            'end_date': end_date.date().isoformat(),
            'source_a': self.source_a.name,
            'source_b': self.source_b.name,
            'summary': {
                'total_a': len(transactions_a),
                'total_b': len(transactions_b),
                'matches': len(matches),
                'mismatches': len(mismatches),
                'only_in_a': len(only_in_a),
                'only_in_b': len(only_in_b)
            },
            'mismatches': mismatches,
            'only_in_a': only_in_a,
            'only_in_b': only_in_b
        }
        
        return results
    
    def _normalize_value(self, value: Any) -> Any:
        """
        Normalize a value for comparison
        
        Args:
            value: Value to normalize
            
        Returns:
            Normalized value
        """
        # Convert to string for comparison
        if value is None:
            return ''
        
        # Handle numeric values
        if isinstance(value, (int, float)):
            # Round to 2 decimal places for monetary values
            if isinstance(value, float):
                return round(value, 2)
            return value
        
        # Handle string values
        if isinstance(value, str):
            # Trim whitespace and convert to lowercase
            return value.strip().lower()
        
        # Handle boolean values
        if isinstance(value, bool):
            return value
        
        # Handle other types
        return str(value)
    
    def generate_report(self, results: Dict[str, Any], format: str = 'text') -> str:
        """
        Generate a report from reconciliation results
        
        Args:
            results: Reconciliation results
            format: Output format ('text', 'csv', or 'json')
            
        Returns:
            Formatted report
        """
        if format == 'json':
            return json.dumps(results, indent=2)
        
        if format == 'csv':
            return self._generate_csv_report(results)
        
        # Default to text report
        return self._generate_text_report(results)
    
    def _generate_text_report(self, results: Dict[str, Any]) -> str:
        """Generate a text report"""
        report = []
        
        # Header
        report.append("==== TRANSACTION RECONCILIATION REPORT ====")
        report.append(f"Period: {results['start_date']} to {results['end_date']}")
        report.append(f"Source A: {results['source_a']}")
        report.append(f"Source B: {results['source_b']}")
        report.append("")
        
        # Summary
        summary = results['summary']
        report.append("=== SUMMARY ===")
        report.append(f"Total transactions in {results['source_a']}: {summary['total_a']}")
        report.append(f"Total transactions in {results['source_b']}: {summary['total_b']}")
        report.append(f"Matching transactions: {summary['matches']}")
        report.append(f"Mismatched transactions: {summary['mismatches']}")
        report.append(f"Transactions only in {results['source_a']}: {summary['only_in_a']}")
        report.append(f"Transactions only in {results['source_b']}: {summary['only_in_b']}")
        report.append("")
        
        # Mismatches
        if results['mismatches']:
            report.append("=== MISMATCHED TRANSACTIONS ===")
            for i, mismatch in enumerate(results['mismatches'][:10], 1):  # Show first 10
                report.append(f"Mismatch {i}: Key = {mismatch['key']}")
                
                for field_mismatch in mismatch['mismatches']:
                    field = field_mismatch['field']
                    value_a = field_mismatch['value_a']
                    value_b = field_mismatch['value_b']
                    report.append(f"  {field}: {value_a} (A) vs {value_b} (B)")
                
                report.append("")
            
            if len(results['mismatches']) > 10:
                report.append(f"... and {len(results['mismatches']) - 10} more mismatches")
                report.append("")
        
        # Transactions only in source A
        if results['only_in_a']:
            report.append(f"=== TRANSACTIONS ONLY IN {results['source_a']} ===")
            for i, tx in enumerate(results['only_in_a'][:10], 1):  # Show first 10
                report.append(f"Transaction {i}: {self._format_transaction(tx)}")
            
            if len(results['only_in_a']) > 10:
                report.append(f"... and {len(results['only_in_a']) - 10} more transactions")
                report.append("")
        
        # Transactions only in source B
        if results['only_in_b']:
            report.append(f"=== TRANSACTIONS ONLY IN {results['source_b']} ===")
            for i, tx in enumerate(results['only_in_b'][:10], 1):  # Show first 10
                report.append(f"Transaction {i}: {self._format_transaction(tx)}")
            
            if len(results['only_in_b']) > 10:
                report.append(f"... and {len(results['only_in_b']) - 10} more transactions")
                report.append("")
        
        return "\n".join(report)
    
    def _format_transaction(self, transaction: Dict[str, Any]) -> str:
        """Format a transaction for display"""
        parts = []
        
        # Always include key fields
        for field in self.key_fields:
            value = transaction.get(field, '')
            parts.append(f"{field}={value}")
        
        # Include a few additional important fields
        important_fields = ['amount', 'currency', 'status', 'type', 'date']
        for field in important_fields:
            if field in transaction and field not in self.key_fields:
                value = transaction.get(field, '')
                parts.append(f"{field}={value}")
        
        return ", ".join(parts)
    
    def _generate_csv_report(self, results: Dict[str, Any]) -> str:
        """Generate a CSV report"""
        # Use pandas for easier CSV generation
        # Mismatches
        mismatches_data = []
        for mismatch in results['mismatches']:
            tx_a = mismatch['transaction_a']
            tx_b = mismatch['transaction_b']
            key = mismatch['key']
            
            for field_mismatch in mismatch['mismatches']:
                field = field_mismatch['field']
                value_a = field_mismatch['value_a']
                value_b = field_mismatch['value_b']
                
                mismatches_data.append({
                    'key': key,
                    'field': field,
                    'value_a': value_a,
                    'value_b': value_b
                })
        
        mismatches_df = pd.DataFrame(mismatches_data)
        
        # Only in A
        only_a_df = pd.DataFrame(results['only_in_a'])
        
        # Only in B
        only_b_df = pd.DataFrame(results['only_in_b'])
        
        # Create a StringIO object to hold CSV data
        import io
        output = io.StringIO()
        
        # Write summary
        output.write("TRANSACTION RECONCILIATION REPORT\n")
        output.write(f"Period: {results['start_date']} to {results['end_date']}\n")
        output.write(f"Source A: {results['source_a']}\n")
        output.write(f"Source B: {results['source_b']}\n\n")
        
        output.write("SUMMARY\n")
        summary = results['summary']
        output.write(f"Total transactions in {results['source_a']}: {summary['total_a']}\n")
        output.write(f"Total transactions in {results['source_b']}: {summary['total_b']}\n")
        output.write(f"Matching transactions: {summary['matches']}\n")
        output.write(f"Mismatched transactions: {summary['mismatches']}\n")
        output.write(f"Transactions only in {results['source_a']}: {summary['only_in_a']}\n")
        output.write(f"Transactions only in {results['source_b']}: {summary['only_in_b']}\n\n")
        
        # Write mismatches
        output.write("MISMATCHED TRANSACTIONS\n")
        if not mismatches_data:
            output.write("No mismatches found.\n\n")
        else:
            mismatches_df.to_csv(output, index=False)
            output.write("\n")
        
        # Write only in A
        output.write(f"TRANSACTIONS ONLY IN {results['source_a']}\n")
        if only_a_df.empty:
            output.write(f"No transactions found only in {results['source_a']}.\n\n")
        else:
            only_a_df.to_csv(output, index=False)
            output.write("\n")
        
        # Write only in B
        output.write(f"TRANSACTIONS ONLY IN {results['source_b']}\n")
        if only_b_df.empty:
            output.write(f"No transactions found only in {results['source_b']}.\n\n")
        else:
            only_b_df.to_csv(output, index=False)
        
        return output.getvalue()

def main():
    """Command line interface for the reconciliation system"""
    parser = argparse.ArgumentParser(description='Reconcile transaction data between two systems')
    
    # Date range arguments
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    
    # Source arguments
    parser.add_argument('--source-a-type', choices=['csv', 'json', 'db', 'api'], required=True, 
                       help='Type of source A')
    parser.add_argument('--source-a-name', required=True, help='Name of source A')
    
    parser.add_argument('--source-b-type', choices=['csv', 'json', 'db', 'api'], required=True, 
                       help='Type of source B')
    parser.add_argument('--source-b-name', required=True, help='Name of source B')
    
    # CSV source arguments
    parser.add_argument('--source-a-csv-file', help='CSV file for source A')
    parser.add_argument('--source-b-csv-file', help='CSV file for source B')
    
    # JSON source arguments
    parser.add_argument('--source-a-json-file', help='JSON file for source A')
    parser.add_argument('--source-b-json-file', help='JSON file for source B')
    
    # DB source arguments
    parser.add_argument('--source-a-db-conn', help='Database connection for source A')
    parser.add_argument('--source-b-db-conn', help='Database connection for source B')
    parser.add_argument('--source-a-db-query', help='SQL query for source A')
    parser.add_argument('--source-b-db-query', help='SQL query for source B')
    
    # API source arguments
    parser.add_argument('--source-a-api-url', help='API URL for source A')
    parser.add_argument('--source-b-api-url', help='API URL for source B')
    parser.add_argument('--source-a-api-endpoint', help='API endpoint for source A')
    parser.add_argument('--source-b-api-endpoint', help='API endpoint for source B')
    parser.add_argument('--source-a-api-token', help='API token for source A')
    parser.add_argument('--source-b-api-token', help='API token for source B')
    
    # Common arguments
    parser.add_argument('--source-a-date-field', required=True, help='Date field for source A')
    parser.add_argument('--source-b-date-field', required=True, help='Date field for source B')
    parser.add_argument('--source-a-date-format', default='%Y-%m-%d', help='Date format for source A')
    parser.add_argument('--source-b-date-format', default='%Y-%m-%d', help='Date format for source B')
    
    # Reconciliation arguments
    parser.add_argument('--key-fields', required=True, help='Comma-separated list of key fields')
    parser.add_argument('--compare-fields', help='Comma-separated list of fields to compare')
    
    # Output arguments
    parser.add_argument('--output', help='Output file')
    parser.add_argument('--format', choices=['text', 'csv', 'json'], default='text', 
                       help='Output format')
    
    args = parser.parse_args()
    
    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError as e:
        logger.error(f"Error parsing dates: {str(e)}")
        return 1
    
    # Create source A
    source_a = None
    if args.source_a_type == 'csv':
        if not args.source_a_csv_file:
            logger.error("Missing source_a_csv_file argument for CSV source")
            return 1
        
        source_a = CSVDataSource(
            name=args.source_a_name,
            file_path=args.source_a_csv_file,
            date_field=args.source_a_date_field,
            date_format=args.source_a_date_format
        )
    elif args.source_a_type == 'json':
        if not args.source_a_json_file:
            logger.error("Missing source_a_json_file argument for JSON source")
            return 1
        
        source_a = JSONDataSource(
            name=args.source_a_name,
            file_path=args.source_a_json_file,
            date_field=args.source_a_date_field,
            date_format=args.source_a_date_format
        )
    elif args.source_a_type == 'db':
        if not args.source_a_db_conn or not args.source_a_db_query:
            logger.error("Missing source_a_db_conn or source_a_db_query argument for DB source")
            return 1
        
        source_a = DatabaseDataSource(
            name=args.source_a_name,
            connection_string=args.source_a_db_conn,
            query=args.source_a_db_query,
            date_field=args.source_a_date_field
        )
    elif args.source_a_type == 'api':
        if not args.source_a_api_url or not args.source_a_api_endpoint:
            logger.error("Missing source_a_api_url or source_a_api_endpoint argument for API source")
            return 1
        
        source_a = APIDataSource(
            name=args.source_a_name,
            base_url=args.source_a_api_url,
            endpoint=args.source_a_api_endpoint,
            auth_token=args.source_a_api_token,
            date_field=args.source_a_date_field,
            date_format=args.source_a_date_format
        )
    
    # Create source B
    source_b = None
    if args.source_b_type == 'csv':
        if not args.source_b_csv_file:
            logger.error("Missing source_b_csv_file argument for CSV source")
            return 1
        
        source_b = CSVDataSource(
            name=args.source_b_name,
            file_path=args.source_b_csv_file,
            date_field=args.source_b_date_field,
            date_format=args.source_b_date_format
        )
    elif args.source_b_type == 'json':
        if not args.source_b_json_file:
            logger.error("Missing source_b_json_file argument for JSON source")
            return 1
        
        source_b = JSONDataSource(
            name=args.source_b_name,
            file_path=args.source_b_json_file,
            date_field=args.source_b_date_field,
            date_format=args.source_b_date_format
        )
    elif args.source_b_type == 'db':
        if not args.source_b_db_conn or not args.source_b_db_query:
            logger.error("Missing source_b_db_conn or source_b_db_query argument for DB source")
            return 1
        
        source_b = DatabaseDataSource(
            name=args.source_b_name,
            connection_string=args.source_b_db_conn,
            query=args.source_b_db_query,
            date_field=args.source_b_date_field
        )
    elif args.source_b_type == 'api':
        if not args.source_b_api_url or not args.source_b_api_endpoint:
            logger.error("Missing source_b_api_url or source_b_api_endpoint argument for API source")
            return 1
        
        source_b = APIDataSource(
            name=args.source_b_name,
            base_url=args.source_b_api_url,
            endpoint=args.source_b_api_endpoint,
            auth_token=args.source_b_api_token,
            date_field=args.source_b_date_field,
            date_format=args.source_b_date_format
        )
    
    # Parse key fields
    key_fields = args.key_fields.split(',')
    
    # Parse comparison fields
    compare_fields = args.compare_fields.split(',') if args.compare_fields else None
    
    # Create reconciliation system
    reconciliation = ReconciliationSystem(source_a, source_b, key_fields)
    
    # Run reconciliation
    results = reconciliation.reconcile(start_date, end_date, compare_fields)
    
    # Generate report
    report = reconciliation.generate_report(results, args.format)
    
    # Output report
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as file:
            file.write(report)
        
        logger.info(f"Report written to {args.output}")
    else:
        print(report)
    
    return 0

if __name__ == "__main__":
    exit(main())