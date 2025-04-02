import csv
import os
import requests
import logging
import time
import argparse
from typing import Dict, Any, List, Optional
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("product_upload.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('product_upload')

class ECommerceAPI:
    """Client for the e-commerce platform API"""
    
    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    def create_product(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new product in the e-commerce platform"""
        url = f"{self.api_url}/products"
        
        response = self._make_request('POST', url, json=product_data)
        return response
    
    def update_product(self, product_id: str, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing product in the e-commerce platform"""
        url = f"{self.api_url}/products/{product_id}"
        
        response = self._make_request('PUT', url, json=product_data)
        return response
    
    def get_product(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Get a product by its ID"""
        url = f"{self.api_url}/products/{product_id}"
        
        response = self._make_request('GET', url)
        return response
    
    def find_product_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """Find a product by its SKU"""
        url = f"{self.api_url}/products"
        params = {
            'filter[sku]': sku
        }
        
        response = self._make_request('GET', url, params=params)
        
        if response and 'data' in response and response['data']:
            return response['data'][0]
        
        return None
    
    def create_category(self, category_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new category in the e-commerce platform"""
        url = f"{self.api_url}/categories"
        
        response = self._make_request('POST', url, json=category_data)
        return response
    
    def get_categories(self) -> List[Dict[str, Any]]:
        """Get all categories from the e-commerce platform"""
        url = f"{self.api_url}/categories"
        params = {
            'limit': 100
        }
        
        all_categories = []
        page = 1
        
        while True:
            params['page'] = page
            response = self._make_request('GET', url, params=params)
            
            if not response or 'data' not in response:
                break
                
            categories = response['data']
            all_categories.extend(categories)
            
            if len(categories) < 100:  # No more pages
                break
                
            page += 1
        
        return all_categories
    
    def upload_image(self, product_id: str, image_url: str) -> Dict[str, Any]:
        """Upload a product image by URL"""
        url = f"{self.api_url}/products/{product_id}/images"
        
        image_data = {
            'src': image_url,
            'position': 1,  # Primary image
            'alt': f"Product image for {product_id}"
        }
        
        response = self._make_request('POST', url, json=image_data)
        return response
    
    def _make_request(self, method: str, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Make an API request with retries and error handling"""
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                response = requests.request(
                    method,
                    url,
                    headers=self.headers,
                    timeout=30,
                    **kwargs
                )
                
                if response.status_code == 429:  # Rate limit exceeded
                    retry_after = int(response.headers.get('Retry-After', retry_delay))
                    logger.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                # Handle 404 for GET requests
                if method == 'GET' and response.status_code == 404:
                    logger.warning(f"Resource not found: {url}")
                    return None
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed: {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retry attempts reached. Giving up.")
                    return None
        
        return None

class ProductDataProcessor:
    """Processes product data from CSV file and transforms it for API upload"""
    
    def __init__(self, api_client: ECommerceAPI):
        self.api_client = api_client
        self.categories_cache = {}
        self._load_categories()
    
    def _load_categories(self) -> None:
        """Load existing categories from the API"""
        logger.info("Loading existing categories from API...")
        categories = self.api_client.get_categories()
        
        for category in categories:
            self.categories_cache[category['name'].lower()] = category['id']
        
        logger.info(f"Loaded {len(self.categories_cache)} categories")
    
    def process_csv_file(self, file_path: str, delimiter: str = ',', batch_size: int = 10) -> Dict[str, Any]:
        """
        Process a CSV file and upload products to the e-commerce platform
        
        Args:
            file_path: Path to the CSV file
            delimiter: CSV delimiter character
            batch_size: Number of products to process in each batch
            
        Returns:
            Dict with summary of the processing
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        results = {
            'total': 0,
            'created': 0,
            'updated': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
        
        logger.info(f"Processing CSV file: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
                
                # Validate required fields
                required_fields = ['sku', 'name', 'price']
                missing_fields = [field for field in required_fields if field not in csv_reader.fieldnames]
                
                if missing_fields:
                    raise ValueError(f"CSV is missing required fields: {', '.join(missing_fields)}")
                
                products_batch = []
                
                for row in csv_reader:
                    results['total'] += 1
                    
                    try:
                        # Clean and validate data
                        product_data = self._transform_row_to_product(row)
                        
                        # Add to current batch
                        products_batch.append(product_data)
                        
                        # Process batch if it reaches the batch size
                        if len(products_batch) >= batch_size:
                            batch_results = self._process_product_batch(products_batch)
                            self._update_results(results, batch_results)
                            products_batch = []
                        
                    except Exception as e:
                        logger.error(f"Error processing row {results['total']}: {str(e)}")
                        results['failed'] += 1
                        results['errors'].append({
                            'row': results['total'],
                            'sku': row.get('sku', 'Unknown'),
                            'error': str(e)
                        })
                
                # Process any remaining products
                if products_batch:
                    batch_results = self._process_product_batch(products_batch)
                    self._update_results(results, batch_results)
            
            logger.info(f"CSV processing completed. "
                       f"Created: {results['created']}, "
                       f"Updated: {results['updated']}, "
                       f"Failed: {results['failed']}, "
                       f"Skipped: {results['skipped']}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing CSV file: {str(e)}")
            raise
    
    def _transform_row_to_product(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Transform a CSV row into a product object for the API"""
        # Clean and validate required fields
        sku = row.get('sku', '').strip()
        if not sku:
            raise ValueError("SKU is required")
        
        name = row.get('name', '').strip()
        if not name:
            raise ValueError("Name is required")
        
        price_str = row.get('price', '').strip().replace('$', '')
        try:
            price = float(price_str)
        except ValueError:
            raise ValueError(f"Invalid price: {row.get('price')}")
        
        # Build the product object
        product = {
            'sku': sku,
            'name': name,
            'price': price,
            'status': row.get('status', 'active').strip().lower(),
            'description': row.get('description', '').strip()
        }
        
        # Handle inventory
        if 'quantity' in row:
            try:
                quantity = int(row['quantity'])
                product['inventory'] = {
                    'manage_stock': True,
                    'quantity': quantity
                }
            except ValueError:
                logger.warning(f"Invalid quantity for SKU {sku}: {row.get('quantity')}")
        
        # Handle weight
        if 'weight' in row and row['weight'].strip():
            try:
                weight = float(row['weight'])
                product['weight'] = weight
            except ValueError:
                logger.warning(f"Invalid weight for SKU {sku}: {row.get('weight')}")
        
        # Handle dimensions
        dimensions = {}
        for dim in ['length', 'width', 'height']:
            if dim in row and row[dim].strip():
                try:
                    dimensions[dim] = float(row[dim])
                except ValueError:
                    logger.warning(f"Invalid {dim} for SKU {sku}: {row.get(dim)}")
        
        if dimensions:
            product['dimensions'] = dimensions
        
        # Handle categories
        if 'category' in row and row['category'].strip():
            categories = [c.strip() for c in row['category'].split('|') if c.strip()]
            if categories:
                product['categories'] = self._get_category_ids(categories)
        
        # Handle images
        if 'image_url' in row and row['image_url'].strip():
            product['image_url'] = row['image_url'].strip()
        
        # Handle attributes
        attributes = {}
        for key in row.keys():
            # Consider fields like 'attr_color', 'attr_size' as custom attributes
            if key.startswith('attr_') and row[key].strip():
                attr_name = key.replace('attr_', '')
                attributes[attr_name] = row[key].strip()
        
        if attributes:
            product['attributes'] = attributes
        
        return product
    
    def _get_category_ids(self, category_names: List[str]) -> List[str]:
        """Get or create categories and return their IDs"""
        category_ids = []
        
        for name in category_names:
            name_lower = name.lower()
            
            if name_lower in self.categories_cache:
                category_ids.append(self.categories_cache[name_lower])
            else:
                # Create the category
                try:
                    category_data = {
                        'name': name,
                        'slug': self._create_slug(name)
                    }
                    
                    category = self.api_client.create_category(category_data)
                    
                    if category and 'id' in category:
                        self.categories_cache[name_lower] = category['id']
                        category_ids.append(category['id'])
                        logger.info(f"Created category: {name}")
                    else:
                        logger.warning(f"Failed to create category: {name}")
                
                except Exception as e:
                    logger.error(f"Error creating category '{name}': {str(e)}")
        
        return category_ids
    
    def _create_slug(self, name: str) -> str:
        """Create a URL slug from a name"""
        # Replace spaces with hyphens and remove special characters
        slug = name.lower().replace(' ', '-')
        slug = ''.join(c for c in slug if c.isalnum() or c == '-')
        
        # Ensure uniqueness with a hash if needed
        if len(slug) < 3:
            hash_suffix = hashlib.md5(name.encode()).hexdigest()[:6]
            slug = f"{slug}-{hash_suffix}"
        
        return slug
    
    def _process_product_batch(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a batch of products"""
        batch_results = {
            'created': 0,
            'updated': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
        
        for product_data in products:
            try:
                sku = product_data['sku']
                logger.info(f"Processing product with SKU: {sku}")
                
                # Check if product already exists
                existing_product = self.api_client.find_product_by_sku(sku)
                
                if existing_product:
                    # Update existing product
                    product_id = existing_product['id']
                    
                    # Remove fields that shouldn't be sent in an update
                    update_data = product_data.copy()
                    image_url = update_data.pop('image_url', None)
                    
                    # Update the product
                    result = self.api_client.update_product(product_id, update_data)
                    
                    if result:
                        batch_results['updated'] += 1
                        logger.info(f"Updated product: {sku}")
                        
                        # Handle image upload separately if provided
                        if image_url:
                            self.api_client.upload_image(product_id, image_url)
                    else:
                        batch_results['failed'] += 1
                        batch_results['errors'].append({
                            'sku': sku,
                            'error': 'Failed to update product'
                        })
                else:
                    # Create new product
                    # Remove image URL from initial creation
                    create_data = product_data.copy()
                    image_url = create_data.pop('image_url', None)
                    
                    # Create the product
                    result = self.api_client.create_product(create_data)
                    
                    if result and 'id' in result:
                        batch_results['created'] += 1
                        logger.info(f"Created product: {sku}")
                        
                        # Upload image if provided
                        if image_url:
                            self.api_client.upload_image(result['id'], image_url)
                    else:
                        batch_results['failed'] += 1
                        batch_results['errors'].append({
                            'sku': sku,
                            'error': 'Failed to create product'
                        })
            
            except Exception as e:
                batch_results['failed'] += 1
                batch_results['errors'].append({
                    'sku': product_data.get('sku', 'Unknown'),
                    'error': str(e)
                })
                logger.error(f"Error processing product {product_data.get('sku', 'Unknown')}: {str(e)}")
        
        return batch_results
    
    def _update_results(self, overall_results: Dict[str, Any], batch_results: Dict[str, Any]) -> None:
        """Update the overall results with batch results"""
        overall_results['created'] += batch_results['created']
        overall_results['updated'] += batch_results['updated']
        overall_results['failed'] += batch_results['failed']
        overall_results['skipped'] += batch_results['skipped']
        overall_results['errors'].extend(batch_results['errors'])

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Process a CSV file and upload products to an e-commerce platform')
    parser.add_argument('file', help='Path to the CSV file')
    parser.add_argument('--api-url', required=True, help='E-commerce API URL')
    parser.add_argument('--api-key', required=True, help='E-commerce API key')
    parser.add_argument('--delimiter', default=',', help='CSV delimiter (default: ,)')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for processing (default: 10)')
    
    args = parser.parse_args()
    
    try:
        # Initialize the API client
        api_client = ECommerceAPI(args.api_url, args.api_key)
        
        # Initialize the product processor
        processor = ProductDataProcessor(api_client)
        
        # Process the CSV file
        results = processor.process_csv_file(args.file, args.delimiter, args.batch_size)
        
        # Print summary
        print("\nProduct Upload Summary:")
        print(f"Total products processed: {results['total']}")
        print(f"Products created: {results['created']}")
        print(f"Products updated: {results['updated']}")
        print(f"Products failed: {results['failed']}")
        print(f"Products skipped: {results['skipped']}")
        
        if results['failed'] > 0:
            print("\nErrors:")
            for error in results['errors'][:10]:  # Show first 10 errors
                print(f"SKU {error['sku']}: {error['error']}")
            
            if len(results['errors']) > 10:
                print(f"... and {len(results['errors']) - 10} more errors. See log file for details.")
        
    except Exception as e:
        logger.error(f"Error in main program: {str(e)}")
        print(f"Error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())