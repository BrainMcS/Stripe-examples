import time
import logging
import requests
from typing import Dict, List, Any, Callable, Optional, Iterator, Tuple
from dataclasses import dataclass
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('api_batch_processor')

@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_retries: int = 3
    initial_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    backoff_factor: float = 2.0
    jitter: bool = True
    retry_on_status_codes: List[int] = None  # HTTP status codes to retry
    
    def __post_init__(self):
        # Default status codes to retry: server errors and rate limiting
        if self.retry_on_status_codes is None:
            self.retry_on_status_codes = [429, 500, 502, 503, 504]

@dataclass
class PaginationConfig:
    """Configuration for pagination behavior"""
    page_size: int = 100
    max_pages: Optional[int] = None  # None means no limit
    page_param: str = 'page'
    size_param: str = 'limit'
    # Different pagination styles:
    # - 'offset': Uses page numbers (1, 2, 3...)
    # - 'cursor': Uses a cursor/token from the previous response
    # - 'link': Uses links in the response (e.g., 'next', 'prev')
    style: str = 'offset'
    # For cursor pagination
    cursor_field: str = 'next_cursor'
    cursor_param: str = 'cursor'
    # For link pagination
    link_field: str = 'links'
    next_link_field: str = 'next'

@dataclass
class BatchConfig:
    """Configuration for batch processing"""
    batch_size: int = 50
    max_concurrent: int = 5
    max_items: Optional[int] = None  # None means no limit

class APIBatchProcessor:
    """
    Utility for processing large datasets via API calls with batching, 
    pagination, error handling, and retries.
    """
    
    def __init__(self, 
                base_url: str, 
                headers: Dict[str, str] = None,
                retry_config: RetryConfig = None,
                pagination_config: PaginationConfig = None,
                batch_config: BatchConfig = None):
        """
        Initialize the API batch processor
        
        Args:
            base_url: Base URL for API calls
            headers: HTTP headers to include in all requests
            retry_config: Configuration for retry behavior
            pagination_config: Configuration for pagination behavior
            batch_config: Configuration for batch processing
        """
        self.base_url = base_url.rstrip('/')
        self.headers = headers or {}
        self.retry_config = retry_config or RetryConfig()
        self.pagination_config = pagination_config or PaginationConfig()
        self.batch_config = batch_config or BatchConfig()
        
        # Create a session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def make_request(self, 
                    method: str, 
                    endpoint: str, 
                    params: Dict[str, Any] = None,
                    json_data: Dict[str, Any] = None,
                    **kwargs) -> requests.Response:
        """
        Make an API request with retry logic
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (will be appended to base_url)
            params: Query parameters
            json_data: JSON body data
            **kwargs: Additional arguments to pass to requests
            
        Returns:
            Response object
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        current_retry = 0
        delay = self.retry_config.initial_delay
        
        while True:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                    **kwargs
                )
                
                # Check if we should retry based on status code
                if (response.status_code in self.retry_config.retry_on_status_codes and 
                    current_retry < self.retry_config.max_retries):
                    
                    # Check for Retry-After header
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            # Try to parse as seconds
                            delay = float(retry_after)
                        except ValueError:
                            # Could be a HTTP date format, for simplicity we'll use our calculated delay
                            pass
                    
                    current_retry += 1
                    self._sleep_with_backoff(current_retry, delay)
                    delay = min(delay * self.retry_config.backoff_factor, self.retry_config.max_delay)
                    continue
                
                # Return the response if we're not retrying
                return response
                
            except requests.RequestException as e:
                if current_retry < self.retry_config.max_retries:
                    logger.warning(f"Request error: {str(e)}. Retrying ({current_retry + 1}/{self.retry_config.max_retries})...")
                    current_retry += 1
                    self._sleep_with_backoff(current_retry, delay)
                    delay = min(delay * self.retry_config.backoff_factor, self.retry_config.max_delay)
                else:
                    logger.error(f"Request failed after {self.retry_config.max_retries} retries: {str(e)}")
                    raise
    
    def _sleep_with_backoff(self, retry_number: int, delay: float) -> None:
        """
        Sleep with exponential backoff and optional jitter
        
        Args:
            retry_number: Current retry number
            delay: Base delay in seconds
        """
        if self.retry_config.jitter:
            # Add jitter to avoid thundering herd problem
            jitter = random.uniform(0, 0.1 * delay * retry_number)
            delay += jitter
        
        logger.info(f"Retrying in {delay:.2f} seconds...")
        time.sleep(delay)
    
    def paginate(self, 
                endpoint: str, 
                method: str = 'GET',
                params: Dict[str, Any] = None,
                data_extractor: Callable[[Dict[str, Any]], List[Any]] = None) -> Iterator[Any]:
        """
        Paginate through an API endpoint, yielding items
        
        Args:
            endpoint: API endpoint
            method: HTTP method
            params: Additional query parameters
            data_extractor: Function to extract data items from response
            
        Yields:
            Data items from each page
        """
        params = params or {}
        page_num = 1
        items_processed = 0
        cursor = None
        next_url = None
        
        # Add pagination parameters based on style
        if self.pagination_config.style == 'offset':
            params[self.pagination_config.size_param] = self.pagination_config.page_size
            params[self.pagination_config.page_param] = page_num
        elif self.pagination_config.style == 'cursor' and cursor:
            params[self.pagination_config.cursor_param] = cursor
        
        while True:
            if self.pagination_config.max_pages and page_num > self.pagination_config.max_pages:
                logger.info(f"Reached maximum pages limit ({self.pagination_config.max_pages})")
                break
            
            # For link-style pagination, use the next_url if available
            request_url = next_url or endpoint
            
            # Make the request
            try:
                if self.pagination_config.style == 'link' and next_url:
                    # For link pagination with a full URL, don't use the base_url
                    response = requests.request(method, next_url, params=params)
                else:
                    response = self.make_request(method, request_url, params=params)
                
                response.raise_for_status()
                data = response.json()
                
                # Extract data items
                if data_extractor:
                    items = data_extractor(data)
                elif isinstance(data, list):
                    items = data
                elif 'data' in data and isinstance(data['data'], list):
                    items = data['data']
                elif 'results' in data and isinstance(data['results'], list):
                    items = data['results']
                elif 'items' in data and isinstance(data['items'], list):
                    items = data['items']
                else:
                    logger.warning(f"Could not extract items from response, using empty list")
                    items = []
                
                # Yield each item
                for item in items:
                    yield item
                    items_processed += 1
                    
                    # Check if we've reached the max items limit
                    if self.batch_config.max_items and items_processed >= self.batch_config.max_items:
                        logger.info(f"Reached maximum items limit ({self.batch_config.max_items})")
                        return
                
                # Handle pagination based on style
                if self.pagination_config.style == 'offset':
                    # If we got fewer items than the page size, we're at the end
                    if len(items) < self.pagination_config.page_size:
                        break
                    
                    # Move to the next page
                    page_num += 1
                    params[self.pagination_config.page_param] = page_num
                
                elif self.pagination_config.style == 'cursor':
                    # Extract cursor from response
                    cursor = self._extract_nested_value(data, self.pagination_config.cursor_field)
                    
                    # If no cursor, we're at the end
                    if not cursor:
                        break
                    
                    # Update cursor parameter
                    params[self.pagination_config.cursor_param] = cursor
                
                elif self.pagination_config.style == 'link':
                    # Extract next URL from response
                    links = self._extract_nested_value(data, self.pagination_config.link_field)
                    if isinstance(links, dict):
                        next_url = links.get(self.pagination_config.next_link_field)
                    else:
                        next_url = None
                    
                    # If no next URL, we're at the end
                    if not next_url:
                        break
                
                else:
                    # Unknown pagination style
                    logger.warning(f"Unknown pagination style: {self.pagination_config.style}")
                    break
                
                logger.info(f"Fetched page {page_num}, yielded {len(items)} items")
                
            except requests.HTTPError as e:
                logger.error(f"HTTP error during pagination: {str(e)}")
                break
            except Exception as e:
                logger.error(f"Error during pagination: {str(e)}")
                break
    
    def _extract_nested_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """
        Extract a value from a nested dictionary using a dot-separated path
        
        Args:
            data: Dictionary to extract from
            field_path: Dot-separated path to the field
            
        Returns:
            Extracted value or None if not found
        """
        if not field_path:
            return None
        
        current = data
        for field in field_path.split('.'):
            if isinstance(current, dict) and field in current:
                current = current[field]
            else:
                return None
        
        return current
    
    def process_batch(self, items: List[Any], processor_func: Callable[[Any], Any]) -> List[Tuple[Any, Any]]:
        """
        Process a batch of items using the provided function
        
        Args:
            items: List of items to process
            processor_func: Function to apply to each item
            
        Returns:
            List of tuples with (original_item, result)
        """
        results = []
        
        for item in items:
            try:
                result = processor_func(item)
                results.append((item, result))
            except Exception as e:
                logger.error(f"Error processing item: {str(e)}")
                results.append((item, e))
        
        return results
    
    def process_batch_concurrent(self, items: List[Any], processor_func: Callable[[Any], Any]) -> List[Tuple[Any, Any]]:
        """
        Process a batch of items concurrently using the provided function
        
        Args:
            items: List of items to process
            processor_func: Function to apply to each item
            
        Returns:
            List of tuples with (original_item, result)
        """
        results = []
        
        with ThreadPoolExecutor(max_workers=self.batch_config.max_concurrent) as executor:
            # Submit all tasks
            future_to_item = {executor.submit(processor_func, item): item for item in items}
            
            # Process results as they complete
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                    results.append((item, result))
                except Exception as e:
                    logger.error(f"Error processing item: {str(e)}")
                    results.append((item, e))
        
        return results
    
    def process_all(self, 
                   endpoint: str, 
                   processor_func: Callable[[Any], Any],
                   method: str = 'GET',
                   params: Dict[str, Any] = None,
                   data_extractor: Callable[[Dict[str, Any]], List[Any]] = None,
                   concurrent: bool = True) -> List[Tuple[Any, Any]]:
        """
        Process all items from a paginated API endpoint
        
        Args:
            endpoint: API endpoint
            processor_func: Function to apply to each item
            method: HTTP method
            params: Additional query parameters
            data_extractor: Function to extract data items from response
            concurrent: Whether to process batches concurrently
            
        Returns:
            List of tuples with (original_item, result)
        """
        all_results = []
        current_batch = []
        
        # Process items in batches
        for item in self.paginate(endpoint, method, params, data_extractor):
            current_batch.append(item)
            
            # Process batch when it reaches the batch size
            if len(current_batch) >= self.batch_config.batch_size:
                if concurrent:
                    batch_results = self.process_batch_concurrent(current_batch, processor_func)
                else:
                    batch_results = self.process_batch(current_batch, processor_func)
                
                all_results.extend(batch_results)
                current_batch = []
                
                logger.info(f"Processed batch of {self.batch_config.batch_size} items, total processed: {len(all_results)}")
        
        # Process any remaining items
        if current_batch:
            if concurrent:
                batch_results = self.process_batch_concurrent(current_batch, processor_func)
            else:
                batch_results = self.process_batch(current_batch, processor_func)
            
            all_results.extend(batch_results)
            logger.info(f"Processed final batch of {len(current_batch)} items, total processed: {len(all_results)}")
        
        return all_results
    
    def batch_upload(self, 
                    endpoint: str, 
                    items: List[Any],
                    pre_processor: Callable[[Any], Any] = None,
                    method: str = 'POST',
                    concurrent: bool = True) -> List[Tuple[Any, requests.Response]]:
        """
        Upload items to an API endpoint in batches
        
        Args:
            endpoint: API endpoint
            items: Items to upload
            pre_processor: Function to apply to items before upload
            method: HTTP method
            concurrent: Whether to process batches concurrently
            
        Returns:
            List of tuples with (original_item, response)
        """
        def uploader(item):
            data = pre_processor(item) if pre_processor else item
            response = self.make_request(method, endpoint, json_data=data)
            return response
        
        all_results = []
        
        # Process items in batches
        for i in range(0, len(items), self.batch_config.batch_size):
            batch = items[i:i + self.batch_config.batch_size]
            
            if concurrent:
                batch_results = self.process_batch_concurrent(batch, uploader)
            else:
                batch_results = self.process_batch(batch, uploader)
            
            all_results.extend(batch_results)
            logger.info(f"Uploaded batch {i // self.batch_config.batch_size + 1}, total uploaded: {len(all_results)}")
        
        return all_results

# Example usage
def example_usage():
    # Set up configs
    retry_config = RetryConfig(
        max_retries=5,
        initial_delay=2,
        backoff_factor=1.5,
        retry_on_status_codes=[429, 500, 502, 503, 504]
    )
    
    pagination_config = PaginationConfig(
        page_size=50,
        style='offset',
        page_param='page',
        size_param='per_page'
    )
    
    batch_config = BatchConfig(
        batch_size=100,
        max_concurrent=5
    )
    
    # Create processor
    processor = APIBatchProcessor(
        base_url='https://api.example.com',
        headers={
            'Authorization': 'Bearer YOUR_API_KEY',
            'Content-Type': 'application/json'
        },
        retry_config=retry_config,
        pagination_config=pagination_config,
        batch_config=batch_config
    )
    
    # Example 1: Process all users
    def process_user(user):
        print(f"Processing user: {user['id']}")
        # Do something with the user
        return {'user_id': user['id'], 'processed': True}
    
    results = processor.process_all(
        endpoint='users',
        processor_func=process_user,
        params={'active': 'true'}
    )
    
    # Example 2: Upload items in batches
    items_to_upload = [
        {'name': f'Item {i}', 'value': i * 10}
        for i in range(1, 501)
    ]
    
    upload_results = processor.batch_upload(
        endpoint='items',
        items=items_to_upload
    )
    
    # Check results
    success_count = sum(1 for _, resp in upload_results if resp.status_code == 201)
    print(f"Successfully uploaded {success_count} out of {len(items_to_upload)} items")

if __name__ == "__main__":
    example_usage()