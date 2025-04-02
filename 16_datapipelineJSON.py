import json
import csv
import argparse
import logging
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_transformer')

class DataTransformer:
    """Transforms nested JSON data into flattened formats"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the data transformer
        
        Args:
            config: Configuration for field mappings and transformations
        """
        self.config = config or {}
        
        # Default delimiter for flattened keys
        self.key_delimiter = self.config.get('key_delimiter', '.')
        
        # Field mappings (old_name -> new_name)
        self.field_mappings = self.config.get('field_mappings', {})
        
        # Field transformations (field_name -> transformation_function)
        self.transformations = {}
        
        # Register default transformers
        self._register_default_transformers()
        
        # Register custom transformers from config
        self._register_custom_transformers()
    
    def _register_default_transformers(self) -> None:
        """Register built-in transformation functions"""
        # Date/time transformers
        self.register_transformer('to_iso_date', self._transform_to_iso_date)
        self.register_transformer('to_unix_timestamp', self._transform_to_unix_timestamp)
        self.register_transformer('format_date', self._transform_format_date)
        
        # String transformers
        self.register_transformer('lowercase', lambda x: str(x).lower() if x is not None else None)
        self.register_transformer('uppercase', lambda x: str(x).upper() if x is not None else None)
        self.register_transformer('capitalize', lambda x: str(x).capitalize() if x is not None else None)
        self.register_transformer('trim', lambda x: str(x).strip() if x is not None else None)
        
        # Numeric transformers
        self.register_transformer('to_int', self._transform_to_int)
        self.register_transformer('to_float', self._transform_to_float)
        self.register_transformer('round', lambda x, digits=2: round(float(x), digits) if x is not None else None)
        
        # Boolean transformers
        self.register_transformer('to_boolean', self._transform_to_boolean)
        
        # Array transformers
        self.register_transformer('join', lambda x, delimiter=',': delimiter.join(map(str, x)) if isinstance(x, list) else x)
        self.register_transformer('split', lambda x, delimiter=',': x.split(delimiter) if isinstance(x, str) else x)
        
        # Other transformers
        self.register_transformer('default', lambda x, default_value=None: x if x is not None else default_value)
    
    def _register_custom_transformers(self) -> None:
        """Register custom transformers from configuration"""
        custom_transformers = self.config.get('custom_transformers', {})
        
        for name, transformer_config in custom_transformers.items():
            if transformer_config.get('type') == 'regex_replace':
                pattern = transformer_config.get('pattern', '')
                replacement = transformer_config.get('replacement', '')
                self.register_transformer(name, lambda x, p=pattern, r=replacement: re.sub(p, r, str(x)) if x is not None else None)
            
            elif transformer_config.get('type') == 'map_values':
                value_map = transformer_config.get('mapping', {})
                default = transformer_config.get('default')
                self.register_transformer(name, lambda x, m=value_map, d=default: m.get(x, d))
    
    def register_transformer(self, name: str, func: callable) -> None:
        """
        Register a transformation function
        
        Args:
            name: Name of the transformer
            func: Transformation function
        """
        self.transformations[name] = func
    
    def flatten_json(self, data: Dict[str, Any], parent_key: str = '', sep: str = None) -> Dict[str, Any]:
        """
        Flatten nested JSON into a single-level dictionary
        
        Args:
            data: Nested JSON data
            parent_key: Key of parent in recursive calls
            sep: Separator for nested keys (defaults to self.key_delimiter)
            
        Returns:
            Flattened dictionary
        """
        sep = sep or self.key_delimiter
        items = []
        
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                # Recursively flatten dictionaries
                items.extend(self.flatten_json(v, new_key, sep).items())
            elif isinstance(v, list):
                # Handle lists - either keep as JSON or expand
                list_handling = self.config.get('list_handling', 'keep')
                
                if list_handling == 'keep':
                    # Keep as JSON string
                    items.append((new_key, json.dumps(v)))
                elif list_handling == 'enumerate':
                    # Create separate keys for each index
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            # Recursively flatten dictionaries in the list
                            items.extend(self.flatten_json(item, f"{new_key}{sep}{i}", sep).items())
                        else:
                            # Add simple values with index
                            items.append((f"{new_key}{sep}{i}", item))
                elif list_handling == 'join':
                    # Join simple values into a string
                    if all(not isinstance(item, (dict, list)) for item in v):
                        items.append((new_key, ','.join(map(str, v))))
                    else:
                        # For complex items, fall back to JSON
                        items.append((new_key, json.dumps(v)))
            else:
                # Simple value
                items.append((new_key, v))
        
        return dict(items)
    
    def apply_mappings(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply field mappings to rename fields
        
        Args:
            data: Input data dictionary
            
        Returns:
            Dictionary with renamed fields
        """
        result = {}
        
        # Process each field
        for old_key, value in data.items():
            # Check if we have a mapping for this key
            new_key = self.field_mappings.get(old_key, old_key)
            
            # Skip fields that map to None (exclude them)
            if new_key is None:
                continue
            
            result[new_key] = value
        
        return result
    
    def apply_transformations(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply field transformations
        
        Args:
            data: Input data dictionary
            
        Returns:
            Dictionary with transformed values
        """
        result = data.copy()
        field_transforms = self.config.get('field_transforms', {})
        
        # Process each field that has a transformation
        for field, transforms in field_transforms.items():
            # Skip if field doesn't exist
            if field not in result:
                continue
            
            value = result[field]
            
            # Apply each transformation in sequence
            for transform in transforms:
                if isinstance(transform, str):
                    # Simple transformation without parameters
                    transformer = self.transformations.get(transform)
                    if transformer:
                        value = transformer(value)
                elif isinstance(transform, dict):
                    # Transformation with parameters
                    transform_name = transform.get('name')
                    transform_args = transform.get('args', {})
                    
                    transformer = self.transformations.get(transform_name)
                    if transformer:
                        value = transformer(value, **transform_args)
            
            # Update the field with transformed value
            result[field] = value
        
        return result
    
    def transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply the full transformation pipeline to a data record
        
        Args:
            data: Input data record
            
        Returns:
            Transformed data record
        """
        # Step 1: Flatten nested structures
        flattened = self.flatten_json(data)
        
        # Step 2: Apply field mappings
        mapped = self.apply_mappings(flattened)
        
        # Step 3: Apply transformations
        transformed = self.apply_transformations(mapped)
        
        return transformed
    
    def transform_dataset(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform a list of data records
        
        Args:
            data_list: List of input data records
            
        Returns:
            List of transformed data records
        """
        return [self.transform_data(record) for record in data_list]
    
    def _transform_to_iso_date(self, value: Any) -> Optional[str]:
        """Transform a value to ISO 8601 date format"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            # Assume Unix timestamp
            try:
                dt = datetime.fromtimestamp(value)
                return dt.isoformat()
            except (ValueError, OverflowError):
                logger.warning(f"Could not convert timestamp {value} to date")
                return None
        
        if isinstance(value, str):
            # Try common date formats
            formats = [
                '%Y-%m-%d',
                '%Y/%m/%d',
                '%m/%d/%Y',
                '%d/%m/%Y',
                '%b %d, %Y',
                '%d %b %Y',
                '%B %d, %Y',
                '%d %B %Y',
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d %H:%M:%S',
                '%m/%d/%Y %H:%M:%S',
                '%d/%m/%Y %H:%M:%S'
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
            
            # Try with dateutil parser as a fallback
            try:
                from dateutil import parser
                dt = parser.parse(value)
                return dt.isoformat()
            except:
                logger.warning(f"Could not parse date string: {value}")
                return value
        
        return str(value)
    
    def _transform_to_unix_timestamp(self, value: Any) -> Optional[int]:
        """Transform a value to Unix timestamp"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            # Already a timestamp
            return int(value)
        
        if isinstance(value, str):
            # Try to parse as date
            iso_date = self._transform_to_iso_date(value)
            if iso_date and iso_date != value:
                try:
                    dt = datetime.fromisoformat(iso_date)
                    return int(dt.timestamp())
                except:
                    logger.warning(f"Could not convert date to timestamp: {value}")
                    return None
        
        return None
    
    def _transform_format_date(self, value: Any, format: str = '%Y-%m-%d') -> Optional[str]:
        """Format a date value"""
        if value is None:
            return None
        
        # First convert to ISO date
        iso_date = self._transform_to_iso_date(value)
        if iso_date and iso_date != value:
            try:
                dt = datetime.fromisoformat(iso_date)
                return dt.strftime(format)
            except:
                logger.warning(f"Could not format date: {value}")
                return value
        
        return value
    
    def _transform_to_int(self, value: Any) -> Optional[int]:
        """Transform a value to integer"""
        if value is None:
            return None
        
        try:
            # Handle string representations of numbers
            if isinstance(value, str):
                # Remove any non-numeric characters except decimal point and negative sign
                value = re.sub(r'[^\d.-]', '', value)
            
            # Convert to float first, then to int, to handle decimal strings
            return int(float(value))
        except (ValueError, TypeError):
            logger.warning(f"Could not convert to integer: {value}")
            return None
    
    def _transform_to_float(self, value: Any) -> Optional[float]:
        """Transform a value to float"""
        if value is None:
            return None
        
        try:
            # Handle string representations of numbers
            if isinstance(value, str):
                # Remove any non-numeric characters except decimal point and negative sign
                value = re.sub(r'[^\d.-]', '', value)
            
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert to float: {value}")
            return None
    
    def _transform_to_boolean(self, value: Any) -> Optional[bool]:
        """Transform a value to boolean"""
        if value is None:
            return None
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        if isinstance(value, str):
            value = value.lower().strip()
            if value in ('true', 'yes', 'y', '1', 't'):
                return True
            if value in ('false', 'no', 'n', '0', 'f'):
                return False
        
        logger.warning(f"Could not convert to boolean: {value}")
        return None
    
    def load_json(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Load JSON data from a file
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            List of data records
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
                # Ensure we always return a list of records
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    # Check if this is a single record or a container with records
                    records_field = self.config.get('records_field')
                    if records_field and records_field in data:
                        # Extract records from the container
                        return data[records_field]
                    else:
                        # Treat as a single record
                        return [data]
                else:
                    logger.error(f"Unexpected JSON format in {file_path}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error loading JSON from {file_path}: {str(e)}")
            raise
    
    def write_json(self, data: List[Dict[str, Any]], file_path: str, indent: int = 2) -> None:
        """
        Write data as JSON to a file
        
        Args:
            data: List of data records
            file_path: Output file path
            indent: JSON indentation level
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=indent)
            
            logger.info(f"Wrote {len(data)} records to {file_path}")
                
        except Exception as e:
            logger.error(f"Error writing JSON to {file_path}: {str(e)}")
            raise
    
    def write_csv(self, data: List[Dict[str, Any]], file_path: str) -> None:
        """
        Write data as CSV to a file
        
        Args:
            data: List of data records
            file_path: Output file path
        """
        if not data:
            logger.warning(f"No data to write to {file_path}")
            return
        
        try:
            # Get all unique keys from all records
            fieldnames = set()
            for record in data:
                fieldnames.update(record.keys())
            
            # Sort fieldnames for consistent output
            fieldnames = sorted(fieldnames)
            
            with open(file_path, 'w', encoding='utf-8', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            
            logger.info(f"Wrote {len(data)} records to {file_path}")
                
        except Exception as e:
            logger.error(f"Error writing CSV to {file_path}: {str(e)}")
            raise

def main():
    """Command line interface for the data transformer"""
    parser = argparse.ArgumentParser(description='Transform nested JSON data into flattened formats')
    parser.add_argument('input_file', help='Input JSON file')
    parser.add_argument('output_file', help='Output file (format determined by extension)')
    parser.add_argument('--config', help='Configuration JSON file for transformations')
    
    args = parser.parse_args()
    
    # Load configuration if provided
    config = None
    if args.config:
        try:
            with open(args.config, 'r', encoding='utf-8') as file:
                config = json.load(file)
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            return 1
    
    # Create transformer
    transformer = DataTransformer(config)
    
    try:
        # Load input data
        data = transformer.load_json(args.input_file)
        
        # Transform data
        transformed_data = transformer.transform_dataset(data)
        
        # Determine output format from extension
        _, ext = os.path.splitext(args.output_file)
        
        if ext.lower() in ('.json', '.js'):
            transformer.write_json(transformed_data, args.output_file)
        elif ext.lower() in ('.csv'):
            transformer.write_csv(transformed_data, args.output_file)
        else:
            logger.error(f"Unsupported output format: {ext}")
            return 1
        
        logger.info(f"Transformation complete: {len(data)} records processed")
        return 0
        
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())