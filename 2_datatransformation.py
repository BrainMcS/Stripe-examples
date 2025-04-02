import csv
import stripe
import datetime
import os
from dateutil.parser import parse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set your Stripe API key
stripe.api_key = os.getenv("STRIPE_API_KEY")
if not stripe.api_key:
    raise ValueError("Stripe API key not found. Please set the STRIPE_API_KEY environment variable.")

def process_subscription_import(csv_file_path):
    """
    Process a CSV file of customer subscription data and import into Stripe.
    
    Args:
        csv_file_path (str): Path to the CSV file
        
    Returns:
        dict: Summary of the import process
    """
    # Results tracking
    results = {
        'total_rows': 0,
        'successful_imports': 0,
        'failed_imports': 0,
        'errors': []
    }
    
    # Cache for prices to avoid repeated API calls
    price_cache = {}
    
    print(f"Starting import from {csv_file_path}")
    
    try:
        with open(csv_file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            
            # Validate CSV headers
            required_headers = {'customer_name', 'email', 'plan_name', 'monthly_amount', 'start_date'}
            if not required_headers.issubset(set(csv_reader.fieldnames)):
                missing = required_headers - set(csv_reader.fieldnames)
                raise ValueError(f"CSV is missing required headers: {missing}")
            
            for row in csv_reader:
                results['total_rows'] += 1
                print(f"\nProcessing row {results['total_rows']}: {row['email']}")
                
                try:
                    # Clean and validate data
                    email = row['email'].strip().lower()
                    if not email or '@' not in email:
                        raise ValueError(f"Invalid email: {email}")
                    
                    customer_name = row['customer_name'].strip()
                    if not customer_name:
                        raise ValueError("Customer name is required")
                    
                    plan_name = row['plan_name'].strip()
                    
                    # Convert amount to cents (Stripe uses smallest currency unit)
                    try:
                        amount_cents = int(float(row['monthly_amount']) * 100)
                    except ValueError:
                        raise ValueError(f"Invalid amount: {row['monthly_amount']}")
                    
                    # Parse the start date
                    try:
                        start_date = parse(row['start_date'])
                    except:
                        raise ValueError(f"Invalid start date: {row['start_date']}")
                    
                    # 1. Check if customer exists
                    existing_customers = stripe.Customer.list(email=email, limit=1)
                    
                    if existing_customers.data:
                        customer = existing_customers.data[0]
                        print(f"Found existing customer with ID: {customer.id}")
                    else:
                        # Create new customer
                        customer = stripe.Customer.create(
                            email=email,
                            name=customer_name,
                            metadata={
                                'imported_from_csv': 'true',
                                'import_date': datetime.datetime.now().isoformat()
                            }
                        )
                        print(f"Created new customer with ID: {customer.id}")
                    
                    # 2. Check if price exists in cache
                    price_key = f"{plan_name}_{amount_cents}"
                    if price_key in price_cache:
                        price_id = price_cache[price_key]
                    else:
                        # Look for existing price
                        prices = stripe.Price.list(
                            lookup_keys=[price_key],
                            active=True,
                            limit=1
                        )
                        
                        if prices.data:
                            price_id = prices.data[0].id
                        else:
                            # Create a product for this plan
                            product = stripe.Product.create(
                                name=plan_name,
                                metadata={
                                    'imported_from_csv': 'true'
                                }
                            )
                            
                            # Create a price for this product
                            price = stripe.Price.create(
                                product=product.id,
                                unit_amount=amount_cents,
                                currency='usd',  # Assuming USD
                                recurring={
                                    'interval': 'month'
                                },
                                lookup_key=price_key,
                                metadata={
                                    'imported_from_csv': 'true'
                                }
                            )
                            price_id = price.id
                        
                        # Cache the price ID
                        price_cache[price_key] = price_id
                    
                    print(f"Using price ID: {price_id}")
                    
                    # 3. Create subscription with trial end based on start date
                    trial_end = int(start_date.timestamp())
                    
                    subscription = stripe.Subscription.create(
                        customer=customer.id,
                        items=[
                            {'price': price_id}
                        ],
                        trial_end=trial_end,
                        metadata={
                            'imported_from_csv': 'true',
                            'import_date': datetime.datetime.now().isoformat(),
                            'original_start_date': row['start_date']
                        }
                    )
                    
                    print(f"Created subscription with ID: {subscription.id}")
                    results['successful_imports'] += 1
                    
                except Exception as e:
                    print(f"Error processing row {results['total_rows']}: {str(e)}")
                    results['failed_imports'] += 1
                    results['errors'].append({
                        'row': results['total_rows'],
                        'email': row.get('email', 'unknown'),
                        'error': str(e)
                    })
        
        print(f"\nImport complete. Successfully imported {results['successful_imports']} of {results['total_rows']} records.")
        if results['failed_imports'] > 0:
            print(f"Failed to import {results['failed_imports']} records. See errors for details.")
        
        return results
    
    except Exception as e:
        print(f"Fatal error during import: {str(e)}")
        results['errors'].append({
            'row': 'N/A',
            'email': 'N/A',
            'error': f"Fatal error: {str(e)}"
        })
        return results


# Example usage
if __name__ == "__main__":
    import_results = process_subscription_import('customer_subscriptions.csv')
    
    print("\nImport Summary:")
    print(f"Total rows processed: {import_results['total_rows']}")
    print(f"Successful imports: {import_results['successful_imports']}")
    print(f"Failed imports: {import_results['failed_imports']}")
    
    if import_results['errors']:
        print("\nErrors:")
        for error in import_results['errors']:
            print(f"Row {error['row']} ({error['email']}): {error['error']}")