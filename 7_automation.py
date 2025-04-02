import stripe
import datetime
import argparse
import logging
import time
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('invoice_reminders.log')
    ]
)
logger = logging.getLogger('invoice_reminders')

# Email configuration
EMAIL_CONFIG = {
    'smtp_server': 'smtp.example.com',
    'smtp_port': 587,
    'smtp_username': 'notifications@example.com',
    'smtp_password': 'your_password_here',
    'from_email': 'billing@example.com',
    'from_name': 'Example Billing Team'
}

def initialize_stripe(api_key: str) -> None:
    """Initialize the Stripe API with the provided key"""
    stripe.api_key = api_key
    logger.info("Stripe API initialized")

def get_upcoming_invoices(days: int = 7) -> List[Dict[str, Any]]:
    """
    Get all invoices scheduled to be finalized in the next X days
    
    Args:
        days (int): Number of days to look ahead
        
    Returns:
        list: List of upcoming invoice objects
    """
    logger.info(f"Finding upcoming invoices for the next {days} days")
    
    # Calculate the date range for upcoming invoices
    now = datetime.datetime.now()
    end_date = now + datetime.timedelta(days=days)
    
    # Convert to Unix timestamps for Stripe API
    now_timestamp = int(now.timestamp())
    end_timestamp = int(end_date.timestamp())
    
    upcoming_invoices = []
    
    try:
        # Get upcoming invoices using the list endpoint with filtering
        has_more = True
        last_id = None
        page = 1
        
        while has_more:
            logger.info(f"Fetching page {page} of upcoming invoices")
            
            # Set up the query parameters
            params = {
                "limit": 100,  # Maximum allowed by Stripe
                "status": "draft",
                # Filter to include only invoices that will be finalized soon
                "created": {
                    "gte": now_timestamp - (30 * 24 * 60 * 60)  # Last 30 days (invoices created recently)
                },
                "due_date": {
                    "gte": now_timestamp,
                    "lte": end_timestamp
                }
            }
            
            # For pagination
            if last_id:
                params["starting_after"] = last_id
            
            # Make the API call
            invoice_list = stripe.Invoice.list(**params)
            
            # Process the results
            for invoice in invoice_list.data:
                # Double-check that this invoice is actually upcoming
                # Some invoices might be drafts without due dates
                if invoice.status == "draft" and hasattr(invoice, "due_date") and invoice.due_date:
                    due_date = datetime.datetime.fromtimestamp(invoice.due_date)
                    if now <= due_date <= end_date:
                        upcoming_invoices.append(invoice)
            
            # Set up for next page if needed
            has_more = invoice_list.has_more
            if has_more and invoice_list.data:
                last_id = invoice_list.data[-1].id
                page += 1
            else:
                has_more = False
        
        logger.info(f"Found {len(upcoming_invoices)} upcoming invoices")
        return upcoming_invoices
        
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error when fetching upcoming invoices: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error when fetching upcoming invoices: {str(e)}")
        raise

def get_customer_details(customer_id: str) -> Dict[str, Any]:
    """
    Get customer details from Stripe
    
    Args:
        customer_id (str): Stripe customer ID
        
    Returns:
        dict: Customer details
    """
    try:
        customer = stripe.Customer.retrieve(customer_id)
        return {
            'id': customer.id,
            'email': customer.email,
            'name': customer.name or customer.email,
            'phone': customer.phone,
            'metadata': customer.metadata
        }
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error when fetching customer {customer_id}: {str(e)}")
        return {
            'id': customer_id,
            'email': None,
            'name': None
        }

def format_invoice_details(invoice) -> Dict[str, Any]:
    """
    Format invoice details for email template
    
    Args:
        invoice: Stripe invoice object
        
    Returns:
        dict: Formatted invoice details
    """
    # Format amount
    amount = invoice.total / 100.0  # Convert from cents to dollars
    currency = invoice.currency.upper()
    
    # Format date
    due_date = datetime.datetime.fromtimestamp(invoice.due_date)
    formatted_date = due_date.strftime("%B %d, %Y")
    
    # Get line items summary
    line_items = []
    try:
        items = stripe.InvoiceItem.list(invoice=invoice.id)
        for item in items.data:
            description = item.description or "Subscription item"
            amount = item.amount / 100.0
            line_items.append({
                'description': description,
                'amount': f"{currency} {amount:.2f}"
            })
    except Exception as e:
        logger.warning(f"Could not fetch line items for invoice {invoice.id}: {e}")
        line_items = [{
            'description': "Your subscription charges",
            'amount': f"{currency} {amount:.2f}"
        }]
    
    return {
        'id': invoice.id,
        'number': invoice.number or invoice.id,
        'amount': amount,
        'currency': currency,
        'formatted_amount': f"{currency} {amount:.2f}",
        'due_date': formatted_date,
        'days_until_due': (due_date - datetime.datetime.now()).days,
        'line_items': line_items,
        'hosted_invoice_url': invoice.hosted_invoice_url if hasattr(invoice, 'hosted_invoice_url') else None
    }

def send_reminder_email(customer: Dict[str, Any], invoice_details: Dict[str, Any]) -> bool:
    """
    Send a reminder email for an upcoming invoice
    
    Args:
        customer (dict): Customer details
        invoice_details (dict): Invoice details
        
    Returns:
        bool: True if the email was sent successfully, False otherwise
    """
    if not customer.get('email'):
        logger.warning(f"Cannot send reminder for invoice {invoice_details['id']} - no email for customer {customer['id']}")
        return False
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Upcoming Invoice #{invoice_details['number']} - Due in {invoice_details['days_until_due']} days"
        msg['From'] = f"{EMAIL_CONFIG['from_name']} <{EMAIL_CONFIG['from_email']}>"
        msg['To'] = customer['email']
        
        # Create the email body
        text = f"""
Hello {customer['name']},

You have an upcoming invoice #{invoice_details['number']} for {invoice_details['formatted_amount']} due on {invoice_details['due_date']}.

Invoice details:
"""
        
        # Add line items
        for item in invoice_details['line_items']:
            text += f"- {item['description']}: {item['amount']}\n"
        
        # Add payment link if available
        if invoice_details['hosted_invoice_url']:
            text += f"\nYou can view and pay your invoice here: {invoice_details['hosted_invoice_url']}\n"
        
        text += """
If you have any questions about this invoice, please contact our support team.

Thank you for your business!

Best regards,
The Example Company Team
"""
        
        # Add text parts
        msg.attach(MIMEText(text, 'plain'))
        
        # Send the email
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['smtp_username'], EMAIL_CONFIG['smtp_password'])
            server.send_message(msg)
        
        logger.info(f"Sent reminder for invoice {invoice_details['id']} to {customer['email']}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending reminder for invoice {invoice_details['id']}: {str(e)}")
        return False

def process_upcoming_invoices(days: int = 7, dry_run: bool = False) -> Dict[str, Any]:
    """
    Process all upcoming invoices and send reminders
    
    Args:
        days (int): Number of days to look ahead
        dry_run (bool): If True, don't actually send emails
        
    Returns:
        dict: Summary of processing results
    """
    start_time = time.time()
    results = {
        'total_invoices': 0,
        'emails_sent': 0,
        'email_failures': 0,
        'errors': []
    }
    
    try:
        # Get upcoming invoices
        upcoming_invoices = get_upcoming_invoices(days)
        results['total_invoices'] = len(upcoming_invoices)
        
        # Process each invoice
        for invoice in upcoming_invoices:
            try:
                # Get customer details
                customer = get_customer_details(invoice.customer)
                
                # Format invoice details for the email
                invoice_details = format_invoice_details(invoice)
                
                # Determine if we should send a reminder to this customer
                should_send = True
                
                # Check if this customer has opted out of reminders
                if customer.get('metadata', {}).get('opt_out_reminders') == 'true':
                    logger.info(f"Customer {customer['id']} has opted out of reminders")
                    should_send = False
                
                # Send the reminder
                if should_send and not dry_run:
                    success = send_reminder_email(customer, invoice_details)
                    if success:
                        results['emails_sent'] += 1
                    else:
                        results['email_failures'] += 1
                elif should_send and dry_run:
                    logger.info(f"DRY RUN: Would send reminder for invoice {invoice.id} to {customer.get('email')}")
                    results['emails_sent'] += 1
                
            except Exception as e:
                logger.error(f"Error processing invoice {invoice.id}: {str(e)}")
                results['errors'].append({
                    'invoice_id': invoice.id,
                    'error': str(e)
                })
        
    except Exception as e:
        logger.error(f"Error in process_upcoming_invoices: {str(e)}")
        results['errors'].append({
            'global_error': str(e)
        })
    
    # Calculate execution time
    execution_time = time.time() - start_time
    results['execution_time'] = f"{execution_time:.2f} seconds"
    
    return results

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Send reminders for upcoming Stripe invoices')
    parser.add_argument('--api-key', required=True, help='Stripe API key')
    parser.add_argument('--days', type=int, default=7, help='Number of days to look ahead (default: 7)')
    parser.add_argument('--dry-run', action='store_true', help='Run without sending actual emails')
    
    args = parser.parse_args()
    
    try:
        # Initialize Stripe
        initialize_stripe(args.api_key)
        
        # Run the process
        logger.info(f"Starting invoice reminder process (dry run: {args.dry_run})")
        results = process_upcoming_invoices(args.days, args.dry_run)
        
        # Log the results
        logger.info(f"Invoice reminder process completed")
        logger.info(f"Total invoices found: {results['total_invoices']}")
        logger.info(f"Emails sent: {results['emails_sent']}")
        logger.info(f"Email failures: {results['email_failures']}")
        logger.info(f"Execution time: {results['execution_time']}")
        
        if results['errors']:
            logger.warning(f"Encountered {len(results['errors'])} errors during processing")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()