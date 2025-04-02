import json
import argparse
from datetime import datetime, timedelta
from collections import defaultdict, Counter

def parse_stripe_webhook_logs(logfile_path, days=7):
    """
    Parse Stripe webhook logs to generate a payment success/failure report.
    
    Args:
        logfile_path (str): Path to the log file
        days (int): Number of days to look back (default: 7)
        
    Returns:
        dict: Report data with payment stats
    """
    # Initialize report data structure
    report = {
        'summary': {
            'start_date': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'total_events': 0,
            'payment_events': 0,
            'successful_payments': 0,
            'failed_payments': 0,
            'total_amount_succeeded': 0,
            'total_amount_failed': 0
        },
        'event_types': Counter(),
        'failures_by_reason': Counter(),
        'success_by_payment_method': Counter(),
        'hourly_distribution': defaultdict(lambda: {'succeeded': 0, 'failed': 0}),
        'daily_totals': defaultdict(lambda: {'succeeded': 0, 'failed': 0, 'amount_succeeded': 0, 'amount_failed': 0})
    }
    
    # Calculate the cutoff date
    cutoff_date = datetime.now() - timedelta(days=days)
    
    try:
        with open(logfile_path, 'r') as file:
            for line_number, line in enumerate(file, 1):
                try:
                    # Parse the log line as JSON
                    log_entry = json.loads(line.strip())
                    
                    # Extract event data
                    event_type = log_entry.get('type', 'unknown')
                    report['event_types'][event_type] += 1
                    report['summary']['total_events'] += 1
                    
                    # Convert timestamp to datetime
                    created_timestamp = log_entry.get('created', 0)
                    event_date = datetime.fromtimestamp(created_timestamp)
                    
                    # Skip events older than our cutoff
                    if event_date < cutoff_date:
                        continue
                    
                    # Extract hour and date for distribution tracking
                    hour = event_date.hour
                    date_str = event_date.strftime('%Y-%m-%d')
                    
                    # Only process payment-related events
                    if event_type in [
                        'charge.succeeded', 'charge.failed', 
                        'payment_intent.succeeded', 'payment_intent.payment_failed'
                    ]:
                        report['summary']['payment_events'] += 1
                        
                        # Extract the relevant object from the event
                        obj = log_entry.get('data', {}).get('object', {})
                        amount = obj.get('amount', 0)
                        currency = obj.get('currency', 'usd').upper()
                        
                        # Convert amount to dollars (Stripe uses cents)
                        amount_dollars = amount / 100.0
                        
                        # Process successful payments
                        if event_type in ['charge.succeeded', 'payment_intent.succeeded']:
                            report['summary']['successful_payments'] += 1
                            report['summary']['total_amount_succeeded'] += amount_dollars
                            report['hourly_distribution'][hour]['succeeded'] += 1
                            report['daily_totals'][date_str]['succeeded'] += 1
                            report['daily_totals'][date_str]['amount_succeeded'] += amount_dollars
                            
                            # Track payment method used
                            payment_method = 'unknown'
                            if 'payment_method_details' in obj:
                                payment_method = obj['payment_method_details'].get('type', 'unknown')
                            elif 'payment_method' in obj:
                                # For payment_intent events
                                payment_method = obj.get('payment_method_types', ['unknown'])[0]
                                
                            report['success_by_payment_method'][payment_method] += 1
                            
                        # Process failed payments
                        elif event_type in ['charge.failed', 'payment_intent.payment_failed']:
                            report['summary']['failed_payments'] += 1
                            report['summary']['total_amount_failed'] += amount_dollars
                            report['hourly_distribution'][hour]['failed'] += 1
                            report['daily_totals'][date_str]['failed'] += 1
                            report['daily_totals'][date_str]['amount_failed'] += amount_dollars
                            
                            # Track failure reason
                            failure_message = obj.get('failure_message', '')
                            failure_code = obj.get('failure_code', '')
                            reason = failure_code if failure_code else 'unknown'
                            
                            if failure_message:
                                reason += f": {failure_message}"
                                
                            report['failures_by_reason'][reason] += 1
                
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse JSON on line {line_number}. Skipping.")
                except Exception as e:
                    print(f"Warning: Error processing line {line_number}: {str(e)}. Skipping.")
    
    except FileNotFoundError:
        print(f"Error: File not found: {logfile_path}")
        return None
    except Exception as e:
        print(f"Error reading log file: {str(e)}")
        return None
    
    # Calculate some derived statistics
    if report['summary']['payment_events'] > 0:
        report['summary']['success_rate'] = (
            report['summary']['successful_payments'] / report['summary']['payment_events'] * 100
        )
    else:
        report['summary']['success_rate'] = 0
    
    # Convert defaultdicts to regular dicts for easier serialization
    report['hourly_distribution'] = dict(report['hourly_distribution'])
    report['daily_totals'] = dict(report['daily_totals'])
    report['event_types'] = dict(report['event_types'])
    report['failures_by_reason'] = dict(report['failures_by_reason'])
    report['success_by_payment_method'] = dict(report['success_by_payment_method'])
    
    return report


def generate_report(report_data, output_format='text'):
    """
    Generate a formatted report from the parsed data.
    
    Args:
        report_data (dict): The report data from parse_stripe_webhook_logs
        output_format (str): Output format ('text', 'json', or 'csv')
    
    Returns:
        str: The formatted report
    """
    if not report_data:
        return "No report data available."
    
    if output_format == 'json':
        return json.dumps(report_data, indent=2)
    
    # Text report format
    summary = report_data['summary']
    
    report_lines = [
        "===== STRIPE PAYMENT PROCESSING REPORT =====",
        f"Period: {summary['start_date']} to {summary['end_date']}",
        f"Total events processed: {summary['total_events']}",
        f"Payment-related events: {summary['payment_events']}",
        "",
        "PAYMENT SUMMARY",
        f"Successful payments: {summary['successful_payments']} ({summary['success_rate']:.2f}%)",
        f"Failed payments: {summary['failed_payments']} ({100 - summary['success_rate']:.2f}%)",
        f"Total amount succeeded: ${summary['total_amount_succeeded']:.2f}",
        f"Total amount failed: ${summary['total_amount_failed']:.2f}",
        "",
        "TOP FAILURE REASONS",
    ]
    
    # Add top 5 failure reasons
    for reason, count in sorted(report_data['failures_by_reason'].items(), 
                                key=lambda x: x[1], reverse=True)[:5]:
        report_lines.append(f"- {reason}: {count}")
    
    report_lines.extend([
        "",
        "PAYMENT METHODS (SUCCESSFUL PAYMENTS)",
    ])
    
    # Add payment method breakdown
    for method, count in sorted(report_data['success_by_payment_method'].items(), 
                               key=lambda x: x[1], reverse=True):
        report_lines.append(f"- {method}: {count}")
    
    report_lines.extend([
        "",
        "DAILY BREAKDOWN",
    ])
    
    # Add daily breakdown
    for date in sorted(report_data['daily_totals'].keys()):
        day_data = report_data['daily_totals'][date]
        total = day_data['succeeded'] + day_data['failed']
        success_rate = (day_data['succeeded'] / total * 100) if total > 0 else 0
        report_lines.append(
            f"- {date}: {day_data['succeeded']} succeeded, {day_data['failed']} failed "
            f"({success_rate:.2f}%) - ${day_data['amount_succeeded']:.2f} processed"
        )
    
    return "\n".join(report_lines)


def main():
    parser = argparse.ArgumentParser(description='Parse Stripe webhook logs and generate payment reports')
    parser.add_argument('logfile', help='Path to the webhook log file')
    parser.add_argument('--days', type=int, default=7, help='Number of days to include in the report (default: 7)')
    parser.add_argument('--format', choices=['text', 'json'], default='text', help='Output format (default: text)')
    parser.add_argument('--output', help='Output file path (default: stdout)')
    
    args = parser.parse_args()
    
    # Parse the logs
    report_data = parse_stripe_webhook_logs(args.logfile, args.days)
    
    if not report_data:
        print("No report data generated. Check for errors above.")
        return
    
    # Generate the report in the specified format
    report = generate_report(report_data, args.format)
    
    # Output the report
    if args.output:
        with open(args.output, 'w') as file:
            file.write(report)
        print(f"Report written to {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()