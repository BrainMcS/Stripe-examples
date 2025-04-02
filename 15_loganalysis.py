import re
import json
import argparse
import logging
import os
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from typing import Dict, List, Any, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('log_analyzer')

class APILogAnalyzer:
    """Analyzes API error logs to identify patterns and generate recommendations"""
    
    def __init__(self, time_window_hours: int = 24):
        """
        Initialize the log analyzer
        
        Args:
            time_window_hours: Time window in hours to analyze
        """
        self.time_window_hours = time_window_hours
        self.error_patterns = [
            # Regex patterns to extract meaningful information from logs
            {
                'name': 'rate_limit',
                'pattern': r'(rate limit|too many requests|429)',
                'recommendation': 'Implement rate limiting with exponential backoff'
            },
            {
                'name': 'authentication',
                'pattern': r'(auth.*failed|invalid.*token|unauthorized|401)',
                'recommendation': 'Check API credentials and token refresh mechanisms'
            },
            {
                'name': 'permission',
                'pattern': r'(permission|access denied|forbidden|403)',
                'recommendation': 'Verify account permissions and API scopes'
            },
            {
                'name': 'not_found',
                'pattern': r'(not found|no such|404)',
                'recommendation': 'Validate resource IDs and API endpoints'
            },
            {
                'name': 'validation',
                'pattern': r'(invalid|malformed|bad request|validation|400)',
                'recommendation': 'Review request payload structure and data types'
            },
            {
                'name': 'server_error',
                'pattern': r'(server error|internal|500)',
                'recommendation': 'Contact API provider for assistance; implement retry logic'
            },
            {
                'name': 'timeout',
                'pattern': r'(timeout|timed out|504|408)',
                'recommendation': 'Increase request timeouts and implement retry logic'
            },
            {
                'name': 'connection',
                'pattern': r'(connection|network|unreachable)',
                'recommendation': 'Check network connectivity and DNS settings'
            }
        ]
        
        # Compile regex patterns for better performance
        for pattern in self.error_patterns:
            pattern['regex'] = re.compile(pattern['pattern'], re.IGNORECASE)
    
    def parse_log_line(self, line: str) -> Dict[str, Any]:
        """
        Parse a log line into structured data
        
        This method attempts to handle multiple common log formats:
        - JSON
        - Key-value pairs
        - Common Log Format / Combined Log Format
        - Custom formats with timestamp, level, and message
        
        Args:
            line: Raw log line
            
        Returns:
            Parsed log entry as a dictionary
        """
        # Skip empty lines
        if not line.strip():
            return {}
        
        try:
            # Try parsing as JSON
            return json.loads(line)
        except json.JSONDecodeError:
            pass
        
        # Try parsing as key-value pairs
        if '=' in line:
            entry = {}
            pairs = re.findall(r'([a-zA-Z0-9_]+)=(?:"([^"]*)"|([^ ]*))', line)
            for key, val1, val2 in pairs:
                entry[key.lower()] = val1 or val2
            
            if entry:
                return entry
        
        # Try to extract timestamp, level, and message
        # Common patterns like: "2023-01-15 12:34:56 ERROR API request failed: Rate limit exceeded"
        timestamp_patterns = [
            r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)',  # ISO-like
            r'(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})',  # Apache-like
            r'(\w{3} \d{2} \d{2}:\d{2}:\d{2})'  # Syslog-like
        ]
        
        level_pattern = r'\b(DEBUG|INFO|WARNING|ERROR|CRITICAL|ALERT|EMERGENCY)\b'
        
        for ts_pattern in timestamp_patterns:
            ts_match = re.search(ts_pattern, line)
            if ts_match:
                timestamp = ts_match.group(1)
                remainder = line[ts_match.end():].strip()
                
                level_match = re.search(level_pattern, remainder)
                if level_match:
                    level = level_match.group(1)
                    message = remainder[level_match.end():].strip()
                    
                    return {
                        'timestamp': timestamp,
                        'level': level.lower(),
                        'message': message
                    }
        
        # Fallback: just return the whole line as a message
        return {'message': line.strip()}
    
    def extract_timestamp(self, log_entry: Dict[str, Any]) -> Optional[datetime]:
        """
        Extract and parse timestamp from a log entry
        
        Args:
            log_entry: Parsed log entry
            
        Returns:
            Parsed datetime object or None if timestamp can't be parsed
        """
        timestamp_field = None
        
        # Look for timestamp in common field names
        for field in ['timestamp', 'time', 'date', '@timestamp', 'created_at']:
            if field in log_entry:
                timestamp_field = log_entry[field]
                break
        
        if not timestamp_field:
            return None
        
        # Try multiple timestamp formats
        formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO 8601 with milliseconds
            '%Y-%m-%dT%H:%M:%SZ',     # ISO 8601
            '%Y-%m-%d %H:%M:%S.%f',   # SQL-like with milliseconds
            '%Y-%m-%d %H:%M:%S',      # SQL-like
            '%d/%b/%Y:%H:%M:%S',      # Apache log format
            '%b %d %H:%M:%S'          # Syslog format
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_field, fmt)
            except ValueError:
                continue
        
        # Try parsing with datetime's built-in parser
        try:
            from dateutil import parser
            return parser.parse(timestamp_field)
        except (ImportError, ValueError):
            pass
        
        return None
    
    def is_error_log(self, log_entry: Dict[str, Any]) -> bool:
        """
        Check if a log entry represents an error
        
        Args:
            log_entry: Parsed log entry
            
        Returns:
            True if this is an error log, False otherwise
        """
        # Check log level if available
        if 'level' in log_entry:
            level = log_entry['level'].lower()
            if level in ['error', 'critical', 'alert', 'emergency']:
                return True
        
        # Check status code if available
        for field in ['status', 'status_code', 'statusCode', 'code']:
            if field in log_entry:
                try:
                    code = int(log_entry[field])
                    if code >= 400:
                        return True
                except (ValueError, TypeError):
                    pass
        
        # Check message for error indicators
        if 'message' in log_entry:
            message = log_entry['message'].lower()
            if any(word in message for word in ['error', 'exception', 'fail', 'failed', 'crash']):
                return True
        
        return False
    
    def categorize_error(self, log_entry: Dict[str, Any]) -> List[str]:
        """
        Categorize an error log entry based on patterns
        
        Args:
            log_entry: Parsed log entry
            
        Returns:
            List of matched error categories
        """
        # Get the message to match against
        message = log_entry.get('message', '')
        
        # Add other relevant fields to the message
        for field in ['error', 'exception', 'detail', 'description']:
            if field in log_entry:
                message += f" {log_entry[field]}"
        
        # Try to match error patterns
        categories = []
        
        for pattern in self.error_patterns:
            if pattern['regex'].search(message):
                categories.append(pattern['name'])
        
        # If no matches, categorize as 'unknown'
        if not categories:
            categories.append('unknown')
        
        return categories
    
    def analyze_logs(self, log_file_path: str) -> Dict[str, Any]:
        """
        Analyze a log file to identify error patterns
        
        Args:
            log_file_path: Path to the log file
            
        Returns:
            Analysis results
        """
        if not os.path.exists(log_file_path):
            logger.error(f"Log file not found: {log_file_path}")
            return {'error': 'File not found'}
        
        # Analysis data structures
        error_counts = Counter()
        error_examples = {}
        endpoint_errors = defaultdict(Counter)
        time_distribution = defaultdict(int)
        status_codes = Counter()
        user_agent_errors = defaultdict(int)
        ip_errors = defaultdict(int)
        
        # Calculate time threshold for filtering logs
        time_threshold = datetime.now() - timedelta(hours=self.time_window_hours)
        
        # Process the log file
        total_entries = 0
        error_entries = 0
        skipped_entries = 0
        
        with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as file:
            for line_num, line in enumerate(file, 1):
                try:
                    # Parse log line
                    log_entry = self.parse_log_line(line)
                    
                    if not log_entry:
                        skipped_entries += 1
                        continue
                    
                    total_entries += 1
                    
                    # Check if entry is within time window
                    timestamp = self.extract_timestamp(log_entry)
                    if timestamp and timestamp < time_threshold:
                        continue
                    
                    # Check if this is an error log
                    if not self.is_error_log(log_entry):
                        continue
                    
                    error_entries += 1
                    
                    # Categorize the error
                    categories = self.categorize_error(log_entry)
                    
                    # Update error counts
                    for category in categories:
                        error_counts[category] += 1
                        
                        # Store example if we don't have one yet
                        if category not in error_examples:
                            error_examples[category] = {
                                'line': line.strip(),
                                'entry': log_entry
                            }
                    
                    # Extract and count endpoint, if available
                    endpoint = None
                    for field in ['endpoint', 'path', 'url', 'request_path']:
                        if field in log_entry:
                            endpoint = log_entry[field]
                            break
                    
                    if endpoint:
                        # Normalize API endpoints - replace IDs with placeholders
                        endpoint = re.sub(r'/[0-9a-f]{8,}(?:-[0-9a-f]{4,}){3,}-[0-9a-f]{12,}', '/:uuid', endpoint)
                        endpoint = re.sub(r'/[0-9a-f]{24,}', '/:id', endpoint)
                        endpoint = re.sub(r'/\d+', '/:id', endpoint)
                        
                        for category in categories:
                            endpoint_errors[endpoint][category] += 1
                    
                    # Time distribution
                    if timestamp:
                        hour = timestamp.hour
                        time_distribution[hour] += 1
                    
                    # Status codes
                    for field in ['status', 'status_code', 'statusCode', 'code']:
                        if field in log_entry:
                            try:
                                code = int(log_entry[field])
                                status_codes[code] += 1
                                break
                            except (ValueError, TypeError):
                                pass
                    
                    # User agent
                    for field in ['user_agent', 'userAgent', 'user-agent']:
                        if field in log_entry:
                            agent = log_entry[field]
                            # Simplify user agent
                            if 'curl' in agent.lower():
                                user_agent_errors['curl'] += 1
                            elif 'python' in agent.lower():
                                user_agent_errors['python'] += 1
                            elif 'node' in agent.lower() or 'axios' in agent.lower():
                                user_agent_errors['node'] += 1
                            elif 'mozilla' in agent.lower():
                                user_agent_errors['browser'] += 1
                            else:
                                user_agent_errors['other'] += 1
                            break
                    
                    # IP addresses
                    for field in ['ip', 'client_ip', 'remote_addr', 'clientIp', 'remoteAddr']:
                        if field in log_entry:
                            ip = log_entry[field]
                            if ip:
                                ip_errors[ip] += 1
                            break
                
                except Exception as e:
                    logger.error(f"Error processing line {line_num}: {str(e)}")
                    skipped_entries += 1
        
        # Prepare the analysis results
        results = {
            'summary': {
                'total_entries': total_entries,
                'error_entries': error_entries,
                'skipped_entries': skipped_entries,
                'time_window_hours': self.time_window_hours,
                'error_rate': round(error_entries / total_entries * 100, 2) if total_entries > 0 else 0
            },
            'error_categories': {
                category: count for category, count in error_counts.most_common()
            },
            'error_examples': error_examples,
            'top_error_endpoints': {
                endpoint: dict(categories.most_common(3)) 
                for endpoint, categories in sorted(
                    endpoint_errors.items(), 
                    key=lambda x: sum(x[1].values()), 
                    reverse=True
                )[:10]
            },
            'time_distribution': dict(sorted(time_distribution.items())),
            'status_codes': dict(status_codes.most_common()),
            'user_agent_errors': dict(
                sorted(user_agent_errors.items(), key=lambda x: x[1], reverse=True)
            ),
            'top_ip_addresses': dict(
                sorted(ip_errors.items(), key=lambda x: x[1], reverse=True)[:10]
            ),
            'recommendations': []
        }
        
        # Generate recommendations
        self._add_recommendations(results)
        
        return results
    
    def _add_recommendations(self, results: Dict[str, Any]) -> None:
        """
        Add recommendations based on the analysis results
        
        Args:
            results: Analysis results to add recommendations to
        """
        recommendations = []
        
        # Add category-specific recommendations
        for category, count in results['error_categories'].items():
            if count > 0:
                for pattern in self.error_patterns:
                    if pattern['name'] == category:
                        recommendations.append({
                            'category': category,
                            'recommendation': pattern['recommendation'],
                            'count': count
                        })
        
        # Add endpoint-specific recommendations
        for endpoint, categories in results['top_error_endpoints'].items():
            top_category = max(categories.items(), key=lambda x: x[1])[0]
            for pattern in self.error_patterns:
                if pattern['name'] == top_category:
                    recommendations.append({
                        'category': f"Endpoint: {endpoint}",
                        'recommendation': f"Focus on fixing {top_category} issues: {pattern['recommendation']}",
                        'count': sum(categories.values())
                    })
        
        # Add time-based recommendations
        time_dist = results['time_distribution']
        if time_dist:
            max_hour = max(time_dist.items(), key=lambda x: x[1])[0]
            if time_dist[max_hour] > sum(time_dist.values()) * 0.2:  # If > 20% of errors in one hour
                recommendations.append({
                    'category': 'Time pattern',
                    'recommendation': f"Investigate system load or scheduled jobs around {max_hour}:00, which has {time_dist[max_hour]} errors",
                    'count': time_dist[max_hour]
                })
        
        # Add user agent recommendations
        user_agents = results['user_agent_errors']
        if user_agents:
            top_agent, count = next(iter(user_agents.items()))
            if count > sum(user_agents.values()) * 0.5:  # If > 50% from one agent
                recommendations.append({
                    'category': 'Client library',
                    'recommendation': f"Review the {top_agent} client implementation for common errors",
                    'count': count
                })
        
        # Add IP-specific recommendations
        ip_errors = results['top_ip_addresses']
        if ip_errors:
            top_ip, count = next(iter(ip_errors.items()))
            if count > results['summary']['error_entries'] * 0.3:  # If > 30% from one IP
                recommendations.append({
                    'category': 'Client IP',
                    'recommendation': f"Work with the client at {top_ip} to resolve their high error rate",
                    'count': count
                })
        
        # Sort recommendations by error count
        results['recommendations'] = sorted(
            recommendations, 
            key=lambda x: x['count'], 
            reverse=True
        )
    
    def generate_report(self, analysis_results: Dict[str, Any], format: str = 'text') -> str:
        """
        Generate a human-readable report from analysis results
        
        Args:
            analysis_results: Results from analyze_logs
            format: Output format ('text', 'json', or 'html')
            
        Returns:
            Formatted report
        """
        if format == 'json':
            return json.dumps(analysis_results, indent=2)
        
        if format == 'html':
            return self._generate_html_report(analysis_results)
        
        # Default to text report
        report = []
        
        # Header
        report.append("==== API ERROR LOG ANALYSIS REPORT ====")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Time window: {analysis_results['summary']['time_window_hours']} hours")
        report.append("")
        
        # Summary
        report.append("=== SUMMARY ===")
        summary = analysis_results['summary']
        report.append(f"Total log entries: {summary['total_entries']:,}")
        report.append(f"Error entries: {summary['error_entries']:,} ({summary['error_rate']}%)")
        report.append(f"Skipped entries: {summary['skipped_entries']:,}")
        report.append("")
        
        # Error categories
        report.append("=== ERROR CATEGORIES ===")
        for category, count in analysis_results['error_categories'].items():
            percent = round(count / summary['error_entries'] * 100, 1) if summary['error_entries'] > 0 else 0
            report.append(f"{category}: {count:,} ({percent}%)")
        report.append("")
        
        # Top error endpoints
        report.append("=== TOP ERROR ENDPOINTS ===")
        for endpoint, categories in analysis_results['top_error_endpoints'].items():
            total = sum(categories.values())
            report.append(f"{endpoint}: {total:,} errors")
            for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
                report.append(f"  - {category}: {count:,}")
        report.append("")
        
        # Status codes
        report.append("=== STATUS CODES ===")
        for code, count in analysis_results['status_codes'].items():
            report.append(f"HTTP {code}: {count:,}")
        report.append("")
        
        # Time distribution
        report.append("=== TIME DISTRIBUTION ===")
        hours = analysis_results['time_distribution']
        max_count = max(hours.values()) if hours else 0
        
        for hour in range(24):
            count = hours.get(str(hour), 0)
            bar = "#" * int(count / max_count * 20) if max_count > 0 else ""
            report.append(f"{hour:02d}:00: {count:4} {bar}")
        report.append("")
        
        # User agent errors
        report.append("=== USER AGENT ERRORS ===")
        for agent, count in analysis_results['user_agent_errors'].items():
            report.append(f"{agent}: {count:,}")
        report.append("")
        
        # Top IP addresses
        report.append("=== TOP ERROR SOURCES (IP) ===")
        for ip, count in analysis_results['top_ip_addresses'].items():
            report.append(f"{ip}: {count:,}")
        report.append("")
        
        # Error examples
        report.append("=== ERROR EXAMPLES ===")
        for category, example in analysis_results['error_examples'].items():
            report.append(f"{category}:")
            report.append(f"  {example['line'][:100]}..." if len(example['line']) > 100 else f"  {example['line']}")
        report.append("")
        
        # Recommendations
        report.append("=== RECOMMENDATIONS ===")
        for i, rec in enumerate(analysis_results['recommendations'], 1):
            report.append(f"{i}. [{rec['category']}] {rec['recommendation']} ({rec['count']:,} errors)")
        
        return "\n".join(report)
    
    def _generate_html_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate an HTML report"""
        # This is a simplified HTML report - in a real implementation,
        # you might use a template engine like Jinja2
        
        html = []
        html.append("<!DOCTYPE html>")
        html.append("<html>")
        html.append("<head>")
        html.append("  <title>API Error Log Analysis Report</title>")
        html.append("  <style>")
        html.append("    body { font-family: Arial, sans-serif; line-height: 1.6; margin: 20px; }")
        html.append("    h1, h2 { color: #333; }")
        html.append("    table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }")
        html.append("    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }")
        html.append("    th { background-color: #f2f2f2; }")
        html.append("    tr:nth-child(even) { background-color: #f9f9f9; }")
        html.append("    .bar { background-color: #4CAF50; height: 20px; display: inline-block; }")
        html.append("    .recommendation { background-color: #e7f3fe; border-left: 6px solid #2196F3; padding: 10px; margin-bottom: 10px; }")
        html.append("  </style>")
        html.append("</head>")
        html.append("<body>")
        
        # Header
        html.append(f"<h1>API Error Log Analysis Report</h1>")
        html.append(f"<p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")
        html.append(f"<p>Time window: {analysis_results['summary']['time_window_hours']} hours</p>")
        
        # Summary
        html.append("<h2>Summary</h2>")
        summary = analysis_results['summary']
        html.append("<table>")
        html.append("  <tr><th>Metric</th><th>Value</th></tr>")
        html.append(f"  <tr><td>Total log entries</td><td>{summary['total_entries']:,}</td></tr>")
        html.append(f"  <tr><td>Error entries</td><td>{summary['error_entries']:,} ({summary['error_rate']}%)</td></tr>")
        html.append(f"  <tr><td>Skipped entries</td><td>{summary['skipped_entries']:,}</td></tr>")
        html.append("</table>")
        
        # Error categories
        html.append("<h2>Error Categories</h2>")
        html.append("<table>")
        html.append("  <tr><th>Category</th><th>Count</th><th>Percentage</th><th>Distribution</th></tr>")
        
        # Calculate max for bar scaling
        max_category_count = max(analysis_results['error_categories'].values()) if analysis_results['error_categories'] else 0
        
        for category, count in analysis_results['error_categories'].items():
            percent = round(count / summary['error_entries'] * 100, 1) if summary['error_entries'] > 0 else 0
            bar_width = int(count / max_category_count * 100) if max_category_count > 0 else 0
            html.append(f"  <tr>")
            html.append(f"    <td>{category}</td>")
            html.append(f"    <td>{count:,}</td>")
            html.append(f"    <td>{percent}%</td>")
            html.append(f"    <td><div class='bar' style='width: {bar_width}%'></div></td>")
            html.append(f"  </tr>")
        
        html.append("</table>")
        
        # Top error endpoints
        html.append("<h2>Top Error Endpoints</h2>")
        html.append("<table>")
        html.append("  <tr><th>Endpoint</th><th>Error Count</th><th>Top Categories</th></tr>")
        
        for endpoint, categories in analysis_results['top_error_endpoints'].items():
            total = sum(categories.values())
            top_cats = ", ".join([f"{cat}: {count}" for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True)])
            html.append(f"  <tr>")
            html.append(f"    <td>{endpoint}</td>")
            html.append(f"    <td>{total:,}</td>")
            html.append(f"    <td>{top_cats}</td>")
            html.append(f"  </tr>")
        
        html.append("</table>")
        
        # Status codes
        html.append("<h2>Status Codes</h2>")
        html.append("<table>")
        html.append("  <tr><th>Status Code</th><th>Count</th></tr>")
        
        for code, count in analysis_results['status_codes'].items():
            html.append(f"  <tr><td>HTTP {code}</td><td>{count:,}</td></tr>")
        
        html.append("</table>")
        
        # Time distribution
        html.append("<h2>Time Distribution</h2>")
        html.append("<table>")
        html.append("  <tr><th>Hour</th><th>Error Count</th><th>Distribution</th></tr>")
        
        hours = analysis_results['time_distribution']
        max_hour_count = max(hours.values()) if hours else 0
        
        for hour in range(24):
            count = hours.get(str(hour), 0)
            bar_width = int(count / max_hour_count * 100) if max_hour_count > 0 else 0
            html.append(f"  <tr>")
            html.append(f"    <td>{hour:02d}:00</td>")
            html.append(f"    <td>{count:,}</td>")
            html.append(f"    <td><div class='bar' style='width: {bar_width}%'></div></td>")
            html.append(f"  </tr>")
        
        html.append("</table>")
        
        # Recommendations
        html.append("<h2>Recommendations</h2>")
        
        for i, rec in enumerate(analysis_results['recommendations'], 1):
            html.append(f"<div class='recommendation'>")
            html.append(f"  <h3>{i}. {rec['category']} ({rec['count']:,} errors)</h3>")
            html.append(f"  <p>{rec['recommendation']}</p>")
            html.append(f"</div>")
        
        # Footer
        html.append("</body>")
        html.append("</html>")
        
        return "\n".join(html)

def main():
    """Command line interface for the log analyzer"""
    parser = argparse.ArgumentParser(description='Analyze API error logs and generate a report')
    parser.add_argument('log_file', help='Path to the log file to analyze')
    parser.add_argument('--format', choices=['text', 'json', 'html'], default='text', help='Output format (default: text)')
    parser.add_argument('--output', help='Output file (default: print to console)')
    parser.add_argument('--hours', type=int, default=24, help='Time window in hours (default: 24)')
    
    args = parser.parse_args()
    
    # Create and run the analyzer
    analyzer = APILogAnalyzer(time_window_hours=args.hours)
    results = analyzer.analyze_logs(args.log_file)
    
    # Generate the report
    report = analyzer.generate_report(results, format=args.format)
    
    # Output the report
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as file:
            file.write(report)
        print(f"Report written to {args.output}")
    else:
        print(report)

if __name__ == "__main__":
    main()