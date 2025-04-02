from flask import Flask, request, redirect, render_template, session, url_for
import stripe
import os
import secrets
from urllib.parse import urlencode
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)  # Generate a secure random secret key

# Stripe configuration
CLIENT_ID = "ca_your_client_id"
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
REDIRECT_URI = "http://localhost:5000/oauth/callback"

# Set the Stripe API key for direct API calls
stripe.api_key = STRIPE_SECRET_KEY

@app.route('/')
def index():
    """Render the home page with a connect button"""
    return render_template('index.html')

@app.route('/connect')
def connect():
    """Initiate the OAuth flow"""
    # Generate a state parameter to prevent CSRF attacks
    state = secrets.token_hex(16)
    session['oauth_state'] = state
    
    # Define the scope of permissions
    # 'read_write' gives access to read and update resources in the connected account
    scope = "read_write"
    
    # Build the authorization URL with necessary parameters
    params = {
        'response_type': 'code',
        'client_id': CLIENT_ID,
        'scope': scope,
        'state': state,
        'redirect_uri': REDIRECT_URI,
        'stripe_user[email]': request.args.get('email', ''),  # Pre-fill email if provided
        'stripe_user[country]': request.args.get('country', 'US')  # Default to US
    }
    
    url = f"https://connect.stripe.com/oauth/authorize?{urlencode(params)}"
    logger.info(f"Redirecting to Stripe OAuth: {url}")
    
    # Redirect the user to Stripe's authorization page
    return redirect(url)

@app.route('/oauth/callback')
def oauth_callback():
    """Handle the OAuth callback from Stripe"""
    # Verify state parameter to prevent CSRF attacks
    if request.args.get('state') != session.get('oauth_state'):
        logger.warning("OAuth state mismatch - possible CSRF attempt")
        return render_template('error.html', error="Invalid state parameter. Possible CSRF attack.")
    
    # Check for errors in the callback
    if 'error' in request.args:
        error = request.args.get('error')
        error_description = request.args.get('error_description', 'No description provided')
        logger.error(f"OAuth error: {error} - {error_description}")
        return render_template('error.html', error=error, description=error_description)
    
    # Exchange the authorization code for an access token
    code = request.args.get('code')
    if not code:
        logger.error("No authorization code received in callback")
        return render_template('error.html', error="No authorization code received")
    
    try:
        logger.info(f"Exchanging authorization code for access token")
        
        # Make the API call to exchange the code for tokens
        response = stripe.OAuth.token(
            grant_type='authorization_code',
            code=code
        )
        
        # Extract the tokens and connected account ID
        access_token = response['access_token']
        refresh_token = response.get('refresh_token')
        connected_account_id = response['stripe_user_id']
        
        # Log the successful connection
        logger.info(f"Successfully connected account: {connected_account_id}")
        
        # Store the tokens in the session (in a real app, store in a secure database)
        session['access_token'] = access_token
        session['refresh_token'] = refresh_token
        session['connected_account_id'] = connected_account_id
        
        # Retrieve basic account information using the connected account ID
        account_info = stripe.Account.retrieve(connected_account_id)
        
        # Retrieve account balance
        balance = stripe.Balance.retrieve(stripe_account=connected_account_id)
        
        # Format the available balance for display
        available_balance = []
        for amount in balance['available']:
            available_balance.append({
                'currency': amount['currency'].upper(),
                'amount': amount['amount'] / 100.0  # Convert cents to dollars
            })
        
        pending_balance = []
        for amount in balance['pending']:
            pending_balance.append({
                'currency': amount['currency'].upper(),
                'amount': amount['amount'] / 100.0  # Convert cents to dollars
            })
        
        # Extract relevant account details for display
        account_details = {
            'id': account_info['id'],
            'business_name': account_info.get('business_profile', {}).get('name', 'Not provided'),
            'display_name': account_info.get('settings', {}).get('dashboard', {}).get('display_name', 'Not provided'),
            'country': account_info.get('country', 'Not provided'),
            'email': account_info.get('email', 'Not provided'),
            'charges_enabled': account_info.get('charges_enabled', False),
            'payouts_enabled': account_info.get('payouts_enabled', False),
            'default_currency': account_info.get('default_currency', 'USD').upper(),
            'account_type': account_info.get('type', 'standard')
        }
        
        # Get recent transactions (for demonstration)
        try:
            transactions = stripe.BalanceTransaction.list(
                limit=5,
                stripe_account=connected_account_id
            )
            
            recent_transactions = []
            for transaction in transactions.data:
                recent_transactions.append({
                    'id': transaction.id,
                    'amount': transaction.amount / 100.0,
                    'currency': transaction.currency.upper(),
                    'status': transaction.status,
                    'type': transaction.type,
                    'created': transaction.created,
                    'description': transaction.description
                })
        except Exception as e:
            logger.error(f"Error fetching transactions: {e}")
            recent_transactions = []
        
        return render_template(
            'connected.html',
            account=account_details,
            available_balance=available_balance,
            pending_balance=pending_balance,
            transactions=recent_transactions
        )
        
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error during OAuth token exchange: {e}")
        return render_template('error.html', error=f"Stripe Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during OAuth callback: {e}")
        return render_template('error.html', error=f"Unexpected Error: {e}")

@app.route('/disconnect')
def disconnect():
    """Disconnect the connected Stripe account"""
    if 'connected_account_id' not in session:
        return redirect(url_for('index'))
    
    try:
        # Get the account ID before we clear it from session
        account_id = session['connected_account_id']
        logger.info(f"Disconnecting account: {account_id}")
        
        # Revoke access to the connected account
        stripe.OAuth.deauthorize(
            client_id=CLIENT_ID,
            stripe_user_id=account_id
        )
        
        # Clear the session data
        session.pop('access_token', None)
        session.pop('refresh_token', None)
        session.pop('connected_account_id', None)
        
        return render_template('disconnected.html')
    
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error during disconnect: {e}")
        return render_template('error.html', error=f"Stripe Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during disconnect: {e}")
        return render_template('error.html', error=f"Unexpected Error: {e}")

@app.route('/refresh_token')
def refresh_token():
    """Refresh the OAuth access token using the refresh token"""
    if 'refresh_token' not in session:
        return redirect(url_for('index'))
    
    try:
        logger.info("Refreshing access token")
        
        # Make the API call to refresh the token
        response = stripe.OAuth.token(
            grant_type='refresh_token',
            refresh_token=session['refresh_token']
        )
        
        # Update the tokens in the session
        session['access_token'] = response['access_token']
        if 'refresh_token' in response:
            session['refresh_token'] = response['refresh_token']
        
        return redirect(url_for('dashboard'))
        
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error during token refresh: {e}")
        return render_template('error.html', error=f"Stripe Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during token refresh: {e}")
        return render_template('error.html', error=f"Unexpected Error: {e}")

@app.route('/dashboard')
def dashboard():
    """Show the dashboard for the connected account"""
    if 'connected_account_id' not in session:
        return redirect(url_for('index'))
    
    try:
        # Retrieve updated account information
        account_id = session['connected_account_id']
        account_info = stripe.Account.retrieve(account_id)
        balance = stripe.Balance.retrieve(stripe_account=account_id)
        
        # Format balances
        available_balance = [
            {
                'currency': amount['currency'].upper(),
                'amount': amount['amount'] / 100.0
            } for amount in balance['available']
        ]
        
        pending_balance = [
            {
                'currency': amount['currency'].upper(),
                'amount': amount['amount'] / 100.0
            } for amount in balance['pending']
        ]
        
        # Get recent payments
        payments = stripe.PaymentIntent.list(
            limit=10,
            stripe_account=account_id
        )
        
        recent_payments = [
            {
                'id': payment.id,
                'amount': payment.amount / 100.0,
                'currency': payment.currency.upper(),
                'status': payment.status,
                'created': payment.created
            } for payment in payments.data
        ]
        
        return render_template(
            'dashboard.html',
            account=account_info,
            available_balance=available_balance,
            pending_balance=pending_balance,
            payments=recent_payments
        )
        
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error loading dashboard: {e}")
        # If token expired, redirect to refresh
        if 'refresh_token' in session and getattr(e, 'code', '') == 'invalid_request':
            return redirect(url_for('refresh_token'))
        return render_template('error.html', error=f"Stripe Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error loading dashboard: {e}")
        return render_template('error.html', error=f"Unexpected Error: {e}")

# Templates for the application (in a real app, these would be in separate files)
# These are simplified examples - a real app would have more complete templates

@app.route('/templates/index.html')
def index_template():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Connect with Stripe</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
            .container { max-width: 800px; margin: 0 auto; }
            .btn { display: inline-block; background: #635BFF; color: white; padding: 10px 20px; 
                   text-decoration: none; border-radius: 4px; }
            .btn:hover { background: #524DFF; }
            .form-group { margin-bottom: 15px; }
            label { display: block; margin-bottom: 5px; }
            input { padding: 8px; width: 300px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Connect Your Stripe Account</h1>
            <p>Connect your Stripe account to access your balance and account information.</p>
            
            <form action="/connect" method="get">
                <div class="form-group">
                    <label for="email">Email (optional):</label>
                    <input type="email" id="email" name="email" placeholder="your@email.com">
                </div>
                
                <div class="form-group">
                    <label for="country">Country:</label>
                    <select name="country" id="country">
                        <option value="US">United States</option>
                        <option value="CA">Canada</option>
                        <option value="GB">United Kingdom</option>
                        <option value="AU">Australia</option>
                    </select>
                </div>
                
                <button type="submit" class="btn">Connect with Stripe</button>
            </form>
        </div>
    </body>
    </html>
    """

@app.route('/templates/connected.html')
def connected_template():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Successfully Connected</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
            .container { max-width: 800px; margin: 0 auto; }
            .info-box { background: #f9f9f9; border: 1px solid #ddd; padding: 20px; margin-bottom: 20px; border-radius: 4px; }
            .btn { display: inline-block; background: #635BFF; color: white; padding: 10px 20px; 
                   text-decoration: none; border-radius: 4px; }
            .btn-secondary { background: #6c757d; }
            .btn-danger { background: #FF4242; }
            .btn:hover { opacity: 0.9; }
            .balance-card { display: inline-block; background: #eef0ff; border: 1px solid #ddd; 
                            padding: 15px; margin-right: 15px; margin-bottom: 15px; border-radius: 4px; min-width: 150px; }
            .balance-amount { font-size: 24px; font-weight: bold; }
            .transactions-table { width: 100%; border-collapse: collapse; }
            .transactions-table th, .transactions-table td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
            .transactions-table th { background-color: #f2f2f2; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Successfully Connected!</h1>
            <p>Your Stripe account has been successfully connected to our platform.</p>
            
            <h2>Account Information</h2>
            <div class="info-box">
                <p><strong>Account ID:</strong> {{ account.id }}</p>
                <p><strong>Business Name:</strong> {{ account.business_name }}</p>
                <p><strong>Display Name:</strong> {{ account.display_name }}</p>
                <p><strong>Country:</strong> {{ account.country }}</p>
                <p><strong>Email:</strong> {{ account.email }}</p>
                <p><strong>Account Type:</strong> {{ account.account_type }}</p>
                <p><strong>Charges Enabled:</strong> {{ 'Yes' if account.charges_enabled else 'No' }}</p>
                <p><strong>Payouts Enabled:</strong> {{ 'Yes' if account.payouts_enabled else 'No' }}</p>
                <p><strong>Default Currency:</strong> {{ account.default_currency }}</p>
            </div>
            
            <h2>Available Balance</h2>
            <div>
                {% for bal in available_balance %}
                <div class="balance-card">
                    <div class="balance-amount">{{ bal.currency }} {{ "%.2f"|format(bal.amount) }}</div>
                    <div>Available</div>
                </div>
                {% endfor %}
                
                {% for bal in pending_balance %}
                <div class="balance-card">
                    <div class="balance-amount">{{ bal.currency }} {{ "%.2f"|format(bal.amount) }}</div>
                    <div>Pending</div>
                </div>
                {% endfor %}
            </div>
            
            {% if transactions %}
            <h2>Recent Transactions</h2>
            <div class="info-box">
                <table class="transactions-table">
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Type</th>
                            <th>Amount</th>
                            <th>Status</th>
                            <th>Description</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for txn in transactions %}
                        <tr>
                            <td>{{ txn.id }}</td>
                            <td>{{ txn.type }}</td>
                            <td>{{ txn.currency }} {{ "%.2f"|format(txn.amount) }}</td>
                            <td>{{ txn.status }}</td>
                            <td>{{ txn.description or 'N/A' }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
            {% endif %}
            
            <div>
                <a href="/dashboard" class="btn">Go to Dashboard</a>
                <a href="/disconnect" class="btn btn-danger">Disconnect Account</a>
            </div>
        </div>
    </body>
    </html>
    """

@app.route('/templates/dashboard.html')
def dashboard_template():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Account Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
            .container { max-width: 800px; margin: 0 auto; }
            .info-box { background: #f9f9f9; border: 1px solid #ddd; padding: 20px; margin-bottom: 20px; border-radius: 4px; }
            .btn { display: inline-block; background: #635BFF; color: white; padding: 10px 20px; 
                   text-decoration: none; border-radius: 4px; }
            .btn-danger { background: #FF4242; }
            .btn:hover { opacity: 0.9; }
            .balance-row { display: flex; flex-wrap: wrap; margin-bottom: 20px; }
            .balance-card { background: #eef0ff; border: 1px solid #ddd; padding: 15px; 
                            margin-right: 15px; margin-bottom: 15px; border-radius: 4px; flex: 0 0 180px; }
            .balance-amount { font-size: 24px; font-weight: bold; }
            .payments-table { width: 100%; border-collapse: collapse; }
            .payments-table th, .payments-table td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
            .payments-table th { background-color: #f2f2f2; }
            .status-badge { display: inline-block; padding: 3px 8px; border-radius: 12px; font-size: 12px; }
            .status-succeeded { background-color: #d4edda; color: #155724; }
            .status-pending { background-color: #fff3cd; color: #856404; }
            .status-failed { background-color: #f8d7da; color: #721c24; }
            .header { display: flex; justify-content: space-between; align-items: center; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Dashboard</h1>
                <a href="/disconnect" class="btn btn-danger">Disconnect Account</a>
            </div>
            
            <h2>Account Information</h2>
            <div class="info-box">
                <p><strong>Account ID:</strong> {{ account.id }}</p>
                <p><strong>Business Name:</strong> {{ account.business_profile.name }}</p>
                <p><strong>Email:</strong> {{ account.email }}</p>
                <p><strong>Charges Enabled:</strong> {{ 'Yes' if account.charges_enabled else 'No' }}</p>
                <p><strong>Payouts Enabled:</strong> {{ 'Yes' if account.payouts_enabled else 'No' }}</p>
            </div>
            
            <h2>Balance</h2>
            <div class="balance-row">
                {% for bal in available_balance %}
                <div class="balance-card">
                    <div class="balance-amount">{{ bal.currency }} {{ "%.2f"|format(bal.amount) }}</div>
                    <div>Available</div>
                </div>
                {% endfor %}
                
                {% for bal in pending_balance %}
                <div class="balance-card">
                    <div class="balance-amount">{{ bal.currency }} {{ "%.2f"|format(bal.amount) }}</div>
                    <div>Pending</div>
                </div>
                {% endfor %}
            </div>
            
            <h2>Recent Payments</h2>
            <div class="info-box">
                {% if payments %}
                <table class="payments-table">
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Amount</th>
                            <th>Status</th>
                            <th>Date</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for payment in payments %}
                        <tr>
                            <td>{{ payment.id }}</td>
                            <td>{{ payment.currency }} {{ "%.2f"|format(payment.amount) }}</td>
                            <td>
                                <span class="status-badge status-{{ payment.status }}">
                                    {{ payment.status }}
                                </span>
                            </td>
                            <td>{{ payment.created|date }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
                {% else %}
                <p>No recent payments found.</p>
                {% endif %}
            </div>
        </div>
    </body>
    </html>
    """

@app.route('/templates/disconnected.html')
def disconnected_template():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Account Disconnected</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
            .container { max-width: 800px; margin: 0 auto; }
            .message-box { background: #f9f9f9; border: 1px solid #ddd; padding: 20px; margin: 20px 0; border-radius: 4px; }
            .btn { display: inline-block; background: #635BFF; color: white; padding: 10px 20px; 
                   text-decoration: none; border-radius: 4px; }
            .btn:hover { background: #524DFF; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Account Disconnected</h1>
            <div class="message-box">
                <p>Your Stripe account has been successfully disconnected from our platform.</p>
                <p>All access tokens have been revoked, and we no longer have access to your Stripe account.</p>
            </div>
            <p>Thank you for trying our service!</p>
            <a href="/" class="btn">Back to Home</a>
        </div>
    </body>
    </html>
    """

@app.route('/templates/error.html')
def error_template():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Error</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
            .container { max-width: 800px; margin: 0 auto; }
            .error-box { background: #FFF0F0; border: 1px solid #FFCCCC; padding: 20px; color: #D8000C; margin: 20px 0; border-radius: 4px; }
            .btn { display: inline-block; background: #635BFF; color: white; padding: 10px 20px; 
                   text-decoration: none; border-radius: 4px; }
            .btn:hover { background: #524DFF; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Error</h1>
            <div class="error-box">
                <h2>{{ error }}</h2>
                {% if description %}
                <p>{{ description }}</p>
                {% endif %}
            </div>
            <a href="/" class="btn">Back to Home</a>
        </div>
    </body>
    </html>
    """

if __name__ == '__main__':
    # For development only - use proper WSGI server in production
    app.run(host='0.0.0.0', port=5000, debug=True)