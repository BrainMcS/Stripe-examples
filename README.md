# Stripe Examples

This repository contains various examples and utilities for working with the Stripe API. It demonstrates how to integrate Stripe's payment processing features, handle errors, and troubleshoot connectivity issues.

## Features

- **Payment Intent API**: Create and manage payment intents for processing payments.
- **Error Handling**: Robust error handling for API requests.
- **Webhook Handling**: Process Stripe webhook events such as `payment_intent.succeeded` and `payment_intent.payment_failed`.
- **Connectivity Debugging**: Tools to troubleshoot SSL and connectivity issues with the Stripe API.
- **Environment Variable Management**: Securely manage API keys using `.env` files.

## Prerequisites

- Python 3.7 or higher
- A Stripe account ([Sign up here](https://stripe.com))
- `pip` for managing Python packages

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/stripe-examples.git
   cd stripe-examples
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up your `.env` file: Create a `.env` file in the root directory with the following content:
   ```
   STRIPE_API_KEY=your_stripe_api_key
   ```

## Usage

### Running Examples

- **Payment Intent Example**:
  ```bash
  python 26_api_paymentintent.py
  ```

- **Error Handling Example**:
  ```bash
  python 27_api_errorhandling.py
  ```

- **Webhook Handling Example**:
  ```bash
  python 38_api_paymetintentwitherrors.py
  ```

- **Connectivity Debugging**:
  ```bash
  python 29_api_troubleshootconnectivity.py
  ```

### Testing Webhooks

To test webhooks locally, use a tool like [Stripe CLI](https://stripe.com/docs/stripe-cli):
```bash
stripe listen --forward-to localhost:8000/webhook
```

## Project Structure

- `26_api_paymentintent.py`: Example for creating payment intents.
- `27_api_errorhandling.py`: Demonstrates robust error handling for API requests.
- `29_api_troubleshootconnectivity.py`: Tools for debugging SSL and connectivity issues.
- `38_api_paymetintentwitherrors.py`: Handles webhook events for payment intents.
- `.env`: Environment variables for API keys and configuration.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Resources

- [Stripe API Documentation](https://stripe.com/docs/api)
- [Stripe CLI](https://stripe.com/docs/stripe-cli)
- [Python SDK for Stripe](https://github.com/stripe/stripe-python)
