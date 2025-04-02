import decimal
from forex_python.converter import CurrencyCodes
import babel.numbers
from typing import Dict, List, Union
from dataclasses import dataclass


# Set decimal precision for monetary calculations
decimal.getcontext().prec = 18

@dataclass
class Currency:
    """Currency information based on ISO 4217 standard"""
    code: str  # ISO 4217 code (e.g., 'USD')
    numeric_code: str  # Numeric code (e.g., '840')
    name: str  # Currency name (e.g., 'US Dollar')
    symbol: str  # Currency symbol (e.g., '$')
    decimals: int  # Number of decimal places (e.g., 2)
    is_rtl: bool = False  # Right-to-left display

    @classmethod
    def from_code(cls, code: str) -> 'Currency':
        """Create a Currency instance from ISO code"""
        code = code.upper()
        # Use forex-python library to get currency details
        currency_data = CurrencyCodes()
        numeric_code = currency_data.get_currency_numeric_code(code)
        name = currency_data.get_currency_name(code)
        symbol = currency_data.get_symbol(code)
        decimals = 2  # Default to 2 decimal places
        
        # Get symbol using babel
        symbol = babel.numbers.get_currency_symbol(code, locale='en_US')
        
        # Determine if RTL based on common RTL currencies
        rtl_currencies = ['AED', 'ILS', 'SAR']
        is_rtl = code in rtl_currencies

        return cls(
            code=code,
            numeric_code=numeric_code,
            name=name,
            symbol=symbol,
            decimals=decimals,
            is_rtl=is_rtl
        )


class Money:
    """Class to handle monetary values with currency information"""
    
    def __init__(self, amount: Union[int, float, str, decimal.Decimal], currency_code: str):
        # Convert amount to Decimal for precise calculations
        self.amount = decimal.Decimal(str(amount))
        self.currency = Currency.from_code(currency_code)
    
    @property
    def formatted(self) -> str:
        """Format the monetary amount with the currency symbol"""
        # Round to appropriate decimal places
        rounded_amount = self.amount.quantize(
            decimal.Decimal('0.' + '0' * self.currency.decimals)
        )
        
        # Format with babel for proper localization
        formatted = babel.numbers.format_currency(
            rounded_amount, 
            self.currency.code
        )
        
        return formatted
    
    def __repr__(self) -> str:
        return f"Money({self.amount}, '{self.currency.code}')"
    
    def __str__(self) -> str:
        return self.formatted


class ExchangeRateProvider:
    """Base class for exchange rate providers"""
    
    def get_rate(self, from_currency: str, to_currency: str) -> decimal.Decimal:
        """Get the exchange rate from one currency to another"""
        raise NotImplementedError("Subclasses must implement get_rate")


class StaticExchangeRateProvider(ExchangeRateProvider):
    """Exchange rate provider with static, predefined rates"""
    
    def __init__(self, base_currency: str, rates: Dict[str, float]):
        self.base_currency = base_currency.upper()
        self.rates = {k.upper(): decimal.Decimal(str(v)) for k, v in rates.items()}
        # Add base currency with rate 1.0
        self.rates[self.base_currency] = decimal.Decimal('1.0')
    
    def get_rate(self, from_currency: str, to_currency: str) -> decimal.Decimal:
        """Get exchange rate between two currencies"""
        from_currency = from_currency.upper()
        to_currency = to_currency.upper()
        
        if from_currency == to_currency:
            return decimal.Decimal('1.0')
        
        # Get rates relative to base currency
        if from_currency not in self.rates:
            raise ValueError(f"Exchange rate not available for {from_currency}")
        if to_currency not in self.rates:
            raise ValueError(f"Exchange rate not available for {to_currency}")
        
        # Calculate cross-rate if neither is the base currency
        from_rate = self.rates[from_currency]
        to_rate = self.rates[to_currency]
        
        # Convert via base currency
        return to_rate / from_rate


class APIExchangeRateProvider(ExchangeRateProvider):
    """Exchange rate provider that uses an external API"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.exchangerate.host"):
        self.api_key = api_key
        self.base_url = base_url
        self.cache = {}  # Simple cache for rates
        self.cache_expiry = {}  # When rates expire
    
    def get_rate(self, from_currency: str, to_currency: str) -> decimal.Decimal:
        """Get current exchange rate from API"""
        import requests
        from datetime import datetime, timedelta
        
        from_currency = from_currency.upper()
        to_currency = to_currency.upper()
        
        if from_currency == to_currency:
            return decimal.Decimal('1.0')
        
        # Check cache first
        cache_key = f"{from_currency}_{to_currency}"
        now = datetime.now()
        
        if cache_key in self.cache and self.cache_expiry.get(cache_key, now) > now:
            return self.cache[cache_key]
        
        # Call the API
        response = requests.get(
            f"{self.base_url}/convert",
            params={
                "from": from_currency,
                "to": to_currency,
                "amount": 1,
                "apikey": self.api_key
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"API error: {response.status_code} - {response.text}")
        
        data = response.json()
        rate = decimal.Decimal(str(data["result"]))
        
        # Cache the result for 1 hour
        self.cache[cache_key] = rate
        self.cache_expiry[cache_key] = now + timedelta(hours=1)
        
        return rate


class CurrencyConverter:
    """Handles currency conversion logic"""
    
    def __init__(self, rate_provider: ExchangeRateProvider):
        self.rate_provider = rate_provider
    
    def convert(self, money: Money, target_currency_code: str) -> Money:
        """Convert money from one currency to another"""
        if money.currency.code == target_currency_code.upper():
            return money
        
        rate = self.rate_provider.get_rate(
            money.currency.code, 
            target_currency_code
        )
        
        converted_amount = money.amount * rate
        
        return Money(converted_amount, target_currency_code)


# Now let's implement the three multi-currency strategies

class SingleAccountProcessor:
    """
    Approach 1: Single merchant account with conversion
    - Simple implementation
    - All transactions settled in one currency
    - Currency conversion handled by processor
    """
    
    def __init__(self, merchant_currency: str, processor_fee_percentage: float = 3.0):
        self.merchant_currency = merchant_currency.upper()
        self.processor_fee_percentage = decimal.Decimal(str(processor_fee_percentage))
        
        # Initialize with a static rate provider for demonstration
        rates = {"USD": 1.0, "EUR": 0.85, "GBP": 0.75, "JPY": 110.2, "CAD": 1.25}
        self.rate_provider = StaticExchangeRateProvider("USD", rates)
        self.converter = CurrencyConverter(self.rate_provider)
    
    def process_payment(self, amount: float, currency_code: str) -> Dict[str, any]:
        """Process a payment in any supported currency"""
        presentment_money = Money(amount, currency_code)
        
        # Apply processor conversion (with markup)
        processor_rate = self.rate_provider.get_rate(currency_code, self.merchant_currency)
        
        # Add conversion fee
        fee_multiplier = 1 - (self.processor_fee_percentage / 100)
        effective_rate = processor_rate * fee_multiplier
        
        # Calculate settlement amount
        settlement_amount = presentment_money.amount * effective_rate
        settlement_money = Money(settlement_amount, self.merchant_currency)
        
        return {
            "presentment_amount": str(presentment_money),
            "settlement_amount": str(settlement_money),
            "exchange_rate": float(processor_rate),
            "effective_rate": float(effective_rate),
            "conversion_fee_percentage": float(self.processor_fee_percentage)
        }


class MultiCurrencyAccountProcessor:
    """
    Approach 2: Multiple currency accounts
    - Separate account for each currency
    - No conversion for supported currencies
    - Fallback to conversion for unsupported currencies
    """
    
    def __init__(self, supported_currencies: List[str], default_currency: str):
        self.supported_currencies = [c.upper() for c in supported_currencies]
        self.default_currency = default_currency.upper()
        self.balances = {currency: decimal.Decimal('0') for currency in self.supported_currencies}
        
        # Initialize with a static rate provider for demonstration
        rates = {"USD": 1.0, "EUR": 0.85, "GBP": 0.75, "JPY": 110.2, "CAD": 1.25}
        self.rate_provider = StaticExchangeRateProvider("USD", rates)
        self.converter = CurrencyConverter(self.rate_provider)
    
    def process_payment(self, amount: float, currency_code: str) -> Dict[str, any]:
        """Process a payment with multiple currency accounts"""
        currency_code = currency_code.upper()
        presentment_money = Money(amount, currency_code)
        
        # Check if we support this currency
        if currency_code in self.supported_currencies:
            # No conversion needed
            self.balances[currency_code] += presentment_money.amount
            
            return {
                "presentment_amount": str(presentment_money),
                "settlement_amount": str(presentment_money),
                "account_used": currency_code,
                "conversion_applied": False
            }
        else:
            # Convert to default currency
            settlement_money = self.converter.convert(presentment_money, self.default_currency)
            self.balances[self.default_currency] += settlement_money.amount
            
            return {
                "presentment_amount": str(presentment_money),
                "settlement_amount": str(settlement_money),
                "account_used": self.default_currency,
                "conversion_applied": True,
                "exchange_rate": float(self.rate_provider.get_rate(currency_code, self.default_currency))
            }
    
    def get_balances(self) -> Dict[str, str]:
        """Get current balances in each currency account"""
        return {currency: str(Money(amount, currency)) 
                for currency, amount in self.balances.items()}


class ThirdPartyForexProcessor:
    """
    Approach 3: Third-party forex service
    - Uses specialized service for currency conversion
    - Better rates than processor conversion
    - Additional integration complexity
    """
    
    def __init__(self, settlement_currency: str, forex_provider: ExchangeRateProvider):
        self.settlement_currency = settlement_currency.upper()
        self.forex_provider = forex_provider
        self.converter = CurrencyConverter(forex_provider)
        self.forex_fee_percentage = decimal.Decimal('1.0')  # Lower than processor fees
    
    def process_payment(self, amount: float, currency_code: str) -> Dict[str, any]:
        """Process a payment using third-party forex service"""
        presentment_money = Money(amount, currency_code)
        
        # Check if conversion is needed
        if currency_code.upper() == self.settlement_currency:
            # No conversion needed
            return {
                "presentment_amount": str(presentment_money),
                "settlement_amount": str(presentment_money),
                "conversion_applied": False
            }
        
        # Get the base exchange rate
        base_rate = self.forex_provider.get_rate(currency_code, self.settlement_currency)
        
        # Apply forex service fee
        fee_multiplier = 1 - (self.forex_fee_percentage / 100)
        effective_rate = base_rate * fee_multiplier
        
        # Calculate settlement amount
        settlement_amount = presentment_money.amount * effective_rate
        settlement_money = Money(settlement_amount, self.settlement_currency)
        
        return {
            "presentment_amount": str(presentment_money),
            "settlement_amount": str(settlement_money),
            "base_exchange_rate": float(base_rate),
            "effective_rate": float(effective_rate),
            "forex_fee_percentage": float(self.forex_fee_percentage),
            "conversion_applied": True
        }


# Example usage demonstrating all three approaches

def run_example():
    print("Multi-Currency Implementation Examples\n")
    
    # Example payment amounts in different currencies
    payments = [
        (100, "USD"),
        (85, "EUR"),
        (75, "GBP"),
        (11000, "JPY"),
        (50, "AUD")  # Currency that might not be directly supported
    ]
    
    # Initialize static exchange rates for demo
    rates = {"USD": 1.0, "EUR": 0.85, "GBP": 0.75, "JPY": 110.2, "CAD": 1.25, "AUD": 1.36}
    static_provider = StaticExchangeRateProvider("USD", rates)
    
    # Approach 1: Single Account with Conversion
    print("=== APPROACH 1: SINGLE MERCHANT ACCOUNT WITH CONVERSION ===")
    processor1 = SingleAccountProcessor(merchant_currency="USD")
    
    for amount, currency in payments:
        result = processor1.process_payment(amount, currency)
        print(f"\nPayment: {Money(amount, currency)}")
        print(f"Settlement: {result['settlement_amount']}")
        print(f"Exchange Rate: {result.get('exchange_rate', 1.0)}")
        print(f"Conversion Fee: {result['conversion_fee_percentage']}%")
    
    # Approach 2: Multiple Currency Accounts
    print("\n\n=== APPROACH 2: MULTIPLE CURRENCY ACCOUNTS ===")
    processor2 = MultiCurrencyAccountProcessor(
        supported_currencies=["USD", "EUR", "GBP", "JPY"],
        default_currency="USD"
    )
    
    for amount, currency in payments:
        result = processor2.process_payment(amount, currency)
        print(f"\nPayment: {Money(amount, currency)}")
        print(f"Settlement: {result['settlement_amount']}")
        print(f"Account Used: {result['account_used']}")
        print(f"Conversion Applied: {result['conversion_applied']}")
        if result['conversion_applied']:
            print(f"Exchange Rate: {result.get('exchange_rate', 1.0)}")
    
    print("\nFinal balances:")
    for currency, balance in processor2.get_balances().items():
        print(f"  {currency}: {balance}")
    
    # Approach 3: Third-Party Forex Service
    print("\n\n=== APPROACH 3: THIRD-PARTY FOREX SERVICE ===")
    processor3 = ThirdPartyForexProcessor(
        settlement_currency="USD",
        forex_provider=static_provider
    )
    
    for amount, currency in payments:
        result = processor3.process_payment(amount, currency)
        print(f"\nPayment: {Money(amount, currency)}")
        print(f"Settlement: {result['settlement_amount']}")
        print(f"Conversion Applied: {result['conversion_applied']}")
        if result['conversion_applied']:
            print(f"Base Exchange Rate: {result.get('base_exchange_rate', 1.0)}")
            print(f"Forex Fee: {result['forex_fee_percentage']}%")


# Run the example
if __name__ == "__main__":
    run_example()