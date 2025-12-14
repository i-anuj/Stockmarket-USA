# Demo Data Generator
# Provides fallback data when API is unavailable or rate limited

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_demo_stock_data(symbol: str, interval: str, days: int = 7) -> pd.DataFrame:
    """
    Generate realistic demo stock data for testing and fallback.
    
    Args:
        symbol: Stock symbol (e.g., 'IBM', 'AAPL')
        interval: Time interval ('1min', '5min', '15min', '30min', '60min')
        days: Number of days of historical data to generate
        
    Returns:
        DataFrame with columns: timestamp, open, high, low, close, volume
    """
    # Base prices for different stocks
    base_prices = {
        'IBM': 150.0,
        'AAPL': 175.0,
        'MSFT': 380.0,
        'GOOGL': 140.0,
        'AMZN': 145.0,
        'TSLA': 240.0,
        'META': 320.0
    }
    
    base_price = base_prices.get(symbol, 100.0)
    
    # Determine number of data points based on interval
    interval_minutes = {
        '1min': 1,
        '5min': 5,
        '15min': 15,
        '30min': 30,
        '60min': 60
    }
    
    minutes = interval_minutes.get(interval, 5)
    points_per_day = (6.5 * 60) // minutes  # Market hours: 9:30 AM - 4:00 PM EST
    total_points = int(points_per_day * days)
    
    # Generate timestamps (market hours only)
    end_time = datetime.now()
    timestamps = []
    current_time = end_time
    
    for _ in range(total_points):
        # Skip weekends
        while current_time.weekday() >= 5:  # Saturday = 5, Sunday = 6
            current_time -= timedelta(days=1)
        
        # Keep within market hours (9:30 AM - 4:00 PM)
        if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 30):
            current_time = current_time.replace(hour=16, minute=0)
            current_time -= timedelta(days=1)
        elif current_time.hour >= 16:
            current_time = current_time.replace(hour=16, minute=0)
        
        timestamps.append(current_time)
        current_time -= timedelta(minutes=minutes)
    
    timestamps.reverse()
    
    # Generate realistic price data with trends and volatility
    np.random.seed(hash(symbol) % 2**32)  # Consistent data for same symbol
    
    # Generate returns with trend and volatility
    trend = np.random.uniform(-0.0001, 0.0001)  # Slight upward or downward trend
    volatility = 0.002  # 0.2% volatility per interval
    
    returns = np.random.normal(trend, volatility, len(timestamps))
    
    # Calculate prices from returns
    prices = base_price * np.exp(np.cumsum(returns))
    
    # Generate OHLC data
    data = []
    for i, (timestamp, close) in enumerate(zip(timestamps, prices)):
        # Add some intraday volatility
        high_factor = 1 + abs(np.random.normal(0, 0.003))
        low_factor = 1 - abs(np.random.normal(0, 0.003))
        
        open_price = prices[i-1] if i > 0 else close
        high = max(open_price, close) * high_factor
        low = min(open_price, close) * low_factor
        
        # Generate volume (higher volume on larger price moves)
        price_change = abs(close - open_price) / open_price
        base_volume = np.random.uniform(1_000_000, 5_000_000)
        volume = int(base_volume * (1 + price_change * 10))
        
        data.append({
            'timestamp': timestamp,
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': volume
        })
    
    df = pd.DataFrame(data)
    df.set_index('timestamp', inplace=True)
    
    return df


def get_demo_data_message(symbol: str) -> str:
    """
    Get a user-friendly message explaining demo data is being used.
    
    Args:
        symbol: Stock symbol
        
    Returns:
        Message string
    """
    return f"""
    ⚠️ **Using Demo Data for {symbol}**
    
    The Alpha Vantage API is currently unavailable (rate limit or connection issue).
    Displaying simulated data for demonstration purposes.
    
    **This is not real market data!**
    """
