# Data processing module for stock market analytics

import pandas as pd
from typing import Dict


def calculate_metrics(df: pd.DataFrame) -> Dict:
    """
    Calculate key metrics from stock data.
    
    Args:
        df: DataFrame with stock data
        
    Returns:
        Dictionary containing calculated metrics
    """
    if df.empty:
        return {}
    
    latest = df.iloc[-1]
    first = df.iloc[0]
    
    current_price = latest['close']
    price_change = current_price - first['open']
    price_change_percent = (price_change / first['open']) * 100
    
    metrics = {
        'current_price': round(current_price, 2),
        'price_change': round(price_change, 2),
        'price_change_percent': round(price_change_percent, 2),
        'high': round(df['high'].max(), 2),
        'low': round(df['low'].min(), 2),
        'total_volume': int(df['volume'].sum()),
        'average_volume': int(df['volume'].mean()),
        'last_updated': df.index[-1]
    }
    
    return metrics


def prepare_chart_data(df: pd.DataFrame, chart_type: str) -> Dict:
    """
    Format data for specific chart types.
    
    Args:
        df: DataFrame with stock data
        chart_type: Type of chart ('price', 'volume', 'pie')
        
    Returns:
        Dictionary with chart-ready data
    """
    if df.empty:
        return {}
    
    if chart_type == 'price':
        return {
            'timestamps': df.index.tolist(),
            'open': df['open'].tolist(),
            'high': df['high'].tolist(),
            'low': df['low'].tolist(),
            'close': df['close'].tolist()
        }
    
    elif chart_type == 'volume':
        return {
            'timestamps': df.index.tolist(),
            'volume': df['volume'].tolist()
        }
    
    elif chart_type == 'pie':
        # Calculate distribution by trading session
        total_volume = df['volume'].sum()
        morning = df.between_time('09:30', '12:00')['volume'].sum() if not df.empty else 0
        afternoon = df.between_time('12:00', '16:00')['volume'].sum() if not df.empty else 0
        extended = total_volume - morning - afternoon
        
        return {
            'labels': ['Morning (9:30-12:00)', 'Afternoon (12:00-16:00)', 'Extended Hours'],
            'values': [morning, afternoon, extended]
        }
    
    return {}


def calculate_trends(df: pd.DataFrame) -> Dict:
    """
    Calculate trend indicators and moving averages.
    
    Args:
        df: DataFrame with stock data
        
    Returns:
        Dictionary with trend indicators
    """
    if df.empty or len(df) < 2:
        return {}
    
    # Calculate moving averages
    df['ma_5'] = df['close'].rolling(window=5).mean()
    df['ma_20'] = df['close'].rolling(window=20).mean()
    
    # Determine trend direction
    latest_price = df['close'].iloc[-1]
    ma_5_latest = df['ma_5'].iloc[-1] if not pd.isna(df['ma_5'].iloc[-1]) else latest_price
    ma_20_latest = df['ma_20'].iloc[-1] if not pd.isna(df['ma_20'].iloc[-1]) else latest_price
    
    trend = "Neutral"
    if latest_price > ma_5_latest > ma_20_latest:
        trend = "Strong Upward"
    elif latest_price > ma_5_latest:
        trend = "Upward"
    elif latest_price < ma_5_latest < ma_20_latest:
        trend = "Strong Downward"
    elif latest_price < ma_5_latest:
        trend = "Downward"
    
    return {
        'trend': trend,
        'ma_5': round(ma_5_latest, 2) if not pd.isna(ma_5_latest) else None,
        'ma_20': round(ma_20_latest, 2) if not pd.isna(ma_20_latest) else None,
        'volatility': round(df['close'].std(), 2)
    }
