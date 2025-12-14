# Technical Indicators Module for Stock Market Analytics

import pandas as pd
from typing import Dict, Tuple


def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Relative Strength Index (RSI).
    
    Args:
        df: DataFrame with stock data
        period: RSI period (default: 14)
        
    Returns:
        Series with RSI values (0-100)
    """
    if df.empty or len(df) < period:
        return pd.Series()
    
    # Calculate price changes
    delta = df['close'].diff()
    
    # Separate gains and losses
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    
    # Calculate RS and RSI
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict:
    """
    Calculate MACD (Moving Average Convergence Divergence).
    
    Args:
        df: DataFrame with stock data
        fast: Fast EMA period (default: 12)
        slow: Slow EMA period (default: 26)
        signal: Signal line period (default: 9)
        
    Returns:
        Dictionary with macd, signal, and histogram values
    """
    if df.empty or len(df) < slow:
        return {'macd': 0, 'signal': 0, 'histogram': 0}
    
    # Calculate EMAs
    exp1 = df['close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['close'].ewm(span=slow, adjust=False).mean()
    
    # Calculate MACD line
    macd_line = exp1 - exp2
    
    # Calculate signal line
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    
    # Calculate histogram
    histogram = macd_line - signal_line
    
    return {
        'macd': round(macd_line.iloc[-1], 4) if not pd.isna(macd_line.iloc[-1]) else 0,
        'signal': round(signal_line.iloc[-1], 4) if not pd.isna(signal_line.iloc[-1]) else 0,
        'histogram': round(histogram.iloc[-1], 4) if not pd.isna(histogram.iloc[-1]) else 0,
        'macd_series': macd_line,
        'signal_series': signal_line,
        'histogram_series': histogram
    }


def calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> Dict:
    """
    Calculate Bollinger Bands.
    
    Args:
        df: DataFrame with stock data
        period: SMA period (default: 20)
        std_dev: Number of standard deviations (default: 2)
        
    Returns:
        Dictionary with upper, middle, and lower band values
    """
    if df.empty or len(df) < period:
        return {'upper': 0, 'middle': 0, 'lower': 0}
    
    # Calculate middle band (SMA)
    sma = df['close'].rolling(window=period).mean()
    
    # Calculate standard deviation
    std = df['close'].rolling(window=period).std()
    
    # Calculate upper and lower bands
    upper_band = sma + (std * std_dev)
    lower_band = sma - (std * std_dev)
    
    return {
        'upper': round(upper_band.iloc[-1], 2) if not pd.isna(upper_band.iloc[-1]) else 0,
        'middle': round(sma.iloc[-1], 2) if not pd.isna(sma.iloc[-1]) else 0,
        'lower': round(lower_band.iloc[-1], 2) if not pd.isna(lower_band.iloc[-1]) else 0,
        'upper_series': upper_band,
        'middle_series': sma,
        'lower_series': lower_band
    }


def calculate_moving_averages(df: pd.DataFrame) -> Dict:
    """
    Calculate Simple Moving Averages for multiple periods.
    
    Args:
        df: DataFrame with stock data
        
    Returns:
        Dictionary with SMA values for 5, 20, 50, 200 periods
    """
    if df.empty:
        return {'ma_5': 0, 'ma_20': 0, 'ma_50': 0, 'ma_200': 0}
    
    result = {}
    periods = [5, 20, 50, 200]
    
    for period in periods:
        if len(df) >= period:
            ma = df['close'].rolling(window=period).mean()
            result[f'ma_{period}'] = round(ma.iloc[-1], 2) if not pd.isna(ma.iloc[-1]) else 0
            result[f'ma_{period}_series'] = ma
        else:
            result[f'ma_{period}'] = 0
            result[f'ma_{period}_series'] = pd.Series()
    
    return result


def get_rsi_signal(rsi_value: float) -> str:
    """
    Get RSI signal based on value.
    
    Args:
        rsi_value: RSI value (0-100)
        
    Returns:
        Signal string: 'Overbought', 'Oversold', or 'Neutral'
    """
    if rsi_value >= 70:
        return 'Overbought'
    elif rsi_value <= 30:
        return 'Oversold'
    else:
        return 'Neutral'


def generate_signals(df: pd.DataFrame, current_price: float) -> Dict:
    """
    Generate overall BUY/SELL/HOLD signal based on all indicators.
    
    Args:
        df: DataFrame with stock data
        current_price: Current stock price
        
    Returns:
        Dictionary with overall signal and confidence score
    """
    if df.empty or len(df) < 20:
        return {
            'signal': 'HOLD',
            'confidence': 5,
            'reasoning': 'Insufficient data for analysis'
        }
    
    signals = []
    reasoning_parts = []
    
    # RSI Signal
    rsi = calculate_rsi(df)
    if not rsi.empty:
        rsi_value = rsi.iloc[-1]
        if rsi_value < 30:
            signals.append(('BUY', 2))
            reasoning_parts.append(f"RSI oversold ({rsi_value:.1f})")
        elif rsi_value > 70:
            signals.append(('SELL', 2))
            reasoning_parts.append(f"RSI overbought ({rsi_value:.1f})")
        else:
            signals.append(('HOLD', 1))
    
    # MACD Signal
    macd_data = calculate_macd(df)
    if macd_data['macd'] != 0:
        if macd_data['macd'] > macd_data['signal']:
            signals.append(('BUY', 1))
            reasoning_parts.append("MACD bullish crossover")
        else:
            signals.append(('SELL', 1))
            reasoning_parts.append("MACD bearish crossover")
    
    # Bollinger Bands Signal
    bb_data = calculate_bollinger_bands(df)
    if bb_data['lower'] != 0:
        if current_price <= bb_data['lower']:
            signals.append(('BUY', 1))
            reasoning_parts.append("Price at lower BB")
        elif current_price >= bb_data['upper']:
            signals.append(('SELL', 1))
            reasoning_parts.append("Price at upper BB")
    
    # Moving Average Signal
    ma_data = calculate_moving_averages(df)
    if ma_data['ma_20'] != 0 and ma_data['ma_50'] != 0:
        if ma_data['ma_20'] > ma_data['ma_50'] and current_price > ma_data['ma_20']:
            signals.append(('BUY', 1))
            reasoning_parts.append("Price above MAs")
        elif ma_data['ma_20'] < ma_data['ma_50'] and current_price < ma_data['ma_20']:
            signals.append(('SELL', 1))
            reasoning_parts.append("Price below MAs")
    
    # Calculate weighted signal
    buy_score = sum(weight for signal, weight in signals if signal == 'BUY')
    sell_score = sum(weight for signal, weight in signals if signal == 'SELL')
    hold_score = sum(weight for signal, weight in signals if signal == 'HOLD')
    
    # Determine overall signal
    if buy_score > sell_score and buy_score > hold_score:
        overall_signal = 'BUY'
        confidence = min(buy_score * 2, 10)
    elif sell_score > buy_score and sell_score > hold_score:
        overall_signal = 'SELL'
        confidence = min(sell_score * 2, 10)
    else:
        overall_signal = 'HOLD'
        confidence = 5
    
    reasoning = '; '.join(reasoning_parts) if reasoning_parts else 'Mixed signals'
    
    return {
        'signal': overall_signal,
        'confidence': confidence,
        'reasoning': reasoning
    }


def calculate_all_indicators(df: pd.DataFrame, current_price: float) -> Dict:
    """
    Calculate all technical indicators at once.
    
    Args:
        df: DataFrame with stock data
        current_price: Current stock price
        
    Returns:
        Dictionary with all indicator values
    """
    if df.empty:
        return {}
    
    # Calculate RSI
    rsi_series = calculate_rsi(df)
    rsi_value = rsi_series.iloc[-1] if not rsi_series.empty else 0
    
    # Calculate MACD
    macd_data = calculate_macd(df)
    
    # Calculate Bollinger Bands
    bb_data = calculate_bollinger_bands(df)
    
    # Calculate Moving Averages
    ma_data = calculate_moving_averages(df)
    
    # Generate overall signal
    signal_data = generate_signals(df, current_price)
    
    return {
        'rsi': round(rsi_value, 2) if not pd.isna(rsi_value) else 0,
        'rsi_signal': get_rsi_signal(rsi_value) if not pd.isna(rsi_value) else 'N/A',
        'macd': macd_data['macd'],
        'macd_signal': macd_data['signal'],
        'macd_histogram': macd_data['histogram'],
        'bb_upper': bb_data['upper'],
        'bb_middle': bb_data['middle'],
        'bb_lower': bb_data['lower'],
        'ma_5': ma_data['ma_5'],
        'ma_20': ma_data['ma_20'],
        'ma_50': ma_data['ma_50'],
        'ma_200': ma_data['ma_200'],
        'overall_signal': signal_data['signal'],
        'signal_confidence': signal_data['confidence'],
        'signal_reasoning': signal_data['reasoning']
    }
