import pytest
import pandas as pd
from src.core import technical_indicators


class TestTechnicalIndicators:
    """Test cases for technical indicators module."""
    
    def setup_method(self):
        """Set up test data."""
        # Create sample stock data
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        self.df = pd.DataFrame({
            'open': [100 + i * 0.1 for i in range(100)],
            'high': [101 + i * 0.1 for i in range(100)],
            'low': [99 + i * 0.1 for i in range(100)],
            'close': [100.5 + i * 0.1 for i in range(100)],
            'volume': [1000 + i * 10 for i in range(100)]
        }, index=dates)
    
    def test_calculate_rsi(self):
        """Test RSI calculation."""
        rsi = technical_indicators.calculate_rsi(self.df)
        assert not rsi.empty
        assert all(0 <= val <= 100 for val in rsi.dropna())
    
    def test_calculate_macd(self):
        """Test MACD calculation."""
        macd_data = technical_indicators.calculate_macd(self.df)
        assert 'macd' in macd_data
        assert 'signal' in macd_data
        assert 'histogram' in macd_data
    
    def test_calculate_bollinger_bands(self):
        """Test Bollinger Bands calculation."""
        bb_data = technical_indicators.calculate_bollinger_bands(self.df)
        assert 'upper' in bb_data
        assert 'middle' in bb_data
        assert 'lower' in bb_data
    
    def test_calculate_moving_averages(self):
        """Test moving averages calculation."""
        ma_data = technical_indicators.calculate_moving_averages(self.df)
        assert 'ma_5' in ma_data
        assert 'ma_20' in ma_data
        assert 'ma_50' in ma_data
        assert 'ma_200' in ma_data
    
    def test_generate_signals(self):
        """Test signal generation."""
        signals = technical_indicators.generate_signals(self.df, 110.0)
        assert 'signal' in signals
        assert signals['signal'] in ['BUY', 'SELL', 'HOLD']
        assert 'confidence' in signals
        assert 1 <= signals['confidence'] <= 10
