# Chart components module using Plotly

import plotly.graph_objects as go
import pandas as pd
from src import config


def create_price_chart(df: pd.DataFrame) -> go.Figure:
    """
    Create line chart showing stock price trends.
    
    Args:
        df: DataFrame with stock data
        
    Returns:
        Plotly Figure object
    """
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df.index,
        y=df['close'],
        mode='lines',
        name='Close Price',
        line=dict(color=config.COLORS['primary'], width=2)
    ))
    
    fig.update_layout(
        title='Stock Price Trend',
        xaxis_title='Time',
        yaxis_title='Price ($)',
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=config.COLORS['text']),
        hovermode='x unified'
    )
    
    return fig


def create_volume_chart(df: pd.DataFrame) -> go.Figure:
    """
    Create bar chart showing trading volume over time.
    
    Args:
        df: DataFrame with stock data
        
    Returns:
        Plotly Figure object
    """
    colors = [config.COLORS['secondary'] if v > df['volume'].mean() 
              else config.COLORS['accent'] for v in df['volume']]
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df.index,
        y=df['volume'],
        name='Volume',
        marker=dict(color=colors)
    ))
    
    fig.update_layout(
        title='Trading Volume Over Time',
        xaxis_title='Time',
        yaxis_title='Volume',
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=config.COLORS['text']),
        showlegend=False
    )
    
    return fig


def create_pie_chart(data: dict) -> go.Figure:
    """
    Create pie chart for data distribution.
    
    Args:
        data: Dictionary with 'labels' and 'values' keys
        
    Returns:
        Plotly Figure object
    """
    fig = go.Figure()
    
    fig.add_trace(go.Pie(
        labels=data.get('labels', []),
        values=data.get('values', []),
        marker=dict(colors=[config.COLORS['primary'], config.COLORS['secondary'], config.COLORS['accent']]),
        textfont=dict(color=config.COLORS['text'])
    ))
    
    fig.update_layout(
        title='Volume Distribution by Trading Session',
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=config.COLORS['text'])
    )
    
    return fig


def create_candlestick_chart(df: pd.DataFrame) -> go.Figure:
    """
    Create candlestick chart for OHLC data.
    
    Args:
        df: DataFrame with stock data
        
    Returns:
        Plotly Figure object
    """
    fig = go.Figure()
    
    fig.add_trace(go.Candlestick(
        x=df.index,
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='OHLC',
        increasing_line_color=config.COLORS['secondary'],
        decreasing_line_color=config.COLORS['accent']
    ))
    
    fig.update_layout(
        title='Candlestick Chart',
        xaxis_title='Time',
        yaxis_title='Price ($)',
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=config.COLORS['text']),
        xaxis_rangeslider_visible=False
    )
    
    return fig


def create_rsi_chart(df: pd.DataFrame, rsi_series: pd.Series) -> go.Figure:
    """
    Create RSI chart with overbought/oversold zones.
    
    Args:
        df: DataFrame with stock data
        rsi_series: RSI values series
        
    Returns:
        Plotly Figure object
    """
    fig = go.Figure()
    
    # RSI line
    fig.add_trace(go.Scatter(
        x=df.index,
        y=rsi_series,
        mode='lines',
        name='RSI',
        line=dict(color=config.COLORS['primary'], width=2)
    ))
    
    # Overbought line (70)
    fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Overbought (70)")
    
    # Oversold line (30)
    fig.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Oversold (30)")
    
    # Middle line (50)
    fig.add_hline(y=50, line_dash="dot", line_color="gray", annotation_text="Neutral (50)")
    
    fig.update_layout(
        title='RSI (Relative Strength Index)',
        xaxis_title='Time',
        yaxis_title='RSI',
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=config.COLORS['text']),
        yaxis=dict(range=[0, 100])
    )
    
    return fig


def create_macd_chart(df: pd.DataFrame, macd_data: dict) -> go.Figure:
    """
    Create MACD chart with signal line and histogram.
    
    Args:
        df: DataFrame with stock data
        macd_data: Dictionary with MACD values
        
    Returns:
        Plotly Figure object
    """
    fig = go.Figure()
    
    # MACD line
    fig.add_trace(go.Scatter(
        x=df.index,
        y=macd_data['macd_series'],
        mode='lines',
        name='MACD',
        line=dict(color=config.COLORS['primary'], width=2)
    ))
    
    # Signal line
    fig.add_trace(go.Scatter(
        x=df.index,
        y=macd_data['signal_series'],
        mode='lines',
        name='Signal',
        line=dict(color=config.COLORS['secondary'], width=2)
    ))
    
    # Histogram
    colors = [config.COLORS.get('success', '#10B981') if val >= 0 else config.COLORS.get('danger', '#EF4444') 
              for val in macd_data['histogram_series']]
    
    fig.add_trace(go.Bar(
        x=df.index,
        y=macd_data['histogram_series'],
        name='Histogram',
        marker=dict(color=colors)
    ))
    
    fig.update_layout(
        title='MACD (Moving Average Convergence Divergence)',
        xaxis_title='Time',
        yaxis_title='MACD',
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=config.COLORS['text'])
    )
    
    return fig


def create_price_chart_with_bb(df: pd.DataFrame, bb_data: dict) -> go.Figure:
    """
    Create price chart with Bollinger Bands overlay.
    
    Args:
        df: DataFrame with stock data
        bb_data: Dictionary with Bollinger Bands values
        
    Returns:
        Plotly Figure object
    """
    fig = go.Figure()
    
    # Price line
    fig.add_trace(go.Scatter(
        x=df.index,
        y=df['close'],
        mode='lines',
        name='Price',
        line=dict(color='white', width=2)
    ))
    
    # Upper band
    fig.add_trace(go.Scatter(
        x=df.index,
        y=bb_data['upper_series'],
        mode='lines',
        name='Upper BB',
        line=dict(color='red', width=1, dash='dash')
    ))
    
    # Middle band (SMA)
    fig.add_trace(go.Scatter(
        x=df.index,
        y=bb_data['middle_series'],
        mode='lines',
        name='Middle BB (SMA)',
        line=dict(color=config.COLORS['secondary'], width=1)
    ))
    
    # Lower band
    fig.add_trace(go.Scatter(
        x=df.index,
        y=bb_data['lower_series'],
        mode='lines',
        name='Lower BB',
        line=dict(color='green', width=1, dash='dash'),
        fill='tonexty',
        fillcolor='rgba(139, 92, 246, 0.1)'
    ))
    
    fig.update_layout(
        title='Price with Bollinger Bands',
        xaxis_title='Time',
        yaxis_title='Price ($)',
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=config.COLORS['text']),
        hovermode='x unified'
    )
    
    return fig


def create_price_chart_with_ma(df: pd.DataFrame, ma_data: dict) -> go.Figure:
    """
    Create price chart with Moving Averages overlay.
    
    Args:
        df: DataFrame with stock data
        ma_data: Dictionary with Moving Average values
        
    Returns:
        Plotly Figure object
    """
    fig = go.Figure()
    
    # Price line
    fig.add_trace(go.Scatter(
        x=df.index,
        y=df['close'],
        mode='lines',
        name='Price',
        line=dict(color='white', width=2)
    ))
    
    # MA lines
    ma_colors = {
        'ma_5': '#10B981',
        'ma_20': '#3B82F6',
        'ma_50': '#F59E0B',
        'ma_200': '#EF4444'
    }
    
    for ma_key, color in ma_colors.items():
        if f'{ma_key}_series' in ma_data and not ma_data[f'{ma_key}_series'].empty:
            period = ma_key.split('_')[1]
            fig.add_trace(go.Scatter(
                x=df.index,
                y=ma_data[f'{ma_key}_series'],
                mode='lines',
                name=f'MA({period})',
                line=dict(color=color, width=1)
            ))
    
    fig.update_layout(
        title='Price with Moving Averages',
        xaxis_title='Time',
        yaxis_title='Price ($)',
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=config.COLORS['text']),
        hovermode='x unified'
    )
    
    return fig
