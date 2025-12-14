# UI components module for Streamlit dashboard

import streamlit as st
from src import config


def apply_custom_css():
    """
    Apply custom CSS for glass morphism effects and color theme.
    """
    css = f"""
    <style>
        /* Main background gradient */
        .stApp {{
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        }}
        
        /* Glass morphism cards */
        .metric-card {{
            background: {config.GLASS_EFFECT['background']};
            backdrop-filter: {config.GLASS_EFFECT['backdrop_filter']};
            -webkit-backdrop-filter: {config.GLASS_EFFECT['backdrop_filter']};
            border: {config.GLASS_EFFECT['border']};
            border-radius: {config.GLASS_EFFECT['border_radius']};
            box-shadow: {config.GLASS_EFFECT['box_shadow']};
            padding: 20px;
            margin: 10px 0;
        }}
        
        /* Text styling */
        h1, h2, h3, h4, h5, h6 {{
            color: {config.COLORS['text']} !important;
            font-weight: 700 !important;
        }}
        
        p, span, div {{
            color: {config.COLORS['text']} !important;
        }}
        
        /* Metric values */
        .metric-value {{
            font-size: 2rem;
            font-weight: bold;
            color: {config.COLORS['secondary']};
        }}
        
        .metric-positive {{
            color: {config.COLORS['secondary']};
        }}
        
        .metric-negative {{
            color: {config.COLORS['accent']};
        }}
        
        /* Sidebar styling */
        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, #1a1a2e 0%, #16213e 100%);
        }}
        
        /* Button styling */
        .stButton > button {{
            background: {config.COLORS['primary']};
            color: {config.COLORS['text']};
            border: none;
            border-radius: 10px;
            padding: 10px 20px;
            font-weight: 600;
        }}
        
        .stButton > button:hover {{
            background: {config.COLORS['secondary']};
            color: #000;
        }}
        
        /* Select box styling */
        .stSelectbox {{
            color: {config.COLORS['text']};
        }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def render_metric_card(title: str, value: str, delta: str = None):
    """
    Display metric in styled card with glass morphism effect.
    
    Args:
        title: Metric title
        value: Metric value
        delta: Optional change indicator
    """
    delta_class = ""
    if delta:
        if delta.startswith("+") or (delta.replace(".", "").replace("%", "").replace("+", "").replace("-", "").isdigit() and float(delta.replace("%", "")) > 0):
            delta_class = "metric-positive"
        else:
            delta_class = "metric-negative"
    
    st.metric(label=title, value=value, delta=delta)


def render_stock_selector(symbols: list) -> str:
    """
    Display dropdown for stock symbol selection.
    
    Args:
        symbols: List of stock symbols
        
    Returns:
        Selected symbol
    """
    selected = st.selectbox(
        "Select Stock Symbol",
        symbols,
        index=0,
        help="Choose a stock symbol to view its data"
    )
    return selected


def render_interval_selector() -> str:
    """
    Display dropdown for time interval selection.
    
    Returns:
        Selected interval
    """
    selected = st.selectbox(
        "Select Time Interval",
        config.TIME_INTERVALS,
        index=1,  # Default to 5min
        help="Choose the time interval for intraday data"
    )
    return selected


def render_loading_skeleton():
    """
    Display animated loading skeleton while data is being fetched.
    """
    skeleton_css = """
    <style>
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        .skeleton {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 10px;
            animation: pulse 1.5s ease-in-out infinite;
        }
        .skeleton-metric {
            height: 100px;
            margin: 10px 0;
        }
        .skeleton-chart {
            height: 400px;
            margin: 20px 0;
        }
    </style>
    """
    
    st.markdown(skeleton_css, unsafe_allow_html=True)
    
    # Metrics skeleton
    cols = st.columns(4)
    for col in cols:
        with col:
            st.markdown('<div class="skeleton skeleton-metric"></div>', unsafe_allow_html=True)
    
    # Chart skeleton
    st.markdown('<div class="skeleton skeleton-chart"></div>', unsafe_allow_html=True)


def render_toast_notification(message: str, type: str = "info"):
    """
    Display toast notification for user feedback.
    
    Args:
        message: Notification message
        type: Notification type ('success', 'error', 'warning', 'info')
    """
    colors = {
        'success': '#10B981',
        'error': '#EF4444',
        'warning': '#F59E0B',
        'info': '#3B82F6'
    }
    
    icons = {
        'success': '✅',
        'error': '❌',
        'warning': '⚠️',
        'info': 'ℹ️'
    }
    
    color = colors.get(type, colors['info'])
    icon = icons.get(type, icons['info'])
    
    if type == 'success':
        st.success(f"{icon} {message}")
    elif type == 'error':
        st.error(f"{icon} {message}")
    elif type == 'warning':
        st.warning(f"{icon} {message}")
    else:
        st.info(f"{icon} {message}")


def render_tooltip(text: str, tooltip: str):
    """
    Render text with tooltip on hover.
    
    Args:
        text: Display text
        tooltip: Tooltip content
    """
    st.markdown(f'<span title="{tooltip}">{text} ℹ️</span>', unsafe_allow_html=True)


def render_price_change_indicator(current_price: float, previous_price: float):
    """
    Render price change with color coding and arrow.
    
    Args:
        current_price: Current stock price
        previous_price: Previous stock price
    """
    if previous_price == 0:
        return
    
    change = current_price - previous_price
    change_percent = (change / previous_price) * 100
    
    if change > 0:
        color = config.COLORS.get('success', '#10B981')
        arrow = '↑'
        st.markdown(
            f'<div style="color: {color}; font-size: 1.2rem; font-weight: bold;">'
            f'{arrow} +${change:.2f} (+{change_percent:.2f}%)</div>',
            unsafe_allow_html=True
        )
    elif change < 0:
        color = config.COLORS.get('danger', '#EF4444')
        arrow = '↓'
        st.markdown(
            f'<div style="color: {color}; font-size: 1.2rem; font-weight: bold;">'
            f'{arrow} ${change:.2f} ({change_percent:.2f}%)</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div style="color: #9CA3AF; font-size: 1.2rem;">→ No change</div>',
            unsafe_allow_html=True
        )


def render_keyboard_shortcuts_legend():
    """Display keyboard shortcuts legend."""
    shortcuts = """
    <div style="background: rgba(255,255,255,0.1); padding: 15px; border-radius: 10px; margin: 10px 0;">
        <h4>⌨️ Keyboard Shortcuts</h4>
        <ul style="list-style: none; padding: 0;">
            <li><strong>R</strong> - Refresh data</li>
            <li><strong>W</strong> - Toggle watchlist</li>
            <li><strong>1-7</strong> - Quick select stock</li>
            <li><strong>Space</strong> - Pause/Resume auto-refresh</li>
            <li><strong>?</strong> - Show this help</li>
        </ul>
    </div>
    """
    st.markdown(shortcuts, unsafe_allow_html=True)


def render_loading_spinner(text: str = "Loading..."):
    """
    Display loading spinner with text.
    
    Args:
        text: Loading message
    """
    with st.spinner(text):
        pass


def add_smooth_transitions():
    """Add CSS for smooth transitions."""
    transition_css = """
    <style>
        /* Smooth transitions */
        .stMetric, .stButton, .stSelectbox {
            transition: all 0.3s ease-in-out;
        }
        
        /* Fade in animation */
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .fade-in {
            animation: fadeIn 0.5s ease-in-out;
        }
        
        /* Price flash animation */
        @keyframes priceFlashGreen {
            0% { background-color: rgba(16, 185, 129, 0); }
            50% { background-color: rgba(16, 185, 129, 0.3); }
            100% { background-color: rgba(16, 185, 129, 0); }
        }
        
        @keyframes priceFlashRed {
            0% { background-color: rgba(239, 68, 68, 0); }
            50% { background-color: rgba(239, 68, 68, 0.3); }
            100% { background-color: rgba(239, 68, 68, 0); }
        }
        
        .price-up {
            animation: priceFlashGreen 1s ease-in-out;
        }
        
        .price-down {
            animation: priceFlashRed 1s ease-in-out;
        }
    </style>
    """
    st.markdown(transition_css, unsafe_allow_html=True)
