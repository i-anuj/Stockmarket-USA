# Configuration module for Stock Market Dashboard

import os

# API Configuration
# Read from environment variable with fallback to default for local development
ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY", "GQ8W5E5K39KHNY8K")
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# Supported Stock Symbols
SUPPORTED_SYMBOLS = ["IBM", "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META"]

# Time Intervals
TIME_INTERVALS = ["1min", "5min", "15min", "30min", "60min"]

# Server Configuration
PORT = 8080

# Color Theme
COLORS = {
    "primary": "#8B5CF6",      # Purple
    "secondary": "#FBBF24",    # Yellow
    "accent": "#F97316",       # Orange
    "background": "rgba(255, 255, 255, 0.1)",
    "text": "#FFFFFF"
}

# Glass Morphism CSS Properties
GLASS_EFFECT = {
    "background": "rgba(255, 255, 255, 0.1)",
    "backdrop_filter": "blur(10px)",
    "border": "1px solid rgba(255, 255, 255, 0.2)",
    "border_radius": "15px",
    "box_shadow": "0 8px 32px 0 rgba(31, 38, 135, 0.37)"
}
