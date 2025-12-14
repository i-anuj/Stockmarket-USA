# Design Document

## Overview

The Stock Market Analytics Dashboard is a Python Streamlit web application that provides real-time stock market data visualization and analysis. The application fetches data from the Alpha Vantage API and presents it through an interactive interface featuring modern glass morphism UI design with purple, yellow, and orange color themes.

The application follows a simple architecture pattern with no database dependency, focusing on API aggregation and real-time data visualization. It runs on port 8080 within a Python virtual environment.

## Architecture

### High-Level Architecture

```
┌─────────────────┐
│   User Browser  │
└────────┬────────┘
         │ HTTP (Port 8080)
         ▼
┌─────────────────────────────────┐
│   Streamlit Application         │
│  ┌──────────────────────────┐   │
│  │  UI Layer (Pages)        │   │
│  │  - Stock Selector        │   │
│  │  - Metrics Display       │   │
│  │  - Chart Components      │   │
│  └──────────┬───────────────┘   │
│             │                    │
│  ┌──────────▼───────────────┐   │
│  │  Business Logic Layer    │   │
│  │  - Data Processing       │   │
│  │  - Metrics Calculation   │   │
│  │  - Chart Data Prep       │   │
│  └──────────┬───────────────┘   │
│             │                    │
│  ┌──────────▼───────────────┐   │
│  │  API Service Layer       │   │
│  │  - Alpha Vantage Client  │   │
│  │  - Response Caching      │   │
│  │  - Error Handling        │   │
│  └──────────┬───────────────┘   │
└─────────────┼───────────────────┘
              │ HTTPS
              ▼
┌─────────────────────────────────┐
│   Alpha Vantage API             │
│   (External Service)            │
└─────────────────────────────────┘
```

### Technology Stack

- **Frontend Framework**: Streamlit (Python-based web framework)
- **Data Visualization**: Plotly (for interactive charts)
- **HTTP Client**: requests library
- **Data Processing**: pandas
- **Technical Analysis**: pandas-ta (for indicators)
- **Runtime**: Python 3.9+
- **Environment**: Virtual environment (venv)
- **State Management**: Streamlit session state

## Components and Interfaces

### 1. API Service Module (`api_service.py`)

**Purpose**: Handles all interactions with the Alpha Vantage API

**Key Functions**:
- `fetch_intraday_data(symbol: str, interval: str, api_key: str) -> dict`
  - Fetches intraday time series data for a given stock symbol
  - Parameters: stock symbol, time interval (1min, 5min, 15min, 30min, 60min)
  - Returns: JSON response containing OHLCV data
  - Handles API errors and rate limiting

- `parse_time_series(response: dict) -> pd.DataFrame`
  - Converts API response to pandas DataFrame
  - Extracts timestamp, open, high, low, close, volume
  - Returns structured DataFrame for analysis

**API Endpoint Structure**:
```
https://www.alphavantage.co/query?
  function=TIME_SERIES_INTRADAY
  &symbol={SYMBOL}
  &interval={INTERVAL}
  &outputsize=full
  &apikey={API_KEY}
```

**Response Format** (from Alpha Vantage):
```json
{
  "Meta Data": {
    "1. Information": "Intraday (5min) open, high, low, close prices and volume",
    "2. Symbol": "IBM",
    "3. Last Refreshed": "2024-11-15 20:00:00",
    "4. Interval": "5min",
    "5. Output Size": "Full size",
    "6. Time Zone": "US/Eastern"
  },
  "Time Series (5min)": {
    "2024-11-15 20:00:00": {
      "1. open": "215.5000",
      "2. high": "215.7500",
      "3. low": "215.4000",
      "4. close": "215.6000",
      "5. volume": "12345"
    }
  }
}
```

### 2. Data Processing Module (`data_processor.py`)

**Purpose**: Transforms raw API data into visualization-ready formats

**Key Functions**:
- `calculate_metrics(df: pd.DataFrame) -> dict`
  - Calculates current price, price change, percentage change
  - Computes high, low, average volume
  - Returns dictionary of key metrics

- `prepare_chart_data(df: pd.DataFrame, chart_type: str) -> dict`
  - Formats data for specific chart types
  - Handles time series aggregation
  - Returns chart-ready data structures

- `calculate_trends(df: pd.DataFrame) -> dict`
  - Identifies upward/downward trends
  - Calculates moving averages
  - Returns trend indicators

### 3. UI Components Module (`ui_components.py`)

**Purpose**: Reusable UI components with glass morphism styling

**Key Functions**:
- `render_metric_card(title: str, value: str, delta: str = None)`
  - Displays metric in styled card with glass morphism effect
  - Shows title, value, and optional change indicator

- `apply_custom_css()`
  - Injects custom CSS for glass morphism effects
  - Applies purple, yellow, orange color theme
  - Ensures text visibility

- `render_stock_selector(symbols: list) -> str`
  - Displays dropdown for stock symbol selection
  - Returns selected symbol

- `render_interval_selector() -> str`
  - Displays dropdown for time interval selection
  - Returns selected interval (1min, 5min, 15min, 30min, 60min)

### 4. Chart Components Module (`charts.py`)

**Purpose**: Creates interactive visualizations using Plotly

**Key Functions**:
- `create_price_chart(df: pd.DataFrame) -> plotly.graph_objects.Figure`
  - Line chart showing stock price over time
  - Displays open, high, low, close prices
  - Styled with theme colors

- `create_volume_chart(df: pd.DataFrame) -> plotly.graph_objects.Figure`
  - Bar chart showing trading volume over time
  - Color-coded by volume intensity

- `create_pie_chart(data: dict) -> plotly.graph_objects.Figure`
  - Pie chart for data distribution (e.g., trading session breakdown)
  - Uses theme colors for segments

- `create_candlestick_chart(df: pd.DataFrame) -> plotly.graph_objects.Figure`
  - Candlestick chart for OHLC data visualization
  - Shows price movements with traditional candlestick patterns

### 5. Main Application (`app.py`)

**Purpose**: Entry point and orchestration of the Streamlit application

**Structure**:
```python
# Configuration
st.set_page_config(page_title="Stock Market Dashboard", layout="wide")

# Session state initialization
if 'cached_data' not in st.session_state:
    st.session_state.cached_data = {}

# UI Layout
- Header with title and logo
- Sidebar with stock selector and interval selector
- Main content area:
  - Metrics row (4 columns: current price, change %, high, low)
  - Charts section:
    - Price trend chart (full width)
    - Volume chart (half width) | Pie chart (half width)
  - Additional metrics section
```

### 6. Configuration Module (`config.py`)

**Purpose**: Centralized configuration management

**Contents**:
- API key storage
- Supported stock symbols list
- Available time intervals
- UI theme colors
- Port configuration
- Auto-refresh settings

### 7. Technical Indicators Module (`technical_indicators.py`)

**Purpose**: Calculate advanced technical analysis indicators

**Key Functions**:
- `calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series`
  - Calculates Relative Strength Index
  - Returns RSI values (0-100 scale)
  - Identifies overbought (>70) and oversold (<30) conditions

- `calculate_macd(df: pd.DataFrame) -> dict`
  - Calculates MACD line, signal line, and histogram
  - Uses 12, 26, 9 period defaults
  - Returns dict with macd, signal, histogram values

- `calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, std: int = 2) -> dict`
  - Calculates upper, middle, and lower bands
  - Returns dict with upper_band, middle_band, lower_band

- `calculate_moving_averages(df: pd.DataFrame) -> dict`
  - Calculates SMA for 5, 20, 50, 200 periods
  - Returns dict with ma_5, ma_20, ma_50, ma_200

- `generate_signals(df: pd.DataFrame, indicators: dict) -> str`
  - Analyzes all indicators
  - Returns "BUY", "SELL", or "HOLD" signal
  - Provides reasoning for the signal

### 8. Watchlist Manager Module (`watchlist_manager.py`)

**Purpose**: Manage user's stock watchlist

**Key Functions**:
- `add_to_watchlist(symbol: str) -> bool`
  - Adds stock to session state watchlist
  - Prevents duplicates
  - Returns success status

- `remove_from_watchlist(symbol: str) -> bool`
  - Removes stock from watchlist
  - Returns success status

- `get_watchlist() -> list`
  - Returns current watchlist
  - Sorted alphabetically

- `is_in_watchlist(symbol: str) -> bool`
  - Checks if symbol is in watchlist

### 9. Auto-Refresh Manager Module (`refresh_manager.py`)

**Purpose**: Handle automatic data refresh functionality

**Key Functions**:
- `initialize_refresh_state()`
  - Sets up session state for auto-refresh
  - Default: enabled, 60-second interval

- `toggle_refresh() -> bool`
  - Toggles auto-refresh on/off
  - Returns new state

- `get_refresh_interval() -> int`
  - Returns current refresh interval in seconds

- `set_refresh_interval(seconds: int)`
  - Updates refresh interval
  - Validates input (min 10s, max 300s)

- `should_refresh() -> bool`
  - Checks if data should be refreshed
  - Based on last refresh time and interval

- `get_countdown() -> int`
  - Returns seconds until next refresh

## Data Models

### StockData (DataFrame Schema)

```python
{
  'timestamp': datetime,      # Trading timestamp
  'open': float,             # Opening price
  'high': float,             # Highest price
  'low': float,              # Lowest price
  'close': float,            # Closing price
  'volume': int              # Trading volume
}
```

### MetricsData (Dictionary)

```python
{
  'current_price': float,
  'price_change': float,
  'price_change_percent': float,
  'high': float,
  'low': float,
  'total_volume': int,
  'average_volume': float,
  'last_updated': datetime
}
```

### ChartConfig (Dictionary)

```python
{
  'colors': {
    'primary': '#8B5CF6',      # Purple
    'secondary': '#FBBF24',    # Yellow
    'accent': '#F97316',       # Orange
    'background': 'rgba(255, 255, 255, 0.1)',
    'text': '#FFFFFF',
    'success': '#10B981',      # Green
    'danger': '#EF4444'        # Red
  },
  'glass_effect': {
    'background': 'rgba(255, 255, 255, 0.1)',
    'backdrop_filter': 'blur(10px)',
    'border': '1px solid rgba(255, 255, 255, 0.2)',
    'border_radius': '15px'
  }
}
```

### TechnicalIndicators (Dictionary)

```python
{
  'rsi': float,                    # RSI value (0-100)
  'rsi_signal': str,               # 'Overbought', 'Oversold', 'Neutral'
  'macd': float,                   # MACD line value
  'macd_signal': float,            # Signal line value
  'macd_histogram': float,         # MACD histogram
  'bb_upper': float,               # Bollinger upper band
  'bb_middle': float,              # Bollinger middle band
  'bb_lower': float,               # Bollinger lower band
  'ma_5': float,                   # 5-period SMA
  'ma_20': float,                  # 20-period SMA
  'ma_50': float,                  # 50-period SMA
  'ma_200': float,                 # 200-period SMA
  'overall_signal': str,           # 'BUY', 'SELL', 'HOLD'
  'signal_strength': int           # 1-10 confidence score
}
```

### WatchlistState (Session State)

```python
{
  'watchlist': list[str],          # List of stock symbols
  'last_updated': datetime,        # Last modification time
  'max_size': int                  # Maximum watchlist size (default: 20)
}
```

### RefreshState (Session State)

```python
{
  'auto_refresh_enabled': bool,    # Auto-refresh on/off
  'refresh_interval': int,         # Seconds between refreshes
  'last_refresh_time': datetime,   # Last data fetch time
  'refresh_count': int,            # Total refreshes this session
  'is_refreshing': bool            # Currently fetching data
}
```

## Error Handling

### API Error Scenarios

1. **Rate Limiting (HTTP 429)**
   - Display user-friendly message about API limits
   - Suggest waiting period
   - Use cached data if available

2. **Invalid Symbol (HTTP 400)**
   - Validate symbol before API call
   - Display error message with valid symbol examples

3. **Network Errors**
   - Catch connection exceptions
   - Display retry option
   - Fallback to cached data

4. **Invalid API Key (HTTP 401)**
   - Display configuration error message
   - Provide instructions for obtaining API key

### Data Processing Errors

1. **Empty Response**
   - Check for empty time series data
   - Display "No data available" message

2. **Malformed Data**
   - Validate response structure
   - Log parsing errors
   - Display generic error message

### UI Error Handling

1. **Chart Rendering Failures**
   - Catch Plotly exceptions
   - Display placeholder message
   - Log error details

## Testing Strategy

### Manual Testing Focus

Given the fast-track requirement to skip automated tests, the testing strategy focuses on milestone validation:

1. **API Integration Testing**
   - Verify successful data fetch for each supported symbol
   - Test different time intervals
   - Validate error handling for invalid inputs

2. **UI Rendering Testing**
   - Verify all charts render correctly
   - Test glass morphism effects across browsers
   - Validate text visibility on themed background
   - Test responsive layout

3. **Metrics Calculation Testing**
   - Verify accuracy of calculated metrics
   - Test with different data ranges
   - Validate edge cases (single data point, missing data)

4. **Performance Testing**
   - Measure page load time
   - Test with full 30-day data sets
   - Verify caching effectiveness

### Validation Milestones

1. **Milestone 1**: API service successfully fetches and parses data
2. **Milestone 2**: UI components render with correct styling
3. **Milestone 3**: Charts display data accurately
4. **Milestone 4**: Metrics calculations are correct
5. **Milestone 5**: Full application runs on port 8080 with all features working

## Deployment Configuration

### Docker Architecture

The application uses Docker containerization for consistent deployment across environments:

```
┌─────────────────────────────────────┐
│   Docker Host                       │
│                                     │
│  ┌───────────────────────────────┐ │
│  │  Docker Container             │ │
│  │                               │ │
│  │  ┌─────────────────────────┐  │ │
│  │  │  Python 3.11 Runtime    │  │ │
│  │  │  - Streamlit App        │  │ │
│  │  │  - Dependencies         │  │ │
│  │  │  - Port 8080            │  │ │
│  │  └─────────────────────────┘  │ │
│  │                               │ │
│  │  Environment Variables:       │ │
│  │  - ALPHA_VANTAGE_API_KEY     │ │
│  └───────────────────────────────┘ │
│           ↕ Port Mapping           │
│        8080:8080                   │
└─────────────────────────────────────┘
```

### Dockerfile Structure

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]
```

### Docker Compose Configuration

```yaml
version: '3.8'
services:
  stock-dashboard:
    build: .
    ports:
      - "8080:8080"
    environment:
      - ALPHA_VANTAGE_API_KEY=${ALPHA_VANTAGE_API_KEY}
    restart: unless-stopped
```

### Environment Variables

The application reads configuration from environment variables:

- `ALPHA_VANTAGE_API_KEY`: API key for Alpha Vantage (required)

Environment variables are loaded from:
1. `.env` file (for Docker Compose)
2. System environment variables
3. Fallback to config.py defaults

### Virtual Environment Setup (Local Development)

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Requirements File

```
streamlit==1.28.0
plotly==5.17.0
pandas==2.2.0
requests==2.31.0
```

### Running the Application

**Using Docker Compose (Recommended)**:
```bash
docker-compose up -d
```

**Using Docker directly**:
```bash
docker build -t stock-dashboard .
docker run -p 8080:8080 -e ALPHA_VANTAGE_API_KEY=your_key stock-dashboard
```

**Local development**:
```bash
streamlit run app.py --server.port 8080
```

### Deployment Considerations

1. **Port Configuration**: Container exposes port 8080, mapped to host port 8080
2. **Environment Isolation**: All dependencies packaged in container
3. **Stateless Design**: No persistent storage required
4. **API Key Security**: Passed via environment variables, not hardcoded
5. **Container Restart**: Configured to restart unless explicitly stopped

## UI Design Specifications

### Color Palette

- **Primary Purple**: #8B5CF6 (for headers, primary buttons)
- **Secondary Yellow**: #FBBF24 (for highlights, positive metrics)
- **Accent Orange**: #F97316 (for alerts, negative metrics)
- **Background**: Dark gradient with glass morphism overlays
- **Text**: White (#FFFFFF) with high contrast

### Glass Morphism Implementation

```css
.glass-card {
  background: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  border: 1px solid rgba(255, 255, 255, 0.2);
  border-radius: 15px;
  box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.37);
}
```

### Layout Structure

- **Header**: Full-width with title and subtitle
- **Sidebar**: Fixed left panel (300px) with controls
- **Main Content**: Responsive grid layout
  - Metrics: 4-column grid on desktop, stacked on mobile
  - Charts: 2-column grid for smaller charts, full-width for main chart

### Typography

- **Headers**: Bold, 24-32px, high contrast
- **Metrics**: Bold, 20-28px, color-coded
- **Labels**: Regular, 14-16px, semi-transparent white
- **Body Text**: Regular, 14px, white with 0.9 opacity


## UX Enhancements Design

### Loading States

**Skeleton Screens**:
```python
def render_loading_skeleton():
    """Display animated skeleton while loading"""
    # Metrics skeleton
    cols = st.columns(4)
    for col in cols:
        with col:
            st.markdown(skeleton_card_html, unsafe_allow_html=True)
    
    # Chart skeleton
    st.markdown(skeleton_chart_html, unsafe_allow_html=True)
```

**Loading Indicators**:
- Spinner for data fetching
- Progress bar for long operations
- Subtle pulse animation for auto-refresh

**Transitions**:
- Fade-in for new data
- Smooth color changes for price updates
- Animated number counters for metrics

### Interactive Feedback

**Tooltips**:
- Hover tooltips for all metrics explaining what they mean
- Info icons with detailed explanations
- Keyboard shortcut hints

**Success/Error Messages**:
- Toast notifications for actions (added to watchlist, etc.)
- Color-coded alerts (green for success, red for error, yellow for warning)
- Auto-dismiss after 3 seconds

**Visual Feedback**:
- Button hover states
- Active state for selected stock
- Disabled state for unavailable actions

### Keyboard Shortcuts

- `R`: Refresh data
- `W`: Toggle watchlist panel
- `1-7`: Quick select stock (1=IBM, 2=AAPL, etc.)
- `Space`: Pause/Resume auto-refresh
- `?`: Show keyboard shortcuts help

## Watchlist Feature Design

### UI Components

**Sidebar Watchlist Panel**:
```
┌─────────────────────────┐
│ ⭐ Watchlist            │
├─────────────────────────┤
│ [+] Add Current Stock   │
├─────────────────────────┤
│ ┌─────────────────────┐ │
│ │ IBM          [×]    │ │
│ │ AAPL         [×]    │ │
│ │ MSFT         [×]    │ │
│ └─────────────────────┘ │
│                         │
│ 3/20 stocks             │
└─────────────────────────┘
```

**Features**:
- Click stock name to load
- Click [×] to remove
- Drag to reorder (future enhancement)
- Show mini price indicator next to each stock

### State Management

```python
# Initialize watchlist in session state
if 'watchlist' not in st.session_state:
    st.session_state.watchlist = []
    st.session_state.watchlist_data = {}  # Cache prices

# Add to watchlist
def add_to_watchlist(symbol):
    if symbol not in st.session_state.watchlist:
        st.session_state.watchlist.append(symbol)
        return True
    return False

# Remove from watchlist
def remove_from_watchlist(symbol):
    if symbol in st.session_state.watchlist:
        st.session_state.watchlist.remove(symbol)
        return True
    return False
```

## Auto-Refresh Feature Design

### UI Components

**Refresh Control Panel**:
```
┌─────────────────────────────────┐
│ 🔄 Auto-Refresh: [ON] [OFF]    │
│ Interval: [30s] [60s] [120s]   │
│ Next refresh in: 45s            │
│ [Refresh Now]                   │
└─────────────────────────────────┘
```

**Visual Indicators**:
- Countdown timer showing seconds until next refresh
- Subtle pulsing icon during refresh
- Last updated timestamp
- Refresh count for session

### Implementation Strategy

```python
# Initialize refresh state
if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = {
        'enabled': True,
        'interval': 60,  # seconds
        'last_refresh': datetime.now(),
        'count': 0
    }

# Auto-refresh logic
if st.session_state.auto_refresh['enabled']:
    time_since_refresh = (datetime.now() - st.session_state.auto_refresh['last_refresh']).seconds
    
    if time_since_refresh >= st.session_state.auto_refresh['interval']:
        # Trigger refresh
        st.rerun()
    else:
        # Show countdown
        countdown = st.session_state.auto_refresh['interval'] - time_since_refresh
        st.sidebar.metric("Next refresh", f"{countdown}s")
```

**Price Change Highlighting**:
- Green background flash for price increase
- Red background flash for price decrease
- Animated arrow indicators (↑ ↓)

## Technical Indicators Design

### Indicator Display Layout

```
┌─────────────────────────────────────────────────┐
│ 📊 Technical Indicators                         │
├─────────────────────────────────────────────────┤
│ RSI (14):  65.4  [Neutral]                     │
│ MACD:      +2.34 [Bullish]                     │
│ BB:        Upper: $215.50 | Lower: $210.20    │
│ MA(20):    $212.85                             │
├─────────────────────────────────────────────────┤
│ Overall Signal: 🟢 BUY (Confidence: 7/10)      │
└─────────────────────────────────────────────────┘
```

### Indicator Calculations

**RSI Calculation**:
```python
def calculate_rsi(df, period=14):
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi
```

**MACD Calculation**:
```python
def calculate_macd(df):
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()
    histogram = macd - signal
    return macd, signal, histogram
```

**Bollinger Bands Calculation**:
```python
def calculate_bollinger_bands(df, period=20, std_dev=2):
    sma = df['close'].rolling(window=period).mean()
    std = df['close'].rolling(window=period).std()
    upper_band = sma + (std * std_dev)
    lower_band = sma - (std * std_dev)
    return upper_band, sma, lower_band
```

### Signal Generation Logic

```python
def generate_overall_signal(indicators):
    """Generate BUY/SELL/HOLD signal based on all indicators"""
    signals = []
    
    # RSI signal
    if indicators['rsi'] < 30:
        signals.append(('BUY', 2))  # Strong buy
    elif indicators['rsi'] > 70:
        signals.append(('SELL', 2))  # Strong sell
    else:
        signals.append(('HOLD', 1))
    
    # MACD signal
    if indicators['macd'] > indicators['macd_signal']:
        signals.append(('BUY', 1))
    else:
        signals.append(('SELL', 1))
    
    # Bollinger Bands signal
    current_price = indicators['current_price']
    if current_price < indicators['bb_lower']:
        signals.append(('BUY', 1))
    elif current_price > indicators['bb_upper']:
        signals.append(('SELL', 1))
    
    # Calculate weighted signal
    buy_score = sum(weight for signal, weight in signals if signal == 'BUY')
    sell_score = sum(weight for signal, weight in signals if signal == 'SELL')
    
    if buy_score > sell_score:
        return 'BUY', min(buy_score, 10)
    elif sell_score > buy_score:
        return 'SELL', min(sell_score, 10)
    else:
        return 'HOLD', 5
```

### Indicator Visualization

**RSI Chart with Zones**:
- Overbought zone (70-100) in red
- Oversold zone (0-30) in green
- Neutral zone (30-70) in gray

**MACD Chart**:
- MACD line in blue
- Signal line in orange
- Histogram bars (green for positive, red for negative)

**Bollinger Bands on Price Chart**:
- Upper band in red
- Middle band (SMA) in yellow
- Lower band in green
- Price line in white

## Performance Optimizations

### Caching Strategy

**Data Caching**:
```python
@st.cache_data(ttl=60)  # Cache for 60 seconds
def fetch_and_parse_data(symbol, interval):
    response = api_service.fetch_intraday_data(symbol, interval)
    return api_service.parse_time_series(response)
```

**Indicator Caching**:
```python
@st.cache_data(ttl=60)
def calculate_all_indicators(df):
    return {
        'rsi': calculate_rsi(df),
        'macd': calculate_macd(df),
        'bb': calculate_bollinger_bands(df),
        'ma': calculate_moving_averages(df)
    }
```

### Lazy Loading

- Load charts only when visible
- Defer indicator calculations until requested
- Progressive image loading for any future images

### Code Splitting

- Separate modules for each feature
- Import only what's needed
- Lazy import heavy libraries

## Accessibility Considerations

- High contrast text (WCAG AA compliant)
- Keyboard navigation support
- Screen reader friendly labels
- Focus indicators for interactive elements
- Alt text for all visual elements
- Semantic HTML structure
