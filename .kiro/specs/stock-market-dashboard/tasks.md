# Implementation Plan

- [x] 1. Set up project structure and virtual environment
  - Create project directory structure with modules for api_service, data_processor, ui_components, charts, and config
  - Initialize Python virtual environment
  - Create requirements.txt with streamlit, plotly, pandas, and requests dependencies
  - Create .gitignore file for Python projects
  - _Requirements: 5.1, 5.2, 5.3_

- [x] 2. Implement configuration module
  - Create config.py with API key, supported stock symbols (IBM, AAPL, MSFT, GOOGL, AMZN, TSLA, META), time intervals, and port settings
  - Define color theme constants (purple #8B5CF6, yellow #FBBF24, orange #F97316)
  - Define glass morphism CSS properties
  - _Requirements: 1.2, 4.1, 5.4_

- [x] 3. Implement API service module
  - Create api_service.py with function to fetch intraday data from Alpha Vantage API
  - Implement fetch_intraday_data() function that constructs API URL with symbol, interval, and API key parameters
  - Implement parse_time_series() function to convert API JSON response to pandas DataFrame with timestamp, open, high, low, close, volume columns
  - Add error handling for HTTP errors (401, 429, 400), network errors, and empty responses
  - Implement basic response caching using Streamlit session state
  - _Requirements: 1.1, 1.3, 1.4, 1.5, 6.5_

- [x] 4. Implement data processing module
  - Create data_processor.py with functions for metrics calculation and data transformation
  - Implement calculate_metrics() function to compute current price, price change, percentage change, high, low, and volume statistics
  - Implement prepare_chart_data() function to format DataFrame for different chart types
  - Implement calculate_trends() function for trend analysis and moving averages
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [x] 5. Implement chart components module
  - Create charts.py with Plotly chart generation functions
  - Implement create_price_chart() function for line chart showing stock price trends over time with theme colors
  - Implement create_volume_chart() function for bar chart displaying trading volume over time
  - Implement create_pie_chart() function for data distribution visualization
  - Implement create_candlestick_chart() function for OHLC candlestick visualization
  - Apply theme colors and ensure labels and legends are visible
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [x] 6. Implement UI components module
  - Create ui_components.py with reusable Streamlit UI components
  - Implement apply_custom_css() function to inject glass morphism styles and color theme
  - Implement render_metric_card() function to display metrics in styled cards with glass effect
  - Implement render_stock_selector() function for dropdown with supported stock symbols
  - Implement render_interval_selector() function for time interval dropdown (1min, 5min, 15min, 30min, 60min)
  - Ensure all text is visible against themed background
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 6.1, 6.2_

- [x] 7. Implement main application
  - Create app.py as the Streamlit application entry point
  - Configure Streamlit page settings with title and wide layout
  - Initialize session state for data caching
  - Implement header section with title and styling
  - Implement sidebar with stock selector and interval selector
  - Implement main content area with metrics row (4 columns for current price, change %, high, low)
  - Integrate API service to fetch data when user selects stock or changes interval
  - Display loading feedback during API calls
  - Implement charts section with price chart (full width), volume chart (half width), and pie chart (half width)
  - Wire all components together to create interactive dashboard
  - _Requirements: 1.1, 1.4, 2.4, 3.1, 3.2, 3.3, 3.4, 3.5, 4.5, 6.1, 6.2, 6.3, 6.4_

- [x] 8. Configure application to run on port 8080
  - Create startup script or configuration to run Streamlit on port 8080
  - Verify application starts correctly in virtual environment
  - _Requirements: 5.1, 5.3_

- [x] 9. Validate complete application
  - Test data fetching for all supported stock symbols (IBM, AAPL, MSFT, GOOGL, AMZN, TSLA, META)
  - Verify all charts render correctly with data
  - Verify metrics calculations are accurate
  - Test switching between different stocks and intervals
  - Verify glass morphism effects and color theme are applied correctly
  - Verify text visibility across all components
  - Test error handling for API failures
  - Verify caching functionality reduces redundant API calls
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 2.5, 3.1, 3.2, 3.3, 3.4, 3.5, 4.1, 4.2, 4.3, 4.4, 4.5, 6.1, 6.2, 6.3, 6.4, 6.5_

- [x] 10. Update configuration module for environment variables
  - Modify config.py to read ALPHA_VANTAGE_API_KEY from environment variables with fallback to default
  - Add support for reading configuration from os.environ
  - Ensure backward compatibility with existing hardcoded values for local development
  - _Requirements: 7.3, 8.4_

- [x] 11. Create Dockerfile for containerization
  - Create Dockerfile with Python 3.11-slim base image
  - Set working directory to /app
  - Copy requirements.txt and install dependencies with --no-cache-dir flag
  - Copy all application files to container
  - Expose port 8080
  - Set CMD to run Streamlit with server.address=0.0.0.0 for external access
  - _Requirements: 7.1, 7.2, 7.4_

- [x] 12. Create Docker Compose configuration
  - Create docker-compose.yml file with version 3.8
  - Define stock-dashboard service with build context
  - Configure port mapping 8080:8080
  - Set up environment variable for ALPHA_VANTAGE_API_KEY from .env file
  - Configure restart policy as unless-stopped
  - _Requirements: 8.1, 8.2, 8.3, 8.4_

- [x] 13. Create environment configuration files
  - Create .env.example file with template for ALPHA_VANTAGE_API_KEY
  - Create .env file with actual API key (add to .gitignore)
  - Update .gitignore to exclude .env file but include .env.example
  - Document environment variable usage in README
  - _Requirements: 7.3, 8.4_

- [x] 14. Update documentation for Docker deployment
  - Update README.md with Docker installation instructions
  - Add docker-compose up command usage
  - Add docker build and docker run examples
  - Document environment variable configuration
  - Add troubleshooting section for Docker-related issues
  - _Requirements: 7.1, 8.1, 8.2_

- [x] 15. Validate Docker deployment
  - Build Docker image successfully without errors
  - Run container using docker-compose up and verify it starts
  - Test application accessibility at http://localhost:8080
  - Verify environment variables are loaded correctly
  - Test docker-compose down stops container gracefully
  - Verify container restart behavior
  - Test with different API keys via environment variables
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 8.1, 8.2, 8.3, 8.4, 8.5_


- [x] 16. Implement technical indicators module
  - Create technical_indicators.py module
  - Implement calculate_rsi() function with 14-period default
  - Implement calculate_macd() function returning macd, signal, and histogram
  - Implement calculate_bollinger_bands() function with 20-period SMA and 2 standard deviations
  - Implement calculate_moving_averages() function for 5, 20, 50, 200 periods
  - Implement generate_signals() function to analyze indicators and return BUY/SELL/HOLD signal
  - Add signal strength calculation (1-10 confidence score)
  - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5_

- [x] 17. Implement watchlist manager module
  - Create watchlist_manager.py module
  - Implement add_to_watchlist() function using session state
  - Implement remove_from_watchlist() function
  - Implement get_watchlist() function returning sorted list
  - Implement is_in_watchlist() function for checking membership
  - Add watchlist size limit (max 20 stocks)
  - Initialize watchlist in session state on first load
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

- [x] 18. Implement auto-refresh manager module
  - Create refresh_manager.py module
  - Implement initialize_refresh_state() function
  - Implement toggle_refresh() function to enable/disable auto-refresh
  - Implement get_refresh_interval() and set_refresh_interval() functions
  - Implement should_refresh() function checking time since last refresh
  - Implement get_countdown() function returning seconds until next refresh
  - Add validation for refresh interval (min 10s, max 300s)
  - _Requirements: 11.1, 11.2, 11.5_

- [x] 19. Enhance UI components with loading states and feedback
  - Update ui_components.py with loading skeleton function
  - Implement render_loading_skeleton() with animated placeholders
  - Add render_toast_notification() for success/error messages
  - Implement render_tooltip() function for contextual help
  - Add CSS animations for smooth transitions
  - Implement price change highlighting (green/red flash)
  - Add keyboard shortcut hints to UI
  - _Requirements: 9.1, 9.2, 9.3, 9.4_

- [x] 20. Implement watchlist UI in sidebar
  - Add watchlist section to sidebar in app.py
  - Display "Add to Watchlist" button for current stock
  - Render watchlist with clickable stock names
  - Add remove button (×) for each watchlist item
  - Show watchlist count (e.g., "3/20 stocks")
  - Implement click handlers to load stock from watchlist
  - Add empty state message when watchlist is empty
  - _Requirements: 10.1, 10.2, 10.3, 10.4_

- [x] 21. Implement auto-refresh UI and functionality
  - Add auto-refresh control panel to sidebar
  - Implement ON/OFF toggle buttons
  - Add interval selector (30s, 60s, 120s options)
  - Display countdown timer showing seconds until next refresh
  - Add "Refresh Now" button for manual refresh
  - Implement auto-refresh logic using st.rerun()
  - Show last updated timestamp
  - Add subtle loading indicator during refresh
  - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5_

- [x] 22. Add technical indicators display to main dashboard
  - Create technical indicators section in app.py
  - Calculate all indicators using technical_indicators module
  - Display RSI with overbought/oversold status
  - Display MACD with signal line and histogram
  - Display Bollinger Bands values (upper, middle, lower)
  - Display moving averages (5, 20, 50, 200 periods)
  - Show overall BUY/SELL/HOLD signal with confidence score
  - Add color coding for signals (green=BUY, red=SELL, yellow=HOLD)
  - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5_

- [x] 23. Create advanced indicator charts
  - Add create_rsi_chart() function to charts.py
  - Implement RSI chart with overbought/oversold zones
  - Add create_macd_chart() function with histogram
  - Update create_price_chart() to include Bollinger Bands overlay
  - Add moving averages overlay to price chart
  - Style charts with theme colors
  - Add interactive tooltips to all indicator charts
  - _Requirements: 12.1, 12.2, 12.3, 12.4_

- [x] 24. Implement price change highlighting
  - Add price comparison logic in app.py
  - Store previous price in session state
  - Calculate price change on each refresh
  - Apply green background flash for price increase
  - Apply red background flash for price decrease
  - Add animated arrow indicators (↑ ↓)
  - Display percentage change with color coding
  - _Requirements: 11.3_

- [x] 25. Add keyboard shortcuts support
  - Implement keyboard event handlers in app.py
  - Add 'R' key for manual refresh
  - Add 'W' key to toggle watchlist visibility
  - Add '1-7' keys for quick stock selection
  - Add 'Space' key to pause/resume auto-refresh
  - Add '?' key to show keyboard shortcuts help modal
  - Display keyboard shortcuts legend in sidebar
  - _Requirements: 9.5_

- [x] 26. Update requirements.txt with new dependencies
  - Add pandas-ta for technical indicators (or implement manually)
  - Verify all existing dependencies are compatible
  - Test installation in clean virtual environment
  - Update Dockerfile to include new dependencies
  - _Requirements: 12.1, 12.2, 12.3, 12.4_

- [x] 27. Integrate all new features into main application
  - Wire technical indicators module to app.py
  - Integrate watchlist manager with UI
  - Connect auto-refresh manager to data fetching
  - Ensure all features work together seamlessly
  - Add feature toggles for easy enable/disable
  - Implement error handling for all new features
  - Add loading states for all async operations
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 10.1, 10.2, 10.3, 10.4, 10.5, 11.1, 11.2, 11.3, 11.4, 11.5, 12.1, 12.2, 12.3, 12.4, 12.5_

- [x] 28. Validate all new features
  - Test technical indicators calculations for accuracy
  - Verify RSI, MACD, Bollinger Bands values against known data
  - Test watchlist add/remove functionality
  - Verify watchlist persists across page interactions
  - Test auto-refresh with different intervals
  - Verify countdown timer accuracy
  - Test price change highlighting with live data
  - Verify keyboard shortcuts work correctly
  - Test loading states and transitions
  - Verify all tooltips display correctly
  - Test with all supported stock symbols
  - Verify performance with auto-refresh enabled
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 10.1, 10.2, 10.3, 10.4, 10.5, 11.1, 11.2, 11.3, 11.4, 11.5, 12.1, 12.2, 12.3, 12.4, 12.5_


- [x] 29. Restructure project for production-ready organization
  - Create proper folder structure (src/, docs/, tests/, scripts/, docker/, config/)
  - Move Python modules to src/ with subdirectories (core/, services/, managers/, ui/)
  - Move all documentation files to docs/ directory
  - Move Docker files to docker/ directory
  - Move scripts to scripts/ directory
  - Add __init__.py files to all Python packages
  - Create setup.py for package installation
  - Create pyproject.toml for modern Python configuration
  - Create LICENSE file (MIT or Apache 2.0)
  - Create CHANGELOG.md for version tracking
  - Create requirements-dev.txt for development dependencies
  - Update all import statements to use new structure
  - Update Dockerfile to reference new paths
  - Update docker-compose.yml to reference new paths
  - Create .github/workflows/ci.yml for CI/CD
  - Keep only essential files in root (README.md, LICENSE, .gitignore, requirements.txt, setup.py, pyproject.toml)
  - Test all imports work correctly
  - Test Docker build with new structure
  - Test application runs successfully
  - Update all documentation links
  - _Requirements: Production readiness, maintainability, scalability_
