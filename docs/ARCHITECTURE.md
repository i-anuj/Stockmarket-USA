# Architecture Documentation

## Overview

The Stock Market Analytics Dashboard is built using a modular, production-ready architecture that separates concerns and promotes maintainability, testability, and scalability.

## Architecture Principles

1. **Separation of Concerns**: Each module has a single, well-defined responsibility
2. **Dependency Injection**: Configuration and services are injected where needed
3. **Testability**: All components are designed to be easily testable
4. **Modularity**: Components can be developed, tested, and deployed independently
5. **Scalability**: Architecture supports future enhancements and features

## Project Structure

```
stock-market-dashboard/
├── src/                          # Source code
│   ├── core/                    # Business logic layer
│   │   ├── technical_indicators.py  # Technical analysis calculations
│   │   └── data_processor.py        # Data transformation and metrics
│   ├── services/                # External services layer
│   │   └── api_service.py           # Alpha Vantage API integration
│   ├── managers/                # State management layer
│   │   ├── watchlist_manager.py     # Watchlist state management
│   │   └── refresh_manager.py       # Auto-refresh state management
│   ├── ui/                      # Presentation layer
│   │   ├── components.py            # Reusable UI components
│   │   └── charts.py                # Chart visualization components
│   ├── app.py                   # Main application entry point
│   └── config.py                # Configuration management
├── tests/                       # Test suite
├── docs/                        # Documentation
├── scripts/                     # Utility scripts
├── docker/                      # Docker configuration
└── config/                      # Configuration files
```

## Layer Descriptions

### Core Layer (`src/core/`)

Contains the business logic and domain-specific functionality.

**technical_indicators.py**
- Calculates technical indicators (RSI, MACD, Bollinger Bands, Moving Averages)
- Generates trading signals based on multiple indicators
- Pure functions with no side effects
- Easily testable with mock data

**data_processor.py**
- Transforms raw API data into usable formats
- Calculates derived metrics (price changes, percentages)
- Handles data validation and cleaning
- Provides data aggregation functions

### Services Layer (`src/services/`)

Handles external integrations and API communications.

**api_service.py**
- Integrates with Alpha Vantage API
- Handles HTTP requests and error handling
- Implements rate limiting and retry logic
- Parses API responses into structured data
- Provides caching mechanisms

### Managers Layer (`src/managers/`)

Manages application state and user preferences.

**watchlist_manager.py**
- Manages user's stock watchlist
- Handles add/remove operations
- Enforces watchlist size limits
- Persists watchlist in session state

**refresh_manager.py**
- Manages auto-refresh functionality
- Handles refresh intervals and timing
- Tracks refresh state and history
- Provides pause/resume capabilities

### UI Layer (`src/ui/`)

Handles presentation and user interaction.

**components.py**
- Reusable UI components (cards, buttons, modals)
- Styling and theming utilities
- Loading states and skeletons
- Toast notifications

**charts.py**
- Plotly chart components
- Candlestick charts
- Technical indicator visualizations
- Interactive chart features

### Application Layer

**app.py**
- Main Streamlit application
- Orchestrates all layers
- Handles user input and routing
- Manages page layout and navigation

**config.py**
- Centralized configuration
- Environment variable management
- Application constants
- Feature flags

## Data Flow

```
User Input → app.py → Managers (State) → Services (API) → Core (Processing) → UI (Display)
                ↓                                              ↓
         Session State                                    Charts/Components
```

1. **User Interaction**: User selects stock and interval in the UI
2. **State Management**: Managers update session state
3. **Data Fetching**: Services fetch data from Alpha Vantage API
4. **Data Processing**: Core layer processes and calculates indicators
5. **Visualization**: UI layer renders charts and components
6. **Display**: Streamlit renders the final page

## Key Design Patterns

### 1. Repository Pattern
- `api_service.py` acts as a repository for stock data
- Abstracts data source details from business logic
- Enables easy switching between data providers

### 2. Service Layer Pattern
- Business logic separated from presentation
- Services can be reused across different UI components
- Facilitates testing and mocking

### 3. State Management Pattern
- Centralized state in Streamlit session state
- Managers provide controlled access to state
- Prevents direct state manipulation

### 4. Component Pattern
- Reusable UI components in `components.py`
- Consistent styling and behavior
- Reduces code duplication

## Configuration Management

Configuration is managed through environment variables and a centralized config module:

```python
# config.py
import os
from dotenv import load_dotenv

load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'demo')
SUPPORTED_STOCKS = ['IBM', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META']
SUPPORTED_INTERVALS = ['1min', '5min', '15min', '30min', '60min']
```

## Error Handling

The application implements comprehensive error handling:

1. **API Errors**: Handled in `api_service.py` with user-friendly messages
2. **Data Validation**: Checked in `data_processor.py`
3. **User Input**: Validated in `app.py` before processing
4. **State Errors**: Managed by state managers with fallbacks

## Testing Strategy

### Unit Tests
- Test individual functions in isolation
- Mock external dependencies
- Focus on business logic correctness

### Integration Tests
- Test component interactions
- Verify data flow between layers
- Test API integration with mock responses

### End-to-End Tests
- Test complete user workflows
- Verify UI rendering and interactions
- Test with real API calls (limited)

## Performance Considerations

1. **Caching**: API responses cached to reduce redundant calls
2. **Lazy Loading**: Data loaded only when needed
3. **Efficient Calculations**: Vectorized operations with pandas/numpy
4. **Session State**: Minimizes re-computation of expensive operations

## Security Considerations

1. **API Key Management**: Keys stored in environment variables
2. **Input Validation**: All user inputs validated before processing
3. **Error Messages**: No sensitive information exposed in errors
4. **Rate Limiting**: API calls rate-limited to prevent abuse

## Scalability

The architecture supports future enhancements:

1. **Multiple Data Sources**: Easy to add new API services
2. **Additional Indicators**: Core layer designed for extensibility
3. **User Authentication**: State management ready for user-specific data
4. **Database Integration**: Repository pattern supports database backends
5. **Microservices**: Layers can be separated into independent services

## Deployment Architecture

### Docker Deployment
```
Docker Container
├── Python 3.11 Runtime
├── Streamlit Server (Port 8080)
├── Application Code (src/)
└── Dependencies (requirements.txt)
```

### Environment Variables
- `ALPHA_VANTAGE_API_KEY`: API key for data access
- `STREAMLIT_SERVER_PORT`: Server port (default: 8080)
- `STREAMLIT_SERVER_ADDRESS`: Server address (default: 0.0.0.0)

## Monitoring and Logging

1. **Application Logs**: Streamlit built-in logging
2. **Error Tracking**: Errors logged with context
3. **Performance Metrics**: Response times tracked
4. **API Usage**: API call counts and rate limits monitored

## Future Enhancements

1. **Database Layer**: Add persistent storage for historical data
2. **Authentication**: User accounts and personalized settings
3. **Real-time Updates**: WebSocket integration for live data
4. **Advanced Analytics**: Machine learning predictions
5. **Portfolio Management**: Track multiple portfolios
6. **Alerts**: Price and indicator-based notifications
7. **Export Features**: Download data and reports
8. **Mobile App**: React Native or Flutter mobile client

## Contributing

When contributing to the project:

1. Follow the established layer structure
2. Add tests for new functionality
3. Update documentation for architectural changes
4. Follow Python best practices (PEP 8)
5. Use type hints for better code clarity
6. Write docstrings for all public functions

## References

- [Streamlit Documentation](https://docs.streamlit.io/)
- [Alpha Vantage API](https://www.alphavantage.co/documentation/)
- [Plotly Documentation](https://plotly.com/python/)
- [Python Best Practices](https://docs.python-guide.org/)
