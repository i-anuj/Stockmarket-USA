# Development Guide

## Getting Started

### Prerequisites

- Python 3.9 or higher
- pip (Python package manager)
- Git
- Alpha Vantage API key ([Get free key](https://www.alphavantage.co/support/#api-key))

### Initial Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd stock-market-dashboard
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   pip install -e .  # Install in editable mode
   ```

4. **Configure environment**:
   ```bash
   cp config/.env.example .env
   # Edit .env and add your API key
   ```

5. **Run the application**:
   ```bash
   streamlit run src/app.py --server.port 8080
   ```

## Development Workflow

### Branch Strategy

- `main`: Production-ready code
- `develop`: Integration branch for features
- `feature/*`: Feature development branches
- `bugfix/*`: Bug fix branches
- `hotfix/*`: Emergency production fixes

### Making Changes

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the coding standards

3. **Run tests**:
   ```bash
   pytest
   ```

4. **Check code quality**:
   ```bash
   black src/
   flake8 src/
   mypy src/
   ```

5. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

6. **Push and create pull request**:
   ```bash
   git push origin feature/your-feature-name
   ```

## Coding Standards

### Python Style Guide

Follow PEP 8 with these specifics:

- **Line Length**: 88 characters (Black default)
- **Indentation**: 4 spaces
- **Quotes**: Double quotes for strings
- **Imports**: Organized in three groups (standard library, third-party, local)

### Code Formatting

Use Black for automatic formatting:
```bash
black src/
```

### Linting

Use flake8 for linting:
```bash
flake8 src/ --max-line-length=88 --extend-ignore=E203,W503
```

### Type Checking

Use mypy for type checking:
```bash
mypy src/ --ignore-missing-imports
```

### Import Organization

```python
# Standard library imports
import os
import sys
from typing import Optional, Dict

# Third-party imports
import pandas as pd
import streamlit as st

# Local imports
from src import config
from src.services import api_service
```

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_technical_indicators.py

# Run specific test
pytest tests/test_technical_indicators.py::TestTechnicalIndicators::test_calculate_rsi
```

### Writing Tests

Place tests in the `tests/` directory with the naming convention `test_*.py`.

Example test structure:
```python
import pytest
from src.core import technical_indicators

class TestTechnicalIndicators:
    def setup_method(self):
        """Set up test fixtures."""
        self.sample_data = create_sample_data()
    
    def test_calculate_rsi(self):
        """Test RSI calculation."""
        result = technical_indicators.calculate_rsi(self.sample_data)
        assert result is not None
        assert all(0 <= val <= 100 for val in result.dropna())
```

### Test Coverage

Aim for at least 80% code coverage:
```bash
pytest --cov=src --cov-report=term-missing
```

## Project Structure

```
src/
├── core/              # Business logic
├── services/          # External integrations
├── managers/          # State management
├── ui/                # UI components
├── app.py            # Main application
└── config.py         # Configuration

tests/
├── test_core/        # Core layer tests
├── test_services/    # Service layer tests
├── test_managers/    # Manager layer tests
└── test_ui/          # UI layer tests
```

## Adding New Features

### 1. Technical Indicators

To add a new technical indicator:

1. Add calculation function to `src/core/technical_indicators.py`:
   ```python
   def calculate_new_indicator(df: pd.DataFrame, period: int = 14) -> pd.Series:
       """Calculate new indicator."""
       # Implementation
       return result
   ```

2. Add tests to `tests/test_technical_indicators.py`

3. Update signal generation logic if needed

4. Add chart visualization to `src/ui/charts.py`

### 2. Data Sources

To add a new data source:

1. Create new service in `src/services/`:
   ```python
   # src/services/new_api_service.py
   def fetch_data(symbol: str) -> pd.DataFrame:
       """Fetch data from new source."""
       # Implementation
       return df
   ```

2. Add configuration to `src/config.py`

3. Update `src/app.py` to support new source

4. Add tests for the new service

### 3. UI Components

To add a new UI component:

1. Add component to `src/ui/components.py`:
   ```python
   def new_component(data, **kwargs):
       """Render new component."""
       # Implementation
   ```

2. Use component in `src/app.py`

3. Add styling if needed

## Debugging

### Streamlit Debugging

1. **Enable debug mode**:
   ```bash
   streamlit run src/app.py --logger.level=debug
   ```

2. **Use st.write() for debugging**:
   ```python
   st.write("Debug:", variable)
   ```

3. **Check session state**:
   ```python
   st.write(st.session_state)
   ```

### Python Debugging

Use pdb for debugging:
```python
import pdb; pdb.set_trace()
```

Or use VS Code debugger with launch configuration:
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Streamlit",
            "type": "python",
            "request": "launch",
            "module": "streamlit",
            "args": ["run", "src/app.py"]
        }
    ]
}
```

## Docker Development

### Building Docker Image

```bash
docker build -f docker/Dockerfile -t stock-dashboard:dev .
```

### Running Docker Container

```bash
docker run -p 8080:8080 \
  -e ALPHA_VANTAGE_API_KEY=your_key \
  stock-dashboard:dev
```

### Docker Compose

```bash
cd docker
docker-compose up -d
docker-compose logs -f
docker-compose down
```

## Environment Variables

Create a `.env` file in the project root:

```bash
# API Configuration
ALPHA_VANTAGE_API_KEY=your_api_key_here

# Application Configuration
STREAMLIT_SERVER_PORT=8080
STREAMLIT_SERVER_ADDRESS=localhost

# Feature Flags
ENABLE_AUTO_REFRESH=true
ENABLE_WATCHLIST=true
MAX_WATCHLIST_SIZE=20

# Development
DEBUG=false
LOG_LEVEL=INFO
```

## Performance Optimization

### Caching

Use Streamlit's caching decorators:
```python
@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_stock_data(symbol: str):
    return api_service.fetch_intraday_data(symbol)
```

### Session State

Minimize session state usage:
```python
# Good: Store only necessary data
st.session_state['current_symbol'] = 'IBM'

# Bad: Store large dataframes
# st.session_state['all_data'] = large_dataframe
```

## Common Issues

### Issue: Import Errors

**Problem**: `ModuleNotFoundError: No module named 'src'`

**Solution**: Install package in editable mode:
```bash
pip install -e .
```

### Issue: API Rate Limiting

**Problem**: Too many API calls

**Solution**: Implement caching and rate limiting:
```python
@st.cache_data(ttl=60)
def cached_api_call(symbol):
    return api_service.fetch_data(symbol)
```

### Issue: Streamlit Rerun Issues

**Problem**: Infinite rerun loop

**Solution**: Use session state to track state:
```python
if 'initialized' not in st.session_state:
    st.session_state.initialized = True
    # Initialization code
```

## Documentation

### Code Documentation

Use docstrings for all public functions:
```python
def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Relative Strength Index (RSI).
    
    Args:
        df: DataFrame with 'close' column
        period: RSI period (default: 14)
    
    Returns:
        Series with RSI values (0-100)
    
    Raises:
        ValueError: If df is empty or missing 'close' column
    """
    # Implementation
```

### API Documentation

Document API endpoints and responses in docstrings.

### Architecture Documentation

Update `docs/ARCHITECTURE.md` for architectural changes.

## Git Commit Messages

Follow conventional commits:

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `style:` Code style changes (formatting)
- `refactor:` Code refactoring
- `test:` Test changes
- `chore:` Build/tooling changes

Examples:
```
feat: add Bollinger Bands indicator
fix: resolve API rate limiting issue
docs: update development guide
refactor: extract chart components
test: add tests for watchlist manager
```

## Resources

- [Streamlit Documentation](https://docs.streamlit.io/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Plotly Documentation](https://plotly.com/python/)
- [pytest Documentation](https://docs.pytest.org/)
- [Black Documentation](https://black.readthedocs.io/)
- [PEP 8 Style Guide](https://pep8.org/)

## Getting Help

- Check existing documentation in `docs/`
- Review code comments and docstrings
- Search for similar issues in the issue tracker
- Ask questions in team chat or discussions
