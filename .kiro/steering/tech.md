# Technology Stack

## Core Technologies

- **Python**: 3.9+ (3.11 recommended)
- **Framework**: Streamlit 1.28.0
- **Visualization**: Plotly 5.17.0
- **Data Processing**: Pandas 2.2.0
- **HTTP Client**: Requests 2.31.0

## Build System

- **Package Manager**: pip
- **Build Tool**: setuptools
- **Project Config**: pyproject.toml + setup.py
- **Editable Install**: `pip install -e .`

## Development Tools

- **Testing**: pytest
- **Formatting**: Black (88 char line length)
- **Linting**: flake8
- **Type Checking**: mypy

## Deployment

- **Container**: Docker with python:3.11-slim base
- **Orchestration**: Docker Compose
- **Port**: 8080 (default)

## External Services

- **API**: Alpha Vantage (stock market data)
- **Rate Limit**: 5 requests/minute (free tier)

## Common Commands

### Development
```bash
# Setup
python -m venv venv
source venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -e .

# Run application
streamlit run src/app.py --server.port 8080

# Run with debug logging
streamlit run src/app.py --logger.level=debug
```

### Testing
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_technical_indicators.py
```

### Code Quality
```bash
# Format code
black src/

# Lint code
flake8 src/ --max-line-length=88 --extend-ignore=E203,W503

# Type check
mypy src/ --ignore-missing-imports
```

### Docker
```bash
# Build image
docker build -f docker/Dockerfile -t stock-dashboard .

# Run with Docker Compose
cd docker && docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

### Validation
```bash
# Validate project structure
python scripts/validate_structure.py
```

## Environment Variables

Required in `.env` file:
- `ALPHA_VANTAGE_API_KEY`: API key for stock data (required)

Optional:
- `STREAMLIT_SERVER_PORT`: Server port (default: 8080)
- `STREAMLIT_SERVER_ADDRESS`: Server address (default: 0.0.0.0)
