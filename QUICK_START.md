# Quick Start Guide

## For Users

### Docker (Recommended)

1. **Get an API key**: [Alpha Vantage](https://www.alphavantage.co/support/#api-key)

2. **Configure environment**:
   ```bash
   cp config/.env.example .env
   # Edit .env and add your API key
   ```

3. **Start the application**:
   ```bash
   cd docker
   docker-compose up -d
   ```

4. **Access**: Open http://localhost:8080

5. **Stop**:
   ```bash
   cd docker
   docker-compose down
   ```

### Local Development

1. **Setup**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   pip install -e .
   ```

2. **Configure**:
   ```bash
   cp config/.env.example .env
   # Edit .env and add your API key
   ```

3. **Run**:
   ```bash
   streamlit run src/app.py --server.port 8080
   # Or use: ./scripts/run.sh
   ```

4. **Access**: Open http://localhost:8080

## For Developers

### Initial Setup

```bash
# Clone and setup
git clone <repository-url>
cd stock-market-dashboard

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -e .

# Configure environment
cp config/.env.example .env
# Edit .env with your API key
```

### Development Workflow

```bash
# Run application
streamlit run src/app.py

# Run tests
pytest

# Check code quality
black src/
flake8 src/
mypy src/

# Validate structure
python scripts/validate_structure.py
```

### Docker Development

```bash
# Build image
docker build -f docker/Dockerfile -t stock-dashboard .

# Run container
docker run -p 8080:8080 -e ALPHA_VANTAGE_API_KEY=your_key stock-dashboard

# Or use docker-compose
cd docker
docker-compose up
```

## Project Structure

```
src/
├── core/              # Business logic (indicators, processing)
├── services/          # External APIs (Alpha Vantage)
├── managers/          # State management (watchlist, refresh)
├── ui/                # UI components (charts, styling)
├── app.py            # Main application
└── config.py         # Configuration

tests/                 # Test suite
docs/                  # Documentation
docker/                # Docker files
scripts/               # Utility scripts
```

## Common Commands

### Running
```bash
# Local
streamlit run src/app.py

# Docker
cd docker && docker-compose up
```

### Testing
```bash
# All tests
pytest

# With coverage
pytest --cov=src

# Specific test
pytest tests/test_technical_indicators.py
```

### Code Quality
```bash
# Format
black src/

# Lint
flake8 src/

# Type check
mypy src/
```

### Docker
```bash
# Build
docker build -f docker/Dockerfile -t stock-dashboard .

# Run
docker run -p 8080:8080 -e ALPHA_VANTAGE_API_KEY=key stock-dashboard

# Compose
cd docker
docker-compose up -d
docker-compose logs -f
docker-compose down
```

## Troubleshooting

### Import Errors

**Problem**: `ModuleNotFoundError: No module named 'src'`

**Solution**:
```bash
pip install -e .
```

### Port Already in Use

**Problem**: Port 8080 is already allocated

**Solution**:
```bash
# Find and stop the process
docker ps
docker stop <container-id>

# Or use a different port
docker run -p 8081:8080 ...
```

### API Rate Limiting

**Problem**: Too many API calls

**Solution**: The app caches responses for 5 minutes. Wait before making more requests.

## Documentation

- **README.md**: Project overview
- **docs/ARCHITECTURE.md**: System architecture
- **docs/DEVELOPMENT.md**: Development guide
- **docs/DEPLOYMENT_GUIDE.md**: Deployment instructions
- **CHANGELOG.md**: Version history

## Support

- Check documentation in `docs/`
- Review code comments
- Run validation: `python scripts/validate_structure.py`

## License

MIT License - See LICENSE file

---

**Version**: 1.0.0  
**Last Updated**: November 17, 2025
