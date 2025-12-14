# Stock Market Analytics Dashboard - Documentation

Welcome to the Stock Market Analytics Dashboard documentation.

## Documentation Index

### Getting Started
- [Main README](../README.md) - Project overview and quick start
- [Quick Start Guide](../QUICK_START.md) - Fast setup instructions
- [Changelog](../CHANGELOG.md) - Version history and changes

### For Developers
- [Development Guide](DEVELOPMENT.md) - Setup, workflow, and coding standards
- [Architecture Documentation](ARCHITECTURE.md) - System design and patterns

### For Deployment
- [Deployment Guide](DEPLOYMENT_GUIDE.md) - Production deployment instructions

### Future Planning
- [Enhancement Roadmap](ENHANCEMENT_ROADMAP.md) - Planned features and improvements

## Quick Links

### Setup & Running
- [Docker Setup](../QUICK_START.md#docker-recommended)
- [Local Development](../QUICK_START.md#local-development)
- [Environment Configuration](DEPLOYMENT_GUIDE.md#environment-variables)

### Development
- [Project Structure](ARCHITECTURE.md#project-structure)
- [Coding Standards](DEVELOPMENT.md#coding-standards)
- [Testing Guide](DEVELOPMENT.md#testing)

### Deployment
- [Docker Deployment](DEPLOYMENT_GUIDE.md#docker-deployment)
- [Production Checklist](DEPLOYMENT_GUIDE.md#production-checklist)

## Project Overview

The Stock Market Analytics Dashboard is an educational tool for analyzing stock market data with real-time visualizations and technical indicators.

### Key Features
- Real-time stock data from Alpha Vantage API
- Technical indicators (RSI, MACD, Bollinger Bands, Moving Averages)
- Interactive charts with Plotly
- Watchlist management
- Auto-refresh functionality
- BUY/SELL/HOLD signal generation
- Docker containerization

### Technology Stack
- **Frontend**: Streamlit 1.28.0
- **Visualization**: Plotly
- **Data Processing**: Pandas, NumPy
- **API**: Alpha Vantage REST API
- **Containerization**: Docker & Docker Compose
- **Testing**: pytest
- **Code Quality**: Black, flake8, mypy

## Project Structure

```
stock-market-dashboard/
├── src/                   # Source code
│   ├── core/             # Business logic
│   ├── services/         # External services
│   ├── managers/         # State management
│   ├── ui/               # UI components
│   ├── app.py            # Main application
│   └── config.py         # Configuration
├── tests/                 # Test suite
├── docs/                  # Documentation (you are here)
├── docker/                # Docker configuration
├── scripts/               # Utility scripts
├── config/                # Configuration files
└── .github/               # CI/CD workflows
```

## Getting Help

1. Check the [Quick Start Guide](../QUICK_START.md) for setup issues
2. Review the [Development Guide](DEVELOPMENT.md) for development questions
3. Consult the [Architecture Documentation](ARCHITECTURE.md) for design questions
4. Check the [Deployment Guide](DEPLOYMENT_GUIDE.md) for deployment issues

## Contributing

When updating documentation:
- Keep it clear and concise
- Include code examples where helpful
- Update cross-references when files change
- Test all code examples

## License

This documentation is part of the Stock Market Analytics Dashboard project and is licensed under the MIT License. See [LICENSE](../LICENSE) for details.

---

**Version**: 1.0.0  
**Last Updated**: November 2025
