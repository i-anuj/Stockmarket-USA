# Project Structure

## Architecture Pattern

Layered architecture with clear separation of concerns:
- **Core Layer**: Business logic and domain functionality
- **Services Layer**: External integrations and API communication
- **Managers Layer**: State management and user preferences
- **UI Layer**: Presentation and user interaction
- **Application Layer**: Orchestration and routing

## Directory Organization

```
src/
├── core/                          # Business logic layer
│   ├── technical_indicators.py   # RSI, MACD, Bollinger Bands, MA calculations
│   └── data_processor.py         # Data transformation and metrics
├── services/                      # External services layer
│   └── api_service.py            # Alpha Vantage API integration
├── managers/                      # State management layer
│   ├── watchlist_manager.py      # Watchlist state (session-based)
│   └── refresh_manager.py        # Auto-refresh state and timing
├── ui/                           # Presentation layer
│   ├── components.py             # Reusable UI components (cards, buttons, etc.)
│   └── charts.py                 # Plotly chart components
├── utils/                        # Utility functions (currently empty)
├── app.py                        # Main Streamlit application entry point
└── config.py                     # Centralized configuration

tests/                            # Test suite
├── test_api_service.py
├── test_managers.py
└── test_technical_indicators.py

docs/                             # Documentation
├── ARCHITECTURE.md               # Architecture details
├── DEVELOPMENT.md                # Development guide
├── DEPLOYMENT_GUIDE.md
└── ENHANCEMENT_ROADMAP.md

docker/                           # Docker configuration
├── Dockerfile
├── docker-compose.yml
└── .dockerignore

config/                           # Configuration templates
└── .env.example

scripts/                          # Utility scripts
├── run.sh
└── validate_structure.py
```

## Module Responsibilities

### Core Layer (`src/core/`)
- Pure business logic with no side effects
- Technical indicator calculations
- Data transformation and validation
- Easily testable with mock data

### Services Layer (`src/services/`)
- External API integration (Alpha Vantage)
- HTTP request handling and error management
- Rate limiting and retry logic
- Response parsing and caching

### Managers Layer (`src/managers/`)
- Streamlit session state management
- User preference persistence
- State validation and constraints
- Provides controlled access to state

### UI Layer (`src/ui/`)
- Reusable Streamlit components
- Plotly chart configurations
- Styling and theming utilities
- Loading states and notifications

## Import Conventions

Always use absolute imports from `src`:
```python
# Standard library
import os
from typing import Optional, Dict

# Third-party
import pandas as pd
import streamlit as st

# Local - use absolute imports
from src import config
from src.services import api_service
from src.core import technical_indicators
```

## File Naming

- Python modules: `snake_case.py`
- Test files: `test_*.py`
- Documentation: `UPPERCASE.md`
- Config files: lowercase or `.env`

## State Management

All application state stored in Streamlit session state:
- Accessed via `st.session_state`
- Managed through manager modules
- Never manipulate session state directly in UI code

## Configuration

- Environment variables in `.env` file
- Application constants in `src/config.py`
- No hardcoded configuration in business logic
- Use `os.environ.get()` with sensible defaults

## Testing Structure

- Mirror `src/` structure in `tests/`
- One test file per source module
- Use pytest fixtures for common setup
- Aim for 80%+ code coverage
