#!/bin/bash
# Startup script for Stock Market Dashboard

# Activate virtual environment
source venv/bin/activate

# Run Streamlit on port 8080
streamlit run src/app.py --server.port 8080 --server.address localhost
