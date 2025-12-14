# Stock Market Analytics Dashboard - Deployment Guide

## Overview

This guide provides complete instructions for deploying the Stock Market Analytics Dashboard using Docker.

## Prerequisites

- **Docker Desktop** installed ([Download](https://www.docker.com/products/docker-desktop))
- **Alpha Vantage API Key** ([Get free key](https://www.alphavantage.co/support/#api-key))

## Quick Deployment (5 minutes)

### Step 1: Configure Environment

```bash
# Copy the environment template
cp .env.example .env

# Edit .env and add your API key
# Change: ALPHA_VANTAGE_API_KEY=your_api_key_here
# To: ALPHA_VANTAGE_API_KEY=YOUR_ACTUAL_KEY
```

### Step 2: Start the Application

```bash
# Start with Docker Compose
docker-compose up -d
```

### Step 3: Access the Dashboard

Open your browser and navigate to:
```
http://localhost:8080
```

### Step 4: Stop the Application

```bash
docker-compose down
```

## Deployment Methods

### Method 1: Docker Compose (Recommended)

**Advantages:**
- Simplest deployment
- Environment variables from .env file
- Automatic restart on failure
- Health checks enabled

**Commands:**
```bash
# Start
docker-compose up -d

# View logs
docker-compose logs -f

# Restart
docker-compose restart

# Stop
docker-compose down

# Rebuild
docker-compose build --no-cache
docker-compose up -d
```

### Method 2: Docker CLI

**Advantages:**
- More control over configuration
- Useful for custom deployments
- Good for CI/CD pipelines

**Commands:**
```bash
# Build image
docker build -t stock-dashboard .

# Run container
docker run -d \
  --name stock-market-dashboard \
  -p 8080:8080 \
  -e ALPHA_VANTAGE_API_KEY=your_key \
  --restart unless-stopped \
  stock-dashboard

# View logs
docker logs -f stock-market-dashboard

# Stop and remove
docker stop stock-market-dashboard
docker rm stock-market-dashboard
```

### Method 3: Local Development

**Advantages:**
- Faster iteration during development
- Direct access to code
- No Docker overhead

**Commands:**
```bash
# Setup virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run application
streamlit run app.py --server.port 8080

# Or use the startup script
./run.sh
```

## Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `ALPHA_VANTAGE_API_KEY` | API key for stock data | Yes | WRFGZ4UZVE8OOV1A |

**Setting Environment Variables:**

**Docker Compose:**
```bash
# Edit .env file
ALPHA_VANTAGE_API_KEY=your_key_here
```

**Docker CLI:**
```bash
docker run -e ALPHA_VANTAGE_API_KEY=your_key ...
```

**Local Development:**
```bash
export ALPHA_VANTAGE_API_KEY=your_key
streamlit run app.py --server.port 8080
```

### Port Configuration

Default port: **8080**

**To change the port:**

**Docker Compose:**
```yaml
# Edit docker-compose.yml
ports:
  - "8081:8080"  # Use 8081 on host
```

**Docker CLI:**
```bash
docker run -p 8081:8080 ...  # Use 8081 on host
```

## Production Deployment

### Cloud Platforms

#### AWS ECS
```bash
# Build and push to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com
docker build -t stock-dashboard .
docker tag stock-dashboard:latest <account>.dkr.ecr.us-east-1.amazonaws.com/stock-dashboard:latest
docker push <account>.dkr.ecr.us-east-1.amazonaws.com/stock-dashboard:latest

# Deploy to ECS with environment variables
```

#### Google Cloud Run
```bash
# Build and deploy
gcloud builds submit --tag gcr.io/PROJECT_ID/stock-dashboard
gcloud run deploy stock-dashboard \
  --image gcr.io/PROJECT_ID/stock-dashboard \
  --platform managed \
  --port 8080 \
  --set-env-vars ALPHA_VANTAGE_API_KEY=your_key
```

#### Azure Container Instances
```bash
# Build and push to ACR
az acr build --registry myregistry --image stock-dashboard .

# Deploy
az container create \
  --resource-group myResourceGroup \
  --name stock-dashboard \
  --image myregistry.azurecr.io/stock-dashboard \
  --ports 8080 \
  --environment-variables ALPHA_VANTAGE_API_KEY=your_key
```

#### DigitalOcean App Platform
```bash
# Use docker-compose.yml or Dockerfile
# Set environment variables in App Platform UI
# Deploy from GitHub repository
```

### Reverse Proxy (Nginx)

```nginx
server {
    listen 80;
    server_name yourdomain.com;

    location / {
        proxy_pass http://localhost:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

### SSL/TLS with Let's Encrypt

```bash
# Install certbot
sudo apt-get install certbot python3-certbot-nginx

# Get certificate
sudo certbot --nginx -d yourdomain.com

# Auto-renewal
sudo certbot renew --dry-run
```

## Monitoring

### Health Checks

**Docker Compose includes health check:**
```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8080/_stcore/health"]
  interval: 30s
  timeout: 10s
  retries: 3
```

**Check health status:**
```bash
docker-compose ps
docker inspect stock-market-dashboard | grep Health
```

### Logs

**View logs:**
```bash
# Docker Compose
docker-compose logs -f

# Docker CLI
docker logs -f stock-market-dashboard

# Last 100 lines
docker logs --tail 100 stock-market-dashboard
```

### Resource Usage

**Monitor resources:**
```bash
# Real-time stats
docker stats stock-market-dashboard

# Container info
docker inspect stock-market-dashboard
```

## Troubleshooting

### Common Issues

#### 1. Port Already in Use
```bash
# Find process using port 8080
lsof -i :8080  # macOS/Linux
netstat -ano | findstr :8080  # Windows

# Kill process or change port in docker-compose.yml
```

#### 2. Container Won't Start
```bash
# Check logs
docker-compose logs -f

# Rebuild without cache
docker-compose build --no-cache
docker-compose up -d

# Check Docker daemon
docker ps
```

#### 3. Environment Variables Not Loading
```bash
# Verify .env file
cat .env

# Check container environment
docker exec stock-market-dashboard env | grep ALPHA

# Restart container
docker-compose restart
```

#### 4. API Rate Limit Errors
```bash
# Free tier: 5 requests/minute
# Wait 60 seconds between requests
# Consider upgrading API key
```

#### 5. Image Build Fails
```bash
# Check Dockerfile syntax
docker build -t stock-dashboard . --no-cache

# Verify requirements.txt
cat requirements.txt

# Check Python version
docker run python:3.11-slim python --version
```

### Debug Mode

**Run container interactively:**
```bash
docker run -it --rm \
  -p 8080:8080 \
  -e ALPHA_VANTAGE_API_KEY=your_key \
  stock-dashboard \
  /bin/bash
```

**Access running container:**
```bash
docker exec -it stock-market-dashboard /bin/bash
```

## Maintenance

### Updates

**Update application:**
```bash
# Pull latest code
git pull

# Rebuild and restart
docker-compose build --no-cache
docker-compose up -d
```

**Update dependencies:**
```bash
# Edit requirements.txt
# Rebuild image
docker-compose build --no-cache
docker-compose up -d
```

### Backup

**Backup configuration:**
```bash
# Backup .env file
cp .env .env.backup

# Backup docker-compose.yml
cp docker-compose.yml docker-compose.yml.backup
```

### Cleanup

**Remove unused resources:**
```bash
# Remove stopped containers
docker container prune

# Remove unused images
docker image prune

# Remove all unused resources
docker system prune -a
```

## Security Best Practices

1. **Never commit .env file to git**
   - Already in .gitignore
   - Use .env.example as template

2. **Use secrets management in production**
   - AWS Secrets Manager
   - Azure Key Vault
   - Google Secret Manager
   - HashiCorp Vault

3. **Keep dependencies updated**
   ```bash
   pip list --outdated
   ```

4. **Use specific version tags**
   ```dockerfile
   FROM python:3.11.5-slim  # Instead of :latest
   ```

5. **Scan for vulnerabilities**
   ```bash
   docker scan stock-dashboard
   ```

## Performance Optimization

### Image Size

**Current image size:** ~500MB (Python 3.11-slim + dependencies)

**Optimization tips:**
- Using slim base image ✓
- Multi-stage builds (not needed for this app)
- Minimize layers ✓
- Use .dockerignore

### Runtime Performance

**Resource limits:**
```yaml
# docker-compose.yml
services:
  stock-dashboard:
    deploy:
      resources:
        limits:
          cpus: '1'
          memory: 512M
```

**Caching:**
- Session state caching enabled ✓
- API response caching ✓

## Support

### Documentation
- README.md - General information
- DOCKER_VALIDATION.md - Validation report
- This file - Deployment guide

### API Documentation
- Alpha Vantage: https://www.alphavantage.co/documentation/

### Docker Documentation
- Docker: https://docs.docker.com/
- Docker Compose: https://docs.docker.com/compose/

## Summary

The Stock Market Analytics Dashboard is fully containerized and ready for deployment. Choose your preferred method:

- **Quick Start**: `docker-compose up -d`
- **Production**: Deploy to cloud platform
- **Development**: Use local Python environment

All configurations are production-ready with health checks, restart policies, and proper environment variable management.
