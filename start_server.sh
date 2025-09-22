#!/bin/bash
# TubeWhale Django Server Startup Script
# Production-ready Django server with Gunicorn

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting TubeWhale Django Server...${NC}"

# Default to production server if no args provided
if [ $# -eq 0 ]; then
    echo -e "${GREEN}🌟 Starting Gunicorn server on 0.0.0.0:8000${NC}"
    exec gunicorn tubewhale_project.wsgi:application \
        --bind 0.0.0.0:8000 \
        --workers 4 \
        --worker-class sync \
        --max-requests 1000 \
        --max-requests-jitter 100 \
        --timeout 30 \
        --keep-alive 2 \
        --log-level info \
        --access-logfile - \
        --error-logfile -
else
    # Execute the provided command
    exec "$@"
fi