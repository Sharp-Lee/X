#!/bin/bash
# Development startup script for MSR Retest Capture

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting MSR Retest Capture System...${NC}"

# Start PostgreSQL if not running
if ! pg_isready -q 2>/dev/null; then
    echo -e "${BLUE}Starting PostgreSQL...${NC}"
    brew services start postgresql@17
    sleep 2
fi

# Navigate to project root
cd "$(dirname "$0")/.."

# Start backend in background
echo -e "${BLUE}Starting backend...${NC}"
cd backend
source .venv/bin/activate
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000 &
BACKEND_PID=$!
cd ..

# Start frontend in background
echo -e "${BLUE}Starting frontend...${NC}"
cd frontend
npm run dev &
FRONTEND_PID=$!
cd ..

echo -e "${GREEN}System started!${NC}"
echo -e "  Backend:  http://localhost:8000"
echo -e "  Frontend: http://localhost:5173"
echo -e "  API Docs: http://localhost:8000/docs"
echo ""
echo "Press Ctrl+C to stop all services"

# Wait for Ctrl+C
trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT
wait
