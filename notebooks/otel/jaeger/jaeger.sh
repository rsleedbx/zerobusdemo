#!/bin/bash

# Start Jaeger with Podman for ZeroBus OTEL Tracing
# Usage (from repo root): ./notebooks/otel/jaeger/jaeger.sh [--persistent]
# Usage (from this directory): ./jaeger.sh [--persistent]

set -e

CONTAINER_NAME="jaeger"
IMAGE="docker.io/jaegertracing/all-in-one:latest"
PERSISTENT=false

# Parse arguments
if [ "$1" == "--persistent" ]; then
    PERSISTENT=true
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Jaeger for OpenTelemetry tracing...${NC}"

# Check if podman is installed
if ! command -v podman &> /dev/null; then
    echo -e "${RED}❌ Podman is not installed. Please install podman first.${NC}"
    echo "Visit: https://podman.io/getting-started/installation"
    exit 1
fi

# Check if Jaeger is already running
if podman ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${YELLOW}⚠️  Jaeger container already exists.${NC}"
    read -p "Do you want to restart it? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Stopping existing container..."
        podman stop ${CONTAINER_NAME} 2>/dev/null || true
        podman rm ${CONTAINER_NAME} 2>/dev/null || true
    else
        echo -e "${GREEN}✅ Using existing Jaeger container${NC}"
        podman start ${CONTAINER_NAME} 2>/dev/null || true
        echo -e "${GREEN}🔗 Jaeger UI: http://localhost:16686${NC}"
        exit 0
    fi
fi

# Check if ports are available
check_port() {
    local port=$1
    if ss -tulpn 2>/dev/null | grep -q ":${port} "; then
        echo -e "${YELLOW}⚠️  Port ${port} is already in use${NC}"
        return 1
    fi
    return 0
}

# Check critical ports
PORTS_OK=true
for port in 16686 4317; do
    if ! check_port $port; then
        PORTS_OK=false
    fi
done

if [ "$PORTS_OK" = false ]; then
    echo -e "${RED}❌ Required ports are in use. Please free them or stop conflicting services.${NC}"
    exit 1
fi

# Pull the latest image
echo "Pulling Jaeger image..."
podman pull ${IMAGE}

# Run Jaeger
if [ "$PERSISTENT" = true ]; then
    echo -e "${GREEN}📁 Setting up persistent storage...${NC}"

    # Create volume if it doesn't exist
    if ! podman volume exists jaeger-data 2>/dev/null; then
        podman volume create jaeger-data
    fi

    # Run with persistent storage
    podman run -d \
        --name ${CONTAINER_NAME} \
        -v jaeger-data:/badger \
        -e SPAN_STORAGE_TYPE=badger \
        -e BADGER_EPHEMERAL=false \
        -e BADGER_DIRECTORY_VALUE=/badger/data \
        -e BADGER_DIRECTORY_KEY=/badger/key \
        -e COLLECTOR_OTLP_ENABLED=true \
        -p 16686:16686 \
        -p 4317:4317 \
        -p 4318:4318 \
        -p 14250:14250 \
        -p 14268:14268 \
        ${IMAGE}

    echo -e "${GREEN}✅ Jaeger started with persistent storage${NC}"
else
    # Run with ephemeral storage (default)
    podman run -d \
        --name ${CONTAINER_NAME} \
        -e COLLECTOR_OTLP_ENABLED=true \
        -p 16686:16686 \
        -p 4317:4317 \
        -p 4318:4318 \
        -p 14250:14250 \
        -p 14268:14268 \
        ${IMAGE}

    echo -e "${GREEN}✅ Jaeger started (ephemeral storage)${NC}"
fi

# Wait for Jaeger to be ready
echo "Waiting for Jaeger to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:16686 > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Verify Jaeger is running
if podman ps | grep -q ${CONTAINER_NAME}; then
    echo -e "${GREEN}✅ Jaeger is running successfully!${NC}"
    echo
    echo -e "${GREEN}📊 Access points:${NC}"
    echo -e "  • Jaeger UI:        ${GREEN}http://localhost:16686${NC}"
    echo -e "  • OTLP gRPC:        ${GREEN}localhost:4317${NC}"
    echo -e "  • OTLP HTTP:        ${GREEN}localhost:4318${NC}"
    echo
    echo -e "${GREEN}📝 OTLP (local Jaeger):${NC} point an app or a private notebook copy at"
    echo "  gRPC localhost:4317 or HTTP http://localhost:4318 (traces/metrics paths per OTLP/HTTP)."
    echo "  The repo Grafana demo is notebooks/otel/grafana/zerobus-otel.ipynb (Grafana Cloud, not Jaeger)."
    echo
    echo -e "${GREEN}🛑 To stop Jaeger:${NC} podman stop ${CONTAINER_NAME}"
    echo -e "${GREEN}🔄 To restart:${NC} podman start ${CONTAINER_NAME}"
    echo -e "${GREEN}🗑️  To remove:${NC} podman rm ${CONTAINER_NAME}"
    if [ "$PERSISTENT" = true ]; then
        echo -e "${GREEN}💾 To remove data:${NC} podman volume rm jaeger-data"
    fi
else
    echo -e "${RED}❌ Failed to start Jaeger. Check the logs:${NC}"
    echo "  podman logs ${CONTAINER_NAME}"
    exit 1
fi