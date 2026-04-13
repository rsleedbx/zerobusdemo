# Setting up Jaeger with Podman for ZeroBus OTEL Tracing

## Quick Start

From the repo root you can run **`./notebooks/otel/jaeger/jaeger.sh`** (optional **`--persistent`**); it pulls the image, checks ports, and starts the all-in-one container.

### 1. Run Jaeger with Podman

```bash
# Pull and run Jaeger all-in-one container
podman run -d \
  --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 14250:14250 \
  -p 14268:14268 \
  -p 14269:14269 \
  -p 9411:9411 \
  docker.io/jaegertracing/all-in-one:latest

# Verify it's running
podman ps | grep jaeger
```

### 2. Access Jaeger UI

Open your browser and navigate to: http://localhost:16686

### 3. Useful Podman Commands

```bash
# Check if Jaeger is running
podman ps -a | grep jaeger

# View Jaeger logs
podman logs jaeger

# Stop Jaeger
podman stop jaeger

# Start Jaeger again
podman start jaeger

# Remove Jaeger container
podman rm jaeger

# Check port bindings
podman port jaeger
```

## Port Explanations

- **16686**: Jaeger UI (Web interface)
- **4317**: OTLP gRPC receiver (OpenTelemetry traces)
- **4318**: OTLP HTTP receiver (Alternative to gRPC)
- **14250**: Jaeger gRPC endpoint (Legacy)
- **14268**: Jaeger HTTP endpoint (Direct trace submission)
- **14269**: Admin port (Health check)
- **9411**: Zipkin compatible endpoint

## Rootless Podman Considerations

If you're running rootless Podman (default on most systems):

```bash
# If you get permission errors, you might need to use higher ports
podman run -d \
  --name jaeger \
  -p 16686:16686 \
  -p 14317:4317 \
  -p 14318:4318 \
  docker.io/jaegertracing/all-in-one:latest

# Then point a local OTLP client at http://localhost:14317 (gRPC) or the matching HTTP port.
```

## Persistent Storage (Optional)

To keep traces between container restarts:

```bash
# Create a volume for persistent storage
podman volume create jaeger-data

# Run with persistent storage
podman run -d \
  --name jaeger \
  -v jaeger-data:/badger \
  -e SPAN_STORAGE_TYPE=badger \
  -e BADGER_EPHEMERAL=false \
  -e BADGER_DIRECTORY_VALUE=/badger/data \
  -e BADGER_DIRECTORY_KEY=/badger/key \
  -p 16686:16686 \
  -p 4317:4317 \
  docker.io/jaegertracing/all-in-one:latest
```

## Troubleshooting

### Check if ports are available
```bash
# Check if ports are already in use
ss -tulpn | grep -E '16686|4317'
# or
netstat -tulpn | grep -E '16686|4317'
```

### Test OTLP connectivity
```bash
# Test if Jaeger is accepting OTLP connections
curl -v http://localhost:4318/v1/traces
# Should return 405 Method Not Allowed (which is expected for GET)
```

### View container details
```bash
# Inspect the container
podman inspect jaeger

# Check resource usage
podman stats jaeger
```

## Running in Production Mode

For better performance with more traces:

```bash
podman run -d \
  --name jaeger \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -e COLLECTOR_OTLP_ENABLED=true \
  -e SPAN_STORAGE_TYPE=badger \
  -e BADGER_EPHEMERAL=false \
  -e BADGER_DIRECTORY_VALUE=/badger/data \
  -e BADGER_DIRECTORY_KEY=/badger/key \
  -v jaeger-data:/badger \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 14250:14250 \
  --memory="2g" \
  --cpus="2" \
  docker.io/jaegertracing/all-in-one:latest
```

## Integration with the repo OTEL demo

The committed Grafana flow is **`notebooks/otel/grafana/zerobus-otel.ipynb`** (exports to Grafana Cloud via HTTP OTLP). It does not target Jaeger.

For local Jaeger, send OTLP to **gRPC `localhost:4317`** or **HTTP `http://localhost:4318`** from your own app or a private notebook fork that wires OpenTelemetry exporters accordingly.

## Viewing Traces in Jaeger

1. Open http://localhost:16686
2. Select "zerobus-ingest" from the Service dropdown
3. Click "Find Traces"
4. Click on any trace to see the detailed span breakdown
5. Use the timeline view to analyze performance bottlenecks

## Clean Up

```bash
# Stop and remove container
podman stop jaeger
podman rm jaeger

# Remove volume if using persistent storage
podman volume rm jaeger-data

# Remove image if no longer needed
podman rmi docker.io/jaegertracing/all-in-one:latest
```