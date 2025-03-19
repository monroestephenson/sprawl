#!/bin/sh

# Script to fix HTTP port issues in sprawl
# This will be run as an init container

# Create a shell script validator instead of relying on Go
cat > /data/http-port-validator.sh <<'EOF'
#!/bin/sh

# Get the HTTP port from the environment
HTTP_PORT="${SPRAWL_HTTP_PORT:-8080}"
if [ -z "$HTTP_PORT" ] || [ "$HTTP_PORT" -eq 0 ]; then
  echo "[HTTP-PORT-VALIDATOR] Warning: Invalid or missing HTTP port. Setting to 8080"
  HTTP_PORT=8080
fi

# Log that we're running
echo "[HTTP-PORT-VALIDATOR] Running with HTTP port $HTTP_PORT"

# Create a file indicating the HTTP port
echo "$HTTP_PORT" > /data/http_port.txt

# Make sure the environment has the proper value
export SPRAWL_HTTP_PORT=$HTTP_PORT

# Log all environment variables for debugging
echo "[HTTP-PORT-VALIDATOR] Environment variables:"
env | sort
EOF

chmod +x /data/http-port-validator.sh

# Create a run script that uses the shell validator
cat > /data/run-validator.sh <<'EOF'
#!/bin/sh
# Run the validator script
/data/http-port-validator.sh > /data/validator.log 2>&1 &
EOF

chmod +x /data/run-validator.sh 