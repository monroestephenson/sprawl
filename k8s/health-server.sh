#!/bin/sh
# Simple health check server using Python's built-in HTTP server

cat > /tmp/health_server.py << 'EOF'
#!/usr/bin/env python3

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import time
import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('health_server')

# Track start time
start_time = time.time()

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        logger.info(f"Health check request received from {self.client_address[0]}, path: {self.path}")
        
        if self.path == '/health' or self.path == '/healthz':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            response = {
                'status': 'ok',
                'uptime': f"{time.time() - start_time:.2f} seconds"
            }
            
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'404 Not Found')

def run_server():
    server_address = ('', 8080)
    httpd = HTTPServer(server_address, HealthHandler)
    logger.info(f"Starting health check server on port 8080")
    httpd.serve_forever()

if __name__ == '__main__':
    # Start the server in a separate thread
    thread = threading.Thread(target=run_server)
    thread.daemon = True
    thread.start()
    
    logger.info("Health server started in background")
    
    # Keep the main thread alive
    while True:
        time.sleep(60)
EOF

# Make it executable
chmod +x /tmp/health_server.py

# Start the health server
python3 /tmp/health_server.py 