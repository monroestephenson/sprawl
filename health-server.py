#!/usr/bin/env python3

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import time
import threading
import logging
import os
import socket

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
                'uptime': f"{time.time() - start_time:.2f} seconds",
                'pod_ip': socket.gethostbyname(socket.gethostname())
            }
            
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'404 Not Found')

def run_server():
    # Get port from environment or use default
    port = int(os.environ.get('SPRAWL_HTTP_PORT', 8080))
    
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, HealthHandler)
    logger.info(f"Starting health check server on 0.0.0.0:{port}")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server stopped by keyboard interrupt")
    finally:
        httpd.server_close()
        logger.info("Server closed")

if __name__ == '__main__':
    run_server() 