#!/usr/bin/env python3
"""
Simple HTTP server for testing wp-connectors HTTP sink
Listens on port 8081 and accepts POST requests
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys

class HTTPSinkHandler(BaseHTTPRequestHandler):
    """Handler for HTTP sink requests"""
    
    def do_POST(self):
        """Handle POST requests"""
        # Get content length
        content_length = int(self.headers.get('Content-Length', 0))
        
        # Read request body
        body = self.rfile.read(content_length)
        
        # Log request details
        print(f"\n{'='*60}")
        print(f"Received POST request to: {self.path}")
        print(f"Headers:")
        for header, value in self.headers.items():
            print(f"  {header}: {value}")
        
        # Try to decode and pretty-print body
        try:
            if self.headers.get('Content-Type', '').startswith('application/json'):
                data = json.loads(body.decode('utf-8'))
                print(f"Body (JSON):")
                print(json.dumps(data, indent=2))
            elif self.headers.get('Content-Encoding') == 'gzip':
                import gzip
                decompressed = gzip.decompress(body)
                print(f"Body (gzip compressed, {len(body)} bytes -> {len(decompressed)} bytes):")
                print(decompressed.decode('utf-8')[:500])  # First 500 chars
            else:
                print(f"Body ({len(body)} bytes):")
                print(body.decode('utf-8', errors='replace')[:500])  # First 500 chars
        except Exception as e:
            print(f"Body (raw, {len(body)} bytes): {body[:100]}")
            print(f"Parse error: {e}")
        
        print(f"{'='*60}\n")
        
        # Send success response
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        
        response = {
            "status": "success",
            "message": "Data received",
            "bytes_received": len(body)
        }
        self.wfile.write(json.dumps(response).encode('utf-8'))
    
    def do_GET(self):
        """Handle GET requests (for health checks)"""
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        
        response = {
            "status": "ok",
            "message": "HTTP Sink Test Server is running"
        }
        self.wfile.write(json.dumps(response).encode('utf-8'))
    
    def log_message(self, format, *args):
        """Override to customize logging"""
        # Suppress default logging, we do our own
        pass

def run_server(port=8081):
    """Run the HTTP server"""
    server_address = ('', port)
    httpd = HTTPServer(server_address, HTTPSinkHandler)
    
    print(f"🚀 HTTP Sink Test Server starting on port {port}")
    print(f"📡 Listening for POST requests at http://localhost:{port}/ingest")
    print(f"💡 Press Ctrl+C to stop\n")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n\n🛑 Server stopped")
        sys.exit(0)

if __name__ == '__main__':
    port = 8081
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    
    run_server(port)
