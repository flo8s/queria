#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUTPUT_DIR="$REPO_DIR/.dev-serve"
PORT="${1:-4000}"

echo "Serving $OUTPUT_DIR at http://localhost:$PORT"
python3 -c "
import functools
from http.server import HTTPServer, SimpleHTTPRequestHandler

class H(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        super().end_headers()
    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

HTTPServer(('', $PORT), functools.partial(H, directory='$OUTPUT_DIR')).serve_forever()
"
