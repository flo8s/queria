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
    def do_GET(self):
        range_header = self.headers.get('Range')
        if not range_header:
            return super().do_GET()
        path = self.translate_path(self.path)
        try:
            f = open(path, 'rb')
        except OSError:
            self.send_error(404)
            return
        import os, re
        size = os.fstat(f.fileno()).st_size
        m = re.match(r'bytes=(\d*)-(\d*)', range_header)
        if not m:
            f.close()
            return super().do_GET()
        start = int(m.group(1)) if m.group(1) else 0
        end = int(m.group(2)) if m.group(2) else size - 1
        end = min(end, size - 1)
        length = end - start + 1
        self.send_response(206)
        self.send_header('Content-Type', self.guess_type(path))
        self.send_header('Content-Length', str(length))
        self.send_header('Content-Range', f'bytes {start}-{end}/{size}')
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()
        f.seek(start)
        self.wfile.write(f.read(length))
        f.close()
    def do_HEAD(self):
        path = self.translate_path(self.path)
        import os
        if os.path.isfile(path):
            self.send_response(200)
            self.send_header('Content-Length', str(os.path.getsize(path)))
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()
        else:
            super().do_HEAD()

HTTPServer(('', $PORT), functools.partial(H, directory='$OUTPUT_DIR')).serve_forever()
"
