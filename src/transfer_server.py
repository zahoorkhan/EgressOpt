import hashlib
import hmac
import os
import secrets
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path.cwd()
FILE = ROOT / "transfer_staging" / "azure_remaining_sf1.tar.gz"
TOKEN_FILE = ROOT / "transfer_staging" / ".transfer_token"

if not FILE.is_file():
    raise SystemExit(f"Missing file: {FILE}")

if not TOKEN_FILE.exists():
    TOKEN_FILE.write_text(secrets.token_urlsafe(48))
    os.chmod(TOKEN_FILE, 0o600)

TOKEN = TOKEN_FILE.read_text().strip()

class Handler(BaseHTTPRequestHandler):
    def authorized(self):
        supplied = self.headers.get("Authorization", "")
        return hmac.compare_digest(supplied, f"Bearer {TOKEN}")

    def respond(self, send_body):
        if not self.authorized():
            self.send_response(401)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if self.path != "/azure_remaining_sf1.tar.gz":
            self.send_error(404)
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(FILE.stat().st_size))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()

        if send_body:
            with FILE.open("rb") as f:
                while chunk := f.read(1024 * 1024):
                    self.wfile.write(chunk)

    def do_GET(self):
        self.respond(True)

    def do_HEAD(self):
        self.respond(False)

    def log_message(self, format, *args):
        pass

server = ThreadingHTTPServer(("127.0.0.1", 8765), Handler)
print("Authenticated transfer server running on 127.0.0.1:8765", flush=True)

try:
    server.serve_forever()
except KeyboardInterrupt:
    server.server_close()
