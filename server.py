"""Local capture server — receives bookmarklet POSTs, parses, uploads to Dropbox.

Start:  python server.py
Test:   curl -X POST http://localhost:8585/capture -H 'Content-Type: application/json' -d '{...}'
"""

import base64
import concurrent.futures
import csv
import json
import os
import sys
import tempfile
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

# Ensure local imports work when running this file directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

load_dotenv()

from cloud.upload import (
    get_dropbox_client,
    build_base_name,
    capitalize_site,
    upload_file,
    DROPBOX_BASE_PATH,
)

# ---------------------------------------------------------------------------
# Custom error types & validation
# ---------------------------------------------------------------------------


class CaptureError(Exception):
    """Base class for expected capture/validation errors."""
    status_code = 400


class SiteDetectionError(CaptureError):
    """Raised when a URL does not map to a known site."""
    pass


class ParserNotFoundError(CaptureError):
    """Raised when no parser is registered for a detected site."""
    pass


class DropboxUploadError(CaptureError):
    """Raised when Dropbox upload fails."""
    status_code = 502


MAX_HTML_SIZE = 5_000_000          # ~5MB
MAX_SCREENSHOT_SIZE = 5_000_000    # decoded bytes (rough guard)


def validate_payload(data: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """Validate incoming JSON payload and return (url, html, category, screenshot_b64)."""
    if not isinstance(data, dict):
        raise CaptureError("Payload must be a JSON object")

    url = data.get("url")
    html = data.get("html")
    category = data.get("category")
    screenshot_b64 = data.get("screenshot_b64", "")

    if not url or not isinstance(url, str):
        raise CaptureError("Missing or invalid 'url'")
    if not html or not isinstance(html, str):
        raise CaptureError("Missing or invalid 'html'")
    if len(html.encode("utf-8")) > MAX_HTML_SIZE:
        raise CaptureError("HTML payload too large")
    if not category or not isinstance(category, str):
        raise CaptureError("Missing or invalid 'category'")
    if screenshot_b64 and not isinstance(screenshot_b64, str):
        raise CaptureError("Invalid 'screenshot_b64'")

    return url, html, category, screenshot_b64


# ---------------------------------------------------------------------------
# Site & parser maps
# ---------------------------------------------------------------------------

# Site detection: hostname substring -> site key
SITE_MAP = {
    "amazon": "amazon",
    "walmart": "walmart",
    "target": "target",
    "costco": "costco",
    "1688": "1688",
}

# Parser modules (lazy-imported)
PARSER_MAP = {
    "amazon": "webpage_data_parsing.parse_amazon",
    "walmart": "webpage_data_parsing.parse_walmart",
    "target": "webpage_data_parsing.parse_target",
}


def detect_site(url: str) -> str:
    """Extract site key from URL hostname."""
    hostname = urlparse(url).hostname or ""
    hostname = hostname.lower()
    for key, site in SITE_MAP.items():
        if key in hostname:
            return site
    raise SiteDetectionError("Unknown site: %s" % hostname)


def get_parser(site: str):
    """Import and return the parser module for a site. Raises if not found."""
    module_name = PARSER_MAP.get(site)
    if not module_name:
        raise ParserNotFoundError("No parser for site: %s" % site)
    import importlib

    return importlib.import_module(module_name)


def process_capture(data: Dict[str, Any]) -> Dict[str, Any]:
    """Main pipeline: validate payload, run parser, upload to Dropbox."""
    start_time = time.time()

    # 1. Validate input
    url, html, category, screenshot_b64 = validate_payload(data)

    # 2. Detect site and load parser
    site = detect_site(url)
    parser_mod = get_parser(site)

    base_name = build_base_name(site, category)
    site_dir = capitalize_site(site)
    folder_path = "%s/%s/%s" % (DROPBOX_BASE_PATH, site_dir, base_name)

    tmp_files = []
    try:
        # 3. Save capture JSON
        capture_data = {
            "url": url,
            "title": data.get("title", ""),
            "timestamp": data.get("timestamp", ""),
            "viewport": data.get("viewport", {}),
            "scrollHeight": data.get("scrollHeight", 0),
            "scrollY": data.get("scrollY", 0),
            "html": html,
        }
        json_fd, json_path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(json_fd, "w", encoding="utf-8") as f:
            json.dump(capture_data, f)
        tmp_files.append(json_path)

        # 4. Save screenshot JPEG (if provided)
        img_path = None
        if screenshot_b64:
            img_fd, img_path = tempfile.mkstemp(suffix=".jpg")
            with os.fdopen(img_fd, "wb") as f:
                decoded = base64.b64decode(screenshot_b64)
                if len(decoded) > MAX_SCREENSHOT_SIZE:
                    raise CaptureError("Screenshot payload too large")
                f.write(decoded)
            tmp_files.append(img_path)

        # 5. Parse HTML -> rows
        rows = parser_mod.parse_html_string(html, category)
        products_found = len(rows)

        # 6. Write CSV
        csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(csv_fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=parser_mod.CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        tmp_files.append(csv_path)

        # 7. Upload all to Dropbox (in parallel)
        dbx = get_dropbox_client()
        uploads = [
            (dbx, json_path, "%s/%s.json" % (folder_path, base_name)),
            (dbx, csv_path, "%s/%s.csv" % (folder_path, base_name)),
        ]
        if img_path:
            uploads.append((dbx, img_path, "%s/%s.jpg" % (folder_path, base_name)))

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(upload_file, *args) for args in uploads]
                for future in concurrent.futures.as_completed(futures):
                    future.result()  # raises on failure
        except Exception as e:
            # Wrap any Dropbox/upload error in a specific error type
            raise DropboxUploadError(f"Dropbox upload failed: {e}")

        duration = time.time() - start_time
        return {
            "success": True,
            "site": site,
            "products_found": products_found,
            "dropbox_folder": folder_path,
            "elapsed_seconds": round(duration, 3),
        }
    finally:
        for f in tmp_files:
            try:
                os.unlink(f)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------


class CaptureHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self._send_cors_headers()
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(
            b"<html><body><h1>Capture server is running</h1>"
            b"<p>Use the bookmarklet to send captures here.</p></body></html>"
        )

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()

    def do_POST(self):
        if self.path != "/capture":
            self._send_json(404, {"success": False, "error": "Not found"})
            return

        # Read and parse JSON body
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)
        except (json.JSONDecodeError, ValueError) as e:
            self._send_json(400, {"success": False, "error": "Invalid JSON: %s" % e})
            return

        # Process capture with better error handling
        try:
            result = process_capture(data)
            self._send_json(200, result)
        except CaptureError as e:
            # Expected, user-facing errors (validation, unknown site, etc.)
            status = getattr(e, "status_code", 400)
            self._send_json(status, {"success": False, "error": str(e)})
        except Exception:
            # Unexpected internal errors: log full details, return generic message
            import traceback

            traceback.print_exc()
            self._send_json(500, {"success": False, "error": "Internal server error"})

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS, GET")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_json(self, status: int, data: Dict[str, Any]):
        self.send_response(status)
        self._send_cors_headers()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format, *args):
        print("[%s] %s" % (datetime.now().strftime("%H:%M:%S"), format % args))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    server = HTTPServer(("localhost", 8585), CaptureHandler)
    print("Capture server running on http://localhost:8585")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.server_close()


if __name__ == "__main__":
    main()
