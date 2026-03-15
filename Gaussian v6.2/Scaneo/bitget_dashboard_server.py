#!/usr/bin/env python3
import argparse
import json
import subprocess
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from build_bitget_dashboard import (
    BASE_DIR,
    build_html,
    latest_file,
    load_rows,
    load_summary,
    make_payload,
)


SCAN_LOCK = threading.Lock()
LATEST_HTML = BASE_DIR / "bitget_scanner_dashboard.html"
SCANNER_SCRIPT = BASE_DIR / "bitget_gaussian_scanner.py"


def latest_scan_paths() -> tuple[Path, Path]:
    csv_path = latest_file("bitget_gaussian_scan_*.csv")
    timestamp = csv_path.stem.split("_")[-1]
    candidates = sorted(
        path
        for path in BASE_DIR.glob(f"bitget_gaussian_scan_*_{timestamp}.json")
        if not path.name.endswith("_errors.json")
    )
    summary_path = candidates[-1] if candidates else latest_file("bitget_gaussian_scan_*.json")
    return csv_path, summary_path


def build_latest_dashboard() -> tuple[str, dict]:
    csv_path, summary_path = latest_scan_paths()
    rows = load_rows(csv_path)
    summary = load_summary(summary_path)
    payload = make_payload(rows, summary, csv_path, summary_path)
    html = build_html(payload)
    LATEST_HTML.write_text(html, encoding="utf-8")
    return html, payload


def run_scan(params: dict) -> dict:
    profile = str(params.get("profile", "base"))
    granularity = str(params.get("granularity", "4H"))
    lookback_days = int(params.get("lookback_days", 365))
    min_trades = int(params.get("min_trades", 0))
    max_workers = int(params.get("max_workers", 8))
    top = int(params.get("top", 20))
    max_symbols = params.get("max_symbols")
    strategy = str(params.get("strategy", "gaussian_v6_2"))
    if max_symbols is not None:
        max_symbols = int(max_symbols)

    command = [
        "python3",
        str(SCANNER_SCRIPT),
        "--profile",
        profile,
        "--strategy",
        strategy,
        "--granularity",
        granularity,
        "--lookback-days",
        str(lookback_days),
        "--min-trades",
        str(min_trades),
        "--max-workers",
        str(max_workers),
        "--top",
        str(top),
        "--config-json",
        json.dumps(params, ensure_ascii=False),
    ]
    if max_symbols is not None:
        command.extend(["--max-symbols", str(max_symbols)])
    
    print(f"Running command: {' '.join(command)}")
    cp = subprocess.run(command, cwd=BASE_DIR, capture_output=True, text=True, check=False)
    if cp.returncode != 0:
        print(f"Scanner failed with code {cp.returncode}")
        print(f"STDOUT: {cp.stdout}")
        print(f"STDERR: {cp.stderr}")
        raise Exception(f"scanner failed: {cp.stderr}")
    
    # The scanner prints JSON summary to stdout (possibly with other text before)
    raw_out = cp.stdout.strip()
    if not raw_out:
         print("Scanner stdout is empty!")
         raise Exception("Scanner stdout is empty")
         
    try:
        # Find first '{' to skip headers like "Iniciando escaneo..."
        start_idx = raw_out.find('{')
        if start_idx == -1:
            raise json.JSONDecodeError("No JSON object found", raw_out, 0)
        json_part = raw_out[start_idx:]
        output_json = json.loads(json_part)
    except json.JSONDecodeError as je:
        print(f"JSON Decode Error: {je}")
        print(f"Raw output: {raw_out}")
        raise
        
    # Rebuild HTML
    build_latest_dashboard()
    return output_json


class Handler(BaseHTTPRequestHandler):
    server_version = "BitgetDashboard/1.0"

    def _json(self, data: dict, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, html: str, status: int = HTTPStatus.OK) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path in {"/", "/index.html"}:
            html, _ = build_latest_dashboard()
            self._html(html)
            return
        if self.path == "/api/state":
            _, payload = build_latest_dashboard()
            self._json(payload)
            return
        self._json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/api/scan":
            self._json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return

        if not SCAN_LOCK.acquire(blocking=False):
            self._json({"error": "scan already running"}, status=HTTPStatus.CONFLICT)
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
            params = json.loads(raw.decode("utf-8") or "{}")
            result = run_scan(params)
            self._json({"ok": True, "scan": result})
        except subprocess.CalledProcessError as exc:
            self._json(
                {
                    "error": "scanner failed",
                    "stdout": exc.stdout[-2000:],
                    "stderr": exc.stderr[-2000:],
                },
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        except Exception as exc:  # noqa: BLE001
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
        finally:
            SCAN_LOCK.release()


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve Bitget dashboard with scan button")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    build_latest_dashboard()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(json.dumps({"url": f"http://{args.host}:{args.port}", "html": str(LATEST_HTML)}, ensure_ascii=False))
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
