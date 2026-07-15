#!/usr/bin/env python3
"""Minimal Azure SharedKey "signing proxy" used by test_aiven_azure_signature_delegation.

ClickHouse's AzureDelegatedKeyPolicy POSTs {"stringToSign": "..."} and expects
{"signature": "<base64>"} back. We compute the final Azure SharedKey step here, exactly as the
Azure SDK's SharedKeyPolicy::GetSignature would: the SharedKey signature is
base64(HMAC-SHA256(key=base64decode(account_key), msg=stringToSign)). The SDK still builds the
canonical Azure StringToSign locally; only this final HMAC step is delegated.

The account/key below are the public, well-known Azurite development credentials (a documented
constant, not a secret); the proxy uses the real key so Azurite accepts the produced signature.

Endpoints:
  POST /sign        -> correct signature (positive path)
  POST /sign_wrong  -> valid-shaped but deliberately wrong signature (negative path)
  GET  /health      -> "OK"
  GET  /count       -> number of /sign requests served (proves delegation was used)
"""
import base64
import hashlib
import hmac
import http.server
import json

# Azurite well-known development account key (public constant, not a secret).
ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
PORT = 8080

sign_count = 0


def _signature(string_to_sign: str) -> str:
    key = base64.b64decode(ACCOUNT_KEY)
    digest = hmac.new(key, string_to_sign.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):  # noqa: D401 - keep the container log quiet
        pass

    def _reply(self, status: int, body: bytes, content_type: str = "application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._reply(200, b"OK", "text/plain")
        elif self.path == "/count":
            self._reply(200, str(sign_count).encode("utf-8"), "text/plain")
        else:
            self._reply(404, b"not found", "text/plain")

    def do_POST(self):
        global sign_count
        length = int(self.headers.get("Content-Length", 0))
        payload = json.loads(self.rfile.read(length).decode("utf-8"))
        string_to_sign = payload["stringToSign"]

        if self.path == "/sign":
            sign_count += 1
            sig = _signature(string_to_sign)
            self._reply(200, json.dumps({"signature": sig}).encode("utf-8"))
        elif self.path == "/sign_wrong":
            # Valid-shaped (base64 of 32 bytes) but incorrect signature; Azurite must reject it.
            wrong = base64.b64encode(b"\x00" * 32).decode("utf-8")
            self._reply(200, json.dumps({"signature": wrong}).encode("utf-8"))
        else:
            self._reply(404, b"not found", "text/plain")


if __name__ == "__main__":
    httpd = http.server.HTTPServer(("0.0.0.0", PORT), Handler)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
