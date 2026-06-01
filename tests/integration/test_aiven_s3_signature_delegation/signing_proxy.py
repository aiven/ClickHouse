#!/usr/bin/env python3
"""Minimal SigV4 "signing proxy" used by test_aiven_s3_signature_delegation.

ClickHouse's AWSAuthV4DelegatedSigner POSTs {"canonicalRequest": "..."} and expects
{"signature": "<hex>"} back. We compute the final SigV4 step here, exactly as the AWS SDK
would (see contrib/aws .../AWSAuthV4Signer.cpp): the proxy only receives the canonical
request, so the date is parsed from its x-amz-date header while region/service/secret are
configured to match the S3 disk (MinIO secret, us-east-1, s3).

Endpoints:
  POST /sign        -> correct signature (positive path)
  POST /sign_wrong  -> deliberately wrong signature (negative path)
  GET  /health      -> "OK"
  GET  /count       -> number of /sign requests served (proves delegation was used)
"""
import hashlib
import hmac
import http.server
import json

SECRET = "ClickHouse_Minio_P@ssw0rd"
REGION = "us-east-1"
SERVICE = "s3"
PORT = 8080

sign_count = 0


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _amzdate_from_canonical_request(canonical_request: str) -> str:
    # Canonical headers are lowercased "name:value" lines; pick out x-amz-date.
    for line in canonical_request.split("\n"):
        if line.startswith("x-amz-date:"):
            return line.split(":", 1)[1].strip()
    raise ValueError("x-amz-date not found in canonical request")


def _signature(canonical_request: str) -> str:
    amzdate = _amzdate_from_canonical_request(canonical_request)
    simple_date = amzdate[:8]
    scope = f"{simple_date}/{REGION}/{SERVICE}/aws4_request"
    canonical_hash = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    string_to_sign = f"AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{canonical_hash}"

    k_date = _hmac(("AWS4" + SECRET).encode("utf-8"), simple_date)
    k_region = _hmac(k_date, REGION)
    k_service = _hmac(k_region, SERVICE)
    k_signing = _hmac(k_service, "aws4_request")
    return hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()


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
        canonical_request = payload["canonicalRequest"]

        if self.path == "/sign":
            sign_count += 1
            sig = _signature(canonical_request)
            self._reply(200, json.dumps({"signature": sig}).encode("utf-8"))
        elif self.path == "/sign_wrong":
            # Valid-shaped but incorrect signature; MinIO must reject it.
            self._reply(200, json.dumps({"signature": "00" * 32}).encode("utf-8"))
        else:
            self._reply(404, b"not found", "text/plain")


if __name__ == "__main__":
    httpd = http.server.HTTPServer(("0.0.0.0", PORT), Handler)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
