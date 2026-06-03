"""Three localhost helper servers, run inside the ClickHouse node container:

  * an HTTPS "origin" that 302-redirects /data -> http://localhost:<data>/data
    (the https->http DOWNGRADE that patch 065 must reject);
  * an HTTP "origin" that 302-redirects /data -> http://localhost:<data>/data
    (a same-scheme CONTROL that must keep working);
  * an HTTP "data" server that serves two JSONEachRow rows.

All three run as daemon threads in a single process so the test only manages
one subprocess. TLS uses a self-signed cert (generated host-side and copied in);
the node's <openSSL><client> is set to verificationMode=none, so the handshake
succeeds and ClickHouse reaches the redirect-follow logic.
"""

import http.server
import ssl
import sys
import threading

DATA_ROWS = b'{"a":1}\n{"a":2}\n'


def _make_handler(behavior, data_port):
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path == "/":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")
                return
            if behavior == "data":
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.end_headers()
                self.wfile.write(DATA_ROWS)
                return
            # redirect behavior: always point at the plain-HTTP data server
            self.send_response(302)
            self.send_header("Location", f"http://localhost:{data_port}/data")
            self.end_headers()

    return Handler


def _serve(port, handler, cert=None, key=None):
    httpd = http.server.ThreadingHTTPServer(("localhost", port), handler)
    if cert and key:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=cert, keyfile=key)
        httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
    httpd.serve_forever()


if __name__ == "__main__":
    https_origin_port = int(sys.argv[1])
    http_origin_port = int(sys.argv[2])
    data_port = int(sys.argv[3])
    cert_file = sys.argv[4]
    key_file = sys.argv[5]

    threading.Thread(
        target=_serve,
        args=(data_port, _make_handler("data", data_port)),
        daemon=True,
    ).start()
    threading.Thread(
        target=_serve,
        args=(http_origin_port, _make_handler("redirect", data_port)),
        daemon=True,
    ).start()
    # HTTPS origin on the main thread (keeps the process alive).
    _serve(
        https_origin_port,
        _make_handler("redirect", data_port),
        cert=cert_file,
        key=key_file,
    )
