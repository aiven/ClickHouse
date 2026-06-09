"""A tiny raw-TCP forwarder, run inside the ClickHouse node container.

The MinIO container is reachable only as `minio1:9001`, but the patch under test relaxes TLS
*only* when the endpoint host is exactly `localhost` or `127.0.0.1`. To drive that loopback-only
code path we need the delta-kernel client to connect to `https://127.0.0.1:<port>`, so this script
listens on a loopback port and relays bytes verbatim to `minio1:9001`.

It is a passthrough (no TLS termination): the TLS session is end-to-end between delta-kernel and
MinIO, so MinIO still presents its self-signed certificate and the SigV4 Host header the client
signed (`127.0.0.1:<port>`) is exactly what MinIO receives - path-style addressing keeps the
signature valid.

Usage: python3 tcp_forward.py <listen_host> <listen_port> <target_host> <target_port>
"""

import socket
import sys
import threading


def _relay(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    try:
        dst.shutdown(socket.SHUT_WR)
    except OSError:
        pass


def _handle(client, target_host, target_port):
    try:
        upstream = socket.create_connection((target_host, target_port))
    except OSError:
        client.close()
        return
    a = threading.Thread(target=_relay, args=(client, upstream))
    b = threading.Thread(target=_relay, args=(upstream, client))
    a.start()
    b.start()
    a.join()
    b.join()
    client.close()
    upstream.close()


def main():
    listen_host = sys.argv[1]
    listen_port = int(sys.argv[2])
    target_host = sys.argv[3]
    target_port = int(sys.argv[4])

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((listen_host, listen_port))
    srv.listen(128)
    print(
        f"forwarding {listen_host}:{listen_port} -> {target_host}:{target_port}",
        flush=True,
    )
    while True:
        client, _ = srv.accept()
        threading.Thread(
            target=_handle, args=(client, target_host, target_port), daemon=True
        ).start()


if __name__ == "__main__":
    main()
