from __future__ import annotations

import base64
import http.server
import json
import signal
import socketserver
import time
from types import FrameType
from typing import Any
from urllib.parse import parse_qs

from cryptography.hazmat.primitives.asymmetric.rsa import (
    RSAPrivateKey,
    RSAPublicKey,
    generate_private_key,
)
import jwt as pyjwt


KID = "stub-key-1"
AUDIENCE = "redpanda"


def base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def generate_rsa_keypair() -> tuple[RSAPrivateKey, RSAPublicKey]:
    private_key = generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key


def public_key_to_jwk(public_key: RSAPublicKey, kid: str) -> dict[str, str]:
    public_numbers = public_key.public_numbers()
    n_bytes = public_numbers.n.to_bytes((public_numbers.n.bit_length() + 7) // 8, "big")
    e_bytes = public_numbers.e.to_bytes((public_numbers.e.bit_length() + 7) // 8, "big")
    return {
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": kid,
        "n": base64url_encode(n_bytes),
        "e": base64url_encode(e_bytes),
    }


class BaseHandler(http.server.BaseHTTPRequestHandler):
    def log_request(self, *args: Any, **kwargs: Any) -> None:
        return

    def json_log(self, response_code: int) -> None:
        log_item = {
            "path": self.path,
            "method": self.command,
            "response_code": response_code,
        }
        print(json.dumps(log_item), flush=True)

    def send_json(self, data: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.json_log(status)


def make_handler(
    private_key: RSAPrivateKey,
    public_key: RSAPublicKey,
    issuer: str,
    token_lifetime: int,
) -> type[BaseHandler]:
    jwk = public_key_to_jwk(public_key, KID)
    jwks_doc: dict[str, list[dict[str, str]]] = {"keys": [jwk]}
    clients: dict[str, dict[str, Any]] = {}

    class OIDCHandler(BaseHandler):
        def do_GET(self) -> None:
            if self.path == "/.well-known/openid-configuration":
                self.send_json(
                    {
                        "issuer": issuer,
                        "jwks_uri": f"{issuer}/jwks",
                        "token_endpoint": f"{issuer}/token",
                    }
                )
            elif self.path == "/jwks":
                self.send_json(jwks_doc)
            else:
                self.send_response(404)
                self.end_headers()
                self.json_log(404)

        def do_POST(self) -> None:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")

            if self.path == "/register":
                data = json.loads(body)
                client_id = data["client_id"]
                clients[client_id] = {
                    "secret": data.get("client_secret", "stub-secret"),
                    "claims": data.get("claims", {}),
                }
                self.send_json({"status": "registered", "client_id": client_id})

            elif self.path == "/token":
                params = parse_qs(body)
                client_id = params.get("client_id", [None])[0]

                if client_id is None or client_id not in clients:
                    self.send_json({"error": "invalid_client"}, status=400)
                    return

                now = time.time()
                payload = {
                    "iss": issuer,
                    "aud": AUDIENCE,
                    "iat": int(now),
                    "exp": int(now) + token_lifetime,
                }
                # Merge registered claims as-is — no validation
                payload.update(clients[client_id]["claims"])

                token = pyjwt.encode(
                    payload,
                    private_key,
                    algorithm="RS256",
                    headers={"kid": KID},
                )

                self.send_json(
                    {
                        "access_token": token,
                        "token_type": "bearer",
                        "expires_in": token_lifetime,
                    }
                )
            else:
                self.send_response(404)
                self.end_headers()
                self.json_log(404)

    return OIDCHandler


def main() -> None:
    import argparse
    import socket

    parser = argparse.ArgumentParser(description="Stub OIDC Provider")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--issuer", type=str, default=None)
    parser.add_argument("--token-lifetime", type=int, default=3600)
    options = parser.parse_args()

    if options.issuer is None:
        hostname = socket.getfqdn()
        options.issuer = f"http://{hostname}:{options.port}"

    private_key, public_key = generate_rsa_keypair()
    handler = make_handler(
        private_key, public_key, options.issuer, options.token_lifetime
    )

    class ReuseAddressTcpServer(socketserver.TCPServer):
        allow_reuse_address = True

    with ReuseAddressTcpServer(("", options.port), handler) as httpd:

        def _stop(_signum: int, _frame: FrameType | None) -> None:
            httpd.server_close()
            exit(0)

        signal.signal(signal.SIGTERM, _stop)
        print(
            json.dumps(
                {"status": "ready", "port": options.port, "issuer": options.issuer}
            ),
            flush=True,
        )
        httpd.serve_forever()


if __name__ == "__main__":
    main()
