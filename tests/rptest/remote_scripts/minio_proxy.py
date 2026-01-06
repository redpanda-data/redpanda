#!/usr/bin/env python3
# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import argparse
import hashlib
import hmac
import http.server
import re
import signal
import socketserver
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime


class AWSV4Signer:
    """
    AWS Signature Version 4 signer for S3 requests.
    Based on https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    """

    def __init__(self, access_key, secret_key, region, service="s3"):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service = service

    def _canonical_uri(self, uri):
        """Create canonical URI by encoding path segments."""
        # Split the URI into segments and encode each one
        if not uri:
            return "/"

        # Parse and reconstruct with proper encoding
        # According to AWS, we need to URI-encode each path segment
        parts = uri.split("/")
        encoded_parts = [urllib.parse.quote(part, safe="") for part in parts]
        canonical = "/".join(encoded_parts)

        # Ensure it starts with /
        if not canonical.startswith("/"):
            canonical = "/" + canonical

        return canonical

    def _canonical_query_string(self, query_string):
        """Create canonical query string by sorting and encoding parameters."""
        if not query_string:
            return ""

        # Parse query parameters
        params = urllib.parse.parse_qsl(query_string, keep_blank_values=True)

        # Sort by parameter name, then by value
        sorted_params = sorted(params)

        # Encode and join
        encoded_params = []
        for key, value in sorted_params:
            encoded_key = urllib.parse.quote(key, safe="")
            encoded_value = urllib.parse.quote(str(value), safe="")
            encoded_params.append(f"{encoded_key}={encoded_value}")

        return "&".join(encoded_params)

    def sign_request(
        self,
        method,
        host,
        uri,
        query_string,
        headers,
        payload_hash,
        timestamp,
        log_fn=None,
    ):
        """
        Create AWS Signature V4 for a request.

        Args:
            method: HTTP method (GET, PUT, POST, etc.)
            host: Target host
            uri: Request URI path
            query_string: Query string (without leading ?)
            headers: Dictionary of headers to sign
            payload_hash: SHA256 hash of the payload
            timestamp: ISO8601 timestamp (YYYYMMDDTHHMMSSZ)
            log_fn: Optional logging function for debugging

        Returns:
            Authorization header value
        """

        def _log(msg):
            if log_fn:
                log_fn(msg)

        # Extract date from timestamp
        datestamp = timestamp[:8]

        # Step 1: Create canonical request
        canonical_uri = self._canonical_uri(uri)
        canonical_querystring = self._canonical_query_string(query_string)

        # Canonical headers: lowercase, sorted, trimmed values
        canonical_headers = (
            "\n".join(
                f"{k.lower()}:{' '.join(v.split())}" for k, v in sorted(headers.items())
            )
            + "\n"
        )
        signed_headers = ";".join(sorted(k.lower() for k in headers.keys()))

        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                canonical_querystring,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )

        _log(f"    Canonical request components:")
        _log(f"      Method: {method}")
        _log(f"      Canonical URI: {canonical_uri}")
        _log(f"      Canonical query: {canonical_querystring}")
        _log(f"      Signed headers: {signed_headers}")
        _log(f"      Payload hash: {payload_hash}")

        # Step 2: Create string to sign
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{datestamp}/{self.region}/{self.service}/aws4_request"
        canonical_request_hash = hashlib.sha256(
            canonical_request.encode("utf-8")
        ).hexdigest()
        string_to_sign = "\n".join(
            [algorithm, timestamp, credential_scope, canonical_request_hash]
        )

        _log(f"    Canonical request hash: {canonical_request_hash}")
        _log(f"    String to sign: {repr(string_to_sign)}")

        # Step 3: Calculate signature
        def sign(key, msg):
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        # Initial key is "AWS4" + secret_key (as bytes)
        k_date = hmac.new(
            f"AWS4{self.secret_key}".encode("utf-8"),
            datestamp.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        k_region = hmac.new(
            k_date, self.region.encode("utf-8"), hashlib.sha256
        ).digest()
        k_service = hmac.new(
            k_region, self.service.encode("utf-8"), hashlib.sha256
        ).digest()
        k_signing = hmac.new(
            k_service, "aws4_request".encode("utf-8"), hashlib.sha256
        ).digest()

        signature = hmac.new(
            k_signing, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        _log(f"    Calculated signature: {signature}")

        # Step 4: Build authorization header
        authorization = (
            f"{algorithm} "
            f"Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )

        return authorization


class MinioProxyHandler(http.server.BaseHTTPRequestHandler):
    """
    HTTP proxy handler that forwards all requests to MinIO backend
    with proper AWS Signature V4 recalculation and optional delay.
    """

    def log_message(self, format, *args):
        # Override to control logging format - we'll use custom logging
        return

    def _log(self, message):
        """Log a message to stdout with timestamp."""
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] {message}", flush=True)

    def do_HEAD(self):
        self._proxy_request()

    def do_GET(self):
        self._proxy_request()

    def do_POST(self):
        self._proxy_request()

    def do_PUT(self):
        self._proxy_request()

    def do_DELETE(self):
        self._proxy_request()

    def _extract_auth_params(self, auth_header):
        """Extract credential, region, and signed headers from Authorization header."""
        if not auth_header or not auth_header.startswith("AWS4-HMAC-SHA256"):
            return None, None, None

        # Extract credential (access_key/date/region/service/aws4_request)
        credential_match = re.search(r"Credential=([^/]+)/\d+/([^/]+)/", auth_header)
        if not credential_match:
            return None, None, None

        access_key = credential_match.group(1)
        region = credential_match.group(2)

        # Extract signed headers list
        signed_headers_match = re.search(r"SignedHeaders=([^,]+)", auth_header)
        if not signed_headers_match:
            return None, None, None

        signed_headers = signed_headers_match.group(1).strip()

        return access_key, region, signed_headers

    def _proxy_request(self):
        """Forward the request to the actual MinIO backend with optional delay and signature fix."""
        request_id = f"{self.command} {self.path}"
        self._log(f">>> Incoming request: {request_id}")

        try:
            # Add configured delay
            if self.server.delay_ms > 0:
                time.sleep(self.server.delay_ms / 1000.0)

            # Read request body if present
            content_length = self.headers.get("Content-Length")
            body = None
            if content_length:
                body = self.rfile.read(int(content_length))
                self._log(f"    Read body: {len(body)} bytes")

            # Parse the path and query string
            parsed_path = urllib.parse.urlparse(self.path)
            uri = parsed_path.path
            query_string = parsed_path.query

            # Build the target URL
            target_url = f"http://{self.server.backend_host}:{self.server.backend_port}{self.path}"
            self._log(f"    Target URL: {target_url}")

            # Prepare headers for backend request
            # Use case-insensitive dict to avoid duplicate headers
            new_headers = {}
            auth_header = None
            x_amz_date = None
            payload_hash = None

            # First pass: collect headers, normalizing case for certain headers
            for key, value in self.headers.items():
                key_lower = key.lower()
                if key_lower == "authorization":
                    auth_header = value
                    self._log(f"    Original Authorization: {value[:80]}...")
                elif key_lower == "x-amz-date":
                    x_amz_date = value
                    self._log(f"    X-Amz-Date: {value}")
                elif key_lower == "x-amz-content-sha256":
                    # Store payload hash but don't add to headers yet
                    payload_hash = value
                    self._log(f"    Original payload hash: {payload_hash}")
                elif key_lower not in ["host", "connection"]:
                    new_headers[key] = value

            # Set the correct host header for the backend
            original_host = self.headers.get("Host", "unknown")
            new_host = f"{self.server.backend_host}:{self.server.backend_port}"
            new_headers["Host"] = new_host
            self._log(f"    Host change: {original_host} -> {new_host}")

            # If this is an AWS signed request, recalculate the signature
            if auth_header and x_amz_date:
                self._log("    Detected AWS SigV4 request, recalculating signature...")

                # Get or calculate payload hash
                if not payload_hash or payload_hash == "UNSIGNED-PAYLOAD":
                    if body:
                        payload_hash = hashlib.sha256(body).hexdigest()
                        self._log(f"    Calculated payload hash: {payload_hash}")
                    else:
                        payload_hash = hashlib.sha256(b"").hexdigest()
                        self._log(f"    Empty payload hash: {payload_hash}")
                else:
                    self._log(f"    Using provided payload hash: {payload_hash}")

                # Add normalized header names
                new_headers["x-amz-content-sha256"] = payload_hash
                new_headers["x-amz-date"] = x_amz_date

                # Extract access key, region, and signed headers from original auth header
                access_key, region, orig_signed_headers = self._extract_auth_params(
                    auth_header
                )

                if access_key and region and orig_signed_headers:
                    self._log(
                        f"    Extracted access_key: {access_key}, region: {region}"
                    )
                    self._log(f"    Original signed headers: {orig_signed_headers}")

                    # Build headers dict with only the headers that were signed
                    # This ensures we sign the same headers as the original request
                    signed_headers_list = [
                        h.strip() for h in orig_signed_headers.split(";")
                    ]
                    headers_to_sign = {}

                    for header_name in signed_headers_list:
                        if header_name == "host":
                            # Use the new host
                            headers_to_sign["Host"] = new_host
                        elif header_name == "x-amz-date":
                            headers_to_sign["x-amz-date"] = x_amz_date
                        elif header_name == "x-amz-content-sha256":
                            headers_to_sign["x-amz-content-sha256"] = payload_hash
                        else:
                            # Find the original header (case-insensitive)
                            for key, value in new_headers.items():
                                if key.lower() == header_name:
                                    headers_to_sign[key] = value
                                    break

                    self._log(f"    Headers to sign: {list(headers_to_sign.keys())}")

                    # Use configured credentials for signing
                    signer = AWSV4Signer(
                        self.server.access_key, self.server.secret_key, region
                    )
                    self._log(
                        f"    Using configured access_key: {self.server.access_key}"
                    )

                    # Recalculate signature with new host
                    new_auth = signer.sign_request(
                        self.command,
                        new_host,
                        uri,
                        query_string,
                        headers_to_sign,
                        payload_hash,
                        x_amz_date,
                        log_fn=self._log,
                    )
                    new_headers["Authorization"] = new_auth
                    self._log(f"    New Authorization: {new_auth[:80]}...")
                else:
                    self._log(f"    ERROR: Could not extract auth params from header")
                    if auth_header:
                        self._log(f"    Auth header was: {auth_header}")
            else:
                # For non-signed requests, just copy the authorization header if present
                if auth_header:
                    new_headers["Authorization"] = auth_header
                    self._log("    Non-AWS auth: copying Authorization header as-is")
                else:
                    self._log("    No Authorization header found")

            # Create the request
            req = urllib.request.Request(target_url, data=body, method=self.command)

            # Set headers
            for key, value in new_headers.items():
                req.add_header(key, value)

            self._log("    Forwarding request to backend...")

            # Forward the request
            with urllib.request.urlopen(req, timeout=30) as response:
                # Send response status
                self.send_response(response.status)
                self._log(f"<<< Backend response: {response.status}")

                # Forward response headers
                for key, value in response.headers.items():
                    if key.lower() not in ["connection", "transfer-encoding"]:
                        self.send_header(key, value)
                self.end_headers()

                # Forward response body
                response_body = response.read()
                self.wfile.write(response_body)
                self._log(f"    Response body: {len(response_body)} bytes")

        except urllib.error.HTTPError as e:
            # Forward HTTP errors from backend
            self._log(f"!!! Backend HTTP error: {e.code} {e.reason}")
            error_body = e.read()
            self._log(
                f"    Error body: {error_body.decode('utf-8', errors='replace')[:500]}"
            )

            self.send_response(e.code)
            for key, value in e.headers.items():
                if key.lower() not in ["connection", "transfer-encoding"]:
                    self.send_header(key, value)
            self.end_headers()
            self.wfile.write(error_body)

        except Exception as e:
            # Handle other errors
            self._log(f"!!! Proxy error: {type(e).__name__}: {str(e)}")
            import traceback

            self._log(f"    Traceback: {traceback.format_exc()}")
            self.send_error(502, f"Proxy error: {str(e)}")


class MinioProxyServer(socketserver.TCPServer):
    """TCP server with reuse address and custom attributes."""

    allow_reuse_address = True

    def __init__(
        self,
        server_address,
        handler_class,
        backend_host,
        backend_port,
        delay_ms,
        access_key,
        secret_key,
    ):
        super().__init__(server_address, handler_class)
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.delay_ms = delay_ms
        self.access_key = access_key
        self.secret_key = secret_key


def main():
    parser = argparse.ArgumentParser(
        description="MinIO proxy with AWS SigV4 signature recalculation and configurable delay"
    )
    parser.add_argument("--port", type=int, default=9000, help="Port to listen on")
    parser.add_argument(
        "--backend-host",
        type=str,
        default="minio-s3",
        help="Backend MinIO hostname",
    )
    parser.add_argument(
        "--backend-port", type=int, default=9000, help="Backend MinIO port"
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=0,
        help="Delay in milliseconds to add to each request",
    )
    parser.add_argument(
        "--access-key",
        type=str,
        default="panda-user",
        help="AWS/MinIO access key for signature recalculation",
    )
    parser.add_argument(
        "--secret-key",
        type=str,
        default="panda-secret",
        help="AWS/MinIO secret key for signature recalculation",
    )

    options = parser.parse_args()

    server = MinioProxyServer(
        ("", options.port),
        MinioProxyHandler,
        options.backend_host,
        options.backend_port,
        options.delay_ms,
        options.access_key,
        options.secret_key,
    )

    def _stop(*args):
        server.server_close()
        exit(0)

    signal.signal(signal.SIGTERM, _stop)

    print(
        f"MinIO proxy started on port {options.port}, "
        f"forwarding to {options.backend_host}:{options.backend_port} "
        f"with {options.delay_ms}ms delay and AWS SigV4 recalculation",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
