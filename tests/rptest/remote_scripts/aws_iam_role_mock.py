import datetime
import http.server
import json
import signal
import socketserver


class BaseHandler(http.server.BaseHTTPRequestHandler):
    def not_allowed(self):
        self.send_response(405)
        self.end_headers()
        self.wfile.write('method not allowed'.encode())
        self.json_log(405)

    def log_request(self, *args, **kwargs):
        return

    def json_log(self, response_code):
        payload = None
        if 'Content-Length' in self.headers:
            payload = self.rfile.read(int(self.headers['Content-Length']))
        log_item = {
            'path': self.path,
            'method': self.command,
            'payload': payload.decode() if payload else None,
            'response_code': response_code,
        }
        print(json.dumps(log_item))


def make_aws_handler(token_ttl):
    class AWSHandler(BaseHandler):
        token_expiry = token_ttl
        canned_role = 'tomato'
        canned_credentials = {
            'Code': 'Success',
            'LastUpdated': '2012-04-26T16:39:16Z',
            'Type': 'AWS-HMAC',
            'AccessKeyId': 'panda-user',
            'SecretAccessKey': 'panda-secret',
            'Token': '__REDPANDA_SKIP_IAM_TOKEN_x',
        }

        def get_canned_credentials(self):
            future = datetime.datetime.utcnow() + datetime.timedelta(
                seconds=self.token_expiry)
            cc = self.canned_credentials
            cc['Expiration'] = future.isoformat()
            return cc

        # noinspection PyPep8Naming
        def do_GET(self):
            if self.path == '/latest/meta-data/iam/security-credentials/':
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(self.canned_role.encode())
                self.json_log(200)
            elif self.path == f'/latest/meta-data/iam/security-credentials/{self.canned_role}':
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps(self.get_canned_credentials()).encode())
                self.json_log(200)
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(f'Bad request for path {self.path}'.encode())
                self.json_log(400)

        # noinspection PyPep8Naming
        def do_POST(self):
            self.not_allowed()

        # noinspection PyPep8Naming
        def do_PUT(self):
            self.not_allowed()

        # noinspection PyPep8Naming
        def do_DELETE(self):
            self.not_allowed()

    return AWSHandler


def make_sts_handler(token_ttl):
    class STSHandler(BaseHandler):
        token_expiry = token_ttl
        canned_credentials = """
<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>panda-user</AccessKeyId>
      <SecretAccessKey>panda-secret</SecretAccessKey>
      <SessionToken>__REDPANDA_SKIP_IAM_TOKEN_x</SessionToken>
      <Expiration>{}</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>
        """

        def get_canned_credentials(self):
            future = datetime.datetime.utcnow() + datetime.timedelta(
                seconds=self.token_expiry)
            return self.canned_credentials.format(future.isoformat())

        # noinspection PyPep8Naming
        def do_GET(self):
            self.not_allowed()

        # noinspection PyPep8Naming
        def do_POST(self):
            if self.path == '/':
                self.send_response(200)
                self.send_header('Content-type', 'application/xml')
                self.end_headers()
                self.wfile.write(
                    self.get_canned_credentials().strip().encode())
                self.json_log(200)
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write('bad request'.encode())
                self.json_log(400)

        # noinspection PyPep8Naming
        def do_PUT(self):
            self.not_allowed()

        # noinspection PyPep8Naming
        def do_DELETE(self):
            self.not_allowed()

    return STSHandler


mocks = {
    'aws': make_aws_handler,
    'sts': make_sts_handler,
}


def main():
    import argparse

    def generate_options():
        p = argparse.ArgumentParser(
            description='AWS EC2 Instance Metadata Mock Server')
        p.add_argument('--port',
                       type=int,
                       help='Port to listen on',
                       default=8080)
        p.add_argument('--mock',
                       type=str,
                       help='Implementation to mock',
                       default='aws')
        p.add_argument('--ttl_sec',
                       type=int,
                       help='Token time to live in seconds',
                       default=3600)
        return p

    parser = generate_options()
    options, _ = parser.parse_known_args()

    class ReuseAddressTcpServer(socketserver.TCPServer):
        allow_reuse_address = True

    with ReuseAddressTcpServer(('', options.port),
                               mocks[options.mock](options.ttl_sec)) as httpd:

        def _stop(*_):
            httpd.server_close()
            exit(0)

        signal.signal(signal.SIGTERM, _stop)
        httpd.serve_forever()


if __name__ == '__main__':
    main()
