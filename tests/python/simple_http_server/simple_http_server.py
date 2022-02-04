import http.server
import socketserver
import json
import signal


class PrintingHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # override this not to log message in BaseHTTPRequest handler format
        return

    def do_HEAD(self):
        self.send_response(200)

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def do_PUT(self):
        self._handle()

    def do_DELETE(self):
        self._handle()

    def _handle(self):
        # set reply
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        # print request content
        req = {}
        req['method'] = self.command
        req['path'] = self.path
        if 'Content-Length' in self.headers:
            req['content_length'] = int(self.headers.get('Content-Length'))
            req['body'] = self.rfile.read(
                req['content_length']).decode('ascii')

        print(json.dumps(req), flush=True)


class ReuseAddressTcpServer(socketserver.TCPServer):
    allow_reuse_address = True


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(description='Simple HTTP server')
        parser.add_argument('--port',
                            type=int,
                            help='Port to listen on',
                            default=8080)
        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    with ReuseAddressTcpServer(("", options.port), PrintingHandler) as httpd:

        def _stop(*args):
            httpd.server_close()
            exit(0)

        signal.signal(signal.SIGTERM, _stop)
        httpd.serve_forever()


if __name__ == '__main__':
    main()
