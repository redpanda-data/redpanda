import cherrypy
import json


class EchoServer(object):
    @cherrypy.expose
    def echo(self):
        body = cherrypy.request.body.read()
        print("Body: ", str(body))
        headers = cherrypy.request.headers
        print("Headers: ", str(headers))
        return json.dumps({"headers": str(headers), "body": str(body)})


cherrypy.quickstart(EchoServer())
