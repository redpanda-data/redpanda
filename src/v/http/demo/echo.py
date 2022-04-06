# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

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
