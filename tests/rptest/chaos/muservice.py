# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.services.service import Service


class MuService:
    logs = {}

    def start(self, service, nodes):
        raise NotImplementedError("%s: subclasses must implement start_node." %
                                  self.__class__.__name__)

    def stop_node(self, service, node):
        raise NotImplementedError("%s: subclasses must implement stop_node." %
                                  self.__class__.__name__)

    def clean_node(self, service, node):
        raise NotImplementedError("%s: subclasses must implement clean_node." %
                                  self.__class__.__name__)


class SeqMuService:
    logs = {}
    muservices = []

    def __init__(self, muservices):
        self.muservices = muservices
        for muservice in self.muservices:
            for key in muservice.logs.keys():
                self.logs[key] = muservice.logs[key]

    def start(self, service, nodes):
        for muservice in self.muservices:
            muservice.start(service, nodes)

    def stop_node(self, service, node):
        for muservice in reversed(self.muservices):
            muservice.stop_node(service, node)

    def clean_node(self, service, node):
        for muservice in reversed(self.muservices):
            muservice.clean_node(service, node)


class MuServiceRunner(Service):
    logs = {}
    muservice = None

    def __init__(self, muservice, context, num_nodes):
        super(MuServiceRunner, self).__init__(context, num_nodes=num_nodes)
        self.muservice = muservice
        self.logs = muservice.logs

    def start(self):
        super(MuServiceRunner, self).start()
        self.muservice.start(self, self.nodes)

    def start_node(self, node):
        pass

    def stop_node(self, node):
        self.muservice.stop_node(self, node)

    def clean_node(self, node):
        self.muservice.clean_node(self, node)
