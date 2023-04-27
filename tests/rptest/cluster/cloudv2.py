# Copyright 2014 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

from ducktape.cluster.cluster_spec import ClusterSpec, WINDOWS
from ducktape.cluster.node_container import NodeContainer
from ducktape.command_line.defaults import ConsoleDefaults
from ducktape.cluster.cluster import Cluster, ClusterNode
from ducktape.cluster.linux_remoteaccount import LinuxRemoteAccount
from ducktape.cluster.windows_remoteaccount import WindowsRemoteAccount
from ducktape.cluster.remoteaccount import RemoteAccountSSHConfig

import json
import os
import traceback


class Cloudv2JsonCluster(Cluster):
    """An implementation of Cluster that uses static settings specified in a cluster file or json-serializeable dict
    """
    def __init__(self, cluster_json=None, *args, **kwargs):
        """Initialize Cloudv2JsonCluster

        K8sJsonCluster can be initialized from:
            - a json-serializeable dict
            - a "cluster_file" containing json

        :param cluster_json: a json-serializeable dict containing node information. If ``cluster_json`` is None,
               load from file
        :param cluster_file (optional): Overrides the default location of the json cluster file

        Example json with a local Vagrant cluster::

            {
              "nodes": [
                {
                  "externally_routable_ip": "192.168.50.151",

                  "ssh_config": {
                    "host": "worker1",
                    "hostname": "127.0.0.1",
                    "identityfile": "/path/to/private_key",
                    "password": null,
                    "port": 2222,
                    "user": "vagrant"
                  }
                },
                {
                  "externally_routable_ip": "192.168.50.151",

                  "ssh_config": {
                    "host": "worker2",
                    "hostname": "127.0.0.1",
                    "identityfile": "/path/to/private_key",
                    "password": null,
                    "port": 2223,
                    "user": "vagrant"
                  }
                }
              ]
            }

        """
        super(Cloudv2JsonCluster, self).__init__()
        self._available_accounts = NodeContainer()
        self._in_use_nodes = NodeContainer()
        if cluster_json is None:
            # This is a directly instantiation of JsonCluster rather than from a subclass (e.g. VagrantCluster)
            cluster_file = kwargs.get("cluster_file")
            if cluster_file is None:
                cluster_file = ConsoleDefaults.CLUSTER_FILE
            cluster_json = json.load(open(os.path.abspath(cluster_file)))
        try:
            for ninfo in cluster_json["nodes"]:
                ssh_config_dict = ninfo.get("ssh_config")
                assert ssh_config_dict is not None, \
                    "Cluster json has a node without a ssh_config field: %s\n Cluster json: %s" % (ninfo, cluster_json)

                ssh_config = RemoteAccountSSHConfig(
                    **ninfo.get("ssh_config", {}))
                remote_account = Cloudv2JsonCluster.make_remote_account(
                    ssh_config, ninfo.get("externally_routable_ip"))
                if remote_account.externally_routable_ip is None:
                    remote_account.externally_routable_ip = self._externally_routable_ip(
                        remote_account)
                self._available_accounts.add_node(remote_account)
        except BaseException as e:
            msg = "JSON cluster definition invalid: %s: %s" % (
                e, traceback.format_exc(limit=16))
            raise ValueError(msg)
        self._id_supplier = 0

    @staticmethod
    def make_remote_account(ssh_config, externally_routable_ip=None):
        """Factory function for creating the correct RemoteAccount implementation."""

        if ssh_config.host and WINDOWS in ssh_config.host:
            return WindowsRemoteAccount(
                ssh_config=ssh_config,
                externally_routable_ip=externally_routable_ip)
        else:
            return LinuxRemoteAccount(
                ssh_config=ssh_config,
                externally_routable_ip=externally_routable_ip)

    def do_alloc(self, cluster_spec):
        allocated_accounts = self._available_accounts.remove_spec(cluster_spec)
        allocated_nodes = []
        for account in allocated_accounts:
            allocated_nodes.append(
                ClusterNode(account, slot_id=self._id_supplier))
            self._id_supplier += 1
        self._in_use_nodes.add_nodes(allocated_nodes)
        return allocated_nodes

    def free_single(self, node):
        self._in_use_nodes.remove_node(node)
        self._available_accounts.add_node(node.account)
        node.account.close()

    def _externally_routable_ip(self, account):
        return None

    def available(self):
        return ClusterSpec.from_nodes(self._available_accounts)

    def used(self):
        return ClusterSpec.from_nodes(self._in_use_nodes)
