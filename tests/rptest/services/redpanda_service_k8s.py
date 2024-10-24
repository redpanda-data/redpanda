# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from typing import Optional

from ducktape.cluster.cluster import ClusterNode

from rptest.clients.helm import HelmTool
from rptest.clients.kubectl import KubectlTool
from rptest.services.redpanda import RedpandaServiceBase
from rptest.services.redpanda_types import SaslCredentials


class RedpandaServiceK8s(RedpandaServiceBase):
    def __init__(self,
                 context,
                 num_brokers,
                 *,
                 cluster_spec=None,
                 superuser: Optional[SaslCredentials] = None,
                 skip_if_no_redpanda_log: Optional[bool] = False):
        super(RedpandaServiceK8s,
              self).__init__(context,
                             num_brokers,
                             cluster_spec=cluster_spec,
                             superuser=superuser,
                             skip_if_no_redpanda_log=skip_if_no_redpanda_log)
        self._trim_logs = False
        self._helm = None
        self.__kubectl = None

    def start_node(self, node, **kwargs):
        pass

    def start(self, **kwargs):
        """
        Install the helm chart which will launch the entire cluster. If
        the cluster is already running, then noop. This function will not
        return until redpanda appears to have started successfully. If
        redpanda does not start within a timeout period the service will
        fail to start.
        """
        self._helm = HelmTool(self)
        self._kubectl = KubectlTool(self)
        self._helm.install()

    def stop_node(self, node, **kwargs):
        """
        Uninstall the helm chart which will tear down the entire cluster. If
        the cluster is already uninstalled, then noop.
        """
        self._helm.uninstall()

    def clean_node(self, node, **kwargs):
        self._helm.uninstall()

    def lsof_node(self, node: ClusterNode, filter: Optional[str] = None):
        """
        Get the list of open files for a running node

        :param filter: If given, this is a grep regex that will filter the files we list

        :return: yields strings
        """
        first = True
        cmd = f"ls -l /proc/$(ls -l /proc/*/exe | grep /opt/redpanda/libexec/redpanda | head -1 | cut -d' ' -f 9 | cut -d / -f 3)/fd"
        if filter is not None:
            cmd += f" | grep {filter}"
        for line in self._kubectl.exec(cmd):
            if first and not filter:
                # First line is a header, skip it
                first = False
                continue
            try:
                filename = line.split()[-1]
            except IndexError:
                # Malformed line
                pass
            else:
                yield filename

    def node_id(self, node, force_refresh=False, timeout_sec=30):
        pass

    def set_cluster_config(self, values: dict, timeout: int = 300):
        """
        Updates the values of the helm release
        """
        self._helm.upgrade_config_cluster(values)
