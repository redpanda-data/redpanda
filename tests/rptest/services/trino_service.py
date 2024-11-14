# Copyright 2024 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from typing import Optional

from ducktape.services.service import Service
from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until
from pyhive import trino
from rptest.services.redpanda import SISettings
from rptest.services.spark_service import QueryEngineBase
import jinja2
from rptest.tests.datalake.query_engine_base import QueryEngineType


class TrinoService(Service, QueryEngineBase):
    """Trino service for querying data generated in datalake."""

    TRINO_HOME = "/opt/trino"
    TRINO_CONF_PATH = os.path.join(
        TRINO_HOME, "/opt/trino/etc/catalog/redpanda.properties")
    PERSISTENT_ROOT = "/var/lib/trino"
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "trino_server.log")
    logs = {"iceberg_rest_logs": {"path": LOG_FILE, "collect_default": True}}

    ICEBERG_CONNECTOR_CONF = jinja2.Template("""
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri={{ catalog_rest_uri }}
fs.native-s3.enabled=true
{%- if s3_region %}
s3.region={{ s3_region }}
{%- endif %}
s3.path-style-access=true
{%- if s3_endpoint %}
s3.endpoint={{ s3_endpoint }}
{%- endif %}
{%- if s3_access_key %}
s3.aws-access-key={{ s3_access_key }}
{%- endif %}
{%- if s3_secret_key %}
s3.aws-secret-key={{ s3_secret_key }}
{%- endif %}""")
    TRINO_LOGGING_CONF = "io.trino=INFO\n"
    TRINO_LOGGING_CONF_FILE = "/opt/trino/etc/log.properties"

    def __init__(self, ctx, iceberg_catalog_rest_uri: str, si: SISettings):
        super(TrinoService, self).__init__(ctx, num_nodes=1)
        self.iceberg_catalog_rest_uri = iceberg_catalog_rest_uri
        self.cloud_storage_access_key = si.cloud_storage_access_key
        self.cloud_storage_secret_key = si.cloud_storage_secret_key
        self.cloud_storage_region = si.cloud_storage_region
        self.cloud_storage_api_endpoint = si.endpoint_url
        self.trino_host: Optional[str] = None
        self.trino_port = 8083

    def start_node(self, node, timeout_sec=120, **kwargs):
        node.account.ssh(f"mkdir -p {TrinoService.PERSISTENT_ROOT}")
        node.account.ssh(f"rm -f {TrinoService.TRINO_CONF_PATH}")
        connector_config = dict(catalog_rest_uri=self.iceberg_catalog_rest_uri,
                                s3_region=self.cloud_storage_region,
                                s3_endpoint=self.cloud_storage_api_endpoint,
                                s3_access_key=self.cloud_storage_access_key,
                                s3_secret_key=self.cloud_storage_secret_key)
        config_str = TrinoService.ICEBERG_CONNECTOR_CONF.render(
            connector_config)
        self.logger.debug(f"Using connector config: {config_str}")
        node.account.create_file(TrinoService.TRINO_CONF_PATH, config_str)
        # Create logger configuration
        node.account.ssh(f"rm -f {TrinoService.TRINO_LOGGING_CONF_FILE}")
        node.account.create_file(TrinoService.TRINO_LOGGING_CONF_FILE,
                                 TrinoService.TRINO_LOGGING_CONF)
        node.account.ssh(
            f"nohup /opt/trino/bin/trino-launcher run 1> {TrinoService.LOG_FILE} 2>&1 &",
            allow_fail=False)
        self.trino_host = node.account.hostname
        self.wait(timeout_sec=timeout_sec)

    def wait_node(self, node, timeout_sec):
        def _ready():
            try:
                self.run_query_fetch_all("show catalogs")
                return True
            except Exception:
                self.logger.debug(f"Exception querying catalog", exc_info=True)
            return False

        wait_until(_ready,
                   timeout_sec=timeout_sec,
                   backoff_sec=1,
                   err_msg="Error waiting for Trino server to start",
                   retry_on_exc=True)
        return True

    def stop_node(self, node, allow_fail=False, **_):
        node.account.ssh("/opt/trino/bin/trino-launcher stop",
                         allow_fail=allow_fail)

    def clean_node(self, node, **_):
        self.stop_node(node, allow_fail=True)
        node.account.remove(TrinoService.PERSISTENT_ROOT, allow_fail=True)

    @staticmethod
    def engine_name():
        return QueryEngineType.TRINO

    def make_client(self):
        assert self.trino_host
        return trino.connect(host=self.trino_host,
                             port=self.trino_port,
                             catalog="redpanda")
