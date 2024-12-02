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

import jinja2
from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from pyhive import trino

from rptest.context import cloud_storage
from rptest.services.spark_service import QueryEngineBase
from rptest.tests.datalake.query_engine_base import QueryEngineType


class TrinoService(Service, QueryEngineBase):
    """Trino service for querying data generated in datalake."""

    TRINO_HOME = "/opt/trino"
    PERSISTENT_ROOT = "/var/lib/trino"
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "trino_server.log")
    TRINO_LOGGING_CONF = "io.trino=INFO\n"
    TRINO_LOGGING_CONF_FILE = "/opt/trino/etc/log.properties"
    logs = {"iceberg_rest_logs": {"path": LOG_FILE, "collect_default": True}}

    REDPANDA_CATALOG_PATH = "/opt/trino/etc/catalog/redpanda.properties"
    REDPANDA_CATALOG_CONF = jinja2.Template("""
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri={{ catalog_rest_uri }}
{{extra_conf}}
""")

    def __init__(self, ctx, iceberg_catalog_rest_uri: str):
        super(TrinoService, self).__init__(ctx, num_nodes=1)
        self.iceberg_catalog_rest_uri = iceberg_catalog_rest_uri
        self.credentials = cloud_storage.Credentials.from_context(ctx)
        self.trino_host: Optional[str] = None
        self.trino_port = 8083

    def start_node(self, node, timeout_sec=120, **kwargs):
        node.account.ssh(f"mkdir -p {TrinoService.PERSISTENT_ROOT}")
        node.account.ssh(f"rm -f {TrinoService.REDPANDA_CATALOG_PATH}")

        extra_conf = ""
        if isinstance(self.credentials, cloud_storage.S3Credentials):
            extra_conf = self.dict_to_conf({
                "fs.native-s3.enabled":
                True,
                "s3.region":
                self.credentials.region,
                "s3.path-style-access":
                True,
                "s3.endpoint":
                self.credentials.endpoint,
                "s3.aws-access-key":
                self.credentials.access_key,
                "s3.aws-secret-key":
                self.credentials.secret_key
            })
        elif isinstance(self.credentials,
                        cloud_storage.AWSInstanceMetadataCredentials):
            extra_conf = self.dict_to_conf({"fs.native-s3.enabled": True})
        elif isinstance(self.credentials,
                        cloud_storage.GCPInstanceMetadataCredentials):
            extra_conf = self.dict_to_conf({"fs.native-gcs.enabled": True})
        elif isinstance(self.credentials,
                        cloud_storage.ABSSharedKeyCredentials):
            extra_conf = self.dict_to_conf({
                "fs.native-azure.enabled":
                True,
                "azure.auth-type":
                "ACCESS_KEY",
                "azure.access-key":
                self.credentials.account_key,
            })
        else:
            raise NotImplementedError(
                f"Unsupported cloud storage credentials: {self.credentials}")

        connector_config = dict(catalog_rest_uri=self.iceberg_catalog_rest_uri,
                                extra_conf=extra_conf)
        config_str = TrinoService.REDPANDA_CATALOG_CONF.render(
            connector_config)
        self.logger.debug(f"Using connector config: {config_str}")
        node.account.create_file(TrinoService.REDPANDA_CATALOG_PATH,
                                 config_str)
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

    def escape_identifier(self, table: str) -> str:
        return f'"{table}"'

    @staticmethod
    def dict_to_conf(d: dict[str, Optional[str | bool]]):
        """
        Convert a dictionary to trino conf.
        """
        def transform_value(v: str | bool):
            if isinstance(v, bool):
                return str(v).lower()
            return v

        return "\n".join(
            [f"{k}={transform_value(v)}" for k, v in d.items() if v])
