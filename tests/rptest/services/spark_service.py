# Copyright 2024 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Optional
from ducktape.services.service import Service
from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until
from pyhive import hive
from rptest.context import cloud_storage
from rptest.services.redpanda import SISettings
from rptest.tests.datalake.query_engine_base import QueryEngineType, QueryEngineBase
import jinja2


class SparkService(Service, QueryEngineBase):
    """Spark service for querying data generated in datalake."""

    SPARK_HOME = "/opt/spark"
    LOGS_DIR = f"{SPARK_HOME}/logs"
    logs = {"spark_sql_logs": {"path": LOGS_DIR, "collect_default": True}}

    SPARK_SERVER_CMD = jinja2.Template(
        """ /opt/spark/sbin/start-thriftserver.sh \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.defaultCatalog=redpanda-iceberg-catalog \
        --conf spark.sql.catalog.redpanda-iceberg-catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.redpanda-iceberg-catalog.type=rest \
        --conf spark.sql.catalog.redpanda-iceberg-catalog.uri={{ catalog }} \
        --conf spark.sql.catalog.redpanda-iceberg-catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
        --conf spark.sql.catalog.redpanda-iceberg-catalog.cache-enabled=false \
        {% if s3 -%}
        --conf spark.sql.catalog.redpanda-iceberg-catalog.s3.endpoint={{ s3 }}
        {% endif %}""")

    def __init__(self, ctx, iceberg_catalog_rest_uri: str):
        super(SparkService, self).__init__(ctx, num_nodes=1)
        self.iceberg_catalog_rest_uri = iceberg_catalog_rest_uri
        self.credentials = cloud_storage.Credentials.from_context(ctx)
        if isinstance(self.credentials, cloud_storage.S3Credentials):
            self.cloud_storage_access_key = self.credentials.access_key
            self.cloud_storage_secret_key = self.credentials.secret_key
            self.cloud_storage_region = self.credentials.region
            self.cloud_storage_api_endpoint = self.credentials.endpoint
        elif isinstance(self.credentials,
                        cloud_storage.AWSInstanceMetadataCredentials):
            pass
        else:
            raise ValueError(
                f"Unsupported cloud storage type {type(self.credentials)}")
        self.spark_host: Optional[SparkService] = None
        self.spark_port = 10000

    def make_env(self):
        env = dict()
        if self.cloud_storage_access_key:
            env["AWS_ACCESS_KEY_ID"] = self.cloud_storage_access_key
        if self.cloud_storage_secret_key:
            env["AWS_SECRET_ACCESS_KEY"] = self.cloud_storage_secret_key
        if self.cloud_storage_region:
            env["AWS_REGION"] = self.cloud_storage_region
        return " ".join([f"{k}={v}" for k, v in env.items()])

    def start_cmd(self):
        cmd = SparkService.SPARK_SERVER_CMD.render(
            catalog=self.iceberg_catalog_rest_uri,
            s3=self.cloud_storage_api_endpoint)
        env = self.make_env()
        return f"{env} {cmd}"

    def start_node(self, node, timeout_sec=120, **kwargs):
        node.account.ssh(self.start_cmd(), allow_fail=False)
        self.spark_host = node.account.hostname
        self.wait(timeout_sec=timeout_sec)

    def wait_node(self, node, timeout_sec):
        def _ready():
            try:
                self.run_query_fetch_all("show databases")
                return True
            except Exception as e:
                self.logger.debug(f"Exception querying spark server",
                                  exc_info=True)
            return False

        wait_until(_ready,
                   timeout_sec=timeout_sec,
                   backoff_sec=1,
                   err_msg="Error waiting for Spark SQL server to start",
                   retry_on_exc=True)
        return True

    def stop_node(self, node, allow_fail=False, **_):
        node.account.ssh(
            f"SPARK_IDENT_STRING=$(id -nu) {SparkService.SPARK_HOME}/sbin/stop-thriftserver.sh",
            allow_fail=True)

    def clean_node(self, node, **_):
        self.stop_node(node, allow_fail=True)
        node.account.remove(SparkService.LOGS_DIR, allow_fail=True)

    @staticmethod
    def engine_name():
        return QueryEngineType.SPARK

    def make_client(self):
        assert self.spark_host
        return hive.connect(host=self.spark_host, port=self.spark_port)
