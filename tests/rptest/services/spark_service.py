# Copyright 2024 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
from typing import Optional
from ducktape.services.service import Service
from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until
from pyhive import hive


class SparkService(Service):
    """Spark service for querying data generated in datalake."""

    SPARK_HOME = "/opt/spark"
    LOGS_DIR = f"{SPARK_HOME}/logs"
    logs = {"spark_sql_logs": {"path": LOGS_DIR, "collect_default": True}}

    SPARK_SERVER_CMD = """AWS_ACCESS_KEY_ID={akey} AWS_SECRET_ACCESS_KEY={skey} AWS_REGION={region} \
    /opt/spark/sbin/start-thriftserver.sh \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.defaultCatalog=redpanda-iceberg-catalog \
        --conf spark.sql.catalog.redpanda-iceberg-catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.redpanda-iceberg-catalog.type=rest \
        --conf spark.sql.catalog.redpanda-iceberg-catalog.uri={catalog} \
        --conf spark.sql.catalog.redpanda-iceberg-catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
        --conf spark.sql.catalog.redpanda-iceberg-catalog.s3.endpoint={s3}
    """

    def __init__(self,
                 ctx,
                 iceberg_catalog_rest_uri: str,
                 cloud_storage_access_key: str = 'panda-user',
                 cloud_storage_secret_key: str = 'panda-secret',
                 cloud_storage_region: str = 'panda-region',
                 cloud_storage_api_endpoint: str = "http://minio-s3:9000"):
        super(SparkService, self).__init__(ctx, num_nodes=1)
        self.iceberg_catalog_rest_uri = iceberg_catalog_rest_uri
        self.cloud_storage_access_key = cloud_storage_access_key
        self.cloud_storage_secret_key = cloud_storage_secret_key
        self.cloud_storage_region = cloud_storage_region
        self.cloud_storage_api_endpoint = cloud_storage_api_endpoint
        self.spark_host: Optional[SparkService] = None
        self.spark_port = 10000

    def start_cmd(self):
        return SparkService.SPARK_SERVER_CMD.format(\
            akey=self.cloud_storage_access_key, \
            skey=self.cloud_storage_secret_key, \
            region=self.cloud_storage_region, \
            catalog=self.iceberg_catalog_rest_uri, \
            s3=self.cloud_storage_api_endpoint)

    def start_node(self, node, timeout_sec=30, **kwargs):
        node.account.ssh(self.start_cmd(), allow_fail=False)
        self.spark_host = node.account.hostname
        self.wait(timeout_sec=timeout_sec)

    def wait_node(self, node, timeout_sec=None):
        def _ready():
            client = self.make_client()
            try:
                cursor = client.cursor()
                try:
                    cursor.execute("show databases")
                    cursor.fetchall()
                    return True
                finally:
                    cursor.close()
            except Exception as e:
                self.logger.debug(f"Exception querying spark server",
                                  exc_info=True)
            finally:
                client.close()
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

    def make_client(self):
        assert self.spark_host
        return hive.connect(host=self.spark_host, port=self.spark_port)
