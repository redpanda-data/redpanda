# Copyright 2024 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os

from ducktape.services.service import Service
from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until
from pyiceberg.catalog import load_catalog

import uuid


class IcebergRESTCatalog(Service):
    """A Iceberg REST service compatible with minio. This service is a thin REST wrapper over 
    a catalog IO implementation controlled via CATALOG_IO__IMPL. Out of the box, it defaults
    to org.apache.iceberg.jdbc.JdbcCatalog over a temporary sqlite db. It can also wrap over a
    S3 based (minio) implementaion by setting org.apache.iceberg.aws.s3.S3FileIO.
    """

    PERSISTENT_ROOT = "/var/lib/iceberg_rest/"
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "iceberg_rest_server.log")
    JAR = "iceberg-rest-image-all.jar"
    JAR_PATH = f"/opt/iceberg-rest-image/build/libs/{JAR}"
    logs = {"iceberg_rest_logs": {"path": LOG_FILE, "collect_default": True}}
    DEFAULT_CATALOG_IMPL = "org.apache.iceberg.jdbc.JdbcCatalog"
    DEFAULT_CATALOG_JDBC_URI = f"jdbc:sqlite:file:/tmp/{uuid.uuid1()}_iceberg_rest_mode=memory"
    DEFAULT_CATALOG_DB_USER = "user"
    DEFAULT_CATALOG_DB_PASS = "password"

    def __init__(self,
                 ctx,
                 cloud_storage_warehouse: str,
                 cloud_storage_access_key: str = 'panda-user',
                 cloud_storage_secret_key: str = 'panda-secret',
                 cloud_storage_region: str = 'panda-region',
                 cloud_storage_api_endpoint: str = "http://minio-s3:9000",
                 node: ClusterNode | None = None):
        super(IcebergRESTCatalog, self).__init__(ctx,
                                                 num_nodes=0 if node else 1)
        self.cloud_storage_access_key = cloud_storage_access_key
        self.cloud_storage_secret_key = cloud_storage_secret_key
        self.cloud_storage_region = cloud_storage_region
        self.cloud_storage_api_endpoint = cloud_storage_api_endpoint
        self.cloud_storage_warehouse = cloud_storage_warehouse

        self.catalog_url = None

    def _make_env(self):
        env = dict()
        env["AWS_ACCESS_KEY_ID"] = self.cloud_storage_access_key
        env["AWS_SECRET_ACCESS_KEY"] = self.cloud_storage_secret_key
        env["AWS_REGION"] = self.cloud_storage_region
        env["CATALOG_WAREHOUSE"] = f"s3a://{self.cloud_storage_warehouse}"
        # This also takes org.apache.iceberg.aws.s3.S3FileIO
        # to use a wrapper around S3 based catalog.
        env["CATALOG_CATALOG__IMPL"] = IcebergRESTCatalog.DEFAULT_CATALOG_IMPL
        env["CATALOG_URI"] = IcebergRESTCatalog.DEFAULT_CATALOG_JDBC_URI
        env["CATALOG_JDBC_USER"] = IcebergRESTCatalog.DEFAULT_CATALOG_DB_USER
        env["CATALOG_JDBC_PASSWORD"] = IcebergRESTCatalog.DEFAULT_CATALOG_DB_PASS
        env["CATALOG_S3_ENDPOINT"] = self.cloud_storage_api_endpoint
        env["CATALOG_IO__IMPL"] = "org.apache.iceberg.aws.s3.S3FileIO"
        return env

    def _cmd(self):
        java = "/opt/java/java-17"
        envs = self._make_env()
        env = " ".join(f"{k}={v}" for k, v in envs.items())
        return f"{env} {java} -jar  {IcebergRESTCatalog.JAR_PATH} \
            1>> {IcebergRESTCatalog.LOG_FILE} 2>> {IcebergRESTCatalog.LOG_FILE} &"

    def client(self, catalog_name="default"):
        assert self.catalog_url
        return load_catalog(
            catalog_name, **{
                "uri": self.catalog_url,
                "s3.endpoint": self.cloud_storage_api_endpoint,
                "s3.access-key-id": self.cloud_storage_access_key,
                "s3.secret-access-key": self.cloud_storage_secret_key,
                "s3.region": self.cloud_storage_region,
            })

    def start_node(self, node, timeout_sec=60, **kwargs):
        node.account.ssh("mkdir -p %s" % IcebergRESTCatalog.PERSISTENT_ROOT,
                         allow_fail=False)
        cmd = self._cmd()
        self.logger.info(
            f"Starting Iceberg REST catalog service on {node.name} with command {cmd}"
        )
        node.account.ssh(cmd, allow_fail=False)
        self.catalog_url = f"http://{node.account.hostname}:8181"

    def wait_node(self, node, timeout_sec=None):
        check_cmd = f"pyiceberg --uri http://localhost:8181 list"

        def _ready():
            out = node.account.ssh_output(check_cmd)
            status_code = int(out.decode('utf-8'))
            self.logger.info(f"health check result status code: {status_code}")
            return status_code == 200

        wait_until(_ready,
                   timeout_sec=timeout_sec,
                   backoff_sec=0.4,
                   err_msg="Error waiting for Iceberg REST catalog to start",
                   retry_on_exc=True)
        return True

    def stop_node(self, node, allow_fail=False, **_):

        node.account.kill_java_processes(IcebergRESTCatalog.JAR,
                                         allow_fail=allow_fail)

        def _stopped():
            out = node.account.ssh_output("jcmd").decode('utf-8')
            return not (IcebergRESTCatalog.JAR in out)

        wait_until(_stopped,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Error stopping Iceberg REST catalog")

    def clean_node(self, node, **_):
        self.stop_node(node, allow_fail=True)
        node.account.remove(IcebergRESTCatalog.PERSISTENT_ROOT,
                            allow_fail=True)
