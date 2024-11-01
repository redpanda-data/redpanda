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
    JAR_PATH = f"/opt/iceberg-rest-catalog/build/libs/{JAR}"
    logs = {"iceberg_rest_logs": {"path": LOG_FILE, "collect_default": True}}

    DB_CATALOG_IMPL = "org.apache.iceberg.jdbc.JdbcCatalog"
    DB_CATALOG_JDBC_URI = f"jdbc:sqlite:file:/tmp/{uuid.uuid1()}_iceberg_rest_mode=memory"
    DB_CATALOG_DB_USER = "user"
    DB_CATALOG_DB_PASS = "password"

    FS_CATALOG_IMPL = "org.apache.iceberg.hadoop.HadoopCatalog"
    FS_CATALOG_CONF_PATH = "/opt/iceberg-rest-catalog/core-site.xml"

    HADOOP_CONF_TMPL = """<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>

<property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
</property>
<property>
    <name>fs.s3a.endpoint</name>
    <value>{fs_endpoint}</value>
</property>
<property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
</property>
<property>
    <name>fs.s3a.endpoint.region</name>
    <value>{fs_region}</value>
</property>
<property>
    <name>fs.s3a.access.key</name>
    <value>{fs_access_key}</value>
</property>
<property>
    <name>fs.s3a.secret.key</name>
    <value>{fs_secret_key}</value>
</property>
<property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>
</property>
</configuration>"""

    def __init__(
            self,
            ctx,
            cloud_storage_bucket: str,
            cloud_storage_catalog_prefix: str = 'redpanda-iceberg-catalog',
            cloud_storage_access_key: str = 'panda-user',
            cloud_storage_secret_key: str = 'panda-secret',
            cloud_storage_region: str = 'panda-region',
            cloud_storage_api_endpoint: str = "http://minio-s3:9000",
            filesystem_wrapper_mode: bool = False,
            node: ClusterNode | None = None):
        super(IcebergRESTCatalog, self).__init__(ctx,
                                                 num_nodes=0 if node else 1)
        self.cloud_storage_access_key = cloud_storage_access_key
        self.cloud_storage_secret_key = cloud_storage_secret_key
        self.cloud_storage_region = cloud_storage_region
        self.cloud_storage_api_endpoint = cloud_storage_api_endpoint
        self.cloud_storage_bucket = cloud_storage_bucket
        self.cloud_storage_catalog_prefix = cloud_storage_catalog_prefix
        self.set_filesystem_wrapper_mode(filesystem_wrapper_mode)
        # This REST server can operate in two modes.
        # 1. filesystem wrapper mode
        # 2. database mode
        # In filesystem_wrapper_mode, it just wraps a HadoopCatalog implementation
        # that coordinates iceberg metadata updates via S3 in a format equivalent to
        # filesystem catalog mode in Redpanda). In db mode the metadata is stored partly
        # in a self contained sqlite db and doesn't need S3 access and partially on S3.
        #
        # The former is useful to validate the correctness of the metadata format written
        # by Redpanda catalog in filesystem mode while still enabling query engines like
        # Trino that only support REST mode.
        #
        # Trino <-> REST server (HadoopCatalog) <-> S3
        # Trino <-> REST server (JDBC Catalog) <-> local sqllite DB.
        self.catalog_url = None

    def compute_warehouse_path(self):
        s3_prefix = "s3"
        if self.filesystem_wrapper_mode:
            # For hadoop catalog compatibility
            s3_prefix = "s3a"
        self.cloud_storage_warehouse = f"{s3_prefix}://{self.cloud_storage_bucket}/{self.cloud_storage_catalog_prefix}"

    def set_filesystem_wrapper_mode(self, mode: bool):
        self.filesystem_wrapper_mode = mode
        self.compute_warehouse_path()

    def _make_env(self):
        env = dict()

        # Common envs
        env["AWS_ACCESS_KEY_ID"] = self.cloud_storage_access_key
        env["AWS_SECRET_ACCESS_KEY"] = self.cloud_storage_secret_key
        env["AWS_REGION"] = self.cloud_storage_region
        env["CATALOG_S3_ENDPOINT"] = self.cloud_storage_api_endpoint
        env["CATALOG_WAREHOUSE"] = self.cloud_storage_warehouse
        if self.filesystem_wrapper_mode:
            env["CATALOG_CATALOG__IMPL"] = IcebergRESTCatalog.FS_CATALOG_IMPL
            env["HADOOP_CONF_FILE_PATH"] = IcebergRESTCatalog.FS_CATALOG_CONF_PATH
        else:
            env["CATALOG_CATALOG__IMPL"] = IcebergRESTCatalog.DB_CATALOG_IMPL
            env["CATALOG_URI"] = IcebergRESTCatalog.DB_CATALOG_JDBC_URI
            env["CATALOG_JDBC_USER"] = IcebergRESTCatalog.DB_CATALOG_DB_USER
            env["CATALOG_JDBC_PASSWORD"] = IcebergRESTCatalog.DB_CATALOG_DB_PASS
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
        # Delete any existing hadoop config and repopulate
        node.account.ssh(f"rm -f {IcebergRESTCatalog.FS_CATALOG_CONF_PATH}")
        config_tmpl = IcebergRESTCatalog.HADOOP_CONF_TMPL.format(
            fs_endpoint=self.cloud_storage_api_endpoint,
            fs_region=self.cloud_storage_region,
            fs_access_key=self.cloud_storage_access_key,
            fs_secret_key=self.cloud_storage_secret_key)
        self.logger.debug(f"Using hadoop config: {config_tmpl}")
        node.account.create_file(IcebergRESTCatalog.FS_CATALOG_CONF_PATH,
                                 config_tmpl)

        cmd = self._cmd()
        self.logger.info(
            f"Starting Iceberg REST catalog service on {node.name} with command {cmd}"
        )
        node.account.ssh(cmd, allow_fail=False)
        self.catalog_url = f"http://{node.account.hostname}:8181"
        self.wait(timeout_sec=30)

    def wait_node(self, node, timeout_sec=None):
        check_cmd = f"pyiceberg --uri {self.catalog_url} create namespace default"

        def _ready():
            try:
                node.account.ssh_output(check_cmd)
                return True
            except Exception as e:
                self.logger.debug(f"Exception querying catalog", exc_info=True)
            return False

        wait_until(_ready,
                   timeout_sec=timeout_sec,
                   backoff_sec=1,
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
