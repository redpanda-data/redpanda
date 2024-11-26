# Copyright 2024 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import tempfile
from typing import Optional

import jinja2
from ducktape.cluster.cluster import ClusterNode
from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from pyiceberg.catalog import load_catalog

from rptest.context import cloud_storage


class IcebergRESTCatalog(Service):
    """A Iceberg REST service compatible with minio. This service is a thin REST wrapper over 
    a catalog IO implementation controlled via CATALOG_IO__IMPL. Out of the box, it defaults
    to org.apache.iceberg.jdbc.JdbcCatalog over a temporary sqlite db. It can also wrap over a
    S3 based (minio) implementaion by setting org.apache.iceberg.aws.s3.S3FileIO.
    """

    # Available after start. Use catalog_url property to access.
    _catalog_url: Optional[str] = None

    PERSISTENT_ROOT = "/var/lib/iceberg_rest/"
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "iceberg_rest_server.log")
    JAR = "iceberg-rest-catalog-all.jar"
    JAR_PATH = f"/opt/iceberg-rest-catalog/build/libs/{JAR}"
    logs = {"iceberg_rest_logs": {"path": LOG_FILE, "collect_default": True}}

    DB_CATALOG_IMPL = "org.apache.iceberg.jdbc.JdbcCatalog"
    DB_CATALOG_JDBC_URI_PREFIX = f"jdbc:sqlite:file:"
    DB_CATALOG_DB_USER = "user"
    DB_CATALOG_DB_PASS = "password"

    FS_CATALOG_IMPL = "org.apache.iceberg.hadoop.HadoopCatalog"
    FS_CATALOG_CONF_PATH = "/opt/iceberg-rest-catalog/core-site.xml"

    HADOOP_CONF_TMPL = jinja2.Template("""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
<property>
    <name>fs.s3a.aws.credentials.provider</name>
{%- if fs_dedicated_nodes %}
    <value>org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider</value>
{% else -%}
    <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
{% endif %}
</property>

<property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>{{ fs_dedicated_nodes }}</value>
</property>

<property>
    <name>fs.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
</property>

{{extra_config}}
</configuration>""")

    def __init__(
            self,
            ctx,
            cloud_storage_bucket: str,
            cloud_storage_catalog_prefix: str = 'redpanda-iceberg-catalog',
            filesystem_wrapper_mode: bool = False,
            node: ClusterNode | None = None):
        super(IcebergRESTCatalog, self).__init__(ctx,
                                                 num_nodes=0 if node else 1)
        self.credentials = cloud_storage.Credentials.from_context(ctx)

        self.cloud_storage_bucket = cloud_storage_bucket
        self.cloud_storage_catalog_prefix = cloud_storage_catalog_prefix
        self.dedicated_nodes = ctx.globals.get("dedicated_nodes", False)
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
        self._catalog_url = None
        self.db_file = None

    def compute_warehouse_path(self):
        if isinstance(self.credentials,
                      cloud_storage.S3Credentials) or isinstance(
                          self.credentials,
                          cloud_storage.AWSInstanceMetadataCredentials):
            s3_prefix = "s3"
            if self.filesystem_wrapper_mode:
                # For hadoop catalog compatibility
                s3_prefix = "s3a"
            self.cloud_storage_warehouse = f"{s3_prefix}://{self.cloud_storage_bucket}/{self.cloud_storage_catalog_prefix}"
        elif isinstance(self.credentials,
                        cloud_storage.GCPInstanceMetadataCredentials):
            self.cloud_storage_warehouse = f"gs://{self.cloud_storage_bucket}/{self.cloud_storage_catalog_prefix}"
        elif isinstance(self.credentials,
                        cloud_storage.ABSSharedKeyCredentials):
            self.cloud_storage_warehouse = f"abfss://{self.cloud_storage_bucket}@{self.credentials.endpoint}/{self.cloud_storage_catalog_prefix}"
        else:
            raise ValueError(
                f"Unsupported credential type: {type(self.credentials)}")

    def set_filesystem_wrapper_mode(self, mode: bool):
        self.filesystem_wrapper_mode = mode
        self.compute_warehouse_path()

    def _make_env(self):
        env = dict()

        # Common envs
        if isinstance(self.credentials, cloud_storage.S3Credentials):
            env["CATALOG_IO__IMPL"] = "org.apache.iceberg.aws.s3.S3FileIO"
            env["AWS_ACCESS_KEY_ID"] = self.credentials.access_key
            env["AWS_SECRET_ACCESS_KEY"] = self.credentials.secret_key
            env["AWS_REGION"] = self.credentials.region
            env["CATALOG_S3_ENDPOINT"] = self.credentials.endpoint
        elif isinstance(self.credentials,
                        cloud_storage.AWSInstanceMetadataCredentials):
            env["CATALOG_IO__IMPL"] = "org.apache.iceberg.aws.s3.S3FileIO"
        elif isinstance(self.credentials,
                        cloud_storage.GCPInstanceMetadataCredentials):
            env["CATALOG_IO__IMPL"] = "org.apache.iceberg.gcp.gcs.GCSFileIO"
        elif isinstance(self.credentials,
                        cloud_storage.ABSSharedKeyCredentials):
            env["CATALOG_IO__IMPL"] = "org.apache.iceberg.azure.adlsv2.ADLSFileIO"
            env["CATALOG_adls_auth_shared__key_account_name"] = self.credentials.account_name
            env["CATALOG_adls_auth_shared__key_account_key"] = self.credentials.account_key
        else:
            raise ValueError(
                f"Unsupported credential type: {type(self.credentials)}")

        env["CATALOG_WAREHOUSE"] = self.cloud_storage_warehouse
        if self.filesystem_wrapper_mode:
            env["CATALOG_CATALOG__IMPL"] = IcebergRESTCatalog.FS_CATALOG_IMPL
            env["HADOOP_CONF_FILE_PATH"] = IcebergRESTCatalog.FS_CATALOG_CONF_PATH
        else:
            self.db_file = tempfile.NamedTemporaryFile()
            env["CATALOG_CATALOG__IMPL"] = IcebergRESTCatalog.DB_CATALOG_IMPL
            env["CATALOG_URI"] = IcebergRESTCatalog.DB_CATALOG_JDBC_URI_PREFIX + self.db_file.name
            env["CATALOG_JDBC_USER"] = IcebergRESTCatalog.DB_CATALOG_DB_USER
            env["CATALOG_JDBC_PASSWORD"] = IcebergRESTCatalog.DB_CATALOG_DB_PASS
        return env

    def _cmd(self):
        java = "/opt/java/java-17"
        envs = self._make_env()
        env = " ".join(f"{k}={v}" for k, v in envs.items())
        return f"{env} {java} -jar  {IcebergRESTCatalog.JAR_PATH} \
            1>> {IcebergRESTCatalog.LOG_FILE} 2>> {IcebergRESTCatalog.LOG_FILE} &"

    def client(self, catalog_name="default"):
        conf = dict()
        conf["uri"] = self.catalog_url

        if isinstance(self.credentials, cloud_storage.S3Credentials):
            conf["s3.endpoint"] = self.credentials.endpoint
            conf["s3.access-key-id"] = self.credentials.access_key
            conf["s3.secret-access-key"] = self.credentials.secret_key
            conf["s3.region"] = self.credentials.region
        elif isinstance(self.credentials,
                        cloud_storage.AWSInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials,
                        cloud_storage.GCPInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials,
                        cloud_storage.ABSSharedKeyCredentials):
            # Legancy pyiceberg https://github.com/apache/iceberg-python/issues/866
            conf["adlfs.account-name"] = self.credentials.account_name
            conf["adlfs.account-key"] = self.credentials.account_key
            # Modern pyiceberg https://github.com/apache/iceberg-python/issues/866
            conf["adls.account-name"] = self.credentials.account_name
            conf["alds.account-key"] = self.credentials.account_key
        else:
            raise ValueError(
                f"Unsupported credential type: {type(self.credentials)}")

        return load_catalog(catalog_name, **conf)

    def start_node(self, node, timeout_sec=60, **kwargs):
        node.account.ssh("mkdir -p %s" % IcebergRESTCatalog.PERSISTENT_ROOT,
                         allow_fail=False)
        # Delete any existing hadoop config and repopulate
        node.account.ssh(f"rm -f {IcebergRESTCatalog.FS_CATALOG_CONF_PATH}")

        extra_config = ""

        if isinstance(self.credentials, cloud_storage.S3Credentials):
            extra_config += self.dict_to_xml_properties({
                "fs.s3a.path.style.access":
                True,
                "fs.s3a.endpoint":
                self.credentials.endpoint,
                "fs.s3a.endpoint.region":
                self.credentials.region,
                "fs.s3a.access.key":
                self.credentials.access_key,
                "fs.s3a.secret.key":
                self.credentials.secret_key,
            })
        elif isinstance(self.credentials,
                        cloud_storage.AWSInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials,
                        cloud_storage.GCPInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials,
                        cloud_storage.ABSSharedKeyCredentials):
            extra_config += self.dict_to_xml_properties({
                f"fs.azure.account.auth.type.{self.credentials.account_name}.dfs.core.windows.net":
                "SharedKey",
                f"fs.azure.account.key.{self.credentials.account_name}.dfs.core.windows.net":
                self.credentials.account_key,
            })
        else:
            raise ValueError(
                f"Unsupported credential type: {type(self.credentials)}")

        config_tmpl = IcebergRESTCatalog.HADOOP_CONF_TMPL.render(
            fs_dedicated_nodes=self.dedicated_nodes, extra_config=extra_config)
        self.logger.debug(f"Using hadoop config: {config_tmpl}")
        node.account.create_file(IcebergRESTCatalog.FS_CATALOG_CONF_PATH,
                                 config_tmpl)

        cmd = self._cmd()
        self.logger.info(
            f"Starting Iceberg REST catalog service on {node.name} with command {cmd}"
        )
        node.account.ssh(cmd, allow_fail=False)
        self._catalog_url = f"http://{node.account.hostname}:8181"
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

    @property
    def catalog_url(self) -> str:
        assert self._catalog_url, "URL not available because service is not started"
        return self._catalog_url

    @staticmethod
    def dict_to_xml_properties(d: dict[str, Optional[str | bool]]):
        """
        Convert a dictionary to hadoop xml properties format.
        If a value is None, it is skipped.
        """
        def transform_value(v: str | bool):
            if isinstance(v, bool):
                return str(v).lower()
            return v

        return "\n".join([
            f"<property><name>{k}</name><value>{transform_value(v)}</value></property>"
            for k, v in d.items() if v is not None
        ])
