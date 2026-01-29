# Copyright 2024 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Optional

from ducktape.utils.util import wait_until
from pyhive import hive

from rptest.context import cloud_storage
from rptest.services.catalog_service import CatalogType
from rptest.services.nessie_catalog import NessieCatalog
from rptest.tests.datalake.query_engine_base import QueryEngineBase, QueryEngineType


class SparkService(QueryEngineBase):
    """Spark service for querying data generated in datalake."""

    SPARK_HOME = "/opt/spark"
    SPARK_SERVER_EXEC = "/opt/spark/sbin/start-thriftserver.sh"
    LOGS_DIR = f"{SPARK_HOME}/logs"

    logs = {"spark_sql_logs": {"path": LOGS_DIR, "collect_default": True}}

    def __init__(
        self,
        ctx,
        iceberg_catalog_uri: str,
        default_warehouse_dir: str,
        catalog_type: CatalogType,
        catalog_name: str = "spark-catalog",
    ):
        super(SparkService, self).__init__(ctx, num_nodes=1)
        self.iceberg_catalog_uri = iceberg_catalog_uri
        self.default_warehouse_dir = default_warehouse_dir
        self.catalog_type = catalog_type
        self.catalog_name = catalog_name

        self.credentials = cloud_storage.Credentials.from_context(ctx)
        self.spark_host: Optional[SparkService] = None
        self.spark_port = 10000

    def make_env(self):
        env = dict()

        if isinstance(self.credentials, cloud_storage.S3Credentials):
            env["AWS_ACCESS_KEY_ID"] = self.credentials.access_key
            env["AWS_SECRET_ACCESS_KEY"] = self.credentials.secret_key
            env["AWS_REGION"] = self.credentials.region
        elif isinstance(self.credentials, cloud_storage.AWSInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials, cloud_storage.GCPInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials, cloud_storage.ABSSharedKeyCredentials):
            pass
        else:
            raise ValueError(f"Unsupported cloud storage type {type(self.credentials)}")

        return " ".join([f"{k}={v}" for k, v in env.items()])

    def start_cmd(self):
        conf_args: dict[str, Optional[str | bool]] = {}

        conf_args.update(
            {
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                f"spark.sql.catalog.{self.catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
                f"spark.sql.catalog.{self.catalog_name}.cache-enabled": False,
                f"spark.sql.catalog.{self.catalog_name}.uri": self.iceberg_catalog_uri,
                "spark.sql.defaultCatalog": f"{self.catalog_name}",
            }
        )

        if self.catalog_type == CatalogType.NESSIE:
            conf_args.update(
                {
                    "spark.jars.packages": f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:{NessieCatalog.NESSIE_VERSION}",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
                    f"spark.sql.catalog.{self.catalog_name}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
                    f"spark.sql.catalog.{self.catalog_name}.ref": "main",
                    f"spark.sql.catalog.{self.catalog_name}.authentication.type": "NONE",
                    f"spark.sql.catalog.{self.catalog_name}.warehouse": self.default_warehouse_dir,
                }
            )
        else:
            conf_args.update(
                {
                    f"spark.sql.catalog.{self.catalog_name}.type": "rest",
                }
            )

        if isinstance(self.credentials, cloud_storage.S3Credentials):
            conf_args.update(
                {
                    f"spark.sql.catalog.{self.catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                    f"spark.sql.catalog.{self.catalog_name}.s3.endpoint": self.credentials.endpoint,
                }
            )
        elif isinstance(self.credentials, cloud_storage.AWSInstanceMetadataCredentials):
            conf_args.update(
                {
                    f"spark.sql.catalog.{self.catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                }
            )
        elif isinstance(self.credentials, cloud_storage.GCPInstanceMetadataCredentials):
            conf_args.update(
                {
                    f"spark.sql.catalog.{self.catalog_name}.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
                }
            )
        elif isinstance(self.credentials, cloud_storage.ABSSharedKeyCredentials):
            conf_args.update(
                {
                    f"spark.sql.catalog.{self.catalog_name}.io-impl": "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
                    f"spark.sql.catalog.{self.catalog_name}.adls.auth.shared-key.account.name": self.credentials.account_name,
                    f"spark.sql.catalog.{self.catalog_name}.adls.auth.shared-key.account.key": self.credentials.account_key,
                }
            )
        else:
            raise ValueError(f"Unsupported cloud storage type {type(self.credentials)}")

        env = self.make_env()
        return f"{env} {SparkService.SPARK_SERVER_EXEC} {SparkService.dict_to_conf_args(conf_args)}"

    def start_node(self, node, timeout_sec=120, **kwargs):
        start_cmd = self.start_cmd()
        self.logger.info(f"Starting Spark SQL server with command: {start_cmd}")
        node.account.ssh(start_cmd, allow_fail=False)
        self.spark_host = node.account.hostname
        self.wait(timeout_sec=timeout_sec)

    def wait_node(self, node, timeout_sec):
        def _ready():
            try:
                self.run_query_fetch_all("show databases")
                return True
            except Exception:
                self.logger.debug("Exception querying spark server", exc_info=True)
            return False

        wait_until(
            _ready,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="Error waiting for Spark SQL server to start",
            retry_on_exc=True,
        )
        return True

    def stop_node(self, node, allow_fail=False, **_):
        node.account.ssh(
            f"SPARK_IDENT_STRING=$(id -nu) {SparkService.SPARK_HOME}/sbin/stop-thriftserver.sh",
            allow_fail=True,
        )

    def clean_node(self, node, **_):
        self.stop_node(node, allow_fail=True)
        node.account.remove(SparkService.LOGS_DIR, allow_fail=True)

    def escape_identifier(self, table: str) -> str:
        return f"`{table}`"

    @staticmethod
    def engine_name():
        return QueryEngineType.SPARK

    def count_parquet_files(self, namespace, table) -> int:
        # Metadata query
        # https://iceberg.apache.org/docs/1.6.1/spark-queries/#files
        return self.run_query_fetch_one(
            f"SELECT count(*) FROM {namespace}.{table}.files"
        )[0]

    def optimize_parquet_files(self, namespace, table) -> None:
        # Spark Procedures provided by Iceberg SQL Extensions
        # https://iceberg.apache.org/docs/1.6.1/spark-procedures/#rewrite_data_files
        self.run_query_fetch_one(
            f'CALL `{self.catalog_name}`.system.rewrite_data_files("{namespace}.{table}")'
        )

    def make_client(self):
        assert self.spark_host
        return hive.connect(
            host=self.spark_host, port=self.spark_port, database=self.catalog_name
        )

    @staticmethod
    def dict_to_conf_args(conf: dict[str, Optional[str | bool]]) -> str:
        def transform_value(v: str | bool):
            if isinstance(v, bool):
                return str(v).lower()
            return v

        return " ".join(
            [
                f"--conf {k}={transform_value(v)}"
                for k, v in conf.items()
                if v is not None
            ]
        )
