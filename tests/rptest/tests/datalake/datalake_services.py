# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import operator
from typing import Any, Literal, Optional

from ducktape.utils.util import wait_until
from pyiceberg.exceptions import NoSuchNamespaceError

from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.context import databricks as databricks_ctx
from rptest.context.gcp import GCPContext
from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog
from rptest.services.catalog_service import CatalogService, CatalogType
from rptest.services.databricks_workspace import DatabricksWorkspace
from rptest.services.datalake.catalog.biglake import BiglakeMetastore
from rptest.services.datalake.catalog.databricks_unity import DatabricksUnity
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.nessie_catalog import NessieCatalog
from rptest.services.redpanda import RedpandaService
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.services.spark_service import SparkService
from rptest.tests.datalake.iceberg import Identifier
from rptest.services.trino_service import TrinoService
from rptest.tests.datalake.query_engine_base import QueryEngineBase, QueryEngineType
from rptest.tests.datalake.query_engine_factory import get_query_engine_by_type
from rptest.util import (
    wait_until_with_progress_check,
)


class DatalakeServices:
    """Utility class for implementing datalake tests. Includes the
    boiler plate to manage dependent services."""

    def __init__(
        self,
        test_ctx,
        redpanda: RedpandaService,
        include_query_engines: list[QueryEngineType] = [
            QueryEngineType.SPARK,
            QueryEngineType.TRINO,
        ],
        catalog_type: CatalogType = CatalogType.REST_JDBC,
        warehouse_name: str = CatalogService.DEFAULT_WAREHOUSE_NAME,
    ):
        self.test_ctx = test_ctx
        self.redpanda = redpanda

        # Tests may rely on setting frequent translations.
        self.redpanda.set_environment(
            {"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"}
        )
        assert self.redpanda.si_settings

        self.warehouse_name = warehouse_name
        self.included_query_engines = include_query_engines

        self.query_engines: list[QueryEngineBase] = []

        self._catalog_type = catalog_type
        self._cloud_storage_bucket = self.redpanda.si_settings.cloud_storage_bucket

    def setUp(self):
        assert len(self.redpanda.started_nodes()) == 0, (
            "DatalakeServices expects to start redpanda itself"
        )

        # create bucket first, or the catalog won't start
        self.redpanda.start_si()

        self._create_catalog_service()
        self.catalog_service.start()

        # Better defaults for testing. We don't want to wait too long
        # for the iceberg translation to happen.
        if self.redpanda._extra_rp_conf.get("iceberg_target_lag_ms") is None:
            self.redpanda.add_extra_rp_conf(
                {
                    "iceberg_target_lag_ms": 10000,
                }
            )

        if not self.catalog_service.catalog_type() == CatalogType.REST_HADOOP:
            # REST catalog mode
            self.redpanda.add_extra_rp_conf(
                {
                    "iceberg_catalog_type": "rest",
                    "iceberg_rest_catalog_endpoint": self.catalog_service.iceberg_rest_url,
                    "iceberg_rest_catalog_client_id": "panda-user",
                    "iceberg_rest_catalog_client_secret": "panda-secret",
                }
            )
        if self.catalog_service.catalog_type() == CatalogType.NESSIE:
            self.redpanda.add_extra_rp_conf(
                {
                    "iceberg_rest_catalog_warehouse": NessieCatalog.NESSIE_DEFAULT_WAREHOUSE,
                    "iceberg_disable_snapshot_tagging": "true",
                }
            )

        if self.catalog_service.catalog_type() == CatalogType.DATABRICKS_UNITY:
            self.redpanda.add_extra_rp_conf(
                {
                    "iceberg_rest_catalog_warehouse": self.warehouse_name,
                    "iceberg_disable_snapshot_tagging": "true",
                }
            )

            ctx = databricks_ctx.DatabricksContext.from_context(self.test_ctx)
            creds = ctx.credentials
            if isinstance(creds, databricks_ctx.PatCredentials):
                self.redpanda.add_extra_rp_conf(
                    {
                        "iceberg_rest_catalog_authentication_mode": "bearer",
                        "iceberg_rest_catalog_token": creds.token,
                    }
                )
            elif isinstance(creds, databricks_ctx.OauthCredentials):
                self.redpanda.add_extra_rp_conf(
                    {
                        "iceberg_rest_catalog_authentication_mode": "oauth2",
                        "iceberg_rest_catalog_client_id": creds.client_id,
                        "iceberg_rest_catalog_client_secret": creds.client_secret,
                        "iceberg_rest_catalog_oauth2_server_uri": f"{ctx.workspace_url}/oidc/v1/token",
                        "iceberg_rest_catalog_oauth2_scope": "all-apis",
                    }
                )
            else:
                raise ValueError(f"Unsupported credentials type {type(creds)}")

        if self.catalog_service.catalog_type() == CatalogType.BIGLAKE:
            ctx = GCPContext.from_context(self.test_ctx)
            self.redpanda.add_extra_rp_conf(
                {
                    "iceberg_rest_catalog_warehouse": self.warehouse_name,
                    "iceberg_rest_catalog_authentication_mode": "gcp",
                    # We use the same user project ID for billing in testing.
                    "iceberg_rest_catalog_gcp_user_project": ctx.project_id,
                }
            )

        self.redpanda.start(start_si=False)

        for engine in self.included_query_engines:
            svc_cls = get_query_engine_by_type(engine)
            catalog_uri = (
                self.catalog_service.vendor_api_url
                if self.catalog_service.catalog_type() == CatalogType.NESSIE
                else self.catalog_service.iceberg_rest_url
            )
            svc = svc_cls(
                self.test_ctx,
                iceberg_catalog_uri=catalog_uri,
                default_warehouse_dir=self.catalog_service.cloud_storage_warehouse,
                catalog_type=self.catalog_service.catalog_type(),
                catalog_name=self.warehouse_name,
            )
            svc.start()
            self.query_engines.append(svc)

    def tearDown(self):
        self.redpanda.stop()
        for engine in self.query_engines:
            engine.stop()
        self.catalog_service.stop()

    def __enter__(self):
        self.setUp()
        return self

    def __exit__(self, *args, **kwargs):
        self.tearDown()

    def query_engine(self, type: QueryEngineType) -> QueryEngineBase:
        for e in self.query_engines:
            assert isinstance(e, QueryEngineBase)
            if e.engine_name() == type:
                return e
        raise Exception(f"Query engine {type} not found")

    def trino(self) -> TrinoService:
        trino = self.service(QueryEngineType.TRINO)
        assert isinstance(trino, TrinoService), "Missing Trino service"
        return trino

    def spark(self) -> SparkService:
        spark = self.service(QueryEngineType.SPARK)
        assert isinstance(spark, SparkService), "Missing Spark service"
        return spark

    def start_counter_stream(
        self,
        topic: str,
        name: str = "ducky_stream",
        count: int = 100,
        interval: str = "",
    ) -> RedpandaConnectService:
        stream_conf = {
            "input": {
                "generate": {
                    "mapping": "root = counter()",
                    "interval": interval,
                    "count": count,
                    "batch_size": 1,
                }
            },
            "pipeline": {"processors": []},
            "output": {
                "redpanda": {
                    "seed_brokers": self.redpanda.brokers_list(),
                    "topic": topic,
                }
            },
        }
        connect = RedpandaConnectService(self.test_ctx, self.redpanda)
        connect.start()
        # create a stream
        connect.start_stream(name, config=stream_conf)
        return connect

    def service(self, engine_type: QueryEngineType) -> QueryEngineBase | None:
        for e in self.query_engines:
            if e.engine_name() == engine_type:
                return e
        return None

    def create_iceberg_enabled_topic(
        self,
        name: str,
        partitions=1,
        replicas=1,
        iceberg_mode: Literal["key_value"]
        | Literal["value_schema_id_prefix"]
        | Literal["value_schema_latest"]
        | str = "key_value",
        target_lag_ms: Optional[int] = None,
        config: dict[str, Any] = dict(),
    ) -> None:
        config[TopicSpec.PROPERTY_ICEBERG_MODE] = iceberg_mode
        if target_lag_ms:
            config[TopicSpec.PROPERTY_ICEBERG_TARGET_LAG_MS] = target_lag_ms
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            topic=name, partitions=partitions, replicas=replicas, config=config
        )

    def set_iceberg_mode_on_topic(self, topic: str, mode: str):
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(topic, "redpanda.iceberg.mode", mode)

    def catalog_client(self):
        return self.catalog_service.client(self.warehouse_name)

    def table_exists(self, id: str | Identifier, client=None):
        if client is None:
            client = self.catalog_client()

        if isinstance(id, str):
            id = ("redpanda", id)

        namespace = id[:-1]

        self.redpanda.logger.debug(f"looking for table {id}")
        try:
            tables = client.list_tables(namespace)
            self.redpanda.logger.debug(f"tables in {namespace}: {tables}")
            return id in tables
        except NoSuchNamespaceError:
            # Namespace doesn't exist, log namespace tree
            for i in range(len(namespace)):
                parent = namespace[:i]
                try:
                    children = client.list_namespaces(parent)
                    self.redpanda.logger.debug(f"namespaces under {parent}: {children}")
                except NoSuchNamespaceError:
                    break
            return False

    def num_tables(self, namespace="redpanda", client=None):
        if client is None:
            client = self.catalog_client()

        return len(client.list_tables(namespace))

    def wait_for_iceberg_table(
        self, namespace: str | Identifier, table, timeout, backoff_sec
    ):
        if isinstance(namespace, str):
            namespace = (namespace,)

        client = self.catalog_client()

        def table_created():
            return self.table_exists(namespace + (table,), client=client)

        wait_until(
            table_created,
            timeout_sec=timeout,
            backoff_sec=backoff_sec,
            err_msg=f"Timed out waiting for {namespace}.{table} to be created in the catalog",
        )

    def wait_for_translation_until_offset(
        self, topic: str, offset: int, partition=0, timeout=30, backoff_sec=5
    ) -> None:
        self.wait_for_iceberg_table("redpanda", topic, timeout, backoff_sec)

        def translation_done():
            assert len(self.query_engines) > 0, (
                "At least one query engine is required to check translation status"
            )

            offsets = dict(
                map(
                    lambda e: (
                        e.engine_name(),
                        e.max_translated_offset("redpanda", topic, partition),
                    ),
                    self.query_engines,
                )
            )
            self.redpanda.logger.debug(f"Current translated offsets: {offsets}")
            return all(
                [
                    max_offset is not None and offset <= max_offset
                    for _, max_offset in offsets.items()
                ]
            )

        wait_until(
            translation_done,
            timeout_sec=timeout,
            backoff_sec=backoff_sec,
            err_msg=f"Timed out waiting for iceberg translation until offset: {offset}",
        )

    def wait_for_translation(
        self,
        topic,
        msg_count,
        timeout=90,
        progress_sec=30,
        backoff_sec=5,
        namespace: Identifier = ("redpanda",),
        table_override=None,
        op=operator.eq,
    ):
        assert op in [operator.eq, operator.gt], f"Suspicious operator {op}"
        table_name = topic
        if table_override:
            table_name = table_override

        self.wait_for_iceberg_table(namespace, table_name, timeout, backoff_sec)

        def get_counts():
            assert len(self.query_engines) > 0, (
                "At least one query engine is required to check translation status"
            )

            return dict(
                map(
                    lambda e: (e.engine_name(), e.count_table(namespace, table_name)),
                    self.query_engines,
                )
            )

        # just want to know that something moved
        def total_count():
            counts = get_counts()
            return sum(c for _, c in counts.items())

        def translation_done():
            counts = get_counts()
            self.redpanda.logger.debug(
                f"Current counts for {table_name}: {counts}, want {op=} {msg_count}"
            )
            return all([op(c, msg_count) for _, c in counts.items()])

        wait_until_with_progress_check(
            check=total_count,
            condition=translation_done,
            timeout_sec=timeout,
            progress_sec=progress_sec,
            backoff_sec=backoff_sec,
            err_msg=f"Timed out waiting for events from {topic} to appear in datalake",
            logger=self.redpanda.logger,
        )

    def produce_to_topic(self, topic, msg_size, msg_count, rate_limit_bps=None):
        KgoVerifierProducer.oneshot(
            self.test_ctx,
            self.redpanda,
            topic,
            msg_size=msg_size,
            msg_count=msg_count,
            rate_limit_bps=rate_limit_bps,
        )

    def _create_catalog_service(self):
        if self._catalog_type == CatalogType.DATABRICKS_UNITY:
            # TODO: Do not allow callers to customize the warehouse name.
            assert self.warehouse_name == CatalogService.DEFAULT_WAREHOUSE_NAME, (
                "Unexpected customization of warehouse name in databricks unity test. We need to create one with a random name."
            )

            dbx_workspace = DatabricksWorkspace(self.test_ctx)
            dbx_catalog_info = dbx_workspace.create_catalog(self._cloud_storage_bucket)

            # Override.
            self.warehouse_name = dbx_catalog_info.name

            self.catalog_service = DatabricksUnity(
                self.test_ctx,
                cloud_storage_bucket=self._cloud_storage_bucket,
                catalog=dbx_catalog_info,
            )
        elif self._catalog_type == CatalogType.REST_JDBC:
            self.catalog_service = IcebergRESTCatalog(
                self.test_ctx,
                cloud_storage_bucket=self._cloud_storage_bucket,
                warehouse_name=self.warehouse_name,
            )
        elif self._catalog_type == CatalogType.REST_HADOOP:
            self.catalog_service = IcebergRESTCatalog(
                self.test_ctx,
                cloud_storage_bucket=self._cloud_storage_bucket,
                warehouse_name=self.warehouse_name,
                filesystem_wrapper_mode=True,
            )
        elif self._catalog_type == CatalogType.NESSIE:
            self.catalog_service = NessieCatalog(
                self.test_ctx,
                cloud_storage_bucket=self._cloud_storage_bucket,
                warehouse_name=self.warehouse_name,
            )
        elif self._catalog_type == CatalogType.BIGLAKE:
            # TODO: Do not allow callers to customize the warehouse name.
            assert self.warehouse_name == CatalogService.DEFAULT_WAREHOUSE_NAME, (
                "Unexpected customization of warehouse name in databricks unity test. We need to create one with a random name."
            )

            # Override.
            self.warehouse_name = f"gs://{self._cloud_storage_bucket}"

            self.catalog_service = BiglakeMetastore(
                self.test_ctx,
                cloud_storage_bucket=self._cloud_storage_bucket,
                gcp_context=GCPContext.from_context(self.test_ctx),
            )
        else:
            raise NotImplementedError(f"No catalog of type {self._catalog_type}")
