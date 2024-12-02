# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any
from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import RedpandaService
from rptest.services.spark_service import SparkService
from rptest.services.trino_service import TrinoService
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.query_engine_factory import get_query_engine_by_type


class DatalakeServices():
    """Utility class for implementing datalake tests. Includes the
    boiler plate to manage dependent services."""
    def __init__(self,
                 test_ctx,
                 redpanda: RedpandaService,
                 filesystem_catalog_mode=True,
                 include_query_engines: list[QueryEngineType] = [
                     QueryEngineType.SPARK, QueryEngineType.TRINO
                 ]):
        self.test_ctx = test_ctx
        self.redpanda = redpanda
        si_settings = self.redpanda.si_settings
        assert si_settings
        self.catalog_service = IcebergRESTCatalog(
            test_ctx,
            cloud_storage_bucket=si_settings.cloud_storage_bucket,
            filesystem_wrapper_mode=filesystem_catalog_mode)
        self.included_query_engines = include_query_engines
        # To be populated later once we have the URI of the catalog
        # available
        self.query_engines: list[Service] = []

    def setUp(self):
        assert len(self.redpanda.started_nodes()) == 0, \
            "DatalakeServices expects to start redpanda itself"

        # create bucket first, or the catalog won't start
        self.redpanda.start_si()

        self.catalog_service.start()

        if not self.catalog_service.filesystem_wrapper_mode:
            # REST catalog mode
            self.redpanda.add_extra_rp_conf({
                "iceberg_catalog_type":
                "rest",
                "iceberg_rest_catalog_endpoint":
                self.catalog_service.catalog_url,
                "iceberg_rest_catalog_client_id":
                "panda-user",
                "iceberg_rest_catalog_client_secret":
                "panda-secret",
            })
        self.redpanda.start(start_si=False)

        for engine in self.included_query_engines:
            svc_cls = get_query_engine_by_type(engine)
            svc = svc_cls(self.test_ctx, self.catalog_service.catalog_url)
            svc.start()
            self.query_engines.append(svc)

    def tearDown(self):
        for engine in self.query_engines:
            engine.stop()
        self.catalog_service.stop()

    def __enter__(self):
        self.setUp()
        return self

    def __exit__(self, *args, **kwargs):
        self.tearDown()

    def trino(self) -> TrinoService:
        trino = self.service(QueryEngineType.TRINO)
        assert trino, "Missing Trino service"
        return trino

    def spark(self) -> SparkService:
        spark = self.service(QueryEngineType.SPARK)
        assert spark, "Missing Spark service"
        return spark

    def service(self, engine_type: QueryEngineType):
        for e in self.query_engines:
            if e.engine_name() == engine_type:
                return e
        return None

    def create_iceberg_enabled_topic(self,
                                     name,
                                     partitions=1,
                                     replicas=1,
                                     iceberg_mode="key_value",
                                     config: dict[str, Any] = dict()):
        config[TopicSpec.PROPERTY_ICEBERG_MODE] = iceberg_mode
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic=name,
                         partitions=partitions,
                         replicas=replicas,
                         config=config)

    def set_iceberg_mode_on_topic(self, topic: str, mode: str):
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(topic, "redpanda.iceberg.mode", mode)

    def catalog_client(self):
        return self.catalog_service.client("redpanda-iceberg-catalog")

    def table_exists(self, table, namespace="redpanda", client=None):
        if client is None:
            client = self.catalog_client()

        namespaces = client.list_namespaces()
        self.redpanda.logger.debug(f"namespaces: {namespaces}")
        return (namespace, ) in namespaces and (
            namespace, table) in client.list_tables(namespace)

    def wait_for_iceberg_table(self, namespace, table, timeout, backoff_sec):
        client = self.catalog_client()

        def table_created():
            return self.table_exists(table, namespace=namespace, client=client)

        wait_until(
            table_created,
            timeout_sec=timeout,
            backoff_sec=backoff_sec,
            err_msg=
            f"Timed out waiting {namespace}.{table} to be created in the catalog"
        )

    def wait_for_translation_until_offset(self,
                                          topic,
                                          offset,
                                          partition=0,
                                          timeout=30,
                                          backoff_sec=5):
        self.wait_for_iceberg_table("redpanda", topic, timeout, backoff_sec)

        def translation_done():
            offsets = dict(
                map(
                    lambda e: (e.engine_name(
                    ), e.max_translated_offset("redpanda", topic, partition)),
                    self.query_engines))
            self.redpanda.logger.debug(
                f"Current translated offsets: {offsets}")
            return all([
                offset and offset <= max_offset
                for _, max_offset in offsets.items()
            ])

        wait_until(
            translation_done,
            timeout_sec=timeout,
            backoff_sec=backoff_sec,
            err_msg=
            f"Timed out waiting for iceberg translation until offset: {offset}"
        )

    def wait_for_translation(self,
                             topic,
                             msg_count,
                             timeout=30,
                             backoff_sec=5):
        self.wait_for_iceberg_table("redpanda", topic, timeout, backoff_sec)

        def translation_done():
            counts = dict(
                map(
                    lambda e:
                    (e.engine_name(), e.count_table("redpanda", topic)),
                    self.query_engines))
            self.redpanda.logger.debug(f"Current counts: {counts}")
            return all([c == msg_count for _, c in counts.items()])

        wait_until(
            translation_done,
            timeout_sec=timeout,
            backoff_sec=backoff_sec,
            err_msg=f"Timed out waiting for events to appear in datalake")

    def produce_to_topic(self,
                         topic,
                         msg_size,
                         msg_count,
                         rate_limit_bps=None):
        KgoVerifierProducer.oneshot(self.test_ctx,
                                    self.redpanda,
                                    topic,
                                    msg_size=msg_size,
                                    msg_count=msg_count,
                                    rate_limit_bps=rate_limit_bps)
