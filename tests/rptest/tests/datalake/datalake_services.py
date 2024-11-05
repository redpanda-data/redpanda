# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any
from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService
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
            cloud_storage_access_key=str(si_settings.cloud_storage_access_key),
            cloud_storage_secret_key=str(si_settings.cloud_storage_secret_key),
            cloud_storage_region=si_settings.cloud_storage_region,
            cloud_storage_api_endpoint=str(si_settings.endpoint_url),
            filesystem_wrapper_mode=filesystem_catalog_mode)
        self.included_query_engines = include_query_engines
        # To be populated later once we have the URI of the catalog
        # available
        self.query_engines = []

    def setUp(self):
        self.catalog_service.start()
        for engine in self.included_query_engines:
            svc_cls = get_query_engine_by_type(engine)
            svc = svc_cls(self.test_ctx, str(self.catalog_service.catalog_url),
                          self.redpanda.si_settings)
            svc.start()
            self.query_engines.append(svc)
        rest_mode = not self.catalog_service.filesystem_wrapper_mode
        if rest_mode:
            # Apply REST end point configurations in Redpanda
            # and restart brokers
            self.redpanda.set_cluster_config(
                {
                    "iceberg_catalog_type": "rest",
                    "iceberg_rest_catalog_endpoint":
                    self.catalog_service.catalog_url,
                    "iceberg_rest_catalog_user_id": "panda-user",
                    "iceberg_rest_catalog_secret": "panda-secret"
                },
                expect_restart=True)
            self.redpanda.logger.debug(
                "Restarting Redpanda for Catalog REST server configuration to take effect."
            )
            brokers = self.redpanda.nodes
            self.redpanda.restart_nodes(brokers)
            self.redpanda._admin.await_stable_leader(
                topic="controller",
                partition=0,
                namespace="redpanda",
                hosts=[n.account.hostname for n in brokers],
                timeout_s=30,
                backoff_s=1)

    def tearDown(self):
        for engine in self.query_engines:
            engine.stop()
        self.catalog_service.stop()

    def __enter__(self):
        self.setUp()
        return self

    def __exit__(self, *args, **kwargs):
        self.tearDown()

    def create_iceberg_enabled_topic(self,
                                     name,
                                     partitions=1,
                                     replicas=1,
                                     translation_interval_ms=3000,
                                     config: dict[str, Any] = dict()):
        config[TopicSpec.PROPERTY_ICEBERG_ENABLED] = "true"
        config[TopicSpec.
               PROPERTY_ICEBERG_TRANSLATION_INTERVAL] = translation_interval_ms
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic=name,
                         partitions=partitions,
                         replicas=replicas,
                         config=config)

    def wait_for_iceberg_table(self, namespace, table, timeout, backoff_sec):
        client = self.catalog_service.client("redpanda-iceberg-catalog")

        def table_created():
            namespaces = client.list_namespaces()
            self.redpanda.logger.debug(f"namespaces: {namespaces}")
            return (namespace, ) in namespaces and (
                namespace, table) in client.list_tables(namespace)

        wait_until(
            table_created,
            timeout_sec=timeout,
            backoff_sec=backoff_sec,
            err_msg=
            f"Timed out waiting {namespace}.{table} to be created in the catalog"
        )

    def wait_for_translation(self,
                             topic,
                             msg_count,
                             timeout=30,
                             backoff_sec=5):
        self.wait_for_iceberg_table("redpanda", topic, timeout, backoff_sec)
        table_name = f"redpanda.{topic}"

        def translation_done():
            return all([
                engine.count_table(table_name) == msg_count
                for engine in self.query_engines
            ])

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
