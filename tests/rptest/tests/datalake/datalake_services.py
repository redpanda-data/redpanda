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
from rptest.tests.datalake.iceberg_rest_catalog import IcebergRESTCatalogTest
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from ducktape.utils.util import wait_until
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.query_engine_factory import get_query_engine_by_type


class DatalakeServicesBase(IcebergRESTCatalogTest):
    """All inclusive base class for implementing datalake tests. Includes the
    boiler plate to enable datalake and bring up related services.
    """
    def __init__(self,
                 test_ctx,
                 filesystem_catalog_mode=True,
                 include_query_engines: list[QueryEngineType] = [
                     QueryEngineType.SPARK, QueryEngineType.TRINO
                 ],
                 *args,
                 **kwargs):
        super(DatalakeServicesBase,
              self).__init__(test_ctx,
                             extra_rp_conf={
                                 "iceberg_enabled": "true",
                                 "iceberg_catalog_commit_interval_ms": 5000
                             },
                             *args,
                             **kwargs)
        self.test_ctx = test_ctx
        self.catalog_service.set_filesystem_wrapper_mode(
            filesystem_catalog_mode)
        self.included_query_engines = include_query_engines
        # To be populated later once we have the URI of the catalog
        # available
        self.query_engines = []

    def setUp(self):
        super().setUp()
        for engine in self.included_query_engines:
            svc_cls = get_query_engine_by_type(engine)
            svc = svc_cls(self.test_ctx, str(self.catalog_service.catalog_url),
                          self.redpanda.si_settings)
            svc.start()
            self.query_engines.append(svc)

    def tearDown(self):
        for engine in self.query_engines:
            engine.stop()
        return super().tearDown()

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
            self.logger.debug(f"namespaces: {namespaces}")
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
