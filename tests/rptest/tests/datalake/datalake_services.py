# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any, Optional
from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.services.spark_service import SparkService
from rptest.services.trino_service import TrinoService
from rptest.tests.datalake.iceberg_rest_catalog import IcebergRESTCatalogTest
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from ducktape.utils.util import wait_until


class DatalakeServicesBase(IcebergRESTCatalogTest):
    """All inclusive base class for implementing datalake tests. Includes the
    boiler plate to enable datalake and bring up related services.
    """
    def __init__(self,
                 test_ctx,
                 filesystem_catalog_mode=True,
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
        self.filesystem_catalog_mode = filesystem_catalog_mode
        self.spark: Optional[SparkService] = None
        self.trino: Optional[TrinoService] = None

    def setUp(self):
        self.catalog_service.set_filesystem_wrapper_mode(
            self.filesystem_catalog_mode)
        super().setUp()
        self.spark = SparkService(self.test_ctx,
                                  str(self.catalog_service.catalog_url),
                                  self.redpanda.si_settings)
        self.spark.start()
        self.trino = TrinoService(self.test_ctx,
                                  str(self.catalog_service.catalog_url),
                                  self.redpanda.si_settings)
        self.trino.start()

    def tearDown(self):
        if self.spark:
            self.spark.stop()
        if self.trino:
            self.trino.stop()
        return super().tearDown()

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
            assert self.spark
            assert self.trino
            return self.spark.count_table(
                table_name) >= msg_count and self.trino.count_table(
                    table_name) >= msg_count

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
