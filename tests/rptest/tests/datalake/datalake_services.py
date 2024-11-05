# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any
from rptest.archival.s3_client import S3Client
from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.services.apache_iceberg_catalog import IcebergCatalogMode, IcebergRESTCatalog
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService, SISettings
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.query_engine_factory import get_query_engine_by_type
from rptest.tests.end_to_end import EndToEndTest


class DatalakeServices(EndToEndTest):
    """Utility class for implementing datalake tests. Includes the
    boiler plate to manage dependent services."""
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeServices, self).__init__(
            test_ctx,
            si_settings=SISettings(test_context=test_ctx,
                                   bypass_bucket_creation=True),
            *args,
            **kwargs)
        assert self.si_settings
        self.s3_client = S3Client(
            region=self.si_settings.cloud_storage_region,
            access_key=self.si_settings.cloud_storage_access_key,
            secret_key=self.si_settings.cloud_storage_secret_key,
            endpoint=self.si_settings.endpoint_url,
            logger=self.logger,
            signature_version=self.si_settings.cloud_storage_signature_version,
            before_call_headers=self.si_settings.before_call_headers,
            use_fips_endpoint=self.si_settings.use_fips_endpoint(),
            addressing_style=self.si_settings.addressing_style)
        self.catalog_service = None
        # To be populated later once we have the URI of the catalog
        # available
        self.query_engines = []

    def start_services(self,
                       num_brokers=1,
                       iceberg_commit_interval_ms=5000,
                       catalog_mode=IcebergCatalogMode.REST,
                       include_query_engines: list[QueryEngineType] = [
                           QueryEngineType.SPARK, QueryEngineType.TRINO
                       ]):
        # Start catalog service
        assert self.si_settings
        self.s3_client.create_bucket(self.si_settings.cloud_storage_bucket)
        self.catalog_service = IcebergRESTCatalog(
            self.test_context,
            cloud_storage_bucket=self.si_settings.cloud_storage_bucket,
            cloud_storage_access_key=str(
                self.si_settings.cloud_storage_access_key),
            cloud_storage_secret_key=str(
                self.si_settings.cloud_storage_secret_key),
            cloud_storage_region=self.si_settings.cloud_storage_region,
            cloud_storage_api_endpoint=str(self.si_settings.endpoint_url),
            mode=catalog_mode)
        self.catalog_service.start()

        # Start the query engines
        for engine in include_query_engines:
            svc_cls = get_query_engine_by_type(engine)
            svc = svc_cls(self.test_ctx, str(self.catalog_service.catalog_url),
                          self.si_settings)
            svc.start()
            self.query_engines.append(svc)

        # Start Redpanda
        extra_rp_conf = {
            "iceberg_enabled": "true",
            "iceberg_catalog_commit_interval_ms": iceberg_commit_interval_ms
        }
        if self.catalog_service.catalog_mode == IcebergCatalogMode.REST:
            # Apply REST end point configurations in Redpanda
            # and restart brokers
            extra_rp_conf.update({
                "iceberg_catalog_type":
                "rest",
                "iceberg_rest_catalog_endpoint":
                self.catalog_service.catalog_url,
                "iceberg_rest_catalog_user_id":
                "panda-user",
                "iceberg_rest_catalog_secret":
                "panda-secret"
            })
        self.start_redpanda(num_nodes=num_brokers, extra_rp_conf=extra_rp_conf)

    def tearDown(self):
        for engine in self.query_engines:
            engine.stop()
        if self.catalog_service:
            self.catalog_service.stop()
        if self.redpanda:
            self.redpanda.stop()
            self.redpanda.delete_bucket_from_si()

    def __enter__(self):
        self.start_services()
        return self

    def __exit__(self, *args, **kwargs):
        self.stop_services()

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
        assert self.catalog_service
        client = self.catalog_service.client("redpanda-iceberg-catalog")

        def table_created():
            namespaces = client.list_namespaces()
            assert self.redpanda
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
