# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum
import http.client
import json
from typing import Optional
import uuid
import re
import requests
import time
import random
import socket

from confluent_kafka.schema_registry import topic_subject_name_strategy, record_subject_name_strategy, topic_record_subject_name_strategy
from confluent_kafka.serialization import (MessageField, SerializationContext)
from ducktape.mark import parametrize, matrix
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.serde_client_utils import SchemaType, SerdeClientType
from rptest.clients.types import TopicSpec
from rptest.services import tls
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import DEFAULT_LOG_ALLOW_LIST, MetricsEndpoint, ResourceSettings, SecurityConfig, LoggingConfig, PandaproxyConfig, SchemaRegistryConfig
from rptest.services.serde_client import SerdeClient
from rptest.tests.cluster_config_test import wait_for_version_status_sync
from rptest.tests.pandaproxy_test import User, PandaProxyTLSProvider
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import inject_remote_script, search_logs_with_timeout


class SchemaIdValidationMode(str, Enum):
    NONE = "none"
    REDPANDA = "redpanda"
    COMPAT = "compat"


def create_topic_names(count):
    return list(f"pandaproxy-topic-{uuid.uuid4()}" for _ in range(count))


def get_subject_name(sns: str, topic: str, field: MessageField,
                     record_name: str):
    return {
        TopicSpec.SubjectNameStrategy.TOPIC_NAME:
        topic_subject_name_strategy,
        TopicSpec.SubjectNameStrategyCompat.TOPIC_NAME:
        topic_subject_name_strategy,
        TopicSpec.SubjectNameStrategy.RECORD_NAME:
        record_subject_name_strategy,
        TopicSpec.SubjectNameStrategyCompat.RECORD_NAME:
        record_subject_name_strategy,
        TopicSpec.SubjectNameStrategy.TOPIC_RECORD_NAME:
        topic_record_subject_name_strategy,
        TopicSpec.SubjectNameStrategyCompat.TOPIC_RECORD_NAME:
        topic_record_subject_name_strategy
    }[sns](SerializationContext(topic, field), record_name)


HTTP_GET_HEADERS = {"Accept": "application/vnd.schemaregistry.v1+json"}

HTTP_POST_HEADERS = {
    "Accept": "application/vnd.schemaregistry.v1+json",
    "Content-Type": "application/vnd.schemaregistry.v1+json"
}

schema1_def = '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
schema2_def = '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"},{"name":"f2","type":"string","default":"foo"}]}'
schema3_def = '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"},{"name":"f2","type":"string"}]}'
invalid_avro = '{"type":"notatype","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

simple_proto_def = """
syntax = "proto3";

message Simple {
  string id = 1;
}"""

imported_proto_def = """
syntax = "proto3";

import "simple";

message Test2 {
  Simple id =  1;
}"""

log_config = LoggingConfig('info',
                           logger_levels={
                               'security': 'trace',
                               'pandaproxy': 'trace',
                               'kafka/client': 'trace'
                           })


class SchemaRegistryEndpoints(RedpandaTest):
    """
    Test schema registry against a redpanda cluster.
    """
    def __init__(self,
                 context,
                 schema_registry_config=SchemaRegistryConfig(),
                 **kwargs):
        super(SchemaRegistryEndpoints, self).__init__(
            context,
            num_brokers=3,
            extra_rp_conf={"auto_create_topics_enabled": False},
            resource_settings=ResourceSettings(num_cpus=1),
            log_config=log_config,
            pandaproxy_config=PandaproxyConfig(),
            schema_registry_config=schema_registry_config,
            **kwargs)

        http.client.HTTPConnection.debuglevel = 1
        http.client.print = lambda *args: self.logger.debug(" ".join(args))

    def _request(self,
                 verb,
                 path,
                 hostname=None,
                 tls_enabled: bool = False,
                 **kwargs):
        """

        :param verb: String, as for first arg to requests.request
        :param path: URI path without leading slash
        :param timeout: Optional requests timeout in seconds
        :return:
        """

        if hostname is None:
            # Pick hostname once: we will retry the same place we got an error,
            # to avoid silently skipping hosts that are persistently broken
            nodes = [n for n in self.redpanda.nodes]
            random.shuffle(nodes)
            node = nodes[0]
            hostname = node.account.hostname

        scheme = "https" if tls_enabled else "http"
        uri = f"{scheme}://{hostname}:8081/{path}"

        if 'timeout' not in kwargs:
            kwargs['timeout'] = 60

        # Error codes that may appear during normal API operation, do not
        # indicate an issue with the service
        acceptable_errors = {409, 422, 404}

        def accept_response(resp):
            return 200 <= resp.status_code < 300 or resp.status_code in acceptable_errors

        self.logger.debug(f"{verb} hostname={hostname} {path} {kwargs}")

        # This is not a retry loop: you get *one* retry to handle issues
        # during startup, after that a failure is a failure.
        r = requests.request(verb, uri, **kwargs)
        if not accept_response(r):
            self.logger.info(
                f"Retrying for error {r.status_code} on {verb} {path} ({r.text})"
            )
            time.sleep(10)
            r = requests.request(verb, uri, **kwargs)
            if accept_response(r):
                self.logger.info(
                    f"OK after retry {r.status_code} on {verb} {path} ({r.text})"
                )
            else:
                self.logger.info(
                    f"Error after retry {r.status_code} on {verb} {path} ({r.text})"
                )

        self.logger.info(
            f"{r.status_code} {verb} hostname={hostname} {path} {kwargs}")

        return r

    def _base_uri(self):
        return f"http://{self.redpanda.nodes[0].account.hostname}:8081"

    def _get_rpk_tools(self):
        sasl_enabled = self.redpanda.sasl_enabled()
        cfg = self.redpanda.security_config() if sasl_enabled else {}
        return RpkTool(self.redpanda,
                       username=cfg.get('sasl_plain_username'),
                       password=cfg.get('sasl_plain_password'),
                       sasl_mechanism=cfg.get('sasl_mechanism'))

    def _get_serde_client(
            self,
            schema_type: SchemaType,
            client_type: SerdeClientType,
            topic: str,
            count: int,
            skip_known_types: Optional[bool] = None,
            subject_name_strategy: Optional[str] = None,
            payload_class: Optional[str] = None,
            compression_type: Optional[TopicSpec.CompressionTypes] = None):
        schema_reg = self.redpanda.schema_reg().split(',', 1)[0]
        sasl_enabled = self.redpanda.sasl_enabled()
        sec_cfg = self.redpanda.security_config() if sasl_enabled else None

        return SerdeClient(self.test_context,
                           self.redpanda.brokers(),
                           schema_reg,
                           schema_type,
                           client_type,
                           count,
                           topic=topic,
                           security_config=sec_cfg,
                           skip_known_types=skip_known_types,
                           subject_name_strategy=subject_name_strategy,
                           payload_class=payload_class,
                           compression_type=compression_type)

    def _get_topics(self):
        return requests.get(
            f"http://{self.redpanda.nodes[0].account.hostname}:8082/topics")

    def _create_topic(self,
                      topic=create_topic_names(1),
                      partition_count=1,
                      replication_factor=1,
                      config={
                          TopicSpec.PROPERTY_CLEANUP_POLICY:
                          TopicSpec.CLEANUP_DELETE
                      }):
        self.logger.debug(f"Creating topic: {topic}")
        rpk_tools = self._get_rpk_tools()
        rpk_tools.create_topic(topic=topic,
                               partitions=partition_count,
                               replicas=replication_factor,
                               config=config)

        def has_topic():
            self_topics = self._get_topics()
            self.logger.info(
                f"name: {topic}, self._get_topics().status_code: {self_topics.status_code}, self_topics.json(): {self_topics.json()}"
            )
            return topic in self_topics.json()

        wait_until(has_topic,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Timeout waiting for topic: {topic}")

        return topic

    def _alter_topic_config(self, topic, set_key, set_value):
        rpk = self._get_rpk_tools()
        rpk.alter_topic_config(topic=topic,
                               set_key=set_key,
                               set_value=set_value)

        def has_config():
            configs = rpk.describe_topic_configs(topic=topic)
            self.logger.warn(f"CONFIGS: {configs}")
            config = configs.get(set_key, None)
            self.logger.warn(f"CONFIG: {config[0]}")
            return config[0] == set_value

        wait_until(has_config, 5)

    def _get_config(self, headers=HTTP_GET_HEADERS, **kwargs):
        return self._request("GET", "config", headers=headers, **kwargs)

    def _set_config(self, data, headers=HTTP_POST_HEADERS, **kwargs):
        return self._request("PUT",
                             f"config",
                             headers=headers,
                             data=data,
                             **kwargs)

    def _get_config_subject(self,
                            subject,
                            fallback=False,
                            headers=HTTP_GET_HEADERS,
                            **kwargs):
        return self._request(
            "GET",
            f"config/{subject}{'?defaultToGlobal=true' if fallback else ''}",
            headers=headers,
            **kwargs)

    def _set_config_subject(self,
                            subject,
                            data,
                            headers=HTTP_POST_HEADERS,
                            **kwargs):
        return self._request("PUT",
                             f"config/{subject}",
                             headers=headers,
                             data=data,
                             **kwargs)

    def _get_mode(self, headers=HTTP_GET_HEADERS, **kwargs):
        return self._request("GET", "mode", headers=headers, **kwargs)

    def _get_schemas_types(self,
                           headers=HTTP_GET_HEADERS,
                           tls_enabled: bool = False,
                           **kwargs):
        return self._request("GET",
                             f"schemas/types",
                             headers=headers,
                             tls_enabled=tls_enabled,
                             **kwargs)

    def _get_schemas_ids_id(self, id, headers=HTTP_GET_HEADERS, **kwargs):
        return self._request("GET",
                             f"schemas/ids/{id}",
                             headers=headers,
                             **kwargs)

    def _get_schemas_ids_id_versions(self,
                                     id,
                                     headers=HTTP_GET_HEADERS,
                                     **kwargs):
        return self._request("GET",
                             f"schemas/ids/{id}/versions",
                             headers=headers,
                             **kwargs)

    def _get_subjects(self, deleted=False, headers=HTTP_GET_HEADERS, **kwargs):
        return self._request("GET",
                             f"subjects{'?deleted=true' if deleted else ''}",
                             headers=headers,
                             **kwargs)

    def _post_subjects_subject(self,
                               subject,
                               data,
                               headers=HTTP_POST_HEADERS,
                               **kwargs):
        return self._request("POST",
                             f"subjects/{subject}",
                             headers=headers,
                             data=data,
                             **kwargs)

    def _post_subjects_subject_versions(self,
                                        subject,
                                        data,
                                        headers=HTTP_POST_HEADERS,
                                        **kwargs):
        return self._request("POST",
                             f"subjects/{subject}/versions",
                             headers=headers,
                             data=data,
                             **kwargs)

    def _get_subjects_subject_versions_version(self,
                                               subject,
                                               version,
                                               headers=HTTP_GET_HEADERS,
                                               **kwargs):
        return self._request("GET",
                             f"subjects/{subject}/versions/{version}",
                             headers=headers,
                             **kwargs)

    def _get_subjects_subject_versions_version_referenced_by(
            self, subject, version, headers=HTTP_GET_HEADERS, **kwargs):
        return self._request(
            "GET",
            f"subjects/{subject}/versions/{version}/referencedBy",
            headers=headers,
            **kwargs)

    def _get_subjects_subject_versions(self,
                                       subject,
                                       deleted=False,
                                       headers=HTTP_GET_HEADERS,
                                       **kwargs):
        return self._request(
            "GET",
            f"subjects/{subject}/versions{'?deleted=true' if deleted else ''}",
            headers=headers,
            **kwargs)

    def _delete_subject(self,
                        subject,
                        permanent=False,
                        headers=HTTP_GET_HEADERS,
                        **kwargs):
        return self._request(
            "DELETE",
            f"subjects/{subject}{'?permanent=true' if permanent else ''}",
            headers=headers,
            **kwargs)

    def _delete_subject_version(self,
                                subject,
                                version,
                                permanent=False,
                                headers=HTTP_GET_HEADERS,
                                **kwargs):
        return self._request(
            "DELETE",
            f"subjects/{subject}/versions/{version}{'?permanent=true' if permanent else ''}",
            headers=headers,
            **kwargs)

    def _post_compatibility_subject_version(self,
                                            subject,
                                            version,
                                            data,
                                            headers=HTTP_POST_HEADERS,
                                            **kwargs):
        return self._request(
            "POST",
            f"compatibility/subjects/{subject}/versions/{version}",
            headers=headers,
            data=data,
            **kwargs)


class SchemaRegistryTestMethods(SchemaRegistryEndpoints):
    """
    Base class for testing schema registry against a redpanda cluster.

    Inherit from this to run the tests.
    """
    def __init__(self, context, **kwargs):
        super(SchemaRegistryTestMethods, self).__init__(context, **kwargs)

    @cluster(num_nodes=3)
    def test_schemas_types(self):
        """
        Verify the schema registry returns the supported types
        """
        self.logger.debug(f"Request schema types with no accept header")
        result_raw = self._get_schemas_types(headers={})
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug(f"Request schema types with defautl accept header")
        result_raw = self._get_schemas_types()
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert set(result) == {"PROTOBUF", "AVRO"}

    @cluster(num_nodes=3)
    def test_get_schema_id_versions(self):
        """
        Verify schema versions
        """

        self.logger.debug("Checking schema 1 versions - expect 40403")
        result_raw = self._get_schemas_ids_id_versions(id=1)
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40403
        assert result_raw.json()["message"] == "Schema 1 not found"

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"

        schema_1_data = json.dumps({"schema": schema1_def})
        schema_2_data = json.dumps({"schema": schema2_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(subject=subject,
                                                          data=schema_1_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Checking schema 1 versions")
        result_raw = self._get_schemas_ids_id_versions(id=1)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [{"subject": subject, "version": 1}]

        self.logger.debug("Posting schema 2 as a subject key")
        result_raw = self._post_subjects_subject_versions(subject=subject,
                                                          data=schema_2_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 2

        self.logger.debug("Checking schema 2 versions")
        result_raw = self._get_schemas_ids_id_versions(id=2)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [{"subject": subject, "version": 2}]

        self.logger.debug("Deleting version 1")
        result_raw = self._delete_subject_version(subject=subject, version=1)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Checking schema 1 versions is empty")
        result_raw = self._get_schemas_ids_id_versions(id=1)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == []

        self.logger.debug("Checking schema 2 versions")
        result_raw = self._get_schemas_ids_id_versions(id=2)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [{"subject": subject, "version": 2}]

        self.logger.debug("Deleting subject")
        result_raw = self._delete_subject(subject=subject)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Checking schema 1 versions is empty")
        result_raw = self._get_schemas_ids_id_versions(id=1)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == []

        self.logger.debug("Checking schema 2 versions is empty")
        result_raw = self._get_schemas_ids_id_versions(id=2)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == []

    @cluster(num_nodes=3)
    def test_post_subjects_subject_versions(self):
        """
        Verify posting a schema
        """

        topic = create_topic_names(1)[0]

        self.logger.debug(f"Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Get empty subjects")
        result_raw = self._get_subjects()
        if result_raw.json() != []:
            self.logger.error(result_raw.json)
        assert result_raw.json() == []

        self.logger.debug("Posting invalid schema as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=json.dumps({"schema": invalid_avro}))
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.unprocessable_entity

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Reposting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Reposting schema 1 as a subject value")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-value", data=schema_1_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Get subjects")
        result_raw = self._get_subjects()
        assert set(result_raw.json()) == {f"{topic}-key", f"{topic}-value"}

        self.logger.debug("Get schema versions for invalid subject")
        result_raw = self._get_subjects_subject_versions(
            subject=f"{topic}-invalid")
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == f"Subject '{topic}-invalid' not found."

        self.logger.debug("Get schema versions for subject key")
        result_raw = self._get_subjects_subject_versions(
            subject=f"{topic}-key")
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1]

        self.logger.debug("Get schema versions for subject value")
        result_raw = self._get_subjects_subject_versions(
            subject=f"{topic}-value")
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1]

        self.logger.debug("Get schema version 1 for invalid subject")
        result_raw = self._get_subjects_subject_versions_version(
            subject=f"{topic}-invalid", version=1)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == f"Subject '{topic}-invalid' not found."

        self.logger.debug("Get invalid schema version for subject")
        result_raw = self._get_subjects_subject_versions_version(
            subject=f"{topic}-key", version=2)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40402
        assert result["message"] == f"Version 2 not found."

        self.logger.debug("Get schema version 1 for subject key")
        result_raw = self._get_subjects_subject_versions_version(
            subject=f"{topic}-key", version=1)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == f"{topic}-key"
        assert result["version"] == 1
        # assert result["schema"] == json.dumps(schema_def)

        self.logger.debug("Get latest schema version for subject key")
        result_raw = self._get_subjects_subject_versions_version(
            subject=f"{topic}-key", version="latest")
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == f"{topic}-key"
        assert result["version"] == 1
        # assert result["schema"] == json.dumps(schema_def)

        self.logger.debug("Get invalid schema version")
        result_raw = self._get_schemas_ids_id(id=2)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40403
        assert result["message"] == "Schema 2 not found"

        self.logger.debug("Get schema version 1")
        result_raw = self._get_schemas_ids_id(id=1)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        # assert result["schema"] == json.dumps(schema_def)

    @cluster(num_nodes=3)
    def test_post_subjects_subject_versions_version_many(self):
        """
        Verify posting a schema many times
        """

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"
        schema_1_data = json.dumps({"schema": schema1_def})

        # Post the same schema many times.
        for _ in range(20):
            result_raw = self._post_subjects_subject_versions(
                subject=subject, data=schema_1_data)
            self.logger.debug(result_raw)
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json()["id"] == 1

    @cluster(num_nodes=3)
    def test_post_subjects_subject(self):
        """
        Verify posting a schema
        """

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"

        self.logger.info(
            "Posting against non-existant subject should be 40401")
        result_raw = self._post_subjects_subject(subject=subject,
                                                 data=json.dumps(
                                                     {"schema": schema1_def}))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == f"Subject '{subject}' not found."

        self.logger.info(
            "Posting invalid schema to non-existant subject should be 40401")
        result_raw = self._post_subjects_subject(subject=subject,
                                                 data=json.dumps(
                                                     {"schema": invalid_avro}))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == f"Subject '{subject}' not found."

        self.logger.info("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=subject, data=json.dumps({"schema": schema1_def}))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.info(
            "Posting invalid schema to existing subject should be 500")
        result_raw = self._post_subjects_subject(subject=subject,
                                                 data=json.dumps(
                                                     {"schema": invalid_avro}))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.internal_server_error
        result = result_raw.json()
        assert result["error_code"] == 500
        assert result[
            "message"] == f"Error while looking up schema under subject {subject}"

        self.logger.info("Posting existing schema should be success")
        result_raw = self._post_subjects_subject(subject=subject,
                                                 data=json.dumps(
                                                     {"schema": schema1_def}))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == subject
        assert result["id"] == 1
        assert result["version"] == 1
        assert result["schema"]

        self.logger.info("Posting new schema should be 40403")
        result_raw = self._post_subjects_subject(subject=subject,
                                                 data=json.dumps(
                                                     {"schema": schema3_def}))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40403
        assert result["message"] == f"Schema not found"

    @cluster(num_nodes=3)
    def test_config(self):
        """
        Smoketest config endpoints
        """
        self.logger.debug("Get initial global config")
        result_raw = self._get_config()
        assert result_raw.json()["compatibilityLevel"] == "BACKWARD"

        self.logger.debug("Set global config")
        result_raw = self._set_config(
            data=json.dumps({"compatibility": "FULL"}))
        assert result_raw.json()["compatibility"] == "FULL"

        # Check out set write shows up in a read
        result_raw = self._get_config()
        self.logger.debug(
            f"response {result_raw.status_code} {result_raw.text}")
        assert result_raw.json()["compatibilityLevel"] == "FULL"

        self.logger.debug("Get invalid subject config")
        result_raw = self._get_config_subject(subject="invalid_subject")
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40408
        assert result_raw.json(
        )["message"] == f"Subject 'invalid_subject' does not have subject-level compatibility configured"

        schema_1_data = json.dumps({"schema": schema1_def})

        topic = create_topic_names(1)[0]

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data)

        self.logger.debug("Get subject config - should fail")
        result_raw = self._get_config_subject(subject=f"{topic}-key")
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40408
        assert result_raw.json(
        )["message"] == f"Subject '{topic}-key' does not have subject-level compatibility configured"

        self.logger.debug("Get subject config - fallback to global")
        result_raw = self._get_config_subject(subject=f"{topic}-key",
                                              fallback=True)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["compatibilityLevel"] == "FULL"

        self.logger.debug("Set subject config")
        result_raw = self._set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "BACKWARD_TRANSITIVE"}))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["compatibility"] == "BACKWARD_TRANSITIVE"

        self.logger.debug("Get subject config - should be overriden")
        result_raw = self._get_config_subject(subject=f"{topic}-key")
        assert result_raw.json()["compatibilityLevel"] == "BACKWARD_TRANSITIVE"

    @cluster(num_nodes=3)
    def test_mode(self):
        """
        Smoketest get_mode endpoint
        """
        self.logger.debug("Get initial global mode")
        result_raw = self._get_mode()
        assert result_raw.json()["mode"] == "READWRITE"

    @cluster(num_nodes=3)
    def test_post_compatibility_subject_version(self):
        """
        Verify compatibility
        """

        topic = create_topic_names(1)[0]

        self.logger.debug(f"Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})
        schema_2_data = json.dumps({"schema": schema2_def})
        schema_3_data = json.dumps({"schema": schema3_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set subject config - NONE")
        result_raw = self._set_config_subject(subject=f"{topic}-key",
                                              data=json.dumps(
                                                  {"compatibility": "NONE"}))
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Check compatibility none, no default")
        result_raw = self._post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=schema_2_data)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == True

        self.logger.debug("Set subject config - BACKWARD")
        result_raw = self._set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "BACKWARD"}))
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Check compatibility backward, with default")
        result_raw = self._post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=schema_2_data)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == True

        self.logger.debug("Check compatibility backward, no default")
        result_raw = self._post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=schema_3_data)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == False

        self.logger.debug("Posting incompatible schema 3 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_3_data)
        assert result_raw.status_code == requests.codes.conflict
        assert result_raw.json()["error_code"] == 409

        self.logger.debug("Posting compatible schema 2 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_2_data)
        assert result_raw.status_code == requests.codes.ok

    @cluster(num_nodes=3)
    def test_delete_subject(self):
        """
        Verify delete subject
        """

        topic = create_topic_names(1)[0]

        self.logger.debug(f"Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})
        schema_2_data = json.dumps({"schema": schema2_def})
        schema_3_data = json.dumps({"schema": schema3_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set subject config - NONE")
        result_raw = self._set_config_subject(subject=f"{topic}-key",
                                              data=json.dumps(
                                                  {"compatibility": "NONE"}))
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 2 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_2_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 3 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_3_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        # Check that permanent delete is refused before soft delete
        self.logger.debug("Prematurely permanently delete subject")
        result_raw = self._delete_subject(subject=f"{topic}-key",
                                          permanent=True)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.not_found

        self.logger.debug("delete version 2")
        result_raw = self._delete_subject_version(subject=f"{topic}-key",
                                                  version=2)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Soft delete subject - expect 1,3")
        result_raw = self._delete_subject(subject=f"{topic}-key")
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1, 3]

        self.logger.debug("Get versions")
        result_raw = self._get_subjects_subject_versions(
            subject=f"{topic}-key")
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.not_found

        self.logger.debug("Get versions - include deleted")
        result_raw = self._get_subjects_subject_versions(
            subject=f"{topic}-key", deleted=True)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1, 2, 3]

        self.logger.debug("Permanently delete subject")
        result_raw = self._delete_subject(subject=f"{topic}-key",
                                          permanent=True)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1, 2, 3]

    @cluster(num_nodes=3)
    def test_delete_subject_version(self):
        """
        Verify delete subject version
        """

        topic = create_topic_names(1)[0]

        self.logger.debug(f"Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})
        schema_2_data = json.dumps({"schema": schema2_def})
        schema_3_data = json.dumps({"schema": schema3_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set subject config - NONE")
        result_raw = self._set_config_subject(subject=f"{topic}-key",
                                              data=json.dumps(
                                                  {"compatibility": "NONE"}))
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 2 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_2_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 3 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_3_data)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Permanently delete version 2")
        result_raw = self._delete_subject_version(subject=f"{topic}-key",
                                                  version=2,
                                                  permanent=True)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40407

        self.logger.debug("Soft delete version 2")
        result_raw = self._delete_subject_version(
            subject=f"{topic}-key",
            version=2,
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Soft delete version 2 - second time")
        result_raw = self._delete_subject_version(
            subject=f"{topic}-key",
            version=2,
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40406

        self.logger.debug("Get versions")
        result_raw = self._get_subjects_subject_versions(
            subject=f"{topic}-key")
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1, 3]

        self.logger.debug("Get versions - include deleted")
        result_raw = self._get_subjects_subject_versions(
            subject=f"{topic}-key", deleted=True)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1, 2, 3]

        self.logger.debug("Permanently delete version 2")
        result_raw = self._delete_subject_version(subject=f"{topic}-key",
                                                  version=2,
                                                  permanent=True)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Permanently delete version 2 - second time")
        result_raw = self._delete_subject_version(subject=f"{topic}-key",
                                                  version=2,
                                                  permanent=True)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40402

    @cluster(num_nodes=3)
    def test_mixed_deletes(self):
        """
        Exercise unfriendly ordering of soft/hard version deletes
        """
        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"
        schema_1_data = json.dumps({"schema": schema1_def})
        schema_2_data = json.dumps({"schema": schema2_def})

        result_raw = self._post_subjects_subject_versions(subject=subject,
                                                          data=schema_1_data)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == {"id": 1}

        result_raw = self._post_subjects_subject_versions(subject=subject,
                                                          data=schema_2_data)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == {"id": 2}

        # A 'latest' hard deletion will always fail because it tries
        # to delete the latest non-soft-deleted version
        r = self._delete_subject_version(subject=subject,
                                         version="latest",
                                         permanent=True)
        assert r.status_code == requests.codes.not_found
        assert r.json()['error_code'] == 40407

        # Latest soft deletions are okay
        r = self._delete_subject_version(subject=subject,
                                         version="latest",
                                         permanent=False)
        assert r.status_code == requests.codes.ok

        # A latest hard deletion still fails, because the 'latest' is
        # version 1
        r = self._delete_subject_version(subject=subject,
                                         version="latest",
                                         permanent=True)
        assert r.status_code == requests.codes.not_found
        assert r.json()['error_code'] == 40407

        # Latest soft deletions are okay
        r = self._delete_subject_version(subject=subject,
                                         version="latest",
                                         permanent=False)
        assert r.status_code == requests.codes.ok

        # Subject should still be visible with deleted=true
        r = self._get_subjects(deleted=True)
        assert r.status_code == requests.codes.ok
        assert r.json() == [subject]

        # Subject with all versions deleted should be invisible to normal listing
        r = self._get_subjects()
        assert r.status_code == requests.codes.ok
        assert r.json() == []

        # Hard-deleting by specific version number & having already soft deleted it
        r = self._delete_subject_version(subject=subject,
                                         version="2",
                                         permanent=True)
        assert r.status_code == requests.codes.ok

        # Hard-deleting by specific version number & having already soft deleted it
        r = self._delete_subject_version(subject=subject,
                                         version="1",
                                         permanent=True)
        assert r.status_code == requests.codes.ok

        # Hard deleting all versions is equivalent to hard deleting the subject,
        # so a subsequent attempt to delete latest version on subject
        # gives a subject_not_found error
        r = self._delete_subject_version(subject=subject,
                                         version="latest",
                                         permanent=True)
        assert r.status_code == requests.codes.not_found
        assert r.json()['error_code'] == 40401

        # Subject is now truly gone, not even visible with deleted=true
        r = self._get_subjects(deleted=True)
        assert r.status_code == requests.codes.ok
        assert r.json() == []

    @cluster(num_nodes=4)
    def test_concurrent_writes(self):
        # Warm up the servers (schema_registry doesn't create topic etc before first access)
        for node in self.redpanda.nodes:
            r = self._request("GET",
                              "subjects",
                              hostname=node.account.hostname)
            assert r.status_code == requests.codes.ok

        node_names = [n.account.hostname for n in self.redpanda.nodes]

        # Expose into StressTest
        logger = self.logger
        python = "python3"

        class StressTest(BackgroundThreadService):
            def __init__(self, context):
                super(StressTest, self).__init__(context, num_nodes=1)
                self.script_path = None

            def _worker(self, idx, node):
                self.script_path = inject_remote_script(
                    node, "schema_registry_test_helper.py")
                ssh_output = node.account.ssh_capture(
                    f"{python} {self.script_path} {' '.join(node_names)}")
                for line in ssh_output:
                    logger.info(line)

            def clean_nodes(selfself, nodes):
                # Remove our remote script
                if self.script_path:
                    for n in nodes:
                        n.account.remove(self.script_path, True)

        svc = StressTest(self.test_context)
        svc.start()
        svc.wait()

    @cluster(num_nodes=3)
    def test_protobuf(self):
        """
        Verify basic protobuf functionality
        """

        self.logger.info("Posting failed schema should be 422")
        result_raw = self._post_subjects_subject_versions(
            subject="imported",
            data=json.dumps({
                "schema": imported_proto_def,
                "schemaType": "PROTOBUF"
            }))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.unprocessable_entity

        self.logger.info("Posting simple as a subject key")
        result_raw = self._post_subjects_subject_versions(subject="simple",
                                                          data=json.dumps({
                                                              "schema":
                                                              simple_proto_def,
                                                              "schemaType":
                                                              "PROTOBUF"
                                                          }))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.info("Posting imported as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject="imported",
            data=json.dumps({
                "schema":
                imported_proto_def,
                "schemaType":
                "PROTOBUF",
                "references": [{
                    "name": "simple",
                    "subject": "simple",
                    "version": 1
                }]
            }))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 2

        result_raw = self._request("GET",
                                   f"subjects/simple/versions/1/schema",
                                   headers=HTTP_GET_HEADERS)
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.text.strip() == simple_proto_def.strip()

        result_raw = self._request("GET",
                                   f"schemas/ids/1",
                                   headers=HTTP_GET_HEADERS)
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["schemaType"] == "PROTOBUF"
        assert result["schema"].strip() == simple_proto_def.strip()

        result_raw = self._get_subjects_subject_versions_version_referenced_by(
            "simple", 1)
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [2]

    @cluster(num_nodes=4)
    @parametrize(protocol=SchemaType.AVRO, client_type=SerdeClientType.Python)
    @parametrize(protocol=SchemaType.AVRO, client_type=SerdeClientType.Java)
    @parametrize(protocol=SchemaType.AVRO, client_type=SerdeClientType.Golang)
    @parametrize(protocol=SchemaType.PROTOBUF,
                 client_type=SerdeClientType.Python,
                 skip_known_types=False)
    @parametrize(protocol=SchemaType.PROTOBUF,
                 client_type=SerdeClientType.Python,
                 skip_known_types=True)
    @parametrize(protocol=SchemaType.PROTOBUF,
                 client_type=SerdeClientType.Java,
                 skip_known_types=False)
    @parametrize(protocol=SchemaType.PROTOBUF,
                 client_type=SerdeClientType.Java,
                 skip_known_types=True)
    @parametrize(protocol=SchemaType.PROTOBUF,
                 client_type=SerdeClientType.Golang)
    def test_serde_client(self,
                          protocol: SchemaType,
                          client_type: SerdeClientType,
                          skip_known_types: Optional[bool] = None):
        """
        Verify basic operation of Schema registry across a range of schema types and serde
        client types
        """

        topic = f"serde-topic-{protocol.name}-{client_type.name}"
        self._create_topic(topic=topic)
        schema_reg = self.redpanda.schema_reg().split(',', 1)[0]
        self.logger.info(
            f"Connecting to redpanda: {self.redpanda.brokers()} schema_Reg: {schema_reg}"
        )
        client = self._get_serde_client(protocol, client_type, topic, 2,
                                        skip_known_types)
        self.logger.debug("Starting client")
        client.start()
        self.logger.debug("Waiting on client")
        client.wait()
        self.logger.debug("Client completed")

        schema = self._get_subjects_subject_versions_version(
            f"{topic}-value", "latest")
        self.logger.info(schema.json())
        if protocol == SchemaType.AVRO:
            assert schema.json().get("schemaType") is None
        else:
            assert schema.json()["schemaType"] == protocol.name

    @cluster(num_nodes=4)
    @matrix(protocol=[SchemaType.AVRO, SchemaType.PROTOBUF],
            client_type=[SerdeClientType.Python],
            validate_schema_id=[True],
            subject_name_strategy=list(TopicSpec.SubjectNameStrategyCompat),
            payload_class=[
                "com.redpanda.Payload", "com.redpanda.A.B.C.D.NestedPayload",
                "com.redpanda.CompressiblePayload"
            ],
            compression_type=[
                TopicSpec.CompressionTypes.NONE,
                TopicSpec.CompressionTypes.ZSTD
            ])
    def test_schema_id_validation(
            self,
            protocol: SchemaType,
            client_type: SerdeClientType,
            skip_known_types: Optional[bool] = None,
            validate_schema_id: Optional[bool] = None,
            subject_name_strategy: Optional[str] = None,
            payload_class: str = "com.redpanda.Payload",
            compression_type: Optional[TopicSpec.CompressionTypes] = None):
        self.redpanda.set_cluster_config(
            {'enable_schema_id_validation': SchemaIdValidationMode.COMPAT})

        def get_next_strategy(subject_name_strategy):
            all_strategies = list(TopicSpec.SubjectNameStrategyCompat)
            index = all_strategies.index(subject_name_strategy)
            return all_strategies[(index + 1) % len(all_strategies)]

        # Create a topic with incorrect strategy
        topic = f"serde-topic-{protocol.name}-{client_type.name}"

        def check_subject():
            expected = get_subject_name(subject_name_strategy, topic,
                                        MessageField.VALUE, payload_class)
            result_raw = self._get_subjects()
            assert result_raw.status_code == requests.codes.ok
            res_subjects = result_raw.json()
            self.logger.debug(
                f"SUBJECTS: {res_subjects}, expected: {expected}")

            return expected in res_subjects

        def bool_alpha(b: bool) -> str:
            return f"{b}".lower()

        self._create_topic(
            topic=topic,
            config={
                TopicSpec.PROPERTY_RECORD_VALUE_SCHEMA_ID_VALIDATION_COMPAT:
                bool_alpha(validate_schema_id),
                TopicSpec.PROPERTY_RECORD_VALUE_SUBJECT_NAME_STRATEGY_COMPAT:
                get_next_strategy(subject_name_strategy),
                TopicSpec.PROPERTY_COMPRESSSION:
                TopicSpec.COMPRESSION_PRODUCER,
            })
        schema_reg = self.redpanda.schema_reg().split(',', 1)[0]
        self.logger.info(
            f"Connecting to redpanda: {self.redpanda.brokers()} schema_Reg: {schema_reg}"
        )

        # Test against misconfigered strategy
        client = self._get_serde_client(
            protocol,
            client_type,
            topic,
            2,
            skip_known_types,
            subject_name_strategy=subject_name_strategy,
            payload_class=payload_class,
            compression_type=compression_type)

        self.logger.debug("Running client, expecting failure")
        try:
            client.run()
            assert False, "expected exit code 87"
        except Exception as ex:
            if "returned non-zero exit status 87" not in ex.args[0]:
                self.logger.debug("RemoteCommandError exit WAS NOT 87!")
                raise
            self.logger.debug("RemoteCommandError exit WAS 87!")
        finally:
            client.reset()

        self.logger.debug("Client completed")

        assert check_subject()

        # Update the strategy to the expected one
        self._alter_topic_config(
            topic=topic,
            set_key=TopicSpec.
            PROPERTY_RECORD_VALUE_SUBJECT_NAME_STRATEGY_COMPAT,
            set_value=subject_name_strategy)

        self.logger.debug("Running client, expecting success")
        client.run()

        batches_decompressed = self.redpanda.metric_sum(
            metric_name=
            "vectorized_kafka_schema_id_cache_batches_decompressed_total",
            metrics_endpoint=MetricsEndpoint.METRICS)

        if compression_type != TopicSpec.CompressionTypes.NONE and payload_class == "com.redpanda.CompressiblePayload":
            assert batches_decompressed > 0
        if compression_type == TopicSpec.CompressionTypes.NONE:
            assert batches_decompressed == 0

        self.logger.debug("Client completed")

    @cluster(num_nodes=3)
    def test_restarts(self):
        admin = Admin(self.redpanda)

        def check_connection(hostname: str):
            result_raw = self._get_subjects(hostname=hostname)
            self.logger.info(result_raw.status_code)
            self.logger.info(result_raw.json())
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json() == []

        def restart_leader():
            leader = admin.get_partition_leader(namespace="kafka",
                                                topic="_schemas",
                                                partition=0)
            self.logger.info(f"Restarting node: {leader}")
            self.redpanda.restart_nodes(self.redpanda.get_node(leader))
            admin.await_stable_leader(topic="_schemas",
                                      partition=0,
                                      namespace="kafka")

        for _ in range(20):
            for n in self.redpanda.nodes:
                check_connection(n.account.hostname)
            restart_leader()

    @cluster(num_nodes=3)
    def test_move_leader(self):
        admin = Admin(self.redpanda)

        def check_connection(hostname: str):
            result_raw = self._get_subjects(hostname=hostname)
            self.logger.info(result_raw.status_code)
            self.logger.info(result_raw.json())
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json() == []

        def get_leader():
            return admin.get_partition_leader(namespace="kafka",
                                              topic="_schemas",
                                              partition=0)

        def move_leader():
            leader = get_leader()
            self.logger.info(f"Transferring leadership from node: {leader}")
            admin.partition_transfer_leadership(namespace="kafka",
                                                topic="_schemas",
                                                partition=0)
            wait_until(lambda: get_leader() != leader,
                       timeout_sec=10,
                       backoff_sec=1,
                       err_msg="Leadership did not stabilize")

        for _ in range(20):
            for n in self.redpanda.nodes:
                check_connection(n.account.hostname)
            move_leader()

    @cluster(num_nodes=3)
    def test_restart_schema_registry(self):
        # Create several schemas on one subject and return the list schemas and schema ids
        def register_schemas(subject: str):
            schemas = [
                json.dumps({"schema": schema1_def}),
                json.dumps({"schema": schema2_def}),
                json.dumps({"schema": schema3_def})
            ]
            schema_ids = []

            for schema in schemas:
                self.logger.debug(f"Post schema {json.loads(schema)}")
                result_raw = self._post_subjects_subject_versions(
                    subject=subject, data=schema)
                self.logger.debug(result_raw)
                assert result_raw.status_code == requests.codes.ok
                res = result_raw.json()
                self.logger.debug(res)
                assert "id" in res
                schema_ids.append(result_raw.json()["id"])

            return schemas, schema_ids

        # Given a list of subjects, check that they exist on the registry
        def check_subjects(subjects: list[str]):
            result_raw = self._get_subjects()
            self.logger.debug(result_raw)
            assert result_raw.status_code == requests.codes.ok
            res_subjects = result_raw.json()
            self.logger.debug(res_subjects)
            assert type(res_subjects) == type([])
            res_subjects.sort()
            subjects.sort()
            assert res_subjects == subjects

        # Given the subject and list of versions, check that the version numbers match
        def check_subject_versions(subject: str, subject_versions: list[int]):
            result_raw = self._get_subjects_subject_versions(subject=subject)
            self.logger.debug(result_raw)
            assert result_raw.status_code == requests.codes.ok
            res_versions = result_raw.json()
            self.logger.debug(res_versions)
            assert type(res_versions) == type([])
            assert res_versions == subject_versions

        # Given the subject, list of schemas, list of versions, and list of ids,
        # check the schema that maps to the id
        def check_each_schema(subject: str, schemas: list[str],
                              schema_ids: list[int],
                              subject_versions: list[int]):
            for idx, version in zip(schema_ids, subject_versions):
                self.logger.debug(
                    f"Fetch schema version {version} on subject {subject}")
                result_raw = self._get_subjects_subject_versions_version(
                    subject=subject, version=version)
                self.logger.debug(result_raw)
                assert result_raw.status_code == requests.codes.ok
                res = result_raw.json()
                self.logger.debug(res)
                assert type(res) == type({})
                assert "subject" in res
                assert "version" in res
                assert "id" in res
                assert "schema" in res
                assert res["subject"] == subject
                assert res["version"] == version
                assert res["id"] == idx
                schema = json.loads(schemas[version - 1])
                assert res["schema"] == schema["schema"]

        admin = Admin(self.redpanda)

        num_subjects = 3
        subjects = {}
        for _ in range(num_subjects):
            subjects[f"{create_topic_names(1)[0]}-key"] = {
                "schemas": [],
                "schema_ids": [],
                "subject_versions": []
            }

        self.logger.debug("Register and check schemas before restart")
        for subject in subjects:
            schemas, schema_ids = register_schemas(subject)
            result_raw = self._get_subjects_subject_versions(subject=subject)
            self.logger.debug(result_raw)
            assert result_raw.status_code == requests.codes.ok
            subject_versions = result_raw.json()
            check_subject_versions(subject, subject_versions)
            check_each_schema(subject, schemas, schema_ids, subject_versions)
            subjects[subject]["schemas"] = schemas
            subjects[subject]["schema_ids"] = schema_ids
            subjects[subject]["subject_versions"] = subject_versions
        check_subjects(list(subjects.keys()))

        self.logger.debug("Restart the schema registry")
        result_raw = admin.restart_service(rp_service='schema-registry')
        search_logs_with_timeout(self.redpanda,
                                 "Restarting the schema registry")
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Check schemas after restart")
        check_subjects(list(subjects.keys()))
        for subject in subjects:
            check_subject_versions(subject, subjects[subject]["schema_ids"])
            check_each_schema(subject, subjects[subject]["schemas"],
                              subjects[subject]["schema_ids"],
                              subjects[subject]["subject_versions"])


class SchemaRegistryBasicAuthTest(SchemaRegistryEndpoints):
    """
    Test schema registry against a redpanda cluster with HTTP Basic Auth enabled.
    """
    username = 'red'
    password = 'panda'

    def __init__(self, context):
        security = SecurityConfig()
        security.enable_sasl = True
        security.endpoint_authn_method = 'sasl'

        schema_registry_config = SchemaRegistryConfig()
        schema_registry_config.authn_method = 'http_basic'

        super(SchemaRegistryBasicAuthTest,
              self).__init__(context,
                             security=security,
                             schema_registry_config=schema_registry_config)

    @cluster(num_nodes=3)
    def test_schemas_types(self):
        """
        Verify the schema registry returns the supported types
        """
        result_raw = self._get_schemas_types(auth=(self.username,
                                                   self.password))
        assert result_raw.json()['error_code'] == 40101

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.debug(f"Request schema types with default accept header")
        result_raw = self._get_schemas_types(auth=(super_username,
                                                   super_password))
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert set(result) == {"PROTOBUF", "AVRO"}

    @cluster(num_nodes=3)
    def test_get_schema_id_versions(self):
        """
        Verify schema versions
        """
        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"

        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=subject,
            data=schema_1_data,
            auth=(super_username, super_password))
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Checking schema 1 versions")
        result_raw = self._get_schemas_ids_id_versions(id=1,
                                                       auth=(self.username,
                                                             self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._get_schemas_ids_id_versions(id=1,
                                                       auth=(super_username,
                                                             super_password))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [{"subject": subject, "version": 1}]

    @cluster(num_nodes=3)
    def test_post_subjects_subject_versions(self):
        """
        Verify posting a schema
        """

        topic = create_topic_names(1)[0]

        schema_1_data = json.dumps({"schema": schema1_def})

        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key",
            data=schema_1_data,
            auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key",
            data=schema_1_data,
            auth=(super_username, super_password))
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Get subjects")
        result_raw = self._get_subjects(auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._get_subjects(auth=(super_username, super_password))
        assert result_raw.json() == [f"{topic}-key"]

        self.logger.debug("Get schema versions for subject key")
        result_raw = self._get_subjects_subject_versions(
            subject=f"{topic}-key", auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._get_subjects_subject_versions(
            subject=f"{topic}-key", auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1]

        self.logger.debug("Get latest schema version for subject key")
        result_raw = self._get_subjects_subject_versions_version(
            subject=f"{topic}-key",
            version="latest",
            auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._get_subjects_subject_versions_version(
            subject=f"{topic}-key",
            version="latest",
            auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == f"{topic}-key"
        assert result["version"] == 1

        self.logger.debug("Get schema version 1")
        result_raw = self._get_schemas_ids_id(id=1,
                                              auth=(self.username,
                                                    self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._get_schemas_ids_id(id=1,
                                              auth=(super_username,
                                                    super_password))
        assert result_raw.status_code == requests.codes.ok

    @cluster(num_nodes=3)
    def test_post_subjects_subject(self):
        """
        Verify posting a schema
        """

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.info("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=subject,
            data=json.dumps({"schema": schema1_def}),
            auth=(super_username, super_password))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        result_raw = self._post_subjects_subject(
            subject=subject,
            data=json.dumps({"schema": schema1_def}),
            auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        self.logger.info("Posting existing schema should be success")
        result_raw = self._post_subjects_subject(
            subject=subject,
            data=json.dumps({"schema": schema1_def}),
            auth=(super_username, super_password))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == subject
        assert result["id"] == 1
        assert result["version"] == 1
        assert result["schema"]

    @cluster(num_nodes=3)
    def test_config(self):
        """
        Smoketest config endpoints
        """
        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.debug("Get initial global config")
        result_raw = self._get_config(auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._get_config(auth=(super_username, super_password))
        assert result_raw.json()["compatibilityLevel"] == "BACKWARD"

        self.logger.debug("Set global config")
        result_raw = self._set_config(data=json.dumps(
            {"compatibility": "FULL"}),
                                      auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._set_config(data=json.dumps(
            {"compatibility": "FULL"}),
                                      auth=(super_username, super_password))
        assert result_raw.json()["compatibility"] == "FULL"

        schema_1_data = json.dumps({"schema": schema1_def})

        topic = create_topic_names(1)[0]

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key",
            data=schema_1_data,
            auth=(super_username, super_password))

        self.logger.debug("Set subject config")
        self.logger.debug("Set subject config")
        result_raw = self._set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "BACKWARD_TRANSITIVE"}),
            auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "BACKWARD_TRANSITIVE"}),
            auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["compatibility"] == "BACKWARD_TRANSITIVE"

        self.logger.debug("Get subject config - should be overriden")
        result_raw = self._get_config_subject(subject=f"{topic}-key",
                                              auth=(self.username,
                                                    self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._get_config_subject(subject=f"{topic}-key",
                                              auth=(super_username,
                                                    super_password))
        assert result_raw.json()["compatibilityLevel"] == "BACKWARD_TRANSITIVE"

    @cluster(num_nodes=3)
    def test_mode(self):
        """
        Smoketest get_mode endpoint
        """
        result_raw = self._get_mode(auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.debug("Get initial global mode")
        result_raw = self._get_mode(auth=(super_username, super_password))
        assert result_raw.json()["mode"] == "READWRITE"

    @cluster(num_nodes=3)
    def test_post_compatibility_subject_version(self):
        """
        Verify compatibility
        """

        topic = create_topic_names(1)[0]

        self.logger.debug(f"Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key",
            data=schema_1_data,
            auth=(super_username, super_password))
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set subject config - NONE")
        result_raw = self._set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "NONE"}),
            auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok

        result_raw = self._post_compatibility_subject_version(
            subject=f"{topic}-key",
            version=1,
            data=schema_1_data,
            auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        self.logger.debug("Check compatibility none, no default")
        result_raw = self._post_compatibility_subject_version(
            subject=f"{topic}-key",
            version=1,
            data=schema_1_data,
            auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == True

    @cluster(num_nodes=3)
    def test_delete_subject(self):
        """
        Verify delete subject
        """

        topic = create_topic_names(1)[0]

        self.logger.debug(f"Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key",
            data=schema_1_data,
            auth=(super_username, super_password))
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Soft delete subject")
        result_raw = self._delete_subject(subject=f"{topic}-key",
                                          auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._delete_subject(subject=f"{topic}-key",
                                          auth=(super_username,
                                                super_password))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1]

        self.logger.debug("Permanently delete subject")
        result_raw = self._delete_subject(subject=f"{topic}-key",
                                          permanent=True,
                                          auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._delete_subject(subject=f"{topic}-key",
                                          permanent=True,
                                          auth=(super_username,
                                                super_password))
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

    @cluster(num_nodes=3)
    def test_delete_subject_version(self):
        """
        Verify delete subject version
        """

        topic = create_topic_names(1)[0]

        self.logger.debug(f"Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key",
            data=schema_1_data,
            auth=(super_username, super_password))
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set subject config - NONE")
        result_raw = self._set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "NONE"}),
            auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Soft delete version 1")
        result_raw = self._delete_subject_version(subject=f"{topic}-key",
                                                  version=1,
                                                  auth=(self.username,
                                                        self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._delete_subject_version(subject=f"{topic}-key",
                                                  version=1,
                                                  auth=(super_username,
                                                        super_password))
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Permanently delete version 1")
        result_raw = self._delete_subject_version(subject=f"{topic}-key",
                                                  version=1,
                                                  permanent=True,
                                                  auth=(self.username,
                                                        self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._delete_subject_version(subject=f"{topic}-key",
                                                  version=1,
                                                  permanent=True,
                                                  auth=(super_username,
                                                        super_password))
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

    @cluster(num_nodes=3)
    def test_protobuf(self):
        """
        Verify basic protobuf functionality
        """

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.info("Posting failed schema should be 422")
        result_raw = self._post_subjects_subject_versions(
            subject="imported",
            data=json.dumps({
                "schema": imported_proto_def,
                "schemaType": "PROTOBUF"
            }),
            auth=(super_username, super_password))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.unprocessable_entity

        self.logger.info("Posting simple as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject="simple",
            data=json.dumps({
                "schema": simple_proto_def,
                "schemaType": "PROTOBUF"
            }),
            auth=(super_username, super_password))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.info("Posting imported as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject="imported",
            data=json.dumps({
                "schema":
                imported_proto_def,
                "schemaType":
                "PROTOBUF",
                "references": [{
                    "name": "simple",
                    "subject": "simple",
                    "version": 1
                }]
            }),
            auth=(super_username, super_password))
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 2

        result_raw = self._request("GET",
                                   f"subjects/simple/versions/1/schema",
                                   headers=HTTP_GET_HEADERS,
                                   auth=(super_username, super_password))
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.text.strip() == simple_proto_def.strip()

        result_raw = self._request("GET",
                                   f"schemas/ids/1",
                                   headers=HTTP_GET_HEADERS,
                                   auth=(super_username, super_password))
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["schemaType"] == "PROTOBUF"
        assert result["schema"].strip() == simple_proto_def.strip()

        # Regular user should fail
        result_raw = self._get_subjects_subject_versions_version_referenced_by(
            "simple", 1, auth=(self.username, self.password))
        assert result_raw.json()['error_code'] == 40101

        result_raw = self._get_subjects_subject_versions_version_referenced_by(
            "simple", 1, auth=(super_username, super_password))
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [2]


class SchemaRegistryTest(SchemaRegistryTestMethods):
    """
    Test schema registry against a redpanda cluster without auth.

    This derived class inherits all the tests from SchemaRegistryTestMethods.
    """
    def __init__(self, context):
        super(SchemaRegistryTest, self).__init__(context)


class SchemaRegistryAutoAuthTest(SchemaRegistryTestMethods):
    """
    Test schema registry against a redpanda cluster with Auto Auth enabled.

    This derived class inherits all the tests from SchemaRegistryTestMethods.
    """
    def __init__(self, context):
        security = SecurityConfig()
        security.kafka_enable_authorization = True
        security.endpoint_authn_method = 'sasl'
        security.auto_auth = True

        super(SchemaRegistryAutoAuthTest, self).__init__(context,
                                                         security=security)


class SchemaRegistryMTLSBase(SchemaRegistryEndpoints):
    topics = [
        TopicSpec(),
    ]

    def __init__(self, *args, **kwargs):
        super(SchemaRegistryMTLSBase, self).__init__(*args, **kwargs)

        self.security = SecurityConfig()

        super_username, super_password, super_algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        self.admin_user = User(0)
        self.admin_user.username = super_username
        self.admin_user.password = super_password
        self.admin_user.algorithm = super_algorithm

        self.schema_registry_config = SchemaRegistryConfig()
        self.schema_registry_config.require_client_auth = True

    def setup_cluster(self, basic_auth_enabled: bool = False):
        tls_manager = tls.TLSCertManager(self.logger)
        self.security.require_client_auth = True
        self.security.principal_mapping_rules = 'RULE:.*CN=(.*).*/$1/'

        if basic_auth_enabled:
            self.security.kafka_enable_authorization = True
            self.security.endpoint_authn_method = 'sasl'
            self.schema_registry_config.authn_method = 'http_basic'
        else:
            self.security.endpoint_authn_method = 'mtls_identity'

        # cert for principal with no explicitly granted permissions
        self.admin_user.certificate = tls_manager.create_cert(
            socket.gethostname(),
            common_name=self.admin_user.username,
            name='test_admin_client')

        self.security.tls_provider = PandaProxyTLSProvider(tls_manager)

        self.schema_registry_config.client_key = self.admin_user.certificate.key
        self.schema_registry_config.client_crt = self.admin_user.certificate.crt

        self.redpanda.set_security_settings(self.security)
        self.redpanda.set_schema_registry_settings(self.schema_registry_config)
        self.redpanda.start()

        admin = Admin(self.redpanda)

        # Create the users
        admin.create_user(self.admin_user.username, self.admin_user.password,
                          self.admin_user.algorithm)

        # Hack: create a user, so that we can watch for this user in order to
        # confirm that all preceding controller log writes landed: this is
        # an indirect way to check that ACLs (and users) have propagated
        # to all nodes before we proceed.
        checkpoint_user = "_test_checkpoint"
        admin.create_user(checkpoint_user, "_password",
                          self.admin_user.algorithm)

        # wait for users to propagate to nodes
        def auth_metadata_propagated():
            for node in self.redpanda.nodes:
                users = admin.list_users(node=node)
                if checkpoint_user not in users:
                    return False
                elif self.security.sasl_enabled(
                ) or self.security.kafka_enable_authorization:
                    assert self.admin_user.username in users
                    assert self.admin_user.username in users
            return True

        wait_until(auth_metadata_propagated, timeout_sec=10, backoff_sec=1)

        # Create topic with rpk instead of KafkaCLITool because rpk is configured to use TLS certs
        self.super_client(basic_auth_enabled).create_topic(self.topic)

    def super_client(self, basic_auth_enabled: bool = False):
        if basic_auth_enabled:
            return RpkTool(self.redpanda,
                           username=self.admin_user.username,
                           password=self.admin_user.password,
                           sasl_mechanism=self.admin_user.algorithm,
                           tls_cert=self.admin_user.certificate)
        else:
            return RpkTool(self.redpanda, tls_cert=self.admin_user.certificate)


class SchemaRegistryMTLSTest(SchemaRegistryMTLSBase):
    def __init__(self, *args, **kwargs):
        super(SchemaRegistryMTLSTest, self).__init__(*args, **kwargs)

    def setUp(self):
        self.setup_cluster()

    @cluster(num_nodes=3)
    def test_mtls(self):
        result_raw = self._get_schemas_types(
            tls_enabled=True,
            verify=self.admin_user.certificate.ca.crt,
            cert=(self.admin_user.certificate.crt,
                  self.admin_user.certificate.key))
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert set(result) == {"PROTOBUF", "AVRO"}


class SchemaRegistryMTLSAndBasicAuthTest(SchemaRegistryMTLSBase):
    def __init__(self, *args, **kwargs):
        super(SchemaRegistryMTLSAndBasicAuthTest,
              self).__init__(*args, **kwargs)

    def setUp(self):
        self.setup_cluster(basic_auth_enabled=True)

    @cluster(num_nodes=3)
    def test_mtls_and_basic_auth(self):
        result_raw = self._get_schemas_types(
            tls_enabled=True,
            auth=(self.admin_user.username, self.admin_user.password),
            verify=self.admin_user.certificate.ca.crt,
            cert=(self.admin_user.certificate.crt,
                  self.admin_user.certificate.key))
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert set(result) == {"PROTOBUF", "AVRO"}


class SchemaValidationEnableWithoutSchemaRegistry(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super(SchemaValidationEnableWithoutSchemaRegistry,
              self).__init__(*args, schema_registry_config=None, **kwargs)
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=1)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_enable_schema_id_validation(self, mode):
        try:
            self.redpanda.set_cluster_config(
                {'enable_schema_id_validation': mode})
            assert False, "expected failure"
        except requests.exceptions.HTTPError as ex:
            print(ex)
            pass


class SchemaValidationWithoutSchemaRegistry(RedpandaTest):
    INVALID_CONFIG_LOG_ALLOW_LIST = DEFAULT_LOG_ALLOW_LIST + [
        re.compile(
            r"enable_schema_id_validation requires schema_registry to be enabled in redpanda.yaml"
        ),
    ]

    def __init__(self, *args, **kwargs):
        super(SchemaValidationWithoutSchemaRegistry,
              self).__init__(*args,
                             extra_rp_conf={
                                 'enable_schema_id_validation':
                                 SchemaIdValidationMode.REDPANDA.value
                             },
                             schema_registry_config=None,
                             **kwargs)

    @cluster(num_nodes=1, log_allow_list=INVALID_CONFIG_LOG_ALLOW_LIST)
    def test_disabled_schema_registry(self):
        rpk = RpkTool(self.redpanda)
        topic = "no_schema_registry"
        rpk.create_topic(
            topic,
            config={
                TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION: 'true',
            })
        try:
            rpk.produce(topic, "key", "value")
            assert False, "expected INVALID_RECORD"
        except RpkException as e:
            print(e)
            assert "INVALID_RECORD" in e.stderr

        wait_until(lambda: self.redpanda.search_log_all(
            "enable_schema_id_validation requires schema_registry to be enabled in redpanda.yaml"
        ),
                   timeout_sec=5)


class SchemaValidationTopicPropertiesTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super(SchemaValidationTopicPropertiesTest,
              self).__init__(*args,
                             extra_rp_conf={
                                 'enable_schema_id_validation':
                                 SchemaIdValidationMode.COMPAT.value
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             **kwargs)
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def _get_topic_properties(self, mode: Optional[SchemaIdValidationMode],
                              enable: Optional[bool],
                              strategy: TopicSpec.SubjectNameStrategy):
        enable_str = f"{enable}".lower()
        config = {}
        if mode == SchemaIdValidationMode.REDPANDA:
            if enable is not None:
                config.update({
                    TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION:
                    enable_str,
                    TopicSpec.PROPERTY_RECORD_VALUE_SCHEMA_ID_VALIDATION:
                    enable_str,
                })
            if strategy is not None:
                config.update({
                    TopicSpec.PROPERTY_RECORD_KEY_SUBJECT_NAME_STRATEGY:
                    strategy.value,
                    TopicSpec.PROPERTY_RECORD_VALUE_SUBJECT_NAME_STRATEGY:
                    strategy.value,
                })

        if mode == SchemaIdValidationMode.COMPAT:
            if enable is not None:
                config.update({
                    TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION_COMPAT:
                    enable_str,
                    TopicSpec.PROPERTY_RECORD_VALUE_SCHEMA_ID_VALIDATION_COMPAT:
                    enable_str,
                })
            if strategy is not None:
                if strategy == TopicSpec.SubjectNameStrategy.TOPIC_NAME:
                    strategy_compat = TopicSpec.SubjectNameStrategyCompat.TOPIC_NAME
                elif strategy == TopicSpec.SubjectNameStrategy.RECORD_NAME:
                    strategy_compat = TopicSpec.SubjectNameStrategyCompat.RECORD_NAME
                elif strategy == TopicSpec.SubjectNameStrategy.TOPIC_RECORD_NAME:
                    strategy_compat = TopicSpec.SubjectNameStrategyCompat.TOPIC_RECORD_NAME

                config.update({
                    TopicSpec.PROPERTY_RECORD_KEY_SUBJECT_NAME_STRATEGY_COMPAT:
                    strategy_compat.value,
                    TopicSpec.PROPERTY_RECORD_VALUE_SUBJECT_NAME_STRATEGY_COMPAT:
                    strategy_compat.value,
                })
        return config

    @cluster(num_nodes=1)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_schema_id_validation_disabled_config(self, mode):
        '''
        When the feature is disabled, the configs should not appear
        '''

        self.redpanda.set_cluster_config(
            {'enable_schema_id_validation': SchemaIdValidationMode.NONE})

        topic = "default-topic"
        self.rpk.create_topic(topic)
        desc = self.rpk.describe_topic_configs(topic)

        all_config = self._get_topic_properties(
            mode, False, TopicSpec.SubjectNameStrategy.TOPIC_NAME)

        for k in all_config.items():
            assert k not in desc

    @cluster(num_nodes=1)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_schema_id_validation_active_config(self, mode):
        '''
        When the feature is active, the configs should be default
        '''

        self.redpanda.set_cluster_config({'enable_schema_id_validation': mode})

        topic = "default-topic"
        self.rpk.create_topic(topic)
        desc = self.rpk.describe_topic_configs(topic)

        config = self._get_topic_properties(
            mode, False, TopicSpec.SubjectNameStrategy.TOPIC_NAME)

        for k, v in config.items():
            assert desc[k] == (v, 'DEFAULT_CONFIG')

    @cluster(num_nodes=1)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_schema_id_validation_active_explicit_default_config(self, mode):
        '''
        If the configuration is explicitly set to default, pretend it isn't
        dynamic, so that tools with a reconcialiation loop aren't confused
        '''

        self.redpanda.set_cluster_config({'enable_schema_id_validation': mode})

        topic = "default-topic"

        config = self._get_topic_properties(
            mode, False, TopicSpec.SubjectNameStrategy.TOPIC_NAME)

        self.rpk.create_topic(topic, config=config)
        desc = self.rpk.describe_topic_configs(topic)

        for k, v in config.items():
            assert desc[k] == (v, 'DEFAULT_CONFIG')

    @cluster(num_nodes=1)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_schema_id_validation_active_nondefault_config(self, mode):
        '''
        If the configuration is explicitly set to non-default, it should show
        as dyamic
        '''

        self.redpanda.set_cluster_config({'enable_schema_id_validation': mode})

        topic = "default-topic"

        config = self._get_topic_properties(
            mode, True, TopicSpec.SubjectNameStrategy.RECORD_NAME)

        self.rpk.create_topic(topic, config=config)

        desc = self.rpk.describe_topic_configs(topic)

        for k, v in config.items():
            assert desc[k] == (v, 'DYNAMIC_TOPIC_CONFIG')

    @cluster(num_nodes=1)
    def test_schema_id_validation_create_collision(self):
        '''
        Test creating a topic where Redpanda and compat modes are incompatible
        '''

        topic = "default-topic"
        try:
            self.rpk.create_topic(
                topic,
                config={
                    TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION:
                    'true',
                    TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION_COMPAT:
                    'false',
                })
            assert False, "Expected failure"
        except RpkException:
            pass

    @cluster(num_nodes=1)
    def test_schema_id_validation_alter_collision(self):
        '''
        Test altering a topic where Redpanda and compat modes are incompatible
        '''

        topic = "default-topic"
        self.rpk.create_topic(
            topic,
            config={
                TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION: 'true',
            })
        try:
            self.rpk.alter_topic_config(
                topic=topic,
                set_key=TopicSpec.
                PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION_COMPAT,
                set_value='false')
            assert False, "Expected failure"
        except RpkException:
            pass


class SchemaRegistryLicenseTest(RedpandaTest):
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, *args, **kwargs):
        super(SchemaRegistryLicenseTest,
              self).__init__(*args,
                             extra_rp_conf={
                                 'enable_schema_id_validation':
                                 SchemaIdValidationMode.NONE.value
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             **kwargs)
        self.redpanda.set_environment({
            '__REDPANDA_LICENSE_CHECK_INTERVAL_SEC':
            f'{self.LICENSE_CHECK_INTERVAL_SEC}'
        })

    def _has_license_nag(self):
        return self.redpanda.search_log_any("Enterprise feature(s).*")

    def _license_nag_is_set(self):
        return self.redpanda.search_log_all(
            f"Overriding default license log annoy interval to: {self.LICENSE_CHECK_INTERVAL_SEC}s"
        )

    @cluster(num_nodes=3)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_license_nag(self, mode):
        wait_until(self._license_nag_is_set,
                   timeout_sec=30,
                   err_msg="Failed to set license nag internal")

        self.logger.debug("Ensuring no license nag")
        time.sleep(self.LICENSE_CHECK_INTERVAL_SEC * 2)
        assert not self._has_license_nag()

        self.logger.debug("Setting cluster config")
        self.redpanda.set_cluster_config({"enable_schema_id_validation": mode})

        self.logger.debug("Waiting for license nag")
        wait_until(self._has_license_nag,
                   timeout_sec=self.LICENSE_CHECK_INTERVAL_SEC * 2,
                   err_msg="License nag failed to appear")
