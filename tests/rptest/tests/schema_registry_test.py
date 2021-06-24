# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import http.client
import json
import logging
import uuid
import requests
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.tests.redpanda_test import RedpandaTest


def create_topic_names(count):
    return list(f"pandaproxy-topic-{uuid.uuid4()}" for _ in range(count))


HTTP_GET_HEADERS = {"Accept": "application/vnd.schemaregistry.v1+json"}

HTTP_POST_HEADERS = {
    "Accept": "application/vnd.schemaregistry.v1+json",
    "Content-Type": "application/vnd.schemaregistry.v1+json"
}

schema1_def = '{"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"}]}'
schema2_def = '{"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2","default":"foo"}]}'
schema3_def = '{"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2"}]}'


class SchemaRegistryTest(RedpandaTest):
    """
    Test schema registry against a redpanda cluster.
    """
    def __init__(self, context):
        super(SchemaRegistryTest, self).__init__(
            context,
            num_brokers=3,
            enable_pp=True,
            enable_sr=True,
            extra_rp_conf={"auto_create_topics_enabled": False},
            num_cores=1)

        http.client.HTTPConnection.debuglevel = 1
        logging.basicConfig()
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.getLogger().level)
        requests_log.propagate = True

    def _base_uri(self):
        return f"http://{self.redpanda.nodes[0].account.hostname}:8081"

    def _get_topics(self):
        return requests.get(
            f"http://{self.redpanda.nodes[0].account.hostname}:8082/topics")

    def _create_topics(self,
                       names=create_topic_names(1),
                       partitions=1,
                       replicas=1,
                       cleanup_policy=TopicSpec.CLEANUP_DELETE):
        self.logger.debug(f"Creating topics: {names}")
        kafka_tools = KafkaCliTools(self.redpanda)
        for name in names:
            kafka_tools.create_topic(
                TopicSpec(name=name,
                          partition_count=partitions,
                          replication_factor=replicas))
        assert set(names).issubset(self._get_topics().json())
        return names

    def _get_config(self, headers=HTTP_GET_HEADERS):
        return requests.get(f"{self._base_uri()}/config", headers=headers)

    def _set_config(self, data, headers=HTTP_POST_HEADERS):
        return requests.put(f"{self._base_uri()}/config",
                            headers=headers,
                            data=data)

    def _get_config_subject(self, subject, headers=HTTP_GET_HEADERS):
        return requests.get(f"{self._base_uri()}/config/{subject}",
                            headers=headers)

    def _set_config_subject(self, subject, data, headers=HTTP_POST_HEADERS):
        return requests.put(f"{self._base_uri()}/config/{subject}",
                            headers=headers,
                            data=data)

    def _get_schemas_types(self, headers=HTTP_GET_HEADERS):
        return requests.get(f"{self._base_uri()}/schemas/types",
                            headers=headers)

    def _get_schemas_ids_id(self, id, headers=HTTP_GET_HEADERS):
        return requests.get(f"{self._base_uri()}/schemas/ids/{id}",
                            headers=headers)

    def _get_subjects(self, headers=HTTP_GET_HEADERS):
        return requests.get(f"{self._base_uri()}/subjects", headers=headers)

    def _post_subjects_subject_versions(self,
                                        subject,
                                        data,
                                        headers=HTTP_POST_HEADERS):
        return requests.post(f"{self._base_uri()}/subjects/{subject}/versions",
                             headers=headers,
                             data=data)

    def _get_subjects_subject_versions_version(self,
                                               subject,
                                               version,
                                               headers=HTTP_GET_HEADERS):
        return requests.get(
            f"{self._base_uri()}/subjects/{subject}/versions/{version}",
            headers=headers)

    def _get_subjects_subject_versions(self,
                                       subject,
                                       deleted=False,
                                       headers=HTTP_GET_HEADERS):
        return requests.get(
            f"{self._base_uri()}/subjects/{subject}/versions{'?deleted=true' if deleted else ''}",
            headers=headers)

    def _delete_subject(self,
                        subject,
                        permanent=False,
                        headers=HTTP_GET_HEADERS):
        return requests.delete(
            f"{self._base_uri()}/subjects/{subject}{'?permanent=true' if permanent else ''}",
            headers=headers)

    def _post_compatibility_subject_version(self,
                                            subject,
                                            version,
                                            data,
                                            headers=HTTP_POST_HEADERS):
        return requests.post(
            f"{self._base_uri()}/compatibility/subjects/{subject}/versions/{version}",
            headers=headers,
            data=data)

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
        assert result == ["AVRO"]

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
        assert result_raw.json() == []

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
        assert result["message"] == "Subject not found"

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
        assert result["message"] == "Subject not found"

        self.logger.debug("Get invalid schema version for subject")
        result_raw = self._get_subjects_subject_versions_version(
            subject=f"{topic}-key", version=2)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == "Subject version not found"

        self.logger.debug("Get schema version 1 for subject key")
        result_raw = self._get_subjects_subject_versions_version(
            subject=f"{topic}-key", version=1)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["name"] == f"{topic}-key"
        assert result["version"] == 1
        # assert result["schema"] == json.dumps(schema_def)

        self.logger.debug("Get latest schema version for subject key")
        result_raw = self._get_subjects_subject_versions_version(
            subject=f"{topic}-key", version="latest")
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["name"] == f"{topic}-key"
        assert result["version"] == 1
        # assert result["schema"] == json.dumps(schema_def)

        self.logger.debug("Get invalid schema version")
        result_raw = self._get_schemas_ids_id(id=2)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == "Schema not found"

        self.logger.debug("Get schema version 1")
        result_raw = self._get_schemas_ids_id(id=1)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        # assert result["schema"] == json.dumps(schema_def)

    @cluster(num_nodes=3)
    def test_config(self):
        """
        Smoketest config endpoints
        """
        self.logger.debug("Get initial global config")
        result_raw = self._get_config()
        assert result_raw.json()["compatibilityLevel"] == "NONE"

        self.logger.debug("Set global config")
        result_raw = self._set_config(
            data=json.dumps({"compatibility": "FULL"}))
        assert result_raw.json()["compatibility"] == "FULL"

        self.logger.debug("Get invalid subject config")
        result_raw = self._get_config_subject(subject="invalid_subject")
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40401

        schema_1_data = json.dumps({"schema": schema1_def})

        topic = create_topic_names(1)[0]

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data)

        self.logger.debug("Get subject config - should be same as global")
        result_raw = self._get_config_subject(subject=f"{topic}-key")
        assert result_raw.json()["compatibilityLevel"] == "FULL"

        self.logger.debug("Set subject config")
        result_raw = self._set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "BACKWARD_TRANSITIVE"}))
        assert result_raw.json()["compatibility"] == "BACKWARD_TRANSITIVE"

        self.logger.debug("Get subject config - should be overriden")
        result_raw = self._get_config_subject(subject=f"{topic}-key")
        assert result_raw.json()["compatibilityLevel"] == "BACKWARD_TRANSITIVE"

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

        self.logger.debug("Permanently delete subject")
        result_raw = self._delete_subject(subject=f"{topic}-key",
                                          permanent=True)
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.not_found

        self.logger.debug("Soft delete subject")
        result_raw = self._delete_subject(subject=f"{topic}-key")
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

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
