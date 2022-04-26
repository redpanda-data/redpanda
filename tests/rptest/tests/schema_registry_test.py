# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import http.client
import json
import uuid
import requests
import time
import random

from rptest.util import inject_remote_script
from rptest.services.cluster import cluster
from ducktape.services.background_thread import BackgroundThreadService

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.python_librdkafka_serde_client import SerdeClient, SchemaType
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import ResourceSettings


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
invalid_avro = '{"type":"notatype","name":"myrecord","fields":[{"type":"string","name":"f1"}]}'

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
            resource_settings=ResourceSettings(num_cpus=1))

        http.client.HTTPConnection.debuglevel = 1
        http.client.print = lambda *args: self.logger.debug(" ".join(args))

        self._ctx = context

    def _request(self, verb, path, hostname=None, **kwargs):
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

        uri = f"http://{hostname}:8081/{path}"

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
        return self._request("GET", "config", headers=headers)

    def _set_config(self, data, headers=HTTP_POST_HEADERS):
        return self._request("PUT", f"config", headers=headers, data=data)

    def _get_config_subject(self,
                            subject,
                            fallback=False,
                            headers=HTTP_GET_HEADERS):
        return self._request(
            "GET",
            f"config/{subject}{'?defaultToGlobal=true' if fallback else ''}",
            headers=headers)

    def _set_config_subject(self, subject, data, headers=HTTP_POST_HEADERS):
        return self._request("PUT",
                             f"config/{subject}",
                             headers=headers,
                             data=data)

    def _get_schemas_types(self, headers=HTTP_GET_HEADERS):
        return self._request("GET", f"schemas/types", headers=headers)

    def _get_schemas_ids_id(self, id, headers=HTTP_GET_HEADERS):
        return self._request("GET", f"schemas/ids/{id}", headers=headers)

    def _get_schemas_ids_id_versions(self, id, headers=HTTP_GET_HEADERS):
        return self._request("GET",
                             f"schemas/ids/{id}/versions",
                             headers=headers)

    def _get_subjects(self, deleted=False, headers=HTTP_GET_HEADERS):
        return self._request("GET",
                             f"subjects{'?deleted=true' if deleted else ''}",
                             headers=headers)

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
                                               headers=HTTP_GET_HEADERS):
        return self._request("GET",
                             f"subjects/{subject}/versions/{version}",
                             headers=headers)

    def _get_subjects_subject_versions_version_referenced_by(
            self, subject, version, headers=HTTP_GET_HEADERS):
        return self._request(
            "GET",
            f"subjects/{subject}/versions/{version}/referencedBy",
            headers=headers)

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
                                            headers=HTTP_POST_HEADERS):
        return self._request(
            "POST",
            f"compatibility/subjects/{subject}/versions/{version}",
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
        assert result_raw.json()["error_code"] == 40401

        schema_1_data = json.dumps({"schema": schema1_def})

        topic = create_topic_names(1)[0]

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self._post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data)

        self.logger.debug("Get subject config - should fail")
        result_raw = self._get_config_subject(subject=f"{topic}-key")
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40401

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

        svc = StressTest(self._ctx)
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

        result_raw = self._get_subjects_subject_versions_version_referenced_by(
            "simple", 1)
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [2]

    @cluster(num_nodes=3)
    def test_serde_client(self):
        """
        Verify basic serialization client
        """
        protocols = [SchemaType.AVRO, SchemaType.PROTOBUF]
        topics = [f"serde-topic-{x.name}" for x in protocols]
        self._create_topics(topics)
        schema_reg = self.redpanda.schema_reg().split(',', 1)[0]
        for i in range(len(protocols)):
            self.logger.info(
                f"Connecting to redpanda: {self.redpanda.brokers()} schema_reg: {schema_reg}"
            )
            client = SerdeClient(self.redpanda.brokers(),
                                 schema_reg,
                                 protocols[i],
                                 topic=topics[i],
                                 logger=self.logger)
            client.run(2)
            schema = self._get_subjects_subject_versions_version(
                f"{topics[i]}-value", "latest")
            self.logger.info(schema.json())
            if protocols[i] == SchemaType.AVRO:
                assert schema.json().get("schemaType") is None
            else:
                assert schema.json()["schemaType"] == protocols[i].name
