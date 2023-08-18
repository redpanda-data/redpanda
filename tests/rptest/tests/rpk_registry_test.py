# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import tempfile
import socket

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SchemaRegistryConfig, SecurityConfig
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services import tls
from rptest.tests.pandaproxy_test import User, PandaProxyTLSProvider
from rptest.util import expect_exception

schema1_avro_def = '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
schema2_avro_def = '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"},{"name":"f2","type":"string","default":"foo"}]}'
incompatible_schema = '{"type":"record","name":"myrecord","fields":[{"name":"f2","type":"string"},{"name":"f3","type":"string","default":"foo"}]}'

schema1_proto_def = """
syntax = "proto3";

message Simple {
  string id = 1;
}"""


class RpkRegistryTest(RedpandaTest):
    username = "red"
    password = "panda"
    mechanism = "SCRAM-SHA-256"

    def __init__(self, ctx, schema_registry_config=SchemaRegistryConfig()):
        super(RpkRegistryTest, self).__init__(
            test_context=ctx,
            schema_registry_config=schema_registry_config,
        )
        # SASL Config
        self.security = SecurityConfig()
        super_username, super_password, super_algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        self.admin_user = User(0)
        self.admin_user.username = super_username
        self.admin_user.password = super_password
        self.admin_user.algorithm = super_algorithm

        self.schema_registry_config = SchemaRegistryConfig()
        self.schema_registry_config.require_client_auth = True

    # Override Redpanda start to create the certs and enable auth.
    def setUp(self):
        tls_manager = tls.TLSCertManager(self.logger)
        self.security.require_client_auth = True

        # Enable Basic Auth
        self.security.kafka_enable_authorization = True
        self.security.endpoint_authn_method = 'sasl'
        self.schema_registry_config.authn_method = 'http_basic'

        # Cert for principal with no explicitly granted permissions
        self.admin_user.certificate = tls_manager.create_cert(
            socket.gethostname(),
            common_name=self.admin_user.username,
            name='test_admin_client')

        # TLS.
        self.security.tls_provider = PandaProxyTLSProvider(tls_manager)
        self.schema_registry_config.client_key = self.admin_user.certificate.key
        self.schema_registry_config.client_crt = self.admin_user.certificate.crt
        self.redpanda.set_security_settings(self.security)
        self.redpanda.set_schema_registry_settings(self.schema_registry_config)

        self.redpanda.start()

        self._rpk = RpkTool(self.redpanda,
                            username=self.admin_user.username,
                            password=self.admin_user.password,
                            sasl_mechanism=self.admin_user.algorithm,
                            tls_cert=self.admin_user.certificate)

        # Create the user which rpk will use to authenticate.
        self._rpk.sasl_create_user(self.admin_user.username,
                                   self.admin_user.password,
                                   self.admin_user.algorithm)

    def create_schema(self, subject, schema, suffix):
        with tempfile.NamedTemporaryFile(suffix=suffix) as tf:
            tf.write(bytes(schema, 'UTF-8'))
            tf.seek(0)
            out = self._rpk.create_schema(subject, tf.name)
            assert out["subject"] == subject

    @cluster(num_nodes=3)
    def test_registry_schema(self):
        subject_1 = "test_subject_1"
        subject_2 = "test_subject_2"

        self.create_schema(subject_1, schema1_avro_def,
                           ".avro")  # version: 1, ID: 1
        self.create_schema(subject_2, schema1_proto_def,
                           ".proto")  # version: 1, ID: 2

        # We can get the schema in multiple ways:

        # - By version, returning a schema for a required subject and version.
        out = self._rpk.get_schema(subject_1, version="1")
        assert len(out) == 1
        assert out[0]["subject"] == subject_1
        assert out[0]["id"] == 1
        assert out[0]["type"] == "AVRO"

        # - By ID, returning all subjects using the schema.
        out = self._rpk.get_schema(id="2")
        assert len(out) == 1  # just 1 subject is using this schema.
        assert out[0]["subject"] == subject_2
        assert out[0]["id"] == 2
        assert out[0]["type"] == "PROTOBUF"

        # - By schema, checking if the schema has been created in the subject.
        with tempfile.NamedTemporaryFile(suffix=".avro") as tf:
            tf.write(bytes(schema1_avro_def, 'UTF-8'))
            tf.seek(0)
            out = self._rpk.get_schema(subject_1, schema_path=tf.name)
            assert len(out) == 1
            assert out[0]["subject"] == subject_1
            assert out[0]["id"] == 1
            assert out[0]["type"] == "AVRO"

        def find_subject(list, subject):
            found = False
            for s in list:
                if s["subject"] == subject:
                    found = True
            return found

        # List Available.
        out = self._rpk.list_schemas()
        assert len(out) == 2
        assert find_subject(out, subject_1)
        assert find_subject(out, subject_2)

        # Soft Delete.
        self._rpk.delete_schema(subject_1, version="1")
        with expect_exception(RpkException, lambda e: "not found" in str(e)):
            self._rpk.get_schema(subject_1, version="1")

        out = self._rpk.list_schemas()
        assert not find_subject(out, subject_1)

        # List Deleted
        out = self._rpk.list_schemas(deleted=True)
        assert find_subject(out, subject_1)

        # Hard Delete
        self._rpk.delete_schema(subject_2, version="1", permanent=True)
        out = self._rpk.list_schemas(deleted=True)
        assert not find_subject(out, subject_2)  # Not in the deleted list.

        # Hard Delete an already soft-deleted schema.
        self._rpk.delete_schema(subject_1, version="1", permanent=True)
        out = self._rpk.list_schemas(deleted=True)
        assert not find_subject(out, subject_1)  # Not in the deleted list.

    @cluster(num_nodes=1)
    def test_registry_compatibility_level(self):
        compat = self._rpk.get_compatibility_level()
        assert compat[0]["subject"] == "{GLOBAL}"
        assert compat[0]["level"] == "BACKWARD"

        subject_1 = "test_subject_1"
        self.create_schema(subject_1, schema2_avro_def, ".avro")

        def check_compatibility(subject, version, schema):
            with tempfile.NamedTemporaryFile(suffix=".avro") as tf:
                tf.write(bytes(schema, 'UTF-8'))
                tf.seek(0)
                out = self._rpk.check_compatibility(subject,
                                                    version=version,
                                                    schema_path=tf.name)
                return out["compatible"]

        assert check_compatibility(subject_1,
                                   version="1",
                                   schema=schema1_avro_def)

        # We can change the compatibility level.
        out = self._rpk.set_compatibility_level(subject_1, "FORWARD")
        assert out[0]["level"] == "FORWARD"

        # And return appropiately if not compatible.
        assert not check_compatibility(
            subject_1, version="1", schema=incompatible_schema)

        # We also fail when a wrong level is passed.
        with expect_exception(
                RpkException,
                lambda e: "unknown compatibility level" in str(e)):
            out = self._rpk.set_compatibility_level(subject_1, "WRONG")

    @cluster(num_nodes=3)
    def test_registry_subject(self):
        subject_1 = "test_subject_1"
        subject_2 = "test_subject_2"

        self.create_schema(subject_1, schema1_avro_def,
                           ".avro")  # version: 1, ID: 1
        self.create_schema(subject_2, schema1_proto_def,
                           ".proto")  # version: 1, ID: 2

        # List.
        out = self._rpk.list_subjects()
        assert len(out) == 2
        assert subject_1 in out
        assert subject_2 in out

        # Soft Delete.
        out = self._rpk.delete_subjects([subject_1])
        assert out[0]["subject"] == subject_1
        assert len(out[0]["versions_deleted"]) == 1  # versions_deleted=[1]

        # List available.
        out = self._rpk.list_subjects()
        assert len(out) == 1
        assert subject_1 not in out

        # List deleted.
        out = self._rpk.list_subjects(deleted=True)
        assert len(out) == 2
        assert subject_1 in out
        assert subject_2 in out

        # Hard Delete a subject.
        out = self._rpk.delete_subjects([subject_2], permanent=True)
        assert out[0]["subject"] == subject_2
        assert len(out[0]["versions_deleted"]) == 1

        # Hard Delete an already soft-deleted subject.
        out = self._rpk.delete_subjects([subject_1], permanent=True)
        assert out[0]["subject"] == subject_1
        assert len(out[0]["versions_deleted"]) == 1

        # Both list deleted and list should be empty.
        out = self._rpk.list_subjects()
        out_deleted = self._rpk.list_subjects(deleted=True)
        assert len(out) == 0
        assert len(out_deleted) == 0
