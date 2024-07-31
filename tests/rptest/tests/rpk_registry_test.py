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
import json

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

imported_proto_def = """
syntax = "proto3";

import "simple";

message Test2 {
  Simple id =  1;
}"""

json_number_schema_def = '{"type": "number"}'


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
        self.schema_registry_config.mode_mutability = True

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

    def create_schema(self, subject, schema, suffix, references=None):
        with tempfile.NamedTemporaryFile(suffix=suffix) as tf:
            tf.write(bytes(schema, 'UTF-8'))
            tf.seek(0)
            out = self._rpk.create_schema(subject,
                                          tf.name,
                                          references=references)
            assert out["subject"] == subject

    @cluster(num_nodes=3)
    def test_registry_schema(self):
        subject_1 = "test_subject_1"
        subject_2 = "test_subject_2"
        subject_3 = "test_subject_3"
        subject_4 = "test_subject_4"

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

        # References. Subject 3 references subject 2
        self.create_schema(
            subject_3,
            imported_proto_def,
            ".proto",
            references=f"simple:{subject_2}:1")  # version: 1, ID: 3

        out = self._rpk.schema_references(subject_2, "1")
        assert len(out) == 1
        assert out[0]["subject"] == subject_3
        assert out[0]["version"] == 1
        assert out[0]["id"] == 3

        # Soft Delete.
        self._rpk.delete_schema(subject_1, version="1")
        with expect_exception(RpkException, lambda e: "not found" in str(e)):
            self._rpk.get_schema(subject_1, version="1")

        out = self._rpk.list_schemas()
        assert not find_subject(out, subject_1)

        # List Deleted
        out = self._rpk.list_schemas(deleted=True)
        assert find_subject(out, subject_1)

        # We can't delete a schema if we still have a reference.
        with expect_exception(
                RpkException, lambda e:
                "One or more references exist to the schema" in str(e)):
            self._rpk.delete_schema(subject_2, version="1", permanent=True)

        # First we delete the subject/schema that use the reference:
        self._rpk.delete_schema(subject_3, version="1", permanent=True)
        # Then we can Hard Delete the Subject.
        self._rpk.delete_schema(subject_2, version="1", permanent=True)
        out = self._rpk.list_schemas(deleted=True)
        assert not find_subject(out, subject_2)  # Not in the deleted list.

        # Hard Delete an already soft-deleted schema.
        self._rpk.delete_schema(subject_1, version="1", permanent=True)
        out = self._rpk.list_schemas(deleted=True)
        assert not find_subject(out, subject_1)  # Not in the deleted list.

        self.create_schema(subject_4, json_number_schema_def,
                           ".json")  # version: 1, ID: 4
        out = self._rpk.get_schema(subject_4, version="1")
        assert len(out) == 1
        assert out[0]["subject"] == subject_4
        assert out[0]["id"] == 4
        assert out[0]["type"] == "JSON"

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

    @cluster(num_nodes=3)
    def test_produce_consume_avro(self):
        # First we register the schemas with their references.
        subject_1 = "subject-for-telephone"
        avro_reference = '{"type":"record","name":"telephone","fields":[{"name":"number","type":"int"},{"name":"identifier","type":"string"}]}'
        self.create_schema(subject_1, avro_reference, ".avro")  # ID 1

        subject_2 = "subject-for-reference"
        avro_with_reference = '{"type":"record","name":"test","fields":[{"name":"name","type":"string"},{"name":"telephone","type":"telephone"}]}'
        self.create_schema(subject_2, avro_with_reference, ".avro",
                           f"telephone:{subject_1}:1")  # ID 2

        test_topic = "test_topic_sr"
        self._rpk.create_topic(test_topic)

        msg_1 = '{"name":"redpanda","telephone":{"number":12341234,"identifier":"home"}}'
        key_1 = "somekey"
        expected_msg_1 = json.loads(msg_1)
        # Produce: unencoded key, encoded value:
        self._rpk.produce(test_topic, msg=msg_1, key=key_1, schema_id=2)

        # We consume as is, i.e: it will show the encoded value.
        out = self._rpk.consume(test_topic, offset="0:1")
        msg = json.loads(out)

        # We check that:
        # - we are not storing the value without encoding.
        # - first byte is the magic number 0 from the serde header.
        # - key is not encoded.
        raw_bytes_string = msg["value"]
        assert raw_bytes_string != expected_msg_1
        assert msg["key"] == key_1

        bytes_from_string = bytes(
            raw_bytes_string.encode().decode('unicode-escape'), 'utf-8')
        assert bytes_from_string[0] == 0

        # Now we decode the same message:
        out = self._rpk.consume(test_topic,
                                offset="0:1",
                                use_schema_registry="value")
        msg = json.loads(out)

        assert json.loads(msg["value"]) == expected_msg_1
        assert msg["key"] == key_1

        # We can produce using different schema for key and value
        msg_2 = '{"number":3211123,"identifier":"work"}'
        expected_msg_2 = json.loads(msg_2)
        self._rpk.produce(test_topic,
                          key=msg_2,
                          msg=msg_1,
                          schema_key_id=1,
                          schema_id=2)

        # And decode them separately
        out = self._rpk.consume(test_topic,
                                offset="1:2",
                                use_schema_registry="key,value")
        msg = json.loads(out)

        assert json.loads(msg["value"]) == expected_msg_1
        assert json.loads(msg["key"]) == expected_msg_2

    @cluster(num_nodes=3)
    def test_produce_consume_proto(self):
        # First we register the schemas with their references.
        subject_1 = "subject_for_person"
        proto_person = """
syntax = "proto3";

message Person {
  message PhoneNumber {
    string number = 1;
  }
  string name = 1;
  int32 id = 2;
  string email = 3;
  repeated .Person.PhoneNumber phones = 4;
}"""
        self.create_schema(subject_1, proto_person, ".proto")  # ID 1

        subject_2 = "subject-for-reference"
        proto_with_reference = """
syntax = "proto3";

import "person.proto";

message AddressBook {
  repeated .Person people = 1;
}
"""
        self.create_schema(subject_2, proto_with_reference, ".proto",
                           f"person.proto:{subject_1}:1")  # ID 2

        test_topic = "test_topic_sr"
        self._rpk.create_topic(test_topic)

        msg_1 = '{"people":[{"name":"foo","id":123,"email":"test@redpanda.com","phones":[{"number":"111"}]},{"name":"igor","id":321,"email":"igor@redpanda.com","phones":[{"number":"1231231"},{"number":"2222333"}]}]}'
        key_1 = "somekey"
        expected_msg_1 = json.loads(msg_1)
        # Produce: unencoded key, encoded value:
        self._rpk.produce(test_topic,
                          msg=msg_1,
                          key=key_1,
                          schema_id=2,
                          proto_msg="AddressBook")

        # We consume as is, i.e: it will show the encoded value.
        out = self._rpk.consume(test_topic, offset="0:1")
        msg = json.loads(out)

        # We check that:
        # - we are not storing the value without encoding.
        # - first byte is the magic number 0 from the serde header.
        # - key is not encoded.
        raw_bytes_string = msg["value"]
        assert raw_bytes_string != expected_msg_1
        assert msg["key"] == key_1

        bytes_from_string = bytes(
            raw_bytes_string.encode().decode('unicode-escape'), 'utf-8')
        assert bytes_from_string[0] == 0

        # Now we decode the same message:
        out = self._rpk.consume(test_topic,
                                offset="0:1",
                                use_schema_registry="value")
        msg = json.loads(out)

        assert json.loads(msg["value"]) == expected_msg_1
        assert msg["key"] == key_1

        # We can produce using different schema for key and value and a different proto message.
        msg_2 = '{"name":"bar","id":2212,"email":"test2@redpanda.com","phones":[{"number":"1431"}]}'
        expected_msg_2 = json.loads(msg_2)
        self._rpk.produce(test_topic,
                          key=msg_2,
                          msg=msg_1,
                          schema_key_id=1,
                          schema_id=2,
                          proto_key_msg="Person",
                          proto_msg="AddressBook")

        # And decode them separately
        out = self._rpk.consume(test_topic,
                                offset="1:2",
                                use_schema_registry="key,value")
        msg = json.loads(out)

        assert json.loads(msg["value"]) == expected_msg_1
        assert json.loads(msg["key"]) == expected_msg_2

    @cluster(num_nodes=3)
    def test_produce_consume_json(self):
        # First we register the schemas with their references.
        subject_1 = "subject_for_warehouse"
        reference_json = """
{
   "type":"object",
   "properties":{
      "latitude":{
         "type":"number"
      },
      "longitude":{
         "type":"number"
      }
   }
}"""

        self.create_schema(subject_1, reference_json, ".json")  # ID 1

        test_topic = "test_topic_sr"
        self._rpk.create_topic(test_topic)

        # Using topicName strategy:
        subject_2 = test_topic + "-value"

        json_with_reference = """
{
   "type":"object",
   "properties":{
      "productId":{
         "type":"integer"
      },
      "productName":{
         "type":"string"
      },
      "tags":{
         "type":"array",
         "items":{
            "type":"string"
         }
      },
      "warehouseLocation":{
         "$ref":"https://example.com/geographical-location.schema.json"
      }
   }
}"""
        self.create_schema(
            subject_2, json_with_reference, ".json",
            f"https://example.com/geographical-location.schema.json:{subject_1}:1"
        )  # ID 2

        # Produce: unencoded key, encoded value:
        key_1 = "somekey"
        msg_1 = '{"productId":123,"productName":"redpanda","tags":["foo","bar"],"warehouseLocation":{"latitude":37.2795481,"longitude":127.047077}}'
        expected_msg_1 = json.loads(msg_1)

        self._rpk.produce(test_topic, msg=msg_1, key=key_1, schema_id="topic")

        # We consume as is, i.e: it will show the encoded value.
        out = self._rpk.consume(test_topic, offset="0:1")
        msg = json.loads(out)

        # We check that:
        # - we are not storing the value without encoding.
        # - first byte is the magic number 0 from the serde header.
        # - key is not encoded.
        raw_bytes_string = msg["value"]
        assert raw_bytes_string != expected_msg_1
        assert msg["key"] == key_1
        bytes_from_string = bytes(
            raw_bytes_string.encode().decode('unicode-escape'), 'utf-8')
        assert bytes_from_string[0] == 0

        # Now we decode the same message:
        out = self._rpk.consume(test_topic,
                                offset="0:1",
                                use_schema_registry="value")
        msg = json.loads(out)

        assert json.loads(msg["value"]) == expected_msg_1
        assert msg["key"] == key_1

        # Finally we will check that we fail when the validation fails
        with expect_exception(
                RpkException,
                lambda e: "jsonschema validation failed" in str(e)):
            # Error here is that productId should be an integer and not a string.
            bad_msg = '{"productId":"123","productName":"redpanda","tags":["foo","bar"],"warehouseLocation":{"latitude":37.2795481,"longitude":127.047077}}'
            self._rpk.produce(test_topic,
                              msg=bad_msg,
                              key=key_1,
                              schema_id="topic")

    @cluster(num_nodes=1)
    def test_registry_mode(self):
        """
        Simple test to assert rpk command works for setting the
        SR mode, not to test the underlying Redpanda behavior.
        """
        read_only_mode = "READONLY"
        read_write_mode = "READWRITE"
        global_subject = "{GLOBAL}"

        # Set the global mode to READONLY
        self._rpk.set_mode(read_only_mode)

        def findModeBySubject(subjects, target):
            for sub in subjects:
                if sub["subject"] == target:
                    return sub["mode"]

        out = self._rpk.get_mode()
        assert findModeBySubject(out, global_subject) == read_only_mode

        subject_1 = "subject-1"
        subject_2 = "subject-2"
        self._rpk.set_mode(read_write_mode, [subject_1, subject_2])

        # Get All
        out = self._rpk.get_mode([subject_1, subject_2], includeGlobal=True)
        assert findModeBySubject(out, global_subject) == read_only_mode
        assert findModeBySubject(out, subject_1) == read_write_mode
        assert findModeBySubject(out, subject_2) == read_write_mode

        # Reset 1
        self._rpk.reset_mode([subject_2])
        out = self._rpk.get_mode([subject_2])
        # It goes back to the global mode (READONLY)
        assert findModeBySubject(out, subject_2) == read_only_mode

        # We do not support import yet
        with expect_exception(RpkException,
                              lambda e: 'invalid mode "IMPORT"' in str(e)):
            self._rpk.set_mode("IMPORT", [subject_1, subject_2], format="text")
