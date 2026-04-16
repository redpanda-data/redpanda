# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import copy
import http.client
import json
import random
import re
import socket
import time
import urllib.parse
import uuid
from enum import Enum
from typing import Any, NamedTuple, Optional, TypeAlias

import requests
from confluent_kafka.schema_registry import (
    Schema,
    SchemaReference,
    SchemaRegistryClient,
    SchemaRegistryError,
    record_subject_name_strategy,
    topic_record_subject_name_strategy,
    topic_subject_name_strategy,
)
from confluent_kafka.serialization import MessageField, SerializationContext
from ducktape.mark import matrix, parametrize
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.serde_client_utils import SchemaType, SerdeClientType
from rptest.clients.types import TopicSpec
from rptest.services import tls
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    DEFAULT_LOG_ALLOW_LIST,
    LoggingConfig,
    MetricsEndpoint,
    PandaproxyConfig,
    RedpandaService,
    ResourceSettings,
    SchemaRegistryConfig,
    SecurityConfig,
)
from rptest.services.redpanda_types import SaslCredentials
from rptest.services.serde_client import SerdeClient
from rptest.tests.pandaproxy_test import PandaProxyTLSProvider, User
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import (
    expect_exception,
    inject_remote_script,
    search_logs_with_timeout,
    wait_until_result,
)
from rptest.utils.log_utils import wait_until_nag_is_set
from rptest.utils.mode_checks import skip_fips_mode

Headers: TypeAlias = dict[str, str] | None


class SchemaIdValidationMode(str, Enum):
    NONE = "none"
    REDPANDA = "redpanda"
    COMPAT = "compat"


def create_topic_names(count: int) -> list[str]:
    return list(f"pandaproxy-topic-{uuid.uuid4()}" for _ in range(count))


def get_subject_name(sns: str, topic: str, field: MessageField, record_name: str):
    return {
        TopicSpec.SubjectNameStrategy.TOPIC_NAME: topic_subject_name_strategy,
        TopicSpec.SubjectNameStrategyCompat.TOPIC_NAME: topic_subject_name_strategy,
        TopicSpec.SubjectNameStrategy.RECORD_NAME: record_subject_name_strategy,
        TopicSpec.SubjectNameStrategyCompat.RECORD_NAME: record_subject_name_strategy,
        TopicSpec.SubjectNameStrategy.TOPIC_RECORD_NAME: topic_record_subject_name_strategy,
        TopicSpec.SubjectNameStrategyCompat.TOPIC_RECORD_NAME: topic_record_subject_name_strategy,
    }[sns](SerializationContext(topic, field), record_name)


HTTP_GET_HEADERS = {"Accept": "application/vnd.schemaregistry.v1+json"}

HTTP_POST_HEADERS = {
    "Accept": "application/vnd.schemaregistry.v1+json",
    "Content-Type": "application/vnd.schemaregistry.v1+json",
}

HTTP_DELETE_HEADERS = {"Accept": "application/vnd.schemaregistry.v1+json"}

schema1_def = (
    '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
)
# Schema 2 is only backwards compatible
schema2_def = '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":["null","string"]},{"name":"f2","type":"string","default":"foo"}]}'
# Schema 3 is not backwards compatible
schema3_def = '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"},{"name":"f2","type":"string"}]}'
invalid_avro = (
    '{"type":"notatype","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
)

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

schema_a_proto_def = """
syntax = "proto3";

message AType {
  float f =  1;
}"""

schema_a_proto_sanitized_def = """
syntax = "proto3";

message AType {
  float f = 1;
}"""

schema_b_proto_def = """
syntax = "proto3";

import "schema_a.proto";

message BType {
  AType at =  1;
}"""

schema_b_proto_sanitized_def = """
syntax = "proto3";

import "schema_a.proto";
message BType {
  .AType at = 1;
}"""

schema_c_proto_def = """
syntax = "proto3";

import "schema_b.proto";

message CType {
  BType bt =  1;
}"""

schema_c_proto_sanitized_def = """
syntax = "proto3";

import "schema_b.proto";
message CType {
  .BType bt = 1;
}"""

schema_d_proto_def = """
syntax = "proto3";

import "schema_e.proto";

message DType {
  EType bt =  1;
}"""

schema_d_proto_sanitized_def = """
syntax = "proto3";

import "schema_e.proto";
message DType {
  .EType bt = 1;
}"""

schema_e_proto_def = """
syntax = "proto3";

message EType {
  string et =  1;
}"""

# Schemas f,g,i,j,k are already sanitized
schema_f_v1_proto_def = """
syntax = "proto3";

message FType {
  string ft1 = 1;
}"""

schema_f_v2_proto_def = """
syntax = "proto3";

message FType {
  string ft1 = 1;
  string ft2 = 2;
}"""

schema_f_v3_proto_def = """
syntax = "proto3";

message FType {
  string ft1 = 1;
  string ft2 = 2;
  string ft3 = 3;
}"""

schema_f_v4_proto_def = """
syntax = "proto3";

message FType {
  string ft1 = 1;
  string ft2 = 2;
  string ft3 = 3;
  string ft4 = 4;
}"""

schema_f_v5_proto_def = """
syntax = "proto3";

message FType {
  string ft1 = 1;
  string ft2 = 2;
  string ft3 = 3;
  string ft4 = 4;
  string ft5 = 5;
}"""

schema_g_proto_def = """
syntax = "proto3";

import "schema_f.proto";
message GType {
  .FType gt = 1;
}"""

schema_h_proto_def = """
syntax = "proto3";

message HType {
  int32 ht = 1;
}"""

schema_i_proto_def = """
syntax = "proto3";

import "schema_h.proto";
message IType {
  .HType it = 1;
}"""

schema_j_proto_def = """
syntax = "proto3";

import "schema_i.proto";
message JType {
  .IType jt = 1;
}"""

schema_k_proto_def = """
syntax = "proto3";

import "schema_j.proto";
message KType {
  .JType kt = 1;
}"""

well_known_proto_def = """
syntax = "proto3";

import "google/protobuf/timestamp.proto";

message Test3 {
  google.protobuf.Timestamp timestamp = 1;
}"""

validate_proto_def = """
syntax = "proto3";

import "buf/validate/validate.proto";

message TestValidate {
  buf.validate.FieldRules field_rules = 1;
  buf.validate.StringRules string_rules = 2;
  buf.validate.Int32Rules int32_rules = 3;
  buf.validate.MessageRules message_rules = 4;
  buf.validate.TimestampRules timestamp_rules = 5;
}"""

json_number_schema_def = '{"type":"number"}'

validation_schemas = dict(
    proto3="""
syntax = "proto3";

message myrecord {
  message Msg1 {
    int32 f1 = 1;
  }
  Msg1 m1 = 1;
  Msg1 m2 = 2;
}
""",
    proto3_incompat="""
syntax = "proto3";

message myrecord {
  // MESSAGE_REMOVED
  message Msg1d {
    int32 f1 = 1;
  }
  // FIELD_NAMED_TYPE_CHANGED
  Msg1d m1 = 1;
}
""",
    proto2="""
syntax = "proto2";

message myrecord {
  message Msg1 {
    required int32 f1 = 1;
  }
  required Msg1 m1 = 1;
  required Msg1 m2 = 2;
}
""",
    proto2_incompat="""
syntax = "proto2";

message myrecord {
  // MESSAGE_REMOVED
  message Msg1d {
    required int32 f1 = 1;
  }
  // FIELD_NAMED_TYPE_CHANGED
  required Msg1d m1 = 1;
}
""",
    avro="""
{
    "type": "record",
    "name": "myrecord",
    "fields": [
        {
            "name": "f1",
            "type": "string"
        },
        {
            "name": "enumF",
            "type": {
                "name": "ABorC",
                "type": "enum",
                "symbols": ["a", "b", "c"]
            }
        }
    ]
}
""",
    avro_incompat="""
{
    "type": "record",
    "name": "myrecord",
    "fields": [
        {
            "name": "f1",
            "type": "int"
        },
        {
            "name": "enumF",
            "type": {
                "name": "ABorC",
                "type": "enum",
                "symbols": ["a"]
            }
        }
    ]
}
""",
    json=r'{"type": "number"}',
    json_incompat=r'{"type": "number", "multipleOf": 20}',
)

imported_schema_proto_def = """
syntax = "proto3";
message AType {
  double d =   2;
  float f =   1;
}"""

imported_schema_sanitized_proto_def = """
syntax = "proto3";

message AType {
  double d = 2;
  float f = 1;
}"""

imported_schema_normalized_proto_def = """
syntax = "proto3";

message AType {
  float f = 1;
  double d = 2;
}"""

imported_schema = {
    "subject": "imported",
    "version": 1,
    "schema": imported_schema_proto_def,
    "sanitized": imported_schema_sanitized_proto_def,
    "normalized": imported_schema_normalized_proto_def,
}

import_schemas = {
    "schema_a": {
        "subject": "schema_a",
        "version": 1,
        "schema": schema_a_proto_def,
        "sanitized": schema_a_proto_sanitized_def,
    },
    "schema_b": {
        "subject": "schema_b",
        "version": 1,
        "references": [{"name": "schema_a.proto", "subject": "schema_a", "version": 1}],
        "schema": schema_b_proto_def,
        "sanitized": schema_b_proto_sanitized_def,
    },
    "schema_c": {
        "subject": "schema_c",
        "version": 1,
        "references": [{"name": "schema_b.proto", "subject": "schema_b", "version": 1}],
        "schema": schema_c_proto_def,
        "sanitized": schema_c_proto_sanitized_def,
    },
    "schema_d": {
        "subject": "schema_d",
        "version": 1,
        "references": [{"name": "schema_e.proto", "subject": "schema_e", "version": 1}],
        "schema": schema_d_proto_def,
        # schema d will be invalid during startup in the test it is used.
        # that's why the 'sanitized' form is not the actual sanitized form but the input form.
        "sanitized": schema_d_proto_def,
    },
    "schema_e": {
        "subject": "schema_e",
        "version": 1,
        "schema": schema_e_proto_def,
    },
    "schema_f_v1": {
        "subject": "schema_f",
        "version": 1,
        "schema": schema_f_v1_proto_def,
        "sanitized": schema_f_v1_proto_def,
    },
    "schema_f_v2": {
        "subject": "schema_f",
        "version": 2,
        "schema": schema_f_v2_proto_def,
        "sanitized": schema_f_v2_proto_def,
    },
    "schema_f_v3": {
        "subject": "schema_f",
        "version": 3,
        "schema": schema_f_v3_proto_def,
        "sanitized": schema_f_v3_proto_def,
    },
    "schema_f_v4": {
        "subject": "schema_f",
        "version": 4,
        "schema": schema_f_v4_proto_def,
        "sanitized": schema_f_v4_proto_def,
    },
    "schema_f_v5": {
        "subject": "schema_f",
        "version": 5,
        "schema": schema_f_v5_proto_def,
        "sanitized": schema_f_v5_proto_def,
    },
    "schema_g_v1": {
        "subject": "schema_g",
        "version": 1,
        "references": [{"name": "schema_f.proto", "subject": "schema_f", "version": 1}],
        "schema": schema_g_proto_def,
        "sanitized": schema_g_proto_def,
    },
    "schema_g_v2": {
        "subject": "schema_g",
        "version": 2,
        "references": [{"name": "schema_f.proto", "subject": "schema_f", "version": 2}],
        "schema": schema_g_proto_def,
        "sanitized": schema_g_proto_def,
    },
    "schema_g_v3": {
        "subject": "schema_g",
        "version": 3,
        "references": [{"name": "schema_f.proto", "subject": "schema_f", "version": 3}],
        "schema": schema_g_proto_def,
        "sanitized": schema_g_proto_def,
    },
    "schema_g_v4": {
        "subject": "schema_g",
        "version": 4,
        "references": [{"name": "schema_f.proto", "subject": "schema_f", "version": 4}],
        "schema": schema_g_proto_def,
        "sanitized": schema_g_proto_def,
    },
    "schema_g_v5": {
        "subject": "schema_g",
        "version": 5,
        "references": [{"name": "schema_f.proto", "subject": "schema_f", "version": 5}],
        "schema": schema_g_proto_def,
        "sanitized": schema_g_proto_def,
    },
    "schema_h": {
        "subject": "schema_h",
        "version": 1,
        "schema": schema_h_proto_def,
        "sanitized": schema_h_proto_def,
    },
    "schema_i": {
        "subject": "schema_i",
        "version": 1,
        "references": [{"name": "schema_h.proto", "subject": "schema_h", "version": 1}],
        "schema": schema_i_proto_def,
        "sanitized": schema_i_proto_def,
    },
    "schema_j": {
        "subject": "schema_j",
        "version": 1,
        "references": [{"name": "schema_i.proto", "subject": "schema_i", "version": 1}],
        "schema": schema_j_proto_def,
        "sanitized": schema_j_proto_def,
    },
    "schema_k": {
        "subject": "schema_k",
        "version": 1,
        "references": [{"name": "schema_j.proto", "subject": "schema_j", "version": 1}],
        "schema": schema_k_proto_def,
        "sanitized": schema_k_proto_def,
    },
}

schema_proto_def = """
syntax = "proto3";

message ProtoType {
  float f = 1;
}"""

schema_proto_b64 = (
    "CgAiFgoJUHJvdG9UeXBlEgkKAWYYASABKAJKYQoGEgQAAAQBCggKAQwSAwAA"
    "EgoKCgIEABIEAgAEAQoKCgMEAAESAwIIEQoLCgQEAAIAEgMDAg4KDAoFBAAC"
    "AAUSAwMCBwoMCgUEAAIAARIDAwgJCgwKBQQAAgADEgMDDA1iBnByb3RvMw=="
)

schema_proto2_def = """
syntax = "proto3";

message ProtoType2 {
  string s = 2;
}"""

schema_avro_def = (
    '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
)

schema_proto_dependee_def = """
syntax = "proto3";

import "schema_proto.proto";
message Dependee {
  ProtoType p =  1;
}"""

schema_proto_dependee2_def = """
syntax = "proto3";

import "schema_proto.proto";
import "schema_proto2.proto";

message Dependee {
    ProtoType p1 = 1;
    ProtoType2 p2 = 2;
}"""

schema_avro_dependee_def = """
{
    "type": "record",
    "name": "myotherrecord",
    "fields": [
        {
            "name": "f2",
            "type": "string"
        }
    ]
}"""

base_schemas = {
    "proto": {
        "subject": "schema_proto",
        "schema": schema_proto_def,
        "type": "PROTOBUF",
    },
    "json": {
        "subject": "schema_json",
        "schema": json_number_schema_def,
        "type": "JSON",
    },
    "avro": {
        "subject": "schema_avro",
        "schema": schema_avro_def,
        "type": "AVRO",
    },
}

base2_schemas = {
    "proto": {
        "subject": "schema_proto2",
        "schema": schema_proto2_def,
        "type": "PROTOBUF",
    }
}

not_dependent_schemas = {
    "proto": {
        "schema": schema_proto_def,
        "type": "PROTOBUF",
    },
    "json": {
        "schema": json_number_schema_def,
        "type": "JSON",
    },
    "avro": {
        "schema": schema_avro_def,
        "type": "AVRO",
    },
}

dependent_schemas = {
    "proto": {
        "subject": "schema_proto_dependee_schema",
        "schema": schema_proto_dependee_def,
        "references": [
            {"name": "schema_proto.proto", "subject": "schema_proto", "version": 1}
        ],
        "type": "PROTOBUF",
        "id": 4,
    },
    "json": {
        "subject": "schema_json_dependee_schema",
        "schema": json_number_schema_def,
        "references": [
            {"name": "schema_json.json", "subject": "schema_json", "version": 1}
        ],
        "type": "JSON",
        "id": 5,
    },
    "avro": {
        "subject": "schema_avro_dependee_schema",
        "schema": schema_avro_dependee_def,
        "references": [
            {"name": "schema_avro.avro", "subject": "schema_avro", "version": 1}
        ],
        "type": "AVRO",
        "id": 6,
    },
}

multi_dependent_schemas = {
    "proto": {
        "subject": "schema_proto_multi_dependee_schema",
        "schema": schema_proto_dependee2_def,
        "references": [
            {"name": "schema_proto.proto", "subject": "schema_proto", "version": 1},
            {"name": "schema_proto2.proto", "subject": "schema_proto2", "version": 1},
        ],
        "type": "PROTOBUF",
    }
}

log_config = LoggingConfig(
    "info",
    logger_levels={
        "security": "trace",
        "schemaregistry": "trace",
        "kafka/client": "trace",
    },
)


class TestCompatDataset(NamedTuple):
    type: SchemaType
    schema_base: str
    schema_backward_compatible: str
    schema_not_backward_compatible: str


def get_compat_dataset(type: SchemaType) -> TestCompatDataset:
    if type == SchemaType.AVRO:
        return TestCompatDataset(
            type=SchemaType.AVRO,
            schema_base=schema1_def,
            schema_backward_compatible=schema2_def,
            schema_not_backward_compatible=schema3_def,
        )
    if type == SchemaType.JSON:
        return TestCompatDataset(
            type=SchemaType.JSON,
            schema_base="""
{
  "type": "object",
  "properties": {
    "aaaa": {"type": "integer"},
    "bbbb": {"type": "boolean"}
  },
  "additionalProperties": false,
  "required": ["aaaa", "bbbb"]
}
""",
            schema_backward_compatible="""
{
  "type": "object",
  "properties": {
    "aaaa": {"type": "number"}
  },
  "additionalProperties": {"type": "boolean"},
  "required": ["aaaa"]
}
""",
            schema_not_backward_compatible="""
{
  "type": "object",
  "properties": {
    "aaaa": {"type": "string"}
  },
  "additionalProperties": {"type": "boolean"},
  "required": ["aaaa"]
}
""",
        )
    assert False, f"Unsupported schema {type=}"


class TestNormalizeDataset(NamedTuple):
    type: SchemaType
    schema_base: str
    schema_canonical: str
    schema_normalized: str


def get_normalize_dataset(type: SchemaType) -> TestNormalizeDataset:
    if type == SchemaType.AVRO:
        return TestNormalizeDataset(
            type=SchemaType.AVRO,
            schema_base="""{
  "name": "myrecord",
  "type": "record",
  "fields": [
    {
      "name": "nested",
      "type": {
        "type": "array",
        "items": {
          "fields": [
            {
              "type": "string",
              "name": "f1"
            }
          ],
          "name": "nested_item",
          "type": "record"
        }
      }
    }
  ]
}""",
            schema_canonical=re.sub(
                r"[\n\t\s]*",
                "",
                """{
  "type": "record",
  "name": "myrecord",
  "fields": [
    {
      "name": "nested",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "nested_item",
          "fields": [
            {
              "name": "f1",
              "type": "string"
            }
          ]
        }
      }
    }
  ]
}""",
            ),
            schema_normalized=re.sub(
                r"[\n\t\s]*",
                "",
                """{
  "type": "record",
  "name": "myrecord",
  "fields": [
    {
      "name": "nested",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "nested_item",
          "fields": [
            {
              "name": "f1",
              "type": "string"
            }
          ]
        }
      }
    }
  ]
}""",
            ),
        )
    if type == SchemaType.JSON:
        return TestNormalizeDataset(
            type=SchemaType.JSON,
            schema_base="""{
  "type": "object",
  "properties": {
    "aaaa": {"type": "number"}
  },
  "additionalProperties": {"type": "boolean"},
  "required": ["aaaa"]
}
""",
            schema_canonical=re.sub(
                r"[\n\t\s]*",
                "",
                """{
  "type": "object",
  "properties": {
    "aaaa": {"type": "number"}
  },
  "additionalProperties": {"type": "boolean"},
  "required": ["aaaa"]
}""",
            ),
            schema_normalized=re.sub(
                r"[\n\t\s]*",
                "",
                """{
  "additionalProperties": {"type": "boolean"},
  "properties": {
    "aaaa": {"type": "number"}
  },
  "required": ["aaaa"],
  "type": "object"
}""",
            ),
        )
    assert False, f"Unsupported schema {type=}"


class ReferenceFormat(str, Enum):
    NONE = ""
    QUALIFIED = "qualified"


class SchemaRegistryRedpandaClient:
    """
    A client for acessing the schema registry.
    """

    def __init__(
        self,
        redpanda: RedpandaService,
    ):
        self.redpanda = redpanda
        self.logger = redpanda.logger

        http.client.HTTPConnection.debuglevel = 1
        http.client.print = lambda *args: self.logger.debug(" ".join(args))

    def request(self, verb, path, hostname=None, tls_enabled: bool = False, **kwargs):
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

        if "timeout" not in kwargs:
            kwargs["timeout"] = 60

        # Error codes that may appear during normal API operation, do not
        # indicate an issue with the service
        acceptable_errors = {401, 403, 404, 409, 422, 501}

        def accept_response(resp):
            return (
                200 <= resp.status_code < 300 or resp.status_code in acceptable_errors
            )

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

        self.logger.info(f"{r.status_code} {verb} hostname={hostname} {path} {kwargs}")

        return r

    def base_uri(self):
        return f"http://{self.redpanda.nodes[0].account.hostname}:8081"

    def get_config(self, headers=HTTP_GET_HEADERS, **kwargs):
        return self.request("GET", "config", headers=headers, **kwargs)

    def set_config(self, data, headers=HTTP_POST_HEADERS, **kwargs):
        return self.request("PUT", "config", headers=headers, data=data, **kwargs)

    def get_config_subject(
        self, subject, fallback=False, headers=HTTP_GET_HEADERS, **kwargs
    ):
        return self.request(
            "GET",
            f"config/{subject}{'?defaultToGlobal=true' if fallback else ''}",
            headers=headers,
            **kwargs,
        )

    def set_config_subject(
        self,
        subject: str,
        data: Any,
        headers: Headers = HTTP_POST_HEADERS,
        **kwargs: Any,
    ):
        return self.request(
            "PUT", f"config/{subject}", headers=headers, data=data, **kwargs
        )

    def delete_config_subject(
        self, subject: str, headers: Headers = HTTP_DELETE_HEADERS, **kwargs: Any
    ):
        return self.request("DELETE", f"config/{subject}", headers=headers, **kwargs)

    def get_mode(self, headers=HTTP_GET_HEADERS, **kwargs):
        return self.request("GET", "mode", headers=headers, **kwargs)

    def set_mode(self, data, force=False, headers=HTTP_POST_HEADERS, **kwargs):
        return self.request(
            "PUT",
            f"mode{'?force=true' if force else ''}",
            headers=headers,
            data=data,
            **kwargs,
        )

    def get_mode_subject(
        self, subject, fallback=False, headers=HTTP_GET_HEADERS, **kwargs
    ):
        return self.request(
            "GET",
            f"mode/{subject}{'?defaultToGlobal=true' if fallback else ''}",
            headers=headers,
            **kwargs,
        )

    def set_mode_subject(
        self,
        subject: str,
        data: Any,
        force: bool = False,
        headers: Headers = HTTP_POST_HEADERS,
        **kwargs: Any,
    ):
        return self.request(
            "PUT",
            f"mode/{subject}{'?force=true' if force else ''}",
            headers=headers,
            data=data,
            **kwargs,
        )

    def delete_mode_subject(
        self, subject: str, headers: Headers = HTTP_DELETE_HEADERS, **kwargs: Any
    ):
        return self.request("DELETE", f"mode/{subject}", headers=headers, **kwargs)

    def get_schemas_types(
        self, headers=HTTP_GET_HEADERS, tls_enabled: bool = False, **kwargs
    ):
        return self.request(
            "GET", "schemas/types", headers=headers, tls_enabled=tls_enabled, **kwargs
        )

    def get_schemas_ids_id(
        self, id, format=None, subject=None, headers=HTTP_GET_HEADERS, **kwargs
    ):
        params = []
        if format is not None:
            params.append(f"format={format}")
        if subject is not None:
            params.append(f"subject={subject}")
        query_string = f"?{'&'.join(params)}" if params else ""
        return self.request(
            "GET", f"schemas/ids/{id}{query_string}", headers=headers, **kwargs
        )

    def get_schemas_ids_id_schema(
        self, id, format=None, subject=None, headers=HTTP_GET_HEADERS, **kwargs
    ):
        params = []
        if format is not None:
            params.append(f"format={format}")
        if subject is not None:
            params.append(f"subject={subject}")
        query_string = f"?{'&'.join(params)}" if params else ""
        return self.request(
            "GET", f"schemas/ids/{id}/schema{query_string}", headers=headers, **kwargs
        )

    def get_schemas_ids_id_versions(
        self, id, subject=None, headers=HTTP_GET_HEADERS, **kwargs
    ):
        query_string = f"?subject={subject}" if subject is not None else ""
        return self.request(
            "GET", f"schemas/ids/{id}/versions{query_string}", headers=headers, **kwargs
        )

    def get_schemas_ids_id_subjects(
        self, id, deleted=False, subject=None, headers=HTTP_GET_HEADERS, **kwargs
    ):
        query_params = []
        if deleted:
            query_params.append("deleted=true")
        if subject is not None:
            query_params.append(f"subject={subject}")
        query_string = f"?{'&'.join(query_params)}" if query_params else ""
        return self.request(
            "GET",
            f"schemas/ids/{id}/subjects{query_string}",
            headers=headers,
            **kwargs,
        )

    def get_subjects(
        self, deleted=False, subject_prefix=None, headers=HTTP_GET_HEADERS, **kwargs
    ):
        params = {}
        if deleted:
            params["deleted"] = "true"
        if subject_prefix:
            params["subjectPrefix"] = subject_prefix
        return self.request("GET", "subjects", params=params, headers=headers, **kwargs)

    def post_subjects_subject(
        self,
        subject,
        data,
        deleted=False,
        normalize=False,
        format=None,
        headers=HTTP_POST_HEADERS,
        **kwargs,
    ):
        params = {}
        if deleted:
            params["deleted"] = "true"
        if normalize:
            params["normalize"] = "true"
        if format is not None:
            params["format"] = format
        return self.request(
            "POST",
            f"subjects/{subject}",
            headers=headers,
            data=data,
            params=params,
            **kwargs,
        )

    def post_subjects_subject_versions(
        self,
        subject: str,
        data: Any,
        normalize: bool = False,
        headers: Headers = HTTP_POST_HEADERS,
        **kwargs: Any,
    ):
        params = {}
        if normalize:
            params["normalize"] = "true"
        return self.request(
            "POST",
            f"subjects/{subject}/versions",
            headers=headers,
            data=data,
            params=params,
            **kwargs,
        )

    def get_subjects_subject_versions_version(
        self,
        subject,
        version,
        format=None,
        reference_format: ReferenceFormat | None = None,
        headers=HTTP_GET_HEADERS,
        **kwargs,
    ):
        params = {}
        if format is not None:
            params["format"] = format
        if reference_format and reference_format != ReferenceFormat.NONE:
            params["referenceFormat"] = reference_format.value
        return self.request(
            "GET",
            f"subjects/{subject}/versions/{version}",
            headers=headers,
            params=params,
            **kwargs,
        )

    def get_subjects_subject_versions_version_schema(
        self, subject, version, format=None, headers=HTTP_GET_HEADERS, **kwargs
    ):
        params = {}
        if format is not None:
            params["format"] = format
        return self.request(
            "GET",
            f"subjects/{subject}/versions/{version}/schema",
            headers=headers,
            params=params,
            **kwargs,
        )

    def get_subjects_subject_versions_version_referenced_by(
        self,
        subject: str,
        version: str,
        headers: Headers = HTTP_GET_HEADERS,
        **kwargs: Any,
    ):
        deprecated = self.request(
            "GET",
            f"subjects/{subject}/versions/{version}/referencedBy",
            headers=headers,
            **kwargs,
        )
        standard = self.request(
            "GET",
            f"subjects/{subject}/versions/{version}/referencedby",
            headers=headers,
            **kwargs,
        )
        assert standard.json() == deprecated.json()
        return standard

    def get_subjects_subject_versions(
        self,
        subject: str,
        deleted: bool = False,
        headers: Headers = HTTP_GET_HEADERS,
        **kwargs: Any,
    ):
        return self.request(
            "GET",
            f"subjects/{subject}/versions{'?deleted=true' if deleted else ''}",
            headers=headers,
            **kwargs,
        )

    def delete_subject(
        self,
        subject: str,
        permanent: bool = False,
        headers: Headers = HTTP_GET_HEADERS,
        **kwargs: Any,
    ):
        return self.request(
            "DELETE",
            f"subjects/{subject}{'?permanent=true' if permanent else ''}",
            headers=headers,
            **kwargs,
        )

    def delete_subject_version(
        self,
        subject: str,
        version: str,
        permanent: bool = False,
        headers: Headers = HTTP_DELETE_HEADERS,
        **kwargs: Any,
    ):
        return self.request(
            "DELETE",
            f"subjects/{subject}/versions/{version}{'?permanent=true' if permanent else ''}",
            headers=headers,
            **kwargs,
        )

    def post_compatibility_subject_version(
        self,
        subject,
        version,
        data,
        headers=HTTP_POST_HEADERS,
        verbose: bool | None = None,
        **kwargs,
    ):
        params = {}
        if verbose is not None:
            params["verbose"] = verbose

        return self.request(
            "POST",
            f"compatibility/subjects/{subject}/versions/{version}",
            params=params,
            headers=headers,
            data=data,
            **kwargs,
        )

    def get_status_ready(
        self, headers=HTTP_GET_HEADERS, tls_enabled: bool = False, **kwargs
    ):
        return self.request(
            "GET", "status/ready", headers=headers, tls_enabled=tls_enabled, **kwargs
        )

    def get_security_acls(self, **kwargs):
        return self.request("GET", "security/acls", **kwargs)

    def post_security_acls(self, data, **kwargs):
        return self.request(
            "POST",
            "security/acls",
            json=data,
            headers={"Content-Type": "application/json"},
            **kwargs,
        )

    def delete_security_acls(self, data, **kwargs):
        return self.request(
            "DELETE",
            "security/acls",
            json=data,
            headers={"Content-Type": "application/json"},
            **kwargs,
        )

    def get_contexts(self, headers: Headers = HTTP_GET_HEADERS, **kwargs: Any):
        return self.request("GET", "contexts", headers=headers, **kwargs)

    def delete_context(
        self, context: str, headers: Headers = HTTP_DELETE_HEADERS, **kwargs: Any
    ):
        return self.request("DELETE", f"contexts/{context}", headers=headers, **kwargs)

    def create_acl(
        self,
        principal,
        resource,
        resource_type,
        pattern_type,
        host,
        operation,
        permission="ALLOW",
    ):
        return {
            "principal": f"User:{principal}",
            "resource": resource,
            "resource_type": resource_type,
            "pattern_type": pattern_type,
            "host": host,
            "operation": operation,
            "permission": permission,
        }

    def post_acl(self, acl, **kwargs):
        """Grant an ACL to the regular user."""
        resp = self.post_security_acls([acl], **kwargs)
        assert resp.status_code == 201, f"Failed to create ACL: {acl=}"


class SchemaRegistryEndpoints(RedpandaTest):
    """
    Test schema registry against a redpanda cluster.
    """

    def __init__(
        self,
        context: TestContext,
        schema_registry_config: SchemaRegistryConfig = SchemaRegistryConfig(),
        num_brokers: int = 3,
        extra_rp_conf: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ):
        merged_rp_conf = {"auto_create_topics_enabled": False}
        if extra_rp_conf:
            merged_rp_conf.update(extra_rp_conf)
        super(SchemaRegistryEndpoints, self).__init__(
            context,
            extra_rp_conf=merged_rp_conf,
            resource_settings=ResourceSettings(num_cpus=1),
            log_config=log_config,
            pandaproxy_config=PandaproxyConfig(),
            schema_registry_config=schema_registry_config,
            **kwargs,
        )

        self.sr_client = SchemaRegistryRedpandaClient(redpanda=self.redpanda)

    def assert_equal(self, first, second, msg=None):
        assert first == second, msg or f"{first} != {second}"

    def assert_not_equal(self, first, second, msg=None):
        assert first != second, msg or f"{first} == {second}"

    def assert_in(self, member, container, msg=None):
        assert member in container, msg or f"{member!r} not found in {container!r}"

    def assert_not_in(self, member, container, msg=None):
        assert member not in container, (
            msg or f"{member!r} unexpectedly found in {container!r}"
        )

    def _get_rpk_tools(self):
        return RpkTool(self.redpanda)

    def _get_serde_client(
        self,
        schema_type: SchemaType,
        client_type: SerdeClientType,
        topic: str,
        count: int,
        skip_known_types: Optional[bool] = None,
        use_latest_version: Optional[bool] = None,
        subject_name_strategy: Optional[str] = None,
        payload_class: Optional[str] = None,
        compression_type: Optional[TopicSpec.CompressionTypes] = None,
        context_name_strategy: Optional[str] = None,
        context_name: Optional[str] = None,
    ):
        schema_reg = self.redpanda.schema_reg().split(",", 1)[0]
        sec_cfg = self.redpanda.kafka_client_security().to_dict()

        return SerdeClient(
            self.test_context,
            self.redpanda.brokers(),
            schema_reg,
            schema_type,
            client_type,
            count,
            topic=topic,
            security_config=sec_cfg if sec_cfg else None,
            skip_known_types=skip_known_types,
            use_latest_version=use_latest_version,
            subject_name_strategy=subject_name_strategy,
            payload_class=payload_class,
            compression_type=compression_type,
            context_name_strategy=context_name_strategy,
            context_name=context_name,
        )

    def _create_topic(
        self,
        topic=create_topic_names(1),
        partition_count=1,
        replication_factor=1,
        config={TopicSpec.PROPERTY_CLEANUP_POLICY: TopicSpec.CLEANUP_DELETE},
    ):
        self.logger.debug(f"Creating topic: {topic}")
        rpk_tools = self._get_rpk_tools()
        rpk_tools.create_topic(
            topic=topic,
            partitions=partition_count,
            replicas=replication_factor,
            config=config,
        )

        def has_topic():
            self_topics = set(rpk_tools.list_topics())
            self.logger.info(f"name: {topic}, self._get_topics(): {self_topics}")
            return topic in self_topics

        wait_until(
            has_topic,
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Timeout waiting for topic: {topic}",
        )

        return topic

    def _alter_topic_config(self, topic, set_key, set_value):
        rpk = self._get_rpk_tools()
        rpk.alter_topic_config(topic=topic, set_key=set_key, set_value=set_value)

        def has_config():
            configs = rpk.describe_topic_configs(topic=topic)
            self.logger.warn(f"CONFIGS: {configs}")
            config = configs.get(set_key, None)
            self.logger.warn(f"CONFIG: {config[0]}")
            return config[0] == set_value

        wait_until(has_config, 5)

    def _push_to_schemas_topic(self, schemas):
        schema_topic = TopicSpec(
            name="_schemas", partition_count=1, replication_factor=1
        )
        self.client().create_topic(schema_topic)

        rpk = self._get_rpk_tools()

        for i_schema, schema in enumerate(schemas):
            key = {
                "keytype": "SCHEMA",
                "subject": schema["subject"],
                "version": schema["version"],
                "magic": 1,
            }
            value = {
                "subject": schema["subject"],
                "version": schema["version"],
                "id": i_schema + 1,
                "schemaType": "PROTOBUF",
                "schema": schema["schema"],
                "deleted": False,
            }
            if "references" in schema:
                value["references"] = schema["references"]
            rpk.produce(topic="_schemas", key=json.dumps(key), msg=json.dumps(value))


class SchemaRegistryTestMethods(SchemaRegistryEndpoints):
    """
    Base class for testing schema registry against a redpanda cluster.

    Inherit from this to run the tests.
    """

    def __init__(self, context: TestContext, **kwargs: Any):
        super(SchemaRegistryTestMethods, self).__init__(context, **kwargs)

    @cluster(num_nodes=3)
    def test_schemas_types(self):
        """
        Verify the schema registry returns the supported types
        """
        self.logger.debug("Request schema types with no accept header")
        result_raw = self.sr_client.get_schemas_types(headers={})
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Request schema types with defautl accept header")
        result_raw = self.sr_client.get_schemas_types()
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert set(result) == {"JSON", "PROTOBUF", "AVRO"}

    @cluster(num_nodes=3)
    def test_get_schema_id_versions(self):
        """
        Verify schema versions
        """

        self.logger.debug("Checking schema 1 versions - expect 40403")
        result_raw = self.sr_client.get_schemas_ids_id_versions(id=1)
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40403
        assert result_raw.json()["message"] == "Schema 1 not found"

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"

        schema_1_data = json.dumps({"schema": schema1_def})
        schema_2_data = json.dumps({"schema": schema2_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject, data=schema_1_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Checking schema 1 versions")
        result_raw = self.sr_client.get_schemas_ids_id_versions(id=1)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [{"subject": subject, "version": 1}]

        self.logger.debug("Posting schema 2 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject, data=schema_2_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 2

        self.logger.debug("Checking schema 2 versions")
        result_raw = self.sr_client.get_schemas_ids_id_versions(id=2)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [{"subject": subject, "version": 2}]

        self.logger.debug("Deleting version 1")
        result_raw = self.sr_client.delete_subject_version(subject=subject, version=1)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Checking schema 1 versions is empty")
        result_raw = self.sr_client.get_schemas_ids_id_versions(id=1)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == []

        self.logger.debug("Checking schema 2 versions")
        result_raw = self.sr_client.get_schemas_ids_id_versions(id=2)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [{"subject": subject, "version": 2}]

        self.logger.debug("Deleting subject")
        result_raw = self.sr_client.delete_subject(subject=subject)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Checking schema 1 versions is empty")
        result_raw = self.sr_client.get_schemas_ids_id_versions(id=1)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == []

        self.logger.debug("Checking schema 2 versions is empty")
        result_raw = self.sr_client.get_schemas_ids_id_versions(id=2)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == []

    @cluster(num_nodes=3)
    def test_get_schema_id_subjects(self):
        """
        Verify schema subjects
        """

        # Given an ID and a list of subjects, check the association
        # Also checks that schema registry returns a sorted list of subjects
        def check_schema_subjects(id: int, subjects: list[str], deleted=False):
            result_raw = self.sr_client.get_schemas_ids_id_subjects(
                id=id, deleted=deleted
            )
            if result_raw.status_code != requests.codes.ok:
                return False
            res_subjects = result_raw.json()
            if type(res_subjects) is not type([]):
                return False
            subjects.sort()
            return res_subjects == subjects and res_subjects == sorted(res_subjects)

        self.logger.debug("Checking schema 1 subjects - expect 40403")
        result_raw = self.sr_client.get_schemas_ids_id_subjects(id=1)
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40403

        topics = create_topic_names(2)
        topic_0 = topics[0]
        topic_1 = topics[1]
        subject_0 = f"{topic_0}-value"
        subject_1 = f"{topic_1}-value"

        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Posting schema 1 as a subject value")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject_0, data=schema_1_data
        )

        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Checking schema 1 subjects - expect subject_0")
        assert check_schema_subjects(id=1, subjects=list([subject_0]))

        self.logger.debug("Posting schema 1 as a subject value (subject_1)")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject_1, data=schema_1_data
        )

        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Checking schema 1 subjects - expect subject_{0,1}")
        assert check_schema_subjects(id=1, subjects=list([subject_0, subject_1]))

        self.logger.debug("Soft delete subject_0")
        result_raw = self.sr_client.delete_subject(subject=subject_0)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Check again, not including deleted")
        assert check_schema_subjects(id=1, subjects=[subject_1])

        self.logger.debug("Check including deleted")
        assert check_schema_subjects(
            id=1, subjects=[subject_0, subject_1], deleted=True
        )

        self.logger.debug("Hard delete subject_0")
        result_raw = self.sr_client.delete_subject(subject=subject_0, permanent=True)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Check including deleted - subject_0 should be gone")
        assert check_schema_subjects(id=1, subjects=[subject_1], deleted=True)

    @cluster(num_nodes=3)
    def test_post_subjects_subject_versions(self):
        """
        Verify posting a schema
        """

        topic = create_topic_names(1)[0]

        self.logger.debug("Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Get empty subjects")
        result_raw = self.sr_client.get_subjects()
        if result_raw.json() != []:
            self.logger.error(result_raw.json)
        assert result_raw.json() == []

        self.logger.debug("Posting invalid schema as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=json.dumps({"schema": invalid_avro})
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.unprocessable_entity

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Reposting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Reposting schema 1 as a subject value")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-value", data=schema_1_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Get subjects")
        result_raw = self.sr_client.get_subjects()
        assert set(result_raw.json()) == {f"{topic}-key", f"{topic}-value"}

        self.logger.debug("Get schema versions for invalid subject")
        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-invalid"
        )
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == f"Subject '{topic}-invalid' not found."

        self.logger.debug("Get schema versions for subject key")
        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-key"
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1]

        self.logger.debug("Get schema versions for subject value")
        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-value"
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1]

        self.logger.debug("Get schema version 1 for invalid subject")
        result_raw = self.sr_client.get_subjects_subject_versions_version(
            subject=f"{topic}-invalid", version=1
        )
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == f"Subject '{topic}-invalid' not found."

        self.logger.debug("Get invalid schema version for subject")
        result_raw = self.sr_client.get_subjects_subject_versions_version(
            subject=f"{topic}-key", version=2
        )
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40402
        assert result["message"] == "Version 2 not found."

        self.logger.debug("Get schema version 1 for subject key")
        result_raw = self.sr_client.get_subjects_subject_versions_version(
            subject=f"{topic}-key", version=1
        )
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == f"{topic}-key"
        assert result["version"] == 1
        # assert result["schema"] == json.dumps(schema_def)

        self.logger.debug("Get latest schema version for subject key")
        result_raw = self.sr_client.get_subjects_subject_versions_version(
            subject=f"{topic}-key", version="latest"
        )
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == f"{topic}-key"
        assert result["version"] == 1
        # assert result["schema"] == json.dumps(schema_def)

        self.logger.debug("Get invalid schema version")
        result_raw = self.sr_client.get_schemas_ids_id(id=2)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40403
        assert result["message"] == "Schema 2 not found"

        self.logger.debug("Get schema version 1")
        result_raw = self.sr_client.get_schemas_ids_id(id=1)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        # assert result["schema"] == json.dumps(schema_def)

        self.logger.debug("Post schema 1 with escape chars in the subject name")
        name = f"{topic}%25252fkey"
        name_quoted = urllib.parse.quote(name)
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=name_quoted, data=schema_1_data
        )
        self.logger.warn(result_raw)
        assert result_raw.status_code == requests.codes.ok
        result_raw = self.sr_client.get_subjects_subject_versions(subject=name_quoted)
        assert result_raw.status_code == requests.codes.ok
        subjs = self.sr_client.get_subjects().json()
        assert name in subjs, (
            f"Expected '{name}' in subjects response, got {json.dumps(subjs, indent=1)}"
        )

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
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=subject, data=schema_1_data
            )
            self.logger.debug(result_raw)
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json()["id"] == 1

    @cluster(num_nodes=3)
    def test_post_subjects_subject_versions_null_metadata_ruleset(self):
        """
        Verify posting a schema with metatada and ruleSet
        These are not supported, but if they're null, we let it pass.
        """

        topic = create_topic_names(1)[0]

        self.logger.debug("Dump the schema with null metadata and ruleSet")
        schema_1_data = json.dumps(
            {"schema": schema1_def, "metadata": None, "ruleSet": None}
        )

        self.logger.debug("Posting schema as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Retrieving schema")
        result_raw = self.sr_client.post_subjects_subject(
            subject=f"{topic}-key", data=schema_1_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

    @cluster(num_nodes=1)
    @matrix(schema_type=[SchemaType.AVRO, SchemaType.PROTOBUF, SchemaType.JSON])
    def test_post_subjects_subject_versions_metadata_properties(self, schema_type):
        """
        Verify posting a schema with metatada.properties.
        """

        def as_java_str(v: str | bool | int | float) -> str:
            if isinstance(v, bool):
                return str(v).lower()
            return str(v)

        topic = create_topic_names(1)[0]

        self.logger.debug("Dump the schema with metadata properties")
        metadata_properties = {
            "string": "string",
            "zero": 0,
            "one": 1,
            "neg": -1,
            "float": 3.14,
            "bool": False,
        }
        expected_properties = {
            k: as_java_str(v) for k, v in metadata_properties.items()
        }

        schema = {
            SchemaType.AVRO: schema1_def,
            SchemaType.PROTOBUF: simple_proto_def,
            SchemaType.JSON: r'{"type": "number"}',
        }[schema_type]

        schema_data = json.dumps(
            {
                "schema": schema,
                "schemaType": schema_type.name,
                "metadata": {"properties": metadata_properties},
            }
        )

        schema_data_no_meta = json.dumps(
            {
                "schema": schema,
                "schemaType": schema_type.name,
            }
        )

        schema_data_null_meta = json.dumps(
            {
                "schema": schema,
                "schemaType": schema_type.name,
                "metadata": None,
            }
        )

        self.logger.debug("Posting schema as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_data
        )
        self.logger.debug(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        schema_id = result_raw.json()["id"]
        version = result_raw.json()["version"]

        self.logger.debug("Reposting should return same id and properties")
        for payload in [schema_data, schema_data_no_meta, schema_data_null_meta]:
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=f"{topic}-key", data=payload
            )
            self.logger.debug(result_raw.content)
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json()["id"] == schema_id
            assert result_raw.json()["version"] == version, (
                f"Expected version: {version}, got: {result_raw.json()['version']}"
            )
            assert result_raw.json()["metadata"]["properties"] == expected_properties, (
                f"Expected: {expected_properties}, got: {result_raw.json()['metadata']['properties']}"
            )

        self.logger.debug(
            "Retrieving schema by schema should return same id and properties"
        )
        for payload in [schema_data, schema_data_no_meta, schema_data_null_meta]:
            result_raw = self.sr_client.post_subjects_subject(
                subject=f"{topic}-key", data=payload
            )
            self.logger.debug(result_raw.content)
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json()["id"] == schema_id, (
                f"Expected id: {schema_id}, got: {result_raw.json()['id']}"
            )
            assert result_raw.json()["version"] == version, (
                f"Expected version: {version}, got: {result_raw.json()['version']}"
            )
            assert result_raw.json()["metadata"]["properties"] == expected_properties, (
                f"Expected: {expected_properties}, got: {result_raw.json()['metadata']['properties']}"
            )

        self.logger.debug("Retrieving schema by subject,version")
        result_raw = self.sr_client.get_subjects_subject_versions_version(
            subject=f"{topic}-key", version=version
        )
        self.logger.debug(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == schema_id, (
            f"Expected id: {schema_id}, got: {result_raw.json()['id']}"
        )
        assert result_raw.json()["version"] == version, (
            f"Expected version: {version}, got: {result_raw.json()['version']}"
        )

        assert result_raw.json()["metadata"]["properties"] == expected_properties, (
            f"Expected: {expected_properties}, got: {result_raw.json()['metadata']['properties']}"
        )

        self.logger.debug("Retrieving schema by id")
        result_raw = self.sr_client.get_schemas_ids_id(id=schema_id)
        self.logger.debug(result_raw.content)
        assert result_raw.status_code == requests.codes.ok

        assert result_raw.json()["metadata"]["properties"] == expected_properties, (
            f"Expected: {expected_properties}, got: {result_raw.json()['metadata']['properties']}"
        )

        metadata_properties.update({"new_prop": "new_val"})
        expected_properties.update({"new_prop": "new_val"})

        schema_data_v2 = json.dumps(
            {
                "schema": schema,
                "schemaType": schema_type.name,
                "metadata": {"properties": metadata_properties},
            }
        )

        self.logger.debug("Posting schema with different properties is compatible")
        result_raw = self.sr_client.post_compatibility_subject_version(
            subject=f"{topic}-key", version=version, data=schema_data_v2
        )
        self.logger.debug(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] is True, (
            f"Expected is_compatible: True, got: {result_raw.content}"
        )

        self.logger.debug(
            "Posting schema with different properties creates a new version"
        )
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_data_v2
        )
        self.logger.debug(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == schema_id + 1, (
            f"Expected id: {schema_id + 1}, got: {result_raw.json()['id']}"
        )
        assert result_raw.json()["version"] == version + 1, (
            f"Expected version: {version + 1}, got: {result_raw.json()['version']}"
        )
        assert result_raw.json()["metadata"]["properties"] == expected_properties, (
            f"Expected: {expected_properties}, got: {result_raw.json()['metadata']['properties']}"
        )

    @cluster(num_nodes=3)
    def test_post_subjects_subject(self):
        """
        Verify posting a schema
        """

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"

        self.logger.info("Posting against non-existant subject should be 40401")
        result_raw = self.sr_client.post_subjects_subject(
            subject=subject, data=json.dumps({"schema": schema1_def})
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == f"Subject '{subject}' not found."

        self.logger.info(
            "Posting invalid schema to non-existant subject should be 40401"
        )
        result_raw = self.sr_client.post_subjects_subject(
            subject=subject, data=json.dumps({"schema": invalid_avro})
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == f"Subject '{subject}' not found."

        self.logger.info("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject, data=json.dumps({"schema": schema1_def})
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.info("Posting invalid schema to existing subject should be 500")
        result_raw = self.sr_client.post_subjects_subject(
            subject=subject, data=json.dumps({"schema": invalid_avro})
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.internal_server_error
        result = result_raw.json()
        assert result["error_code"] == 500
        assert (
            result["message"]
            == f"Error while looking up schema under subject {subject}"
        )

        self.logger.info("Posting existing schema should be success")
        result_raw = self.sr_client.post_subjects_subject(
            subject=subject, data=json.dumps({"schema": schema1_def})
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == subject
        assert result["id"] == 1
        assert result["version"] == 1
        assert result["schema"]

        self.logger.info("Posting new schema should be 40403")
        result_raw = self.sr_client.post_subjects_subject(
            subject=subject, data=json.dumps({"schema": schema3_def})
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40403
        assert result["message"] == "Schema not found"

        self.logger.info("Soft deleting the schema")
        result_raw = self.sr_client.delete_subject_version(
            subject=subject, version=1, permanent=False
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.info("Posting deleted existing schema should be fail (no subject)")
        result_raw = self.sr_client.post_subjects_subject(
            subject=subject, data=json.dumps({"schema": schema1_def})
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40401
        assert result["message"] == f"Subject '{subject}' not found."

        self.logger.info("Posting deleted existing schema should be success")
        result_raw = self.sr_client.post_subjects_subject(
            subject=subject,
            data=json.dumps(
                {"schema": schema1_def},
            ),
            deleted=True,
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == subject
        assert result["id"] == 1
        assert result["version"] == 1
        assert result["schema"]

        self.logger.info("Posting compatible schema should be success")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject, data=json.dumps({"schema": schema2_def})
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.info("Posting deleted existing schema should be fail (no schema)")
        result_raw = self.sr_client.post_subjects_subject(
            subject=subject, data=json.dumps({"schema": schema1_def})
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.not_found
        result = result_raw.json()
        assert result["error_code"] == 40403
        assert result["message"] == "Schema not found"

    @cluster(num_nodes=3)
    def test_config(self):
        """
        Smoketest config endpoints
        """
        self.logger.debug("Get initial global config")
        result_raw = self.sr_client.get_config()
        assert result_raw.json()["compatibilityLevel"] == "BACKWARD"

        self.logger.debug("Set global config")
        result_raw = self.sr_client.set_config(
            data=json.dumps({"compatibility": "FULL"})
        )
        assert result_raw.json()["compatibility"] == "FULL"

        # Check out set write shows up in a read
        result_raw = self.sr_client.get_config()
        self.logger.debug(f"response {result_raw.status_code} {result_raw.text}")
        assert result_raw.json()["compatibilityLevel"] == "FULL"

        self.logger.debug("Get invalid subject config")
        result_raw = self.sr_client.get_config_subject(subject="invalid_subject")
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40408
        assert (
            result_raw.json()["message"]
            == "Subject 'invalid_subject' does not have subject-level compatibility configured"
        )

        schema_1_data = json.dumps({"schema": schema1_def})

        topic = create_topic_names(1)[0]

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )

        self.logger.debug("Get subject config - should fail")
        result_raw = self.sr_client.get_config_subject(subject=f"{topic}-key")
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40408
        assert (
            result_raw.json()["message"]
            == f"Subject '{topic}-key' does not have subject-level compatibility configured"
        )

        self.logger.debug("Get subject config - fallback to global")
        result_raw = self.sr_client.get_config_subject(
            subject=f"{topic}-key", fallback=True
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["compatibilityLevel"] == "FULL"

        self.logger.debug("Set subject config")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "BACKWARD_TRANSITIVE"}),
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["compatibility"] == "BACKWARD_TRANSITIVE"

        self.logger.debug("Get subject config - should be overriden")
        result_raw = self.sr_client.get_config_subject(subject=f"{topic}-key")
        assert result_raw.json()["compatibilityLevel"] == "BACKWARD_TRANSITIVE"

        prev_compat = result_raw.json()["compatibilityLevel"]
        global_config = self.sr_client.get_config().json()

        result_raw = self.sr_client.delete_config_subject(subject=f"{topic}-key")
        assert result_raw.json()["compatibilityLevel"] == prev_compat, (
            f"{json.dumps(result_raw.json(), indent=1)}"
        )

        self.logger.debug("Second DELETE should return 40401")
        result_raw = self.sr_client.delete_config_subject(subject=f"{topic}-key")
        assert result_raw.status_code == requests.codes.not_found, (
            result_raw.status_code
        )
        assert result_raw.json()["error_code"] == 40401, (
            f"Wrong err code: {result_raw.json()}"
        )
        assert result_raw.json()["message"] == f"Subject '{topic}-key' not found.", (
            f"{json.dumps(result_raw.json(), indent=1)}"
        )

        self.logger.debug(
            "GET config/{subject} should indicate missing subject-level config"
        )
        result_raw = self.sr_client.get_config_subject(subject=f"{topic}-key")
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40408
        assert (
            result_raw.json()["message"]
            == f"Subject '{topic}-key' does not have subject-level compatibility configured"
        )

        result_raw = self.sr_client.get_config_subject(
            subject=f"{topic}-key", fallback=True
        )
        assert (
            result_raw.json()["compatibilityLevel"]
            == global_config["compatibilityLevel"]
        )

        self.logger.debug("Subject compatibility should reflect the new global config")
        global_config = self.sr_client.set_config(
            data=json.dumps({"compatibility": "NONE"})
        )
        assert global_config.json()["compatibility"] == "NONE"

        result_raw = self.sr_client.get_config_subject(
            subject=f"{topic}-key", fallback=True
        )
        assert (
            result_raw.json()["compatibilityLevel"]
            == global_config.json()["compatibility"]
        )

        self.logger.debug("DELETE on non-existant subject should 404")
        result_raw = self.sr_client.delete_config_subject(subject="foo-key")
        assert result_raw.status_code == requests.codes.not_found, (
            result_raw.status_code
        )
        assert result_raw.json()["error_code"] == 40401, (
            f"Wrong err code: {result_raw.json()}"
        )
        assert result_raw.json()["message"] == "Subject 'foo-key' not found.", (
            f"{json.dumps(result_raw.json(), indent=1)}"
        )

    @cluster(num_nodes=3)
    @parametrize(dataset_type=SchemaType.AVRO)
    @parametrize(dataset_type=SchemaType.JSON)
    def test_normalize(self, dataset_type: SchemaType):
        dataset = get_normalize_dataset(dataset_type)
        self.logger.debug(f"testing with {dataset=}")

        topics = create_topic_names(2)[0]
        canonical_topic = topics[0]
        normalize_topic = topics[1]

        base_schema = json.dumps(
            {"schema": dataset.schema_base, "schemaType": str(dataset.type)}
        )

        self.logger.debug("Register a schema against a subject - not normalized")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{canonical_topic}-key", data=base_schema, normalize=False
        )
        self.logger.debug(result_raw)
        self.logger.debug(result_raw.content)
        assert result_raw.status_code == requests.codes.ok, (
            f"expected {requests.codes.ok} but got {result_raw.status_code}"
        )
        v1_id = result_raw.json()["id"]

        self.logger.debug("Checking that the returned schema is canonical")
        result_raw = self.sr_client.get_schemas_ids_id(id=v1_id)
        self.logger.debug(result_raw)
        self.logger.debug(result_raw.content)
        assert result_raw.status_code == requests.codes.ok, (
            f"expected {requests.codes.ok} but got {result_raw.status_code}"
        )
        assert result_raw.json()["schema"] == dataset.schema_canonical, (
            f"expected:\n{dataset.schema_canonical}\ngot:\n{result_raw.json()['schema']}"
        )

        self.logger.debug("Register a schema against a subject - normalized")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{normalize_topic}-key", data=base_schema, normalize=True
        )
        self.logger.debug(result_raw)
        self.logger.debug(result_raw.content)
        assert result_raw.status_code == requests.codes.ok, (
            f"expected {requests.codes.ok} but got {result_raw.status_code}"
        )
        v1_id = result_raw.json()["id"]

        self.logger.debug("Checking that the returned schema is normalized")
        result_raw = self.sr_client.get_schemas_ids_id(id=v1_id)
        self.logger.debug(result_raw)
        self.logger.debug(result_raw.content)
        assert result_raw.status_code == requests.codes.ok, (
            f"expected {requests.codes.ok} but got {result_raw.status_code}"
        )
        assert result_raw.json()["schema"] == dataset.schema_normalized, (
            f"expected:\n{dataset.schema_normalized}\ngot:\n{result_raw.json()['schema']}"
        )

    @cluster(num_nodes=3)
    @parametrize(dataset_type=SchemaType.AVRO)
    @parametrize(dataset_type=SchemaType.JSON)
    def test_post_compatibility_subject_version(self, dataset_type: SchemaType):
        """
        Verify compatibility
        """
        dataset = get_compat_dataset(dataset_type)
        self.logger.debug(f"testing with {dataset=}")

        topic = create_topic_names(1)[0]

        self.logger.debug("Register a schema against a subject")
        schema_1_data = json.dumps(
            {"schema": dataset.schema_base, "schemaType": str(dataset.type)}
        )
        schema_2_data = json.dumps(
            {
                "schema": dataset.schema_backward_compatible,
                "schemaType": str(dataset.type),
            }
        )
        schema_3_data = json.dumps(
            {
                "schema": dataset.schema_not_backward_compatible,
                "schemaType": str(dataset.type),
            }
        )

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        v1_id = result_raw.json()["id"]

        self.logger.debug("Set subject config - NONE")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key", data=json.dumps({"compatibility": "NONE"})
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Check compatibility none, no default")
        result_raw = self.sr_client.post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=schema_2_data
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == True
        assert result_raw.json().get("messages", None) is None

        self.logger.debug("Set subject config - BACKWARD")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key", data=json.dumps({"compatibility": "BACKWARD"})
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Check compatibility backward, with default")
        result_raw = self.sr_client.post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=schema_2_data
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == True
        assert result_raw.json().get("messages", None) is None

        self.logger.debug("Check compatibility backward, no default, verbose")
        result_raw = self.sr_client.post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=schema_3_data, verbose=True
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == False

        self.logger.debug("Check compatibility backward, no default, not verbose")
        result_raw = self.sr_client.post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=schema_3_data, verbose=False
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == False
        assert result_raw.json().get("messages", None) is None, (
            f"Expected no messages, got {result_raw.json()['messages']}"
        )

        self.logger.debug("Posting incompatible schema 3 as a subject key")

        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_3_data
        )
        assert result_raw.status_code == requests.codes.conflict
        assert result_raw.json()["error_code"] == 409

        self.logger.debug("Posting compatible schema 2 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_2_data
        )
        assert result_raw.status_code == requests.codes.ok
        v2_id = result_raw.json()["id"]
        assert v1_id != v2_id

        self.logger.debug("Posting schema 1 as a subject key again")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == v1_id

        self.logger.debug("Soft delete schema 1")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=1
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 1 again, expect incompatible")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )
        assert result_raw.status_code == requests.codes.conflict

        self.logger.debug("Set subject config - NONE")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key", data=json.dumps({"compatibility": "NONE"})
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 1 again, expect same id")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == v1_id

    @cluster(num_nodes=3)
    def test_post_compatibility_subject_version_transitive_order(self):
        """
        Verify the compatibility message shows the latest failing schema
        """

        topic = create_topic_names(1)[0]

        schema_1_data = json.dumps({"schema": schema1_def})
        schema_2_data = json.dumps({"schema": schema2_def})
        schema_3_data = json.dumps({"schema": schema3_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )
        self.logger.debug(f"{result_raw=}")
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set subject config - BACKWARD_TRANSITIVE")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "BACKWARD_TRANSITIVE"}),
        )
        self.logger.debug(f"{result_raw=}")
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 2 (compatible with schema 1)")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_2_data
        )
        self.logger.debug(result_raw, result_raw.json())
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug(
            "Check compatibility schema 3 (incompatible with both schema 1 and 2) with verbose=True"
        )
        result_raw = self.sr_client.post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=schema_3_data, verbose=True
        )
        self.logger.debug(result_raw, result_raw.json())
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == False

        messages = result_raw.json().get("messages", [])
        assert not any(schema1_def in m for m in messages), (
            "Expected schema 3 to be compared against schema 2 only (not schema 1)"
        )
        assert any(schema2_def in m for m in messages), (
            "Expected schema 3 to be compared against schema 2 only (not schema 1)"
        )

    @cluster(num_nodes=3)
    @parametrize(schemas=["avro", "avro_incompat", "AVRO"])
    @parametrize(schemas=["proto3", "proto3_incompat", "PROTOBUF"])
    @parametrize(schemas=["proto2", "proto2_incompat", "PROTOBUF"])
    @parametrize(schemas=["json", "json_incompat", "JSON"])
    def test_compatibility_messages(self, schemas: list[str]):
        """
        Verify compatibility messages
        """

        topic = create_topic_names(1)[0]

        self.logger.debug("Register a schema against a subject")
        schema_data = json.dumps(
            {
                "schema": validation_schemas[schemas[0]],
                "schemaType": schemas[2],
            }
        )
        incompatible_data = json.dumps(
            {
                "schema": validation_schemas[schemas[1]],
                "schemaType": schemas[2],
            }
        )

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.debug("Posting schema as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        result_raw.json()["id"]

        self.logger.debug("Set subject config - BACKWARD")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key", data=json.dumps({"compatibility": "BACKWARD"})
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Check compatibility full")
        result_raw = self.sr_client.post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=incompatible_data, verbose=True
        )

        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == False
        msgs = result_raw.json()["messages"]
        for message in ["oldSchemaVersion", "oldSchema", "compatibility"]:
            assert any(message in m for m in msgs), (
                f"Expected to find an instance of '{message}', got {msgs}"
            )

        self.logger.debug(
            "Check post incompatible schema error message (expect verbose messages)"
        )
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=incompatible_data
        )

        assert result_raw.status_code == 409
        msg = result_raw.json()["message"]
        for message in ["oldSchemaVersion", "oldSchema", "compatibility"]:
            assert message in msg, (
                f"Expected to find an instance of '{message}', got {msgs}"
            )

    @cluster(num_nodes=3)
    def test_delete_subject(self):
        """
        Verify delete subject
        """

        topic = create_topic_names(1)[0]

        self.logger.debug("Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})
        schema_2_data = json.dumps({"schema": schema2_def})
        schema_3_data = json.dumps({"schema": schema3_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set subject config - NONE")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key", data=json.dumps({"compatibility": "NONE"})
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 2 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_2_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 3 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_3_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        # Check that permanent delete is refused before soft delete
        self.logger.debug("Prematurely permanently delete subject")
        result_raw = self.sr_client.delete_subject(
            subject=f"{topic}-key", permanent=True
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.not_found

        self.logger.debug("delete version 2")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=2
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Soft delete subject - expect 1,3")
        result_raw = self.sr_client.delete_subject(subject=f"{topic}-key")
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1, 3]

        self.logger.debug("Get versions")
        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-key"
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.not_found

        self.logger.debug("Get versions - include deleted")
        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-key", deleted=True
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1, 2, 3]

        self.logger.debug("Permanently delete subject")
        result_raw = self.sr_client.delete_subject(
            subject=f"{topic}-key", permanent=True
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1, 2, 3]

    @cluster(num_nodes=3)
    def test_delete_subject_version(self):
        """
        Verify delete subject version
        """

        topic = create_topic_names(1)[0]

        self.logger.debug("Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})
        schema_2_data = json.dumps({"schema": schema2_def})
        schema_3_data = json.dumps({"schema": schema3_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set subject config - NONE")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key", data=json.dumps({"compatibility": "NONE"})
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 2 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_2_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Posting schema 3 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_3_data
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Permanently delete version 2")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=2, permanent=True
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40407

        self.logger.debug("Soft delete version 2")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key",
            version=2,
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Soft delete version 2 - second time")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key",
            version=2,
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40406

        self.logger.debug("Get versions")
        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-key"
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1, 3]

        self.logger.debug("Get versions - include deleted")
        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-key", deleted=True
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1, 2, 3]

        self.logger.debug("Permanently delete version 2")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=2, permanent=True
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Permanently delete version 2 - second time")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=2, permanent=True
        )
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

        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject, data=schema_1_data
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject, data=schema_2_data
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 2

        # A 'latest' hard deletion will always fail because it tries
        # to delete the latest non-soft-deleted version
        r = self.sr_client.delete_subject_version(
            subject=subject, version="latest", permanent=True
        )
        assert r.status_code == requests.codes.not_found
        assert r.json()["error_code"] == 40407

        # Latest soft deletions are okay
        r = self.sr_client.delete_subject_version(
            subject=subject, version="latest", permanent=False
        )
        assert r.status_code == requests.codes.ok

        # A latest hard deletion still fails, because the 'latest' is
        # version 1
        r = self.sr_client.delete_subject_version(
            subject=subject, version="latest", permanent=True
        )
        assert r.status_code == requests.codes.not_found
        assert r.json()["error_code"] == 40407

        # Latest soft deletions are okay
        r = self.sr_client.delete_subject_version(
            subject=subject, version="latest", permanent=False
        )
        assert r.status_code == requests.codes.ok

        # Subject should still be visible with deleted=true
        r = self.sr_client.get_subjects(deleted=True)
        assert r.status_code == requests.codes.ok
        assert r.json() == [subject]

        # Subject with all versions deleted should be invisible to normal listing
        r = self.sr_client.get_subjects()
        assert r.status_code == requests.codes.ok
        assert r.json() == []

        # Hard-deleting by specific version number & having already soft deleted it
        r = self.sr_client.delete_subject_version(
            subject=subject, version="2", permanent=True
        )
        assert r.status_code == requests.codes.ok

        # Hard-deleting by specific version number & having already soft deleted it
        r = self.sr_client.delete_subject_version(
            subject=subject, version="1", permanent=True
        )
        assert r.status_code == requests.codes.ok

        # Hard deleting all versions is equivalent to hard deleting the subject,
        # so a subsequent attempt to delete latest version on subject
        # gives a subject_not_found error
        r = self.sr_client.delete_subject_version(
            subject=subject, version="latest", permanent=True
        )
        assert r.status_code == requests.codes.not_found
        assert r.json()["error_code"] == 40401

        # Subject is now truly gone, not even visible with deleted=true
        r = self.sr_client.get_subjects(deleted=True)
        assert r.status_code == requests.codes.ok
        assert r.json() == []

    @cluster(num_nodes=4)
    def test_concurrent_writes(self):
        # Warm up the servers (schema_registry doesn't create topic etc before first access)
        for node in self.redpanda.nodes:
            r = self.sr_client.request(
                "GET", "subjects", hostname=node.account.hostname
            )
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
                    node, "schema_registry_test_helper.py"
                )
                ssh_output = node.account.ssh_capture(
                    f"{python} {self.script_path} {' '.join(node_names)}"
                )
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
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject="imported",
            data=json.dumps({"schema": imported_proto_def, "schemaType": "PROTOBUF"}),
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.unprocessable_entity

        self.logger.info("Posting simple as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject="simple",
            data=json.dumps({"schema": simple_proto_def, "schemaType": "PROTOBUF"}),
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.info("Posting imported as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject="imported",
            data=json.dumps(
                {
                    "schema": imported_proto_def,
                    "schemaType": "PROTOBUF",
                    "references": [
                        {"name": "simple", "subject": "simple", "version": 1}
                    ],
                }
            ),
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 2

        result_raw = self.sr_client.request(
            "GET", "subjects/simple/versions/1/schema", headers=HTTP_GET_HEADERS
        )
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.text.strip() == simple_proto_def.strip()

        result_raw = self.sr_client.request(
            "GET", "schemas/ids/1", headers=HTTP_GET_HEADERS
        )
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["schemaType"] == "PROTOBUF"
        assert result["schema"].strip() == simple_proto_def.strip()

        result_raw = self.sr_client.get_subjects_subject_versions_version_referenced_by(
            "simple", 1
        )
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [2]

        # invalid subject should error with 40401
        result_raw = self.sr_client.get_subjects_subject_versions_version_referenced_by(
            "invalid_subject", 1
        )
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40401

        # invalid version should error with 40402
        result_raw = self.sr_client.get_subjects_subject_versions_version_referenced_by(
            "simple", 2
        )
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40402

    @cluster(num_nodes=3)
    def test_json(self):
        """
        Verify basic json functionality
        """

        self.logger.info("Posting number as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject="simple",
            data=json.dumps({"schema": json_number_schema_def, "schemaType": "JSON"}),
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        result_raw = self.sr_client.request(
            "GET", "subjects/simple/versions/1/schema", headers=HTTP_GET_HEADERS
        )
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.text.strip() == json_number_schema_def.strip()

        result_raw = self.sr_client.request(
            "GET", "schemas/ids/1", headers=HTTP_GET_HEADERS
        )
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["schemaType"] == "JSON"
        assert result["schema"].strip() == json_number_schema_def.strip()

    @cluster(num_nodes=3)
    def test_unsanitized_import(self):
        """
        Verify sanitization during startup
        """
        # Test setup: Simulate import of a schema by writting directly into the _schemas topic.
        # This schema is neither sanitized nor normalized
        self._push_to_schemas_topic([imported_schema])

        schema_def = imported_schema["schema"]
        # Normalization:off - /subjects/{subject}
        result_raw = self.sr_client.post_subjects_subject(
            subject="imported",
            data=json.dumps({"schema": schema_def, "schemaType": "PROTOBUF"}),
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Expected {requests.codes.ok} but got {result_raw.status_code}, "
            f"for request 'POST subjects/imported'"
        )
        result = result_raw.json()["schema"].strip()
        expected_result = imported_schema["sanitized"].strip()
        assert result == expected_result, (
            f"Expected:\n{expected_result}\nGot:\n{result}\n"
            "for request 'POST subjects/imported'"
        )

        # Normalization:on - /subjects/{subject}
        # This should fail to find it, as the schema was not normalized when stored.
        result_raw = self.sr_client.post_subjects_subject(
            subject="imported",
            data=json.dumps({"schema": schema_def, "schemaType": "PROTOBUF"}),
            normalize=True,
        )
        assert result_raw.status_code == 404, (
            f"Expected 404 but got {result_raw.status_code}, "
            f"for request 'POST subjects/imported?normalize=true'"
        )

        # Normalization:off - /subjects/{subject}/versions
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject="imported",
            data=json.dumps({"schema": schema_def, "schemaType": "PROTOBUF"}),
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Expected {requests.codes.ok} but got {result_raw.status_code}, "
            f"for request 'POST subjects/imported/versions'"
        )
        result_id = result_raw.json()["id"]
        assert result_id == 1, (
            f"Expected id 1 but got {result_id}, "
            "for request 'POST subjects/imported/versions'"
        )

        # Normalization:on - /subjects/{subject}/versions
        # This should fail to find it and create a new one, as the schema was
        # not normalized when stored.
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject="imported",
            data=json.dumps({"schema": schema_def, "schemaType": "PROTOBUF"}),
            normalize=True,
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Expected {requests.codes.ok} but got {result_raw.status_code}, "
            "for request 'POST subjects/imported/versions?normalize=true'"
        )
        result_id = result_raw.json()["id"]
        assert result_id == 2, (
            f"Expected id 2 but got {result_id}, "
            "for request 'POST subjects/imported/versions?normalize=true'"
        )

    @cluster(num_nodes=4)
    @parametrize(protocol=SchemaType.AVRO, client_type=SerdeClientType.Python)
    @parametrize(protocol=SchemaType.AVRO, client_type=SerdeClientType.Java)
    @parametrize(protocol=SchemaType.AVRO, client_type=SerdeClientType.Golang)
    @parametrize(
        protocol=SchemaType.PROTOBUF,
        client_type=SerdeClientType.Python,
        skip_known_types=False,
    )
    @parametrize(
        protocol=SchemaType.PROTOBUF,
        client_type=SerdeClientType.Python,
        skip_known_types=True,
    )
    @parametrize(
        protocol=SchemaType.PROTOBUF,
        client_type=SerdeClientType.Java,
        skip_known_types=False,
    )
    @parametrize(
        protocol=SchemaType.PROTOBUF,
        client_type=SerdeClientType.Java,
        skip_known_types=True,
    )
    @parametrize(
        protocol=SchemaType.PROTOBUF,
        client_type=SerdeClientType.Java,
        use_latest_version=True,
    )
    @parametrize(protocol=SchemaType.PROTOBUF, client_type=SerdeClientType.Golang)
    def test_serde_client(
        self,
        protocol: SchemaType,
        client_type: SerdeClientType,
        skip_known_types: Optional[bool] = None,
        use_latest_version: Optional[bool] = None,
    ):
        """
        Verify basic operation of Schema registry across a range of schema types and serde
        client types
        """

        topic = f"serde-topic-{protocol.name}-{client_type.name}"
        self._create_topic(topic=topic)
        schema_reg = self.redpanda.schema_reg().split(",", 1)[0]
        self.logger.info(
            f"Connecting to redpanda: {self.redpanda.brokers()} schema_Reg: {schema_reg}"
        )
        client = self._get_serde_client(
            protocol, client_type, topic, 2, skip_known_types, use_latest_version
        )
        self.logger.debug("Starting client")
        client.start()
        self.logger.debug("Waiting on client")
        client.wait()
        self.logger.debug("Client completed")

        schema = self.sr_client.get_subjects_subject_versions_version(
            f"{topic}-value", "latest"
        )
        self.logger.info(schema.json())
        assert schema.json()["schemaType"] == protocol.name
        assert schema.json()["deleted"] is False

    @cluster(num_nodes=4)
    @matrix(
        protocol=[SchemaType.AVRO, SchemaType.PROTOBUF, SchemaType.JSON],
        client_type=[SerdeClientType.Python],
        validate_schema_id=[True],
        subject_name_strategy=list(TopicSpec.SubjectNameStrategyCompat),
        payload_class=[
            "com.redpanda.Payload",
            "com.redpanda.A.B.C.D.NestedPayload",
            "com.redpanda.CompressiblePayload",
        ],
        compression_type=[
            TopicSpec.CompressionTypes.NONE,
            TopicSpec.CompressionTypes.ZSTD,
        ],
    )
    def test_schema_id_validation(
        self,
        protocol: SchemaType,
        client_type: SerdeClientType,
        skip_known_types: Optional[bool] = None,
        validate_schema_id: Optional[bool] = None,
        subject_name_strategy: Optional[str] = None,
        payload_class: str = "com.redpanda.Payload",
        compression_type: Optional[TopicSpec.CompressionTypes] = None,
    ):
        self.redpanda.set_cluster_config(
            {"enable_schema_id_validation": SchemaIdValidationMode.COMPAT}
        )

        def get_next_strategy(subject_name_strategy):
            all_strategies = list(TopicSpec.SubjectNameStrategyCompat)
            index = all_strategies.index(subject_name_strategy)
            return all_strategies[(index + 1) % len(all_strategies)]

        # Create a topic with incorrect strategy
        topic = f"serde-topic-{protocol.name}-{client_type.name}"

        def check_subject():
            expected = get_subject_name(
                subject_name_strategy, topic, MessageField.VALUE, payload_class
            )
            result_raw = self.sr_client.get_subjects()
            assert result_raw.status_code == requests.codes.ok
            res_subjects = result_raw.json()
            self.logger.debug(f"SUBJECTS: {res_subjects}, expected: {expected}")

            return expected in res_subjects

        def bool_alpha(b: bool) -> str:
            return f"{b}".lower()

        self._create_topic(
            topic=topic,
            config={
                TopicSpec.PROPERTY_RECORD_VALUE_SCHEMA_ID_VALIDATION_COMPAT: bool_alpha(
                    validate_schema_id
                ),
                TopicSpec.PROPERTY_RECORD_VALUE_SUBJECT_NAME_STRATEGY_COMPAT: get_next_strategy(
                    subject_name_strategy
                ),
                TopicSpec.PROPERTY_COMPRESSSION: TopicSpec.COMPRESSION_PRODUCER,
            },
        )
        schema_reg = self.redpanda.schema_reg().split(",", 1)[0]
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
            compression_type=compression_type,
        )

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
            set_key=TopicSpec.PROPERTY_RECORD_VALUE_SUBJECT_NAME_STRATEGY_COMPAT,
            set_value=subject_name_strategy,
        )

        self.logger.debug("Running client, expecting success")
        client.run()

        batches_decompressed = self.redpanda.metric_sum(
            metric_name="vectorized_kafka_schema_id_cache_batches_decompressed_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
        )

        if (
            compression_type != TopicSpec.CompressionTypes.NONE
            and payload_class == "com.redpanda.CompressiblePayload"
        ):
            assert batches_decompressed > 0
        if compression_type == TopicSpec.CompressionTypes.NONE:
            assert batches_decompressed == 0

        self.logger.debug("Client completed")

    @cluster(num_nodes=3)
    @parametrize(move_controller_leader=False)
    @parametrize(move_controller_leader=True)
    def test_restarts(self, move_controller_leader: bool):
        admin = Admin(self.redpanda)
        rpk = RpkTool(self.redpanda)

        def check_connection(hostname: str):
            result_raw = self.sr_client.get_subjects(hostname=hostname)
            self.logger.info(result_raw.status_code)
            self.logger.info(result_raw.json())
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json() == []

        def get_leader_epoch():
            def rpk_get_leader_epoch():
                partitions = rpk.describe_topic("_schemas")
                par_0 = next((p for p in partitions if p.id == 0), None)
                if not par_0:
                    return (False, (-1))
                return (True, par_0.leader_epoch)

            leader_epoch = wait_until_result(
                rpk_get_leader_epoch,
                timeout_sec=10,
                backoff_sec=1,
                err_msg="Could not get leader info",
            )
            return leader_epoch

        def restart_leader():
            leader = admin.get_partition_leader(
                namespace="kafka", topic="_schemas", partition=0
            )

            if move_controller_leader:
                admin.partition_transfer_leadership(
                    namespace="redpanda", topic="controller", partition=0
                )
            last_epoch = get_leader_epoch()
            self.logger.info(f"Restarting node: {leader}")
            self.redpanda.restart_nodes(self.redpanda.get_node(leader))

            def get_new_leader():
                new_epoch = get_leader_epoch()
                had_election = last_epoch < new_epoch
                return (had_election, new_epoch)

            new_epoch = wait_until_result(
                get_new_leader,
                timeout_sec=20,
                backoff_sec=1,
                err_msg="Leadership did not stabilize",
            )
            self.logger.info(f"Epoch: {new_epoch}")

        for i in range(20):
            self.logger.info(f"Iteration {i}")
            for n in self.redpanda.nodes:
                check_connection(n.account.hostname)
            restart_leader()

    @cluster(num_nodes=3)
    def test_move_leader(self):
        admin = Admin(self.redpanda)

        def check_connection(hostname: str):
            result_raw = self.sr_client.get_subjects(hostname=hostname)
            self.logger.info(result_raw.status_code)
            self.logger.info(result_raw.json())
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json() == []

        def get_leader():
            return admin.get_partition_leader(
                namespace="kafka", topic="_schemas", partition=0
            )

        def move_leader():
            leader = get_leader()
            self.logger.info(f"Transferring leadership from node: {leader}")
            admin.partition_transfer_leadership(
                namespace="kafka", topic="_schemas", partition=0
            )
            wait_until(
                lambda: get_leader() != leader,
                timeout_sec=10,
                backoff_sec=1,
                err_msg="Leadership did not stabilize",
            )

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
                json.dumps({"schema": schema3_def}),
            ]
            schema_ids = []

            for schema in schemas:
                self.logger.debug(f"Post schema {json.loads(schema)}")
                result_raw = self.sr_client.post_subjects_subject_versions(
                    subject=subject, data=schema
                )
                self.logger.debug(result_raw)
                assert result_raw.status_code == requests.codes.ok
                res = result_raw.json()
                self.logger.debug(res)
                assert "id" in res
                schema_ids.append(result_raw.json()["id"])

            return schemas, schema_ids

        # Given a list of subjects, check that they exist on the registry
        def check_subjects(subjects: list[str]):
            result_raw = self.sr_client.get_subjects()
            self.logger.debug(result_raw)
            assert result_raw.status_code == requests.codes.ok
            res_subjects = result_raw.json()
            self.logger.debug(res_subjects)
            assert type(res_subjects) is type([])
            res_subjects.sort()
            subjects.sort()
            assert res_subjects == subjects

        # Given the subject and list of versions, check that the version numbers match
        def check_subject_versions(subject: str, subject_versions: list[int]):
            result_raw = self.sr_client.get_subjects_subject_versions(subject=subject)
            self.logger.debug(result_raw)
            assert result_raw.status_code == requests.codes.ok
            res_versions = result_raw.json()
            self.logger.debug(res_versions)
            assert type(res_versions) is type([])
            assert res_versions == subject_versions

        # Given the subject, list of schemas, list of versions, and list of ids,
        # check the schema that maps to the id
        def check_each_schema(
            subject: str,
            schemas: list[str],
            schema_ids: list[int],
            subject_versions: list[int],
        ):
            for idx, version in zip(schema_ids, subject_versions):
                self.logger.debug(
                    f"Fetch schema version {version} on subject {subject}"
                )
                result_raw = self.sr_client.get_subjects_subject_versions_version(
                    subject=subject, version=version
                )
                self.logger.debug(result_raw)
                assert result_raw.status_code == requests.codes.ok
                res = result_raw.json()
                self.logger.debug(res)
                assert type(res) is type({})
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
                "subject_versions": [],
            }

        self.sr_client.set_config(data=json.dumps({"compatibility": "NONE"}))

        self.logger.debug("Register and check schemas before restart")
        for subject in subjects:
            schemas, schema_ids = register_schemas(subject)
            result_raw = self.sr_client.get_subjects_subject_versions(subject=subject)
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
        result_raw = admin.restart_service(rp_service="schema-registry")
        search_logs_with_timeout(self.redpanda, "Restarting the schema registry")
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Check schemas after restart")
        check_subjects(list(subjects.keys()))
        for subject in subjects:
            check_subject_versions(subject, subjects[subject]["schema_ids"])
            check_each_schema(
                subject,
                subjects[subject]["schemas"],
                subjects[subject]["schema_ids"],
                subjects[subject]["subject_versions"],
            )

    @cluster(num_nodes=3)
    def test_always_normalize_option(self):
        # Post a schema with and without normalization
        result_raw = self.sr_client.post_subjects_subject_versions(
            normalize=False,
            subject="test_subject",
            data=json.dumps(
                {"schema": imported_schema_proto_def, "schemaType": "PROTOBUF"}
            ),
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Expected {requests.codes.ok} but got {result_raw.status_code} during test setup"
        )
        result_id = result_raw.json()["id"]
        assert result_id == 1, f"Expected id 1 but got {result_id} during test setup"

        result_raw = self.sr_client.post_subjects_subject_versions(
            normalize=True,
            subject="test_subject",
            data=json.dumps(
                {"schema": imported_schema_proto_def, "schemaType": "PROTOBUF"}
            ),
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Expected {requests.codes.ok} but got {result_raw.status_code} during test setup"
        )
        result_id = result_raw.json()["id"]
        assert result_id == 2, f"Expected id 2 but got {result_id} during test setup"

        def test_schemas_ids_id(id, expected_schema):
            result_raw = self.sr_client.request(
                "GET", f"schemas/ids/{id}", headers=HTTP_GET_HEADERS
            )
            result = result_raw.json()["schema"].strip()
            assert result_raw.status_code == requests.codes.ok, (
                f"Expected {requests.codes.ok} but got {result_raw.status_code}, "
                f"with content {result_raw.content}"
            )
            assert result == expected_schema, (
                f"Expected:\n{expected_schema}\ngot:\n{result}"
            )

        def test_subjects_subject(schema, expected_version=None, norm=False):
            result_raw = self.sr_client.post_subjects_subject(
                subject="test_subject",
                data=json.dumps(
                    {
                        "schema": schema,
                        "schemaType": "PROTOBUF",
                    }
                ),
                normalize=norm,
            )
            if expected_version:
                assert result_raw.status_code == requests.codes.ok, (
                    f"Expected {requests.codes.ok} but got {result_raw.status_code}, "
                    f"with content {result_raw.content}"
                )
                result = result_raw.json()["version"]
                assert result == expected_version, (
                    f"Expected version {expected_version} but got {result}"
                )
            else:
                assert result_raw.status_code == 404, (
                    f"Expected 404 but got {result_raw.status_code}, "
                    f"with content {result_raw.content}"
                )

        sanitized = imported_schema_sanitized_proto_def.strip()
        normalized = imported_schema_normalized_proto_def.strip()

        test_schemas_ids_id(1, sanitized)
        test_schemas_ids_id(2, normalized)
        test_subjects_subject(sanitized, expected_version=1)
        test_subjects_subject(sanitized, expected_version=2, norm=True)
        test_subjects_subject(normalized, expected_version=2)
        test_subjects_subject(normalized, expected_version=2, norm=True)

        # Update always_normalize and re-run all tests
        self.redpanda.set_cluster_config({"schema_registry_always_normalize": True})

        # Verify that always_normalize doesn't affect lookup by id
        test_schemas_ids_id(1, sanitized)
        test_schemas_ids_id(2, normalized)
        # Verify that always_normalize always treats normalization set to on
        test_subjects_subject(sanitized, expected_version=2)
        test_subjects_subject(sanitized, expected_version=2, norm=True)
        # Sanity check on normalized input
        test_subjects_subject(normalized, expected_version=2)
        test_subjects_subject(normalized, expected_version=2, norm=True)

    @cluster(num_nodes=3)
    @matrix(stype=["json", "proto"])
    def test_missing_references(self, stype):
        """
        Missing references should return an error message with details of the missing reference
        """

        self.sr_client.set_config(data=json.dumps({"compatibility": "NONE"}))

        base_schema = not_dependent_schemas[stype]
        schema = dependent_schemas[stype]
        subject = schema["subject"]
        ref_subject = schema["references"][0]["subject"]
        ref_version = schema["references"][0]["version"]

        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject,
            data=json.dumps(
                {
                    "schema": base_schema["schema"],
                    "schemaType": base_schema["type"],
                }
            ),
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Expected {requests.codes.ok} but got {result_raw.status_code} during test setup. "
            f"Request content: {result_raw.content}. Processing {stype}."
        )

        for endpoint in [
            self.sr_client.post_subjects_subject_versions,
            self.sr_client.post_subjects_subject,
        ]:
            result_raw = endpoint(
                subject=subject,
                data=json.dumps(
                    {
                        "schema": schema["schema"],
                        "schemaType": schema["type"],
                        "references": schema["references"],
                    }
                ),
            )
            assert result_raw.status_code == requests.codes.unprocessable_entity, (
                f"Expected {requests.codes.unprocessable_entity} but got {result_raw.status_code}. "
                f"Request content: {result_raw.content}. Processing {type}."
            )
            assert (
                f'No schema reference found for subject "{ref_subject}" and version {ref_version}'
                in result_raw.json()["message"]
            ), (
                f"Expected result to contain missing references, but got {result_raw.json()}."
            )

    @cluster(num_nodes=3)
    def test_soft_deleted_references(self):
        """
        Soft-deleted references should be valid references and
        schemas that reference a soft-deleted ref should be accepted
        """

        # Test setup. Insert and soft-delete schemas to be referencedby

        for name in ["proto", "json", "avro"]:
            schema = base_schemas[name]
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=schema["subject"],
                data=json.dumps(
                    {"schema": schema["schema"], "schemaType": schema["type"]}
                ),
            )
            assert result_raw.status_code == requests.codes.ok, (
                f"Expected {requests.codes.ok} but got {result_raw.status_code} during test setup. "
                f"Request content: {result_raw.content}. Processing {name}."
            )

            result_raw = self.sr_client.delete_subject_version(
                subject=schema["subject"], version=1
            )
            assert result_raw.status_code == requests.codes.ok, (
                f"Expected {requests.codes.ok} but got {result_raw.status_code} during test setup. "
                f"Request content: {result_raw.content}. Processing {name}."
            )

        # Register schemas that reference the soft-deleted schemas
        for name in ["proto", "json", "avro"]:
            schema = dependent_schemas[name]
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=schema["subject"],
                data=json.dumps(
                    {
                        "schema": schema["schema"],
                        "schemaType": schema["type"],
                        "references": schema["references"],
                    }
                ),
            )
            assert result_raw.status_code == requests.codes.ok, (
                f"Expected {requests.codes.ok} but got {result_raw.status_code}. "
                f"Request content: {result_raw.content}. Processing {name}."
            )
            result_id = result_raw.json()["id"]
            assert result_id == schema["id"], (
                f"Expected id {schema['id']} but got {result_id}. "
                f"Request content: {result_raw.content}. Processing {name}."
            )

    @cluster(num_nodes=3)
    def test_format_keyword(self):
        """
        Test support for the format keyword
        """

        # Format values to test
        default_format = ""
        bad_value = "bad_value"
        resolved = "resolved"
        ignore_extensions = "ignore_extensions"
        serialized = "serialized"

        entries = {
            "proto_def": {
                "schema": schema_proto_def.strip(),
                "type": "PROTOBUF",
                "subject": "schema_proto",
                "version": 1,
                "id": 1,
                "failing_format": [ignore_extensions],
            },
            "proto_b64": {
                "schema": schema_proto_b64.strip(),
                "type": "PROTOBUF",
                "subject": "schema_proto",
                "version": 1,
                "id": 1,
                "failing_format": [ignore_extensions],
            },
            "avro_def": {
                "schema": schema_avro_def.strip(),
                "type": "AVRO",
                "subject": "schema_avro",
                "version": 1,
                "id": 2,
                "failing_format": [resolved],
            },
            "json_def": {
                "schema": json_number_schema_def.strip(),
                "type": "JSON",
                "subject": "schema_json",
                "version": 1,
                "id": 3,
            },
        }

        def insert_schema(schema_entry):
            schema = schema_entry["schema"]
            schema_type = schema_entry["type"]
            subject = schema_entry["subject"]
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=subject,
                data=json.dumps({"schema": schema, "schemaType": schema_type}),
            )
            assert result_raw.status_code == requests.codes.ok, (
                f"Expected {requests.codes.ok} but got {result_raw.status_code} during test setup. "
                f"Request content: {result_raw.content}. Posting {schema_type}."
            )

        insert_schema(entries["proto_def"])
        insert_schema(entries["avro_def"])
        insert_schema(entries["json_def"])

        def is_successful(schema_entry, format):
            if "failing_format" not in schema_entry:
                return True
            return format not in schema_entry["failing_format"]

        def test_runner(test_func):
            for entry in ["proto_def", "avro_def", "json_def"]:
                for format in [default_format, bad_value, resolved, ignore_extensions]:
                    successful = is_successful(entries[entry], format)
                    test_func(entries[entry], successful, format)

            for entry in ["proto_b64", "avro_def", "json_def"]:
                successful = is_successful(entries[entry], serialized)
                test_func(entries[entry], successful, serialized)

        def test_ids_id(schema_entry, successful, format=None):
            id = schema_entry["id"]
            result_raw = self.sr_client.get_schemas_ids_id(id, format)
            if successful:
                assert result_raw.status_code == requests.codes.ok, (
                    f"expected {requests.codes.ok} but got {result_raw.status_code} for id {id} and format '{format}'"
                )
                schema = schema_entry["schema"]
                result = result_raw.json()["schema"].strip()
                assert result == schema, (
                    f"expected:\n{schema}\ngot:\n{result}\nfor id {id} and format '{format}'"
                )
            else:
                assert result_raw.status_code == 501, (
                    f"expected {501} but got {result_raw.status_code} for id {id} and format '{format}'"
                )

        test_runner(test_ids_id)

        def test_subjects_subject(
            schema_entry, successful, format=None, expected_output=None
        ):
            subject = schema_entry["subject"]
            schema = schema_entry["schema"]
            schema_type = schema_entry["type"]
            expected_schema = expected_output if expected_output else schema
            result_raw = self.sr_client.post_subjects_subject(
                subject=subject,
                format=format,
                data=json.dumps({"schema": schema, "schemaType": schema_type}),
            )
            if successful:
                assert result_raw.status_code == requests.codes.ok, (
                    f"expected {requests.codes.ok} but got {result_raw.status_code} "
                    f"for subject {subject}, schema {schema} and format '{format}'"
                )
                result = result_raw.json()["schema"].strip()
                assert result == expected_schema.strip(), (
                    f"expected:\n{schema}\ngot:\n{result}\n"
                    f"for subject {subject}, schema {schema} and format '{format}'"
                )
            else:
                assert result_raw.status_code == 501, (
                    f"expected {501} but got {result_raw.status_code} "
                    f"for subject {subject}, schema {schema} and format '{format}'"
                )

        # Test that the schema is discoverable in serialzed mode
        test_subjects_subject(
            entries["proto_b64"],
            successful=True,
            expected_output=schema_proto_def.strip(),
        )

        test_runner(test_subjects_subject)

        def test_subjects_subject_versions_version(
            schema_entry, successful, format=None
        ):
            subject = schema_entry["subject"]
            version = schema_entry["version"]
            schema = schema_entry["schema"]
            result_raw = self.sr_client.get_subjects_subject_versions_version(
                subject=subject, version=version, format=format
            )

            if successful:
                assert result_raw.status_code == requests.codes.ok, (
                    f"expected {requests.codes.ok} but got {result_raw.status_code} "
                    f"for subject {subject}, schema {schema} and format '{format}'"
                )
                result = result_raw.json()["schema"].strip()
                assert result == schema.strip(), (
                    f"expected:\n{schema}\ngot:\n{result}\n"
                    f"for subject {subject}, schema {schema} and format '{format}'"
                )
            else:
                assert result_raw.status_code == 501, (
                    f"expected {501} but got {result_raw.status_code} "
                    f"for subject {subject}, schema {schema} and format '{format}'"
                )

        test_runner(test_subjects_subject_versions_version)

        def test_subjects_subject_versions_version_schema(
            schema_entry, successful, format=None
        ):
            subject = schema_entry["subject"]
            version = schema_entry["version"]
            schema = schema_entry["schema"]
            result_raw = self.sr_client.get_subjects_subject_versions_version_schema(
                subject=subject, version=version, format=format
            )
            schema = schema.strip()

            if successful:
                assert result_raw.status_code == requests.codes.ok, (
                    f"expected {requests.codes.ok} but got {result_raw.status_code} "
                    f"for subject {subject}, schema {schema} and format '{format}'"
                )
                result = result_raw.text.strip()

                assert result == schema.strip(), (
                    f"expected:\n{schema}\ngot:\n{result}\n"
                    f"for subject {subject}, schema {schema} and format '{format}'"
                )
            else:
                assert result_raw.status_code == 501, (
                    f"expected {501} but got {result_raw.status_code} "
                    f"for subject {subject}, schema {schema} and format '{format}'"
                )

        test_runner(test_subjects_subject_versions_version_schema)

    @cluster(num_nodes=1)
    def test_qualified_subjects_flag_off(self):
        """
        With enable_qualified_subjects off (default), the qualified syntax is not parsed, and all
        subjects are treated as if they are in the default context.
        """

        # Register a schema with qualified-looking subject name
        # Flag OFF: treated as literal subject name in default context
        # Flag ON: would create a separate .ctx context
        qualified_subject = ":.ctx:my-subject"
        result = self.sr_client.post_subjects_subject_versions(
            subject=qualified_subject, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Verify only the default context exists (no .ctx context was created)
        result = self.sr_client.get_contexts()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json(), ["."])


class SchemaRegistryModeNotMutableTest(SchemaRegistryEndpoints):
    """
    Test that mode cannot be mutated when mode_mutability=False.
    """

    def __init__(self, context, **kwargs):
        self.schema_registry_config = SchemaRegistryConfig()
        self.schema_registry_config.mode_mutability = False

        super(SchemaRegistryModeNotMutableTest, self).__init__(
            context, schema_registry_config=self.schema_registry_config, **kwargs
        )

    @cluster(num_nodes=3)
    def test_mode_immutable(self):
        subject = f"{create_topic_names(1)[0]}-key"

        result_raw = self.sr_client.get_mode()
        assert result_raw.status_code == 200
        assert result_raw.json()["mode"] == "READWRITE"

        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "INVALID"}))
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42204

        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "READONLY"}))
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42205
        assert result_raw.json()["message"] == "Mode changes are not allowed"

        # Check that setting it to the same value is still refused
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "READWRITE"}))
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42205
        assert result_raw.json()["message"] == "Mode changes are not allowed"

        result_raw = self.sr_client.set_mode_subject(
            subject=subject, data=json.dumps({"mode": "READONLY"})
        )
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42205
        assert result_raw.json()["message"] == "Mode changes are not allowed"

        result_raw = self.sr_client.delete_mode_subject(subject=subject)
        assert result_raw.status_code == 404
        assert result_raw.json()["error_code"] == 40401
        assert result_raw.json()["message"] == f"Subject '{subject}' not found."


class SchemaRegistryModeMutableTest(SchemaRegistryEndpoints):
    """
    Test schema registry mode against a redpanda cluster.
    """

    def __init__(self, context: TestContext, **kwargs: Any):
        self.schema_registry_config = SchemaRegistryConfig()
        self.schema_registry_config.mode_mutability = True
        super(SchemaRegistryModeMutableTest, self).__init__(
            context, schema_registry_config=self.schema_registry_config, **kwargs
        )

    @cluster(num_nodes=3)
    def test_mode(self):
        """
        Smoketest mode endpoints
        """
        subject = f"{create_topic_names(1)[0]}-key"
        not_subject = f"{create_topic_names(1)[0]}-key"

        self.logger.debug("Get initial global mode")
        result_raw = self.sr_client.get_mode()
        assert result_raw.status_code == 200
        assert result_raw.json()["mode"] == "READWRITE"

        self.logger.debug("Set invalid global mode")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "INVALID"}))
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42204

        self.logger.debug("Set global mode")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "READONLY"}))
        assert result_raw.status_code == 200
        assert result_raw.json()["mode"] == "READONLY"

        self.logger.debug("Get global mode")
        result_raw = self.sr_client.get_mode()
        assert result_raw.status_code == 200
        assert result_raw.json()["mode"] == "READONLY"

        self.logger.debug("Get mode for non-existant subject")
        result_raw = self.sr_client.get_mode_subject(subject=not_subject)
        assert result_raw.status_code == 404
        assert result_raw.json()["error_code"] == 40409

        self.logger.debug("Get mode for non-existant subject, with fallback")
        result_raw = self.sr_client.get_mode_subject(subject=not_subject, fallback=True)
        assert result_raw.status_code == 200
        assert result_raw.json()["mode"] == "READONLY"

        self.logger.debug("Set mode for non-existant subject (allowed)")
        result_raw = self.sr_client.set_mode_subject(
            subject=subject, data=json.dumps({"mode": "READWRITE"})
        )
        assert result_raw.status_code == 200
        assert result_raw.json()["mode"] == "READWRITE"

        self.logger.debug("Set invalid subject mode")
        result_raw = self.sr_client.set_mode_subject(
            subject="test-sub", data=json.dumps({"mode": "INVALID"})
        )
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42204

        self.logger.debug("Get mode for non-existant subject")
        result_raw = self.sr_client.get_mode_subject(subject=subject, fallback=False)
        assert result_raw.status_code == 200
        assert result_raw.json()["mode"] == "READWRITE"

        self.logger.debug("Delete mode for non-existant subject")
        result_raw = self.sr_client.delete_mode_subject(subject=subject)
        assert result_raw.status_code == 200
        assert result_raw.json()["mode"] == "READWRITE"

        self.logger.debug("Get mode for non-existant subject")
        result_raw = self.sr_client.get_mode_subject(subject=subject, fallback=False)
        assert result_raw.status_code == 404
        assert result_raw.json()["error_code"] == 40409

        self.logger.debug("Set global mode to READWRITE")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "READWRITE"}))
        assert result_raw.status_code == 200

        self.logger.debug("Add a schema")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject, data=json.dumps({"schema": schema1_def})
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set global mode to UNSUPPORTED")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "UNSUPPORTED"}))
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42204
        assert (
            result_raw.json()["message"]
            == "Invalid mode. Valid values are READWRITE, READONLY, IMPORT"
        )

        self.logger.debug("Set subject mode to UNSUPPORTED")
        result_raw = self.sr_client.set_mode_subject(
            subject="test-sub", data=json.dumps({"mode": "UNSUPPORTED"})
        )
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42204
        assert (
            result_raw.json()["message"]
            == "Invalid mode. Valid values are READWRITE, READONLY, IMPORT"
        )

    @cluster(num_nodes=3)
    def test_mode_readonly(self):
        """
        Test endpoints when in READONLY
        """
        ro_subject = f"ro-{create_topic_names(1)[0]}-key"
        rw_subject = f"rw-{create_topic_names(1)[0]}-key"

        schema1 = json.dumps({"schema": schema1_def})
        schema2 = json.dumps({"schema": schema2_def})

        self.logger.info("Posting schema 1 as ro_subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=ro_subject, data=json.dumps({"schema": schema1_def})
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set global mode to readonly")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "READONLY"}))
        assert result_raw.status_code == 200
        assert result_raw.json()["mode"] == "READONLY"

        self.logger.debug("Override mode for rw_subject")
        result_raw = self.sr_client.set_mode_subject(
            subject=rw_subject, data=json.dumps({"mode": "READWRITE"})
        )
        assert result_raw.status_code == 200
        assert result_raw.json()["mode"] == "READWRITE"

        self.logger.info("Posting schema 1 as rw_subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=rw_subject, data=schema1
        )
        assert result_raw.status_code == requests.codes.ok

        # mode
        result_raw = self.sr_client.get_mode()
        assert result_raw.status_code == 200

        for sub in [ro_subject, rw_subject]:
            result_raw = self.sr_client.get_mode_subject(subject=sub, fallback=True)
            assert result_raw.status_code == 200

        # config
        result_raw = self.sr_client.get_config()
        assert result_raw.status_code == 200

        for sub in [ro_subject, rw_subject]:
            result_raw = self.sr_client.get_config_subject(subject=sub, fallback=True)
            assert result_raw.status_code == 200

        # This is the default, check that setting it to the default/existing is failure, not quiet success
        compat_back = json.dumps({"compatibility": "BACKWARD"})
        result_raw = self.sr_client.set_config(data=compat_back)
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42205
        assert (
            result_raw.json()["message"]
            == "Subject null in context . is in read-only mode"
        )

        result_raw = self.sr_client.set_config_subject(
            subject=ro_subject, data=compat_back
        )
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42205
        assert (
            result_raw.json()["message"]
            == f"Subject {ro_subject} in context . is in read-only mode"
        )

        result_raw = self.sr_client.set_config_subject(
            subject=rw_subject, data=compat_back
        )
        assert result_raw.status_code == 200

        # The config doesn't exist, but the mode is checked first
        result_raw = self.sr_client.delete_config_subject(subject=ro_subject)
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42205
        assert (
            result_raw.json()["message"]
            == f"Subject {ro_subject} in context . is in read-only mode"
        )

        result_raw = self.sr_client.delete_config_subject(subject=rw_subject)
        assert result_raw.status_code == 200

        # subjects
        result_raw = self.sr_client.get_subjects()
        assert result_raw.status_code == 200

        for sub in [ro_subject, rw_subject]:
            result_raw = self.sr_client.get_subjects_subject_versions(subject=sub)
            assert result_raw.status_code == 200

            result_raw = self.sr_client.get_subjects_subject_versions_version(
                subject=sub, version=1
            )
            assert result_raw.status_code == 200

            result_raw = (
                self.sr_client.get_subjects_subject_versions_version_referenced_by(
                    subject=sub, version=1
                )
            )
            assert result_raw.status_code == 200

            self.logger.info("Checking for schema 1 as subject key")
            result_raw = self.sr_client.post_subjects_subject(subject=sub, data=schema1)
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json()["id"] == 1
            assert result_raw.json()["version"] == 1

            self.logger.info("Checking for schema 1 as subject key")
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=sub, data=schema1
            )
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json()["id"] == 1

            self.logger.info("Checking schema 2 as subject key")
            result_raw = self.sr_client.post_subjects_subject(subject=sub, data=schema2)
            assert result_raw.status_code == 404
            assert result_raw.json()["error_code"] == 40403
            assert result_raw.json()["message"] == "Schema not found"

        self.logger.info("Posting schema 2 as ro_subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=ro_subject, data=schema2
        )
        assert result_raw.status_code == 422
        assert result_raw.json()["error_code"] == 42205
        assert (
            result_raw.json()["message"]
            == f"Subject {ro_subject} in context . is in read-only mode"
        )

        self.logger.info("Posting schema 2 as rw_subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=rw_subject, data=schema2
        )
        assert result_raw.status_code == 200

        # compatibility
        for sub in [ro_subject, rw_subject]:
            result_raw = self.sr_client.post_compatibility_subject_version(
                subject=sub, version=1, data=schema2
            )
            assert result_raw.status_code == 200

        # schemas
        result_raw = self.sr_client.get_schemas_types()
        assert result_raw.status_code == 200

        result_raw = self.sr_client.get_schemas_ids_id(id=1)
        assert result_raw.status_code == 200

        result_raw = self.sr_client.get_schemas_ids_id_subjects(id=1)
        assert result_raw.status_code == 200

        result_raw = self.sr_client.get_schemas_ids_id_versions(id=1)
        assert result_raw.status_code == 200

    @cluster(num_nodes=3)
    def test_enabling_import_mode(self):
        """
        Test the conditions on enabling import mode
        """
        sub1 = "test-subject-1"
        sub2 = "test-subject-2"

        schema1 = json.dumps({"schema": schema1_def})
        schema2 = json.dumps({"schema": schema2_def})

        # Test criteria for enabling global-level import mode (schema registry is empty)
        self.logger.info("Testing global import mode enablement conditions")

        self.logger.debug("Creating schema for global import mode testing")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=schema1
        )
        self.assert_equal(result_raw.status_code, 200)

        self.logger.debug(
            "Try to enable global import mode with existing schema - should fail"
        )
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "IMPORT"}))
        self.assert_equal(result_raw.status_code, 422)
        self.assert_equal(result_raw.json()["error_code"], 42205)

        self.logger.debug(
            "Try to enable global import mode with existing schema with force=true - should succeed"
        )
        result_raw = self.sr_client.set_mode(
            data=json.dumps({"mode": "IMPORT"}), force=True
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["mode"], "IMPORT")

        self.logger.debug("Reset to READWRITE mode")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "READWRITE"}))
        self.assert_equal(result_raw.status_code, 200)

        self.logger.debug("Soft delete schema version")
        result_raw = self.sr_client.delete_subject_version(subject=sub1, version=1)
        self.assert_equal(result_raw.status_code, 200)

        self.logger.debug(
            "Try to enable global import mode after soft delete - should still fail"
        )
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "IMPORT"}))
        self.assert_equal(result_raw.status_code, 422)
        self.assert_equal(result_raw.json()["error_code"], 42205)

        self.logger.debug("Hard delete schema version")
        result_raw = self.sr_client.delete_subject_version(
            subject=sub1, version=1, permanent=True
        )
        self.assert_equal(result_raw.status_code, 200)

        self.logger.debug(
            "Enable global import mode after hard delete - should succeed"
        )
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "IMPORT"}))
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["mode"], "IMPORT")

        self.logger.debug("Reset global mode for further testing")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "READWRITE"}))
        self.assert_equal(result_raw.status_code, 200)

        # Test subject-level criteria for enabling subject-level import mode (subject is empty)
        self.logger.info("Testing subject-level import mode enablement conditions")

        self.logger.debug("Creating schema for subject-level import mode testing")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub2, data=schema2
        )
        self.assert_equal(result_raw.status_code, 200)

        self.logger.debug(
            "Try to enable subject import mode with existing schema - should fail"
        )
        result_raw = self.sr_client.set_mode_subject(
            subject=sub2, data=json.dumps({"mode": "IMPORT"})
        )
        self.assert_equal(result_raw.status_code, 422)
        self.assert_equal(result_raw.json()["error_code"], 42205)

        self.logger.debug(
            "Try to enable subject import mode with existing schema with force=true - should succeed"
        )
        result_raw = self.sr_client.set_mode_subject(
            subject=sub2, data=json.dumps({"mode": "IMPORT"}), force=True
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["mode"], "IMPORT")

        self.logger.debug("Reset to READWRITE mode")
        result_raw = self.sr_client.set_mode_subject(
            subject=sub2, data=json.dumps({"mode": "READWRITE"})
        )
        self.assert_equal(result_raw.status_code, 200)

        self.logger.debug("Soft delete schema for subject")
        result_raw = self.sr_client.delete_subject_version(subject=sub2, version=1)
        self.assert_equal(result_raw.status_code, 200)

        self.logger.debug(
            "Try to enable subject import mode after soft delete - should still fail"
        )
        result_raw = self.sr_client.set_mode_subject(
            subject=sub2, data=json.dumps({"mode": "IMPORT"})
        )
        self.assert_equal(result_raw.status_code, 422)
        self.assert_equal(result_raw.json()["error_code"], 42205)

        self.logger.debug("Hard delete schema for subject")
        result_raw = self.sr_client.delete_subject_version(
            subject=sub2, version=1, permanent=True
        )
        self.assert_equal(result_raw.status_code, 200)

        self.logger.debug(
            "Enable subject import mode after hard delete - should succeed"
        )
        result_raw = self.sr_client.set_mode_subject(
            subject=sub2, data=json.dumps({"mode": "IMPORT"})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["mode"], "IMPORT")

        self.logger.debug("Cleaning up - reset modes")
        result_raw = self.sr_client.delete_mode_subject(subject=sub2)
        self.assert_equal(result_raw.status_code, 200)

    @cluster(num_nodes=3)
    @matrix(subject_scope=[False, True])
    def test_import_mode_behaviour(self, subject_scope):
        """Test expected import mode behaviour"""
        sub1 = "test-subject-1"
        expected_ver_to_id = {}

        if subject_scope:
            self.logger.debug(f"Enable IMPORT mode for subject {sub1}")
            result_raw = self.sr_client.set_mode_subject(
                subject=sub1, data=json.dumps({"mode": "IMPORT"})
            )
            self.assert_equal(result_raw.status_code, 200)
            self.assert_equal(result_raw.json()["mode"], "IMPORT")
        else:
            self.logger.debug("Enable IMPORT mode globally")
            result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "IMPORT"}))
            self.assert_equal(result_raw.status_code, 200)
            self.assert_equal(result_raw.json()["mode"], "IMPORT")

        self.logger.debug("Post schema without id - should fail")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result_raw.status_code, 422)
        self.assert_equal(result_raw.json()["error_code"], 42205)

        self.logger.debug("Post schema with arbitrary id - should succeed")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def, "id": 4})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 4)
        expected_ver_to_id[1] = 4

        self.logger.debug("Re-post existing schema without id - should succeed")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 4)

        self.logger.debug(
            "Post the same schema again with a different id - should succeed"
        )
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def, "id": 2})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 2)
        expected_ver_to_id[2] = 2

        self.logger.debug(
            "Post a compatible schema with arbitrary version - should succeed"
        )
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1,
            data=json.dumps({"schema": schema2_def, "id": 6, "version": 7}),
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 6)
        expected_ver_to_id[7] = 6

        self.logger.debug("Post an incompatible schema - should succeed")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema3_def, "id": 7})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 7)
        expected_ver_to_id[8] = 7

        self.logger.debug("Try to overwrite an existing schema id - should fail")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema2_def, "id": 7})
        )
        self.assert_equal(result_raw.status_code, 422)
        self.assert_equal(result_raw.json()["error_code"], 42205)

        self.logger.debug(
            "Try to overwrite an existing schema version (with different schema id) - should succeed"
        )
        # Note: version=1 here corresponds to the earlier schema id 4 using the schema definition schema1_def
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1,
            data=json.dumps({"schema": schema2_def, "id": 8, "version": 1}),
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 8)
        self.assert_equal(expected_ver_to_id[1], 4)
        expected_ver_to_id[1] = 8

        self.logger.debug(
            f"Finally, sanity check the expected set of schemas - expecting: {expected_ver_to_id=}"
        )
        rpk = self._get_rpk_tools()
        resp = rpk.list_schemas([sub1])
        got_ver_to_id = {int(elem["version"]): elem["id"] for elem in resp}
        self.assert_equal(expected_ver_to_id, got_ver_to_id)

    @cluster(num_nodes=1)
    def test_import_with_metadata_properties(self):
        """
        Verify importing a schema with metatada.properties.
        """

        subject = f"{create_topic_names(1)[0]}-key"
        result_raw = self.sr_client.set_mode_subject(
            subject=subject, data=json.dumps({"mode": "IMPORT"})
        )
        self.assert_equal(result_raw.status_code, 200)

        metadata_properties = {
            "string": "string",
        }

        schemas = [
            {
                "schema": schema1_def,
                "schemaType": str(SchemaType.AVRO),
                "metadata": {"properties": metadata_properties},
                "id": 1,
                "version": 1,
            },
            {
                "schema": schema1_def,
                "schemaType": str(SchemaType.AVRO),
                "id": 2,
                "version": 2,
            },
        ]

        self.logger.debug("Importing schemas")
        for schema in schemas:
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=subject, data=json.dumps(schema)
            )
            self.logger.debug(result_raw.content)
            assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Retrieving schemas")
        for schema in schemas:
            v = schema["version"]
            result_raw = self.sr_client.get_subjects_subject_versions_version(
                subject=subject, version=v
            )
            self.logger.debug(result_raw.content)
            assert result_raw.status_code == requests.codes.ok
            for f in ["schema", "schemaType", "id", "version", "metadata"]:
                assert result_raw.json().get(f) == schema.get(f), (
                    f"Expected: {schema.get(f)}, got: {result_raw.json().get(f)}"
                )

    @cluster(num_nodes=3)
    def test_schema_id_smaller_than_one(self):
        sub = "test-subject-1"

        self.logger.debug("Enable IMPORT mode to allow posting with specific ids")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "IMPORT"}))
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["mode"], "IMPORT")

        self.logger.debug("Post a schema with id=0 - expect schema_id=0")
        result_raw = self.sr_client.post_subjects_subject_versions(
            sub, data=json.dumps({"id": 0, "schema": schema1_def})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 0)

        self.logger.debug("Post another schema with id=-1 - should fail")
        result_raw = self.sr_client.post_subjects_subject_versions(
            sub, data=json.dumps({"id": -1, "schema": schema2_def})
        )
        self.assert_equal(result_raw.status_code, 422)
        self.assert_equal(result_raw.json()["error_code"], 42205)

        self.logger.debug("Enable READWRITE mode")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "READWRITE"}))
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["mode"], "READWRITE")

        self.logger.debug(
            "Post another schema with id=-1 (now in r/w mode) - should succeed"
        )
        result_raw = self.sr_client.post_subjects_subject_versions(
            sub, data=json.dumps({"id": -1, "schema": schema2_def})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 1)

    @cluster(num_nodes=3)
    def test_schema_id_exhausted(self):
        sub = "test-subject-1"

        self.logger.debug("Enable IMPORT mode to allow posting specific versions")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "IMPORT"}))
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["mode"], "IMPORT")

        self.logger.debug("Post a schema with INT_MAX version")
        result_raw = self.sr_client.post_subjects_subject_versions(
            sub,
            data=json.dumps({"id": 1, "version": 2147483647, "schema": schema1_def}),
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 1)

        self.logger.debug("Post another schema - expect version exhausted error")
        result_raw = self.sr_client.post_subjects_subject_versions(
            sub, data=json.dumps({"id": 2, "schema": schema2_def})
        )
        self.assert_equal(result_raw.status_code, 500)
        self.assert_equal(
            result_raw.json()["message"], f"Versions exhausted for subject {sub}"
        )

    @cluster(num_nodes=3)
    def test_imported_schemas_with_dependencies_issues(self):
        """
        Verify SR behavior when importing schemas in the wrong order and missing dependencies
        """

        def assert_request_code(raw, expected, endpoint):
            assert raw.status_code == expected, (
                f"Expected {expected} but got {raw.status_code}, "
                f"for request '{endpoint}' with content: {raw.content}"
            )

        # Test setup: Simulate import of schemas by writting directly into the _schemas topic
        # Note that trying to commit these schemas in this order, using POST subjects/{subject}/version
        # would fail as schemas 'schema_b' and 'schema_d' have unmet dependencies
        schemas = [
            # schema_a has no dependencies
            import_schemas["schema_a"],
            # schema_c is dependent on schema_b, which is loaded later on
            import_schemas["schema_c"],
            # schema_b is dependent on schema_a, which is already loaded
            import_schemas["schema_b"],
            # schema_d is dependent on schema_e, which is not currently present
            import_schemas["schema_d"],
            # all schema_f are valid
            import_schemas["schema_f_v1"],
            import_schemas["schema_f_v3"],
            import_schemas["schema_f_v5"],
            # all schema_g depend on the equivalent schema f version
            import_schemas["schema_g_v1"],
            # Missing dependency - schema_f_v2
            import_schemas["schema_g_v2"],
            import_schemas["schema_g_v3"],
            # Missing dependency - schema_f_v4
            import_schemas["schema_g_v4"],
            # dependency error deep in dep chain
            # schema_i depends on schema_h which is missing
            import_schemas["schema_i"],
            # schema_j depends on schema_i
            import_schemas["schema_j"],
            # schema_k depends on schema_j
            import_schemas["schema_k"],
        ]

        self._push_to_schemas_topic(schemas)

        valid_entries = [
            (1, "schema_a"),
            (2, "schema_c"),
            (3, "schema_b"),
            (5, "schema_f_v1"),
            (6, "schema_f_v3"),
            (7, "schema_f_v5"),
            (8, "schema_g_v1"),
            (10, "schema_g_v3"),
        ]
        # These are schemas that having missing dependencies at startup
        invalid_entries = [
            (4, "schema_d"),
            (9, "schema_g_v2"),
            (11, "schema_g_v4"),
            (12, "schema_i"),
            (13, "schema_j"),
            (14, "schema_k"),
        ]

        # Test /schemas/ids/{id}
        def test_schemas_ids_id(id, expected_successful):
            endpoint = f"GET schemas/ids/{id}"
            result_raw = self.sr_client.request(
                "GET", f"schemas/ids/{id}", headers=HTTP_GET_HEADERS
            )
            if expected_successful:
                assert_request_code(result_raw, requests.codes.ok, endpoint)

                result = result_raw.json()["schema"].strip()
                expected_result = schemas[id - 1]["sanitized"].strip()
                # Currently, schemas are not sanitized through this endpoint
                assert result == expected_result, (
                    f"Expected:\n{result}\nGot:\n{expected_result}\n"
                    f"for request 'GET schemas/ids/{id}"
                )
            else:
                assert_request_code(result_raw, 422, endpoint)

        # All schemas should be retrievable by id.
        for id, _ in valid_entries + invalid_entries:
            test_schemas_ids_id(id, expected_successful=True)

        # Test /schemas/ids/{id}/versions
        def test_schemas_ids_id_versions(id, expected_successful):
            endpoint = f"GET schemas/ids/{id}/versions"
            result_raw = self.sr_client.request(
                "GET", f"schemas/ids/{id}/versions", headers=HTTP_GET_HEADERS
            )
            if expected_successful:
                assert_request_code(result_raw, requests.codes.ok, endpoint)
            else:
                assert_request_code(result_raw, 422, endpoint)

        # Versions should be retrievable for all ids.
        for id, _ in valid_entries + invalid_entries:
            test_schemas_ids_id_versions(id, expected_successful=True)

        # Test /schemas/ids/{id}/subjects
        def test_schemas_ids_id_subjects(id, expected_successful):
            endpoint = (f"GET schemas/ids/{id}/subjects",)
            result_raw = self.sr_client.request(
                "GET", f"schemas/ids/{id}/subjects", headers=HTTP_GET_HEADERS
            )
            if expected_successful:
                assert_request_code(result_raw, requests.codes.ok, endpoint)
            else:
                assert_request_code(result_raw, 422, endpoint)

        # Subjects should be retrievable for all ids.
        for id, _ in valid_entries + invalid_entries:
            test_schemas_ids_id_subjects(id, expected_successful=True)

        # Test /subjects
        result_raw = self.sr_client.request("GET", "subjects", headers=HTTP_GET_HEADERS)
        assert_request_code(result_raw, requests.codes.ok, "GET subjects")

        # All subjects should be present, regardless if their schemas are valid or not
        expected_subjects = set(
            [
                "schema_a",
                "schema_b",
                "schema_c",
                "schema_d",
                "schema_f",
                "schema_g",
                "schema_i",
                "schema_j",
                "schema_k",
            ]
        )
        subjects = set(result_raw.json())
        assert subjects == expected_subjects, (
            f"Expected {expected_subjects} but got {subjects}, "
            "for request 'GET subjects'"
        )

        def test_subjects_subject(entry, expected_code):
            lookup_schema = import_schemas[entry]
            subject = lookup_schema["subject"]
            schema_def = lookup_schema["schema"]
            version = lookup_schema["version"]
            references = (
                lookup_schema["references"] if "references" in lookup_schema else []
            )
            result_raw = self.sr_client.post_subjects_subject(
                subject=subject,
                data=json.dumps(
                    {
                        "schema": schema_def,
                        "schemaType": "PROTOBUF",
                        "references": references,
                    }
                ),
            )
            endpoint = (f"POST subjects/{subject}",)
            assert_request_code(result_raw, expected_code, endpoint)
            if expected_code == requests.codes.ok:
                result = result_raw.json()
                assert result["version"] == version, (
                    f"Expected version {version} but got {result['version']}, "
                    f"for request 'POST subjects/{subject}'"
                )

        # Test /subjects/{subject}
        for _, s in valid_entries:
            test_subjects_subject(s, expected_code=requests.codes.ok)

        # These schemas should fail, as the *input* schema has an unsatisfied dependency
        for _, s in invalid_entries:
            test_subjects_subject(s, expected_code=422)

        # Test /subjects/{subject}/versions/{version}
        def test_subjects_subject_versions_version(entry, expected_successful):
            lookup_schema = import_schemas[entry]
            subject = lookup_schema["subject"]
            version = lookup_schema["version"]
            expected_schema = lookup_schema["sanitized"].strip()
            # references = lookup_schema["references"] if "references" in lookup_schema else []
            result_raw = self.sr_client.get_subjects_subject_versions_version(
                subject=subject, version=version
            )
            endpoint = f"GET subjects/{subject}/versions/{version}"
            if expected_successful:
                assert_request_code(result_raw, requests.codes.ok, endpoint)

                result = result_raw.json()["schema"].strip()
                assert result == expected_schema, (
                    f"Expected:\n{expected_schema}\nGot:\n{result}\nfor request "
                    f"'GET subjects/{subject}/versions/{version}'"
                )
            else:
                assert_request_code(result_raw, 422, endpoint)

        # All schemas should be retrievable through subject/version.
        for _, s in valid_entries + invalid_entries:
            test_subjects_subject_versions_version(s, expected_successful=True)

        # Test /subjects/{subject}/versions/{version}/schema
        def test_subjects_subject_versions_version_schema(entry, expected_successful):
            lookup_schema = import_schemas[entry]
            subject = lookup_schema["subject"]
            version = lookup_schema["version"]
            schema_def = lookup_schema["sanitized"].strip()
            result_raw = self.sr_client.request(
                "GET",
                f"subjects/{subject}/versions/{version}/schema",
                headers=HTTP_GET_HEADERS,
            )

            endpoint = f"GET subjects/{subject}/versions/{version}/schema"
            if expected_successful:
                assert_request_code(result_raw, requests.codes.ok, endpoint)

                result = result_raw.content.decode().strip()
                assert result == schema_def, (
                    f"Expected:\n{schema_def}\nGot:\n{result}\n"
                    f"for request 'GET subjects/{subject}/versions/{version}/schema'"
                )
            else:
                assert_request_code(result_raw, 422, endpoint)

        # All schemas should be retrievable through subject/version.
        for _, s in valid_entries + invalid_entries:
            test_subjects_subject_versions_version_schema(s, expected_successful=True)

        # Test /subjects/{subject}/versions/{version}/referencedby
        def test_referenced_by(entry, expected_result):
            lookup_schema = import_schemas[entry]
            subject = lookup_schema["subject"]
            version = lookup_schema["version"]
            result_raw = (
                self.sr_client.get_subjects_subject_versions_version_referenced_by(
                    subject, version
                )
            )

            endpoint = f"GET subjects/{subject}/versions/{version}/referencedby"
            assert_request_code(result_raw, requests.codes.ok, endpoint)
            result = result_raw.json()
            assert result == expected_result, (
                f"Expected {expected_result} but got {result}, "
                f"for request 'GET subjects/{subject}/versions/{version}/referencedby'"
            )

        test_referenced_by("schema_a", [3])
        test_referenced_by("schema_b", [2])
        test_referenced_by("schema_c", [])
        test_referenced_by("schema_d", [])
        test_referenced_by("schema_f_v1", [8])
        test_referenced_by("schema_f_v3", [10])
        test_referenced_by("schema_f_v5", [])
        test_referenced_by("schema_g_v1", [])
        test_referenced_by("schema_g_v2", [])
        test_referenced_by("schema_g_v3", [])
        test_referenced_by("schema_g_v4", [])

        # This is the last of the endpoint to be checked, as it will change the state
        # Test /subjects/{subject}/versions
        def test_subjects_subject_versions(
            entry, expected_successful, expected_id=None, include_id=False
        ):
            lookup_schema = import_schemas[entry]
            subject = lookup_schema["subject"]
            schema_def = lookup_schema["schema"]
            references = (
                lookup_schema["references"] if "references" in lookup_schema else []
            )
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=subject,
                data=json.dumps(
                    {
                        "schema": schema_def,
                        "schemaType": "PROTOBUF",
                        "references": references,
                        "version": lookup_schema["version"],
                        "id": expected_id if include_id else -1,
                    }
                ),
            )
            endpoint = f"POST subjects/{subject}/versions"
            if expected_successful:
                assert_request_code(result_raw, requests.codes.ok, endpoint)
                result = result_raw.json()
                assert result["id"] == expected_id, (
                    f"Expected id {expected_id} but got {result['id']}, "
                    f"for request 'POST subjects/{subject}/versions'"
                )
            else:
                assert_request_code(result_raw, 422, endpoint)

        for id, s in valid_entries:
            test_subjects_subject_versions(s, expected_id=id, expected_successful=True)

        # These schemas should fail, as the *input* schema has an unsatisfied dependencies
        for id, s in invalid_entries:
            test_subjects_subject_versions(s, expected_id=id, expected_successful=False)

        # Insert missing dependency, schema_e, and retry the failed schema_d requests
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject="schema_e",
            data=json.dumps({"schema": schema_e_proto_def, "schemaType": "PROTOBUF"}),
        )
        assert_request_code(
            result_raw, requests.codes.ok, "POST subjects/schema_e/versions"
        )

        # Validate that schema_d is now accepted as a referee
        test_referenced_by("schema_e", [4])

        test_schemas_ids_id(4, expected_successful=True)
        test_schemas_ids_id_versions(4, expected_successful=True)
        test_schemas_ids_id_subjects(4, expected_successful=True)
        # Lookup still fails cause schema_d is stored not in it's canonical form
        test_subjects_subject("schema_d", expected_code=404)

        # Force enable IMPORT mode to allow re-posting a specific versions of schemas
        result_raw = self.sr_client.set_mode(
            force=True, data=json.dumps({"mode": "IMPORT"})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["mode"], "IMPORT")

        # Validate that schema_d can now be posted anew.
        # Note that a new version will be created, as the old one was invalid at
        # startup and it was not in canonical form. Thus it cannot be found and a
        # new version is created.
        test_subjects_subject_versions(
            "schema_d", expected_id=16, expected_successful=True, include_id=True
        )
        # After pushing, schema_d is not stored by its canonical form
        import_schemas["schema_d"]["sanitized"] = schema_d_proto_sanitized_def
        test_subjects_subject_versions_version("schema_d", expected_successful=True)
        test_subjects_subject_versions_version_schema(
            "schema_d", expected_successful=True
        )

        # Add schema_g_v5. schema_g_v5 is valid, but schema_g_v4 and schema_g_v2 are not,
        # so compatibility checks failed due to them.
        test_subjects_subject_versions("schema_g_v5", expected_successful=False)

        # Fix problematic schemas by adding missing dependency for g_v2 and deleting g_v4.
        test_subjects_subject_versions(
            "schema_f_v2", expected_id=17, expected_successful=True, include_id=True
        )

        result_raw = self.sr_client.delete_subject_version("schema_g", version=4)
        assert_request_code(
            result_raw, requests.codes.ok, "DELETE subjects/schema_g/versions/4"
        )

        test_subjects_subject_versions(
            "schema_g_v5", expected_id=18, expected_successful=True, include_id=True
        )

        # Fix problematic dependency chain by adding base schema.
        test_subjects_subject_versions(
            "schema_h", expected_id=19, expected_successful=True, include_id=True
        )
        test_schemas_ids_id(12, expected_successful=True)
        test_schemas_ids_id(13, expected_successful=True)
        test_schemas_ids_id(14, expected_successful=True)

    @cluster(num_nodes=3)
    @matrix(subject_scope=[False, True])
    def test_readwrite_mode_id_behaviour(self, subject_scope):
        """Test expected READWRITE mode behaviour when the id is specified when trying to register a schema"""
        sub1 = "test-subject-1"
        expected_ver_to_id = {}

        if subject_scope:
            self.logger.debug(f"Configure READWRITE mode for subject {sub1}")
            # Overwrite a global-scoped IMPORT-mode to test that subject-level overwriting works
            result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "IMPORT"}))
            self.assert_equal(result_raw.status_code, 200)
            self.assert_equal(result_raw.json()["mode"], "IMPORT")

            result_raw = self.sr_client.set_mode_subject(
                subject=sub1, data=json.dumps({"mode": "READWRITE"})
            )
            self.assert_equal(result_raw.status_code, 200)
            self.assert_equal(result_raw.json()["mode"], "READWRITE")
        else:
            self.logger.debug(f"Configure READWRITE mode for subject {sub1}")
            # Noop

        self.logger.debug(
            "Post a schema for the first time while specifying an id - should fail"
        )
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def, "id": 1})
        )
        self.assert_equal(result_raw.status_code, 422)
        self.assert_equal(result_raw.json()["error_code"], 42205)

        self.logger.debug("Post a schema without an id - should succeed")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 1)
        expected_ver_to_id[1] = 1

        self.logger.debug("Post the schema again with the same id - should succeed")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def, "id": 1})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 1)

        self.logger.debug("Post the schema again with a different id - should fail")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def, "id": 2})
        )
        self.assert_equal(result_raw.status_code, 422)
        self.assert_equal(result_raw.json()["error_code"], 42205)

        self.logger.debug(
            f"Finally, sanity check the expected set of schemas - expecting: {expected_ver_to_id=}"
        )
        rpk = self._get_rpk_tools()
        resp = rpk.list_schemas([sub1])
        got_ver_to_id = {int(elem["version"]): elem["id"] for elem in resp}
        self.assert_equal(expected_ver_to_id, got_ver_to_id)

    @cluster(num_nodes=3)
    @matrix(subject_scope=[False, True])
    def test_readwrite_mode_version_behaviour(self, subject_scope):
        """Test expected READWRITE mode behaviour when the version is specified when trying to register a schema"""
        sub1 = "test-subject-1"
        expected_ver_to_id = {}

        if subject_scope:
            self.logger.debug(f"Configure READWRITE mode for subject {sub1}")
            # Overwrite a global-scoped IMPORT-mode to test that subject-level overwriting works
            result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "IMPORT"}))
            self.assert_equal(result_raw.status_code, 200)
            self.assert_equal(result_raw.json()["mode"], "IMPORT")

            result_raw = self.sr_client.set_mode_subject(
                subject=sub1, data=json.dumps({"mode": "READWRITE"})
            )
            self.assert_equal(result_raw.status_code, 200)
            self.assert_equal(result_raw.json()["mode"], "READWRITE")
        else:
            self.logger.debug(f"Configure READWRITE mode for subject {sub1}")
            # Noop

        self.logger.debug("Post a schema with an arbitrary version - should fail")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def, "version": 7})
        )
        self.assert_equal(result_raw.status_code, 422)
        self.assert_equal(result_raw.json()["error_code"], 42201)

        self.logger.debug("Post a schema with a max + 1 version - should succeed")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def, "version": 1})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 1)
        expected_ver_to_id[1] = 1

        self.logger.debug(
            "Post the schema again with the same version - should succeed"
        )
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def, "version": 1})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 1)

        self.logger.debug(
            "Post the schema again with an arbitrary version - should succeed"
        )
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=sub1, data=json.dumps({"schema": schema1_def, "version": 9})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 1)

        self.logger.debug(
            f"Finally, sanity check the expected set of schemas - expecting: {expected_ver_to_id=}"
        )
        rpk = self._get_rpk_tools()
        resp = rpk.list_schemas([sub1])
        got_ver_to_id = {int(elem["version"]): elem["id"] for elem in resp}
        self.assert_equal(expected_ver_to_id, got_ver_to_id)

    @cluster(num_nodes=3)
    def test_id_lookup_multiple_matches(self):
        """
        Test the behaviour of the schema lookup/registration endpoint when
        there are multiple existing schemas with different ids but identical
        schema definition.
        """
        sub = "test-subject-1"

        self.logger.debug("Enable IMPORT mode to allow posting specific ids")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "IMPORT"}))
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["mode"], "IMPORT")

        self.logger.debug("Post the schema first with id 1")
        result_raw = self.sr_client.post_subjects_subject_versions(
            sub, data=json.dumps({"id": 1, "schema": schema1_def})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 1)

        self.logger.debug("Post the same schema again now with id 2")
        result_raw = self.sr_client.post_subjects_subject_versions(
            sub, data=json.dumps({"id": 2, "schema": schema1_def})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 2)

        self.logger.debug("Enable READWRITE mode to post without id")
        result_raw = self.sr_client.set_mode(data=json.dumps({"mode": "READWRITE"}))
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["mode"], "READWRITE")

        # POST /subjects/{subject}/versions/{version}
        self.logger.debug(
            "Post the schema definition again to post_subjects_subject_versions - should return id 2"
        )
        result_raw = self.sr_client.post_subjects_subject_versions(
            sub, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 2)

        # POST /subjects/{subject}
        self.logger.debug(
            "Post the schema definition again to post_subjects_subject - should return id 2"
        )
        result_raw = self.sr_client.post_subjects_subject(
            sub, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result_raw.status_code, 200)
        self.assert_equal(result_raw.json()["id"], 2)


class SchemaRegistryContextTest(SchemaRegistryEndpoints):
    """
    Tests for context-qualified subject functionality.

    These tests verify that Schema Registry correctly handles context-qualified
    subjects (e.g., ":.ctx:subject") for isolation, references, config, and mode.
    """

    def __init__(self, context: TestContext, **kwargs: Any):
        schema_registry_config = SchemaRegistryConfig()
        schema_registry_config.mode_mutability = True
        super().__init__(
            context,
            schema_registry_config=schema_registry_config,
            extra_rp_conf={"schema_registry_enable_qualified_subjects": True},
            **kwargs,
        )

    @cluster(num_nodes=1)
    def test_contexts(self):
        """Verify context-aware endpoints work with qualified subjects."""

        schema_data = json.dumps({"schema": schema1_def})
        compat_schema_data = json.dumps({"schema": schema2_def})
        ctx_subject = ":.ctx1:sub1"

        # Register in context
        result = self.sr_client.post_subjects_subject_versions(
            subject=ctx_subject, data=schema_data
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Lookup in context
        result = self.sr_client.post_subjects_subject(
            subject=ctx_subject, data=schema_data
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Compatibility check in context
        result = self.sr_client.post_compatibility_subject_version(
            subject=ctx_subject, version="latest", data=compat_schema_data
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["is_compatible"], True)

        # List versions in context
        result = self.sr_client.get_subjects_subject_versions(subject=ctx_subject)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json(), [1])

        # Get specific version in context
        result = self.sr_client.get_subjects_subject_versions_version(
            subject=ctx_subject, version=1
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["version"], 1)

        # Get schema only for specific version in context
        result = self.sr_client.get_subjects_subject_versions_version_schema(
            subject=ctx_subject, version=1
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Get referenced-by in context (empty list, no references)
        result = self.sr_client.get_subjects_subject_versions_version_referenced_by(
            subject=ctx_subject, version=1
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json(), [])

        # Delete specific version in context
        result = self.sr_client.delete_subject_version(subject=ctx_subject, version=1)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json(), 1)

        # Delete subject in context (cleanup)
        result = self.sr_client.delete_subject(subject=ctx_subject, permanent=True)
        self.assert_equal(result.status_code, requests.codes.ok)

    @cluster(num_nodes=1)
    def test_context_isolation(self):
        """Verify contexts are isolated: independent IDs, no cross-context lookups."""

        # Register in default context
        result = self.sr_client.post_subjects_subject_versions(
            subject="sub1", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["id"], 1)

        # Register same schema in .ctx1 - should get id=1 (independent counter)
        result = self.sr_client.post_subjects_subject_versions(
            subject=":.ctx1:sub1", data=json.dumps({"schema": schema2_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["id"], 1)

        # Lookup schema in different context - should fail
        result = self.sr_client.post_subjects_subject(
            subject=":.ctx2:sub1", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.not_found)

    @cluster(num_nodes=1)
    def test_context_references(self):
        """Test schema references work with context-qualified subjects."""
        # Test all schema types using existing test data
        for schema_type in ["proto", "avro", "json"]:
            base = base_schemas[schema_type]
            dependent = dependent_schemas[schema_type]
            ref_name = dependent["references"][0]["name"]

            ctx = f"ctx-{schema_type}"
            ctx_ref_subject = f":.{ctx}:base"
            ctx_main_subject = f":.{ctx}:dependent"

            # Register base schema in context
            ref_data = json.dumps(
                {"schema": base["schema"], "schemaType": base["type"]}
            )
            result = self.sr_client.post_subjects_subject_versions(
                subject=ctx_ref_subject, data=ref_data
            )
            self.assert_equal(result.status_code, requests.codes.ok)

            # Register dependent schema with in-context reference
            main_data = json.dumps(
                {
                    "schema": dependent["schema"],
                    "schemaType": dependent["type"],
                    "references": [
                        {"name": ref_name, "subject": ctx_ref_subject, "version": 1}
                    ],
                }
            )
            result = self.sr_client.post_subjects_subject_versions(
                subject=ctx_main_subject, data=main_data
            )
            self.assert_equal(result.status_code, requests.codes.ok)

            # Verify referenced-by works with context subjects
            result = self.sr_client.get_subjects_subject_versions_version_referenced_by(
                subject=ctx_ref_subject, version=1
            )
            self.assert_equal(result.status_code, requests.codes.ok)
            self.assert_equal(len(result.json()), 1)

        # Test cross-context references
        base = base_schemas["proto"]
        dependent = dependent_schemas["proto"]
        cross_ref_subject = ":.ctx-cross-ref:base"
        cross_main_subject = ":.ctx-cross-main:dependent"

        result = self.sr_client.post_subjects_subject_versions(
            subject=cross_ref_subject,
            data=json.dumps({"schema": base["schema"], "schemaType": base["type"]}),
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        result = self.sr_client.post_subjects_subject_versions(
            subject=cross_main_subject,
            data=json.dumps(
                {
                    "schema": dependent["schema"],
                    "schemaType": dependent["type"],
                    "references": [
                        {
                            "name": dependent["references"][0]["name"],
                            "subject": cross_ref_subject,
                            "version": 1,
                        }
                    ],
                }
            ),
        )
        self.assert_equal(result.status_code, requests.codes.ok)

    @cluster(num_nodes=1)
    def test_context_config(self):
        """Test context-level config operations."""

        ctx = "test-ctx"
        ctx_prefix = f":.{ctx}:"
        ctx_subject = f"{ctx_prefix}test-sub"

        # Register a schema in the context first
        result = self.sr_client.post_subjects_subject_versions(
            subject=ctx_subject, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Set context-level config (empty subject = context-level)
        result = self.sr_client.set_config_subject(
            subject=ctx_prefix,
            data=json.dumps({"compatibility": "NONE"}),
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Get context-level config
        result = self.sr_client.get_config_subject(subject=ctx_prefix)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["compatibilityLevel"], "NONE")

        # Subject-level config should fall back to context-level
        result = self.sr_client.get_config_subject(subject=ctx_subject, fallback=True)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["compatibilityLevel"], "NONE")

        # Default context should not be affected by context-level config
        result = self.sr_client.get_config()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["compatibilityLevel"], "BACKWARD")

        # Set subject-level config (different from context-level)
        result = self.sr_client.set_config_subject(
            subject=ctx_subject,
            data=json.dumps({"compatibility": "FULL"}),
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Subject should now have FULL (not context's NONE)
        result = self.sr_client.get_config_subject(subject=ctx_subject, fallback=False)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["compatibilityLevel"], "FULL")

        # Delete subject-level config
        result = self.sr_client.delete_config_subject(subject=ctx_subject)
        self.assert_equal(result.status_code, requests.codes.ok)

        # Subject should fall back to context-level NONE
        result = self.sr_client.get_config_subject(subject=ctx_subject, fallback=True)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["compatibilityLevel"], "NONE")

        # Delete the context-level config
        result = self.sr_client.delete_config_subject(subject=ctx_prefix)
        self.assert_equal(result.status_code, requests.codes.ok)

        # Subject should fall back to default BACKWARD
        result = self.sr_client.get_config_subject(subject=ctx_subject, fallback=True)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["compatibilityLevel"], "BACKWARD")

    @cluster(num_nodes=1)
    def test_default_context(self):
        """Test that qualified subject syntax works for the default context."""
        schema_data = json.dumps({"schema": schema1_def})

        # Register in default context with unqualified subject
        result = self.sr_client.post_subjects_subject_versions(
            subject="default-ctx-subject", data=schema_data
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["id"], 1)

        # Look up in default context with qualified subject
        result = self.sr_client.post_subjects_subject(
            subject=":.:default-ctx-subject", data=schema_data
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["id"], 1)

        # Set the compatibility level with qualified subject
        result = self.sr_client.set_config_subject(
            subject=":.:", data=json.dumps({"compatibility": "FULL"})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["compatibility"], "FULL")

        # Look up the compatibility level of default context
        result = self.sr_client.get_config()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["compatibilityLevel"], "FULL")

        # Delete the compatibility level with qualified subject
        result = self.sr_client.delete_config_subject(subject=":.:")
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["compatibilityLevel"], "FULL")

        # Look up the compatibility level of default context
        result = self.sr_client.get_config()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["compatibilityLevel"], "BACKWARD")

        # Set the mode with qualified subject
        result = self.sr_client.set_mode_subject(
            subject=":.:", data=json.dumps({"mode": "READONLY"}), force=True
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["mode"], "READONLY")

        # Look up the mode of default context
        result = self.sr_client.get_mode()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["mode"], "READONLY")

        # Delete the mode with qualified subject
        result = self.sr_client.delete_mode_subject(subject=":.:")
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["mode"], "READONLY")

        # Look up the mode of default context
        result = self.sr_client.get_mode()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["mode"], "READWRITE")

    @cluster(num_nodes=1)
    def test_context_mode(self):
        """Test context-level mode operations."""

        ctx = "test-ctx"
        ctx_prefix = f":.{ctx}:"
        ctx_subject = f"{ctx_prefix}test-sub"

        # Register a schema in the context first
        result = self.sr_client.post_subjects_subject_versions(
            subject=ctx_subject, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Set context-level mode (empty subject = context-level)
        result = self.sr_client.set_mode_subject(
            subject=ctx_prefix,
            data=json.dumps({"mode": "READONLY"}),
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Get context-level mode
        result = self.sr_client.get_mode_subject(subject=ctx_prefix)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["mode"], "READONLY")

        # Subject-level mode should fall back to context-level
        result = self.sr_client.get_mode_subject(subject=ctx_subject, fallback=True)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["mode"], "READONLY")

        # Default context should not be affected by context-level mode
        result = self.sr_client.get_mode()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["mode"], "READWRITE")

        # Set subject-level mode (different from context-level)
        result = self.sr_client.set_mode_subject(
            subject=ctx_subject,
            data=json.dumps({"mode": "READWRITE"}),
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Subject should now have READWRITE (not context's READONLY)
        result = self.sr_client.get_mode_subject(subject=ctx_subject, fallback=False)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["mode"], "READWRITE")

        # Delete subject-level mode
        result = self.sr_client.delete_mode_subject(subject=ctx_subject)
        self.assert_equal(result.status_code, requests.codes.ok)

        # Subject should fall back to context-level READONLY
        result = self.sr_client.get_mode_subject(subject=ctx_subject, fallback=True)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["mode"], "READONLY")

        # Delete the context-level mode
        result = self.sr_client.delete_mode_subject(subject=ctx_prefix)
        self.assert_equal(result.status_code, requests.codes.ok)

        # Subject should fall back to default READWRITE
        result = self.sr_client.get_mode_subject(subject=ctx_subject, fallback=True)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["mode"], "READWRITE")

    @cluster(num_nodes=1)
    def test_delete_context_config_and_mode_before_set(self):
        """Deleting context-level config or mode when neither has been set
        should return 404. This is a regression test to protect against a
        bug where deleting /config/{subject} and /mode/{subject} with a
        context-only qualifier (e.g. ":.ctx:") would deadlock because the
        handler built tombstones from empty written_at sequences."""

        result = self.sr_client.delete_config_subject(subject=":.test-ctx:")
        self.assert_equal(result.status_code, requests.codes.not_found)

        result = self.sr_client.delete_mode_subject(subject=":.test-ctx:")
        self.assert_equal(result.status_code, requests.codes.not_found)

    @cluster(num_nodes=1)
    def test_context_record_persistence(self):
        # First, register a schema in the default context (no CONTEXT record)
        result = self.sr_client.post_subjects_subject_versions(
            subject="default-ctx-sub", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Verify no CONTEXT record was written for default context
        time.sleep(1)  # Give some time for logs to be flushed
        assert not self.redpanda.search_log_any("Writing CONTEXT record for ctx=\\."), (
            "CONTEXT record should not be written for default context"
        )

        # Now register a schema in a non-default context
        ctx = ".test-context"
        ctx_subject = f":{ctx}:test-sub"

        result = self.sr_client.post_subjects_subject_versions(
            subject=ctx_subject, data=json.dumps({"schema": schema2_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Verify context was persisted (CONTEXT record written and consumed)
        result = self.sr_client.get_contexts()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_in(".", result.json())
        self.assert_in(ctx, result.json())

    @cluster(num_nodes=1)
    def test_context_unqualified_references(self):
        base_schema = schema_proto_def
        dependent_schema = schema_proto_dependee_def

        ctx = ".unqual-ref-ctx"

        # 1. Register base schema in a non-default context
        base_subject = f":{ctx}:base-subject"
        result = self.sr_client.post_subjects_subject_versions(
            subject=base_subject,
            data=json.dumps({"schema": base_schema, "schemaType": "PROTOBUF"}),
        )
        self.assert_equal(result.status_code, 200)

        # 2. Register dependent schema in SAME context with UNQUALIFIED reference
        # The reference "base-subject" should resolve to ":.unqual-ref-ctx:base-subject"
        dependent_subject = f":{ctx}:dependent-subject"
        result = self.sr_client.post_subjects_subject_versions(
            subject=dependent_subject,
            data=json.dumps(
                {
                    "schema": dependent_schema,
                    "schemaType": "PROTOBUF",
                    "references": [
                        {
                            "name": "schema_proto.proto",
                            "subject": "base-subject",  # UNQUALIFIED - should inherit context
                            "version": 1,
                        }
                    ],
                }
            ),
        )
        self.assert_equal(result.status_code, 200)

        # 3. Verify the referenced-by relationship exists
        result = self.sr_client.get_subjects_subject_versions_version_referenced_by(
            subject=base_subject, version=1
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json(), [2])  # dependent_subject v1 has schema id 2

        # 4. Test that unqualified references in DEFAULT context don't find
        # schemas from other contexts
        default_dependent_subject = "default-ctx-dependent"
        result = self.sr_client.post_subjects_subject_versions(
            subject=default_dependent_subject,
            data=json.dumps(
                {
                    "schema": dependent_schema,
                    "schemaType": "PROTOBUF",
                    "references": [
                        {
                            "name": "schema_proto.proto",
                            "subject": "base-subject",  # UNQUALIFIED - resolves to default context
                            "version": 1,
                        }
                    ],
                }
            ),
        )
        # Should fail because "base-subject" doesn't exist in default context
        self.assert_equal(result.status_code, 422)

        # 5. Test qualified cross-context reference works
        result = self.sr_client.post_subjects_subject_versions(
            subject=default_dependent_subject,
            data=json.dumps(
                {
                    "schema": dependent_schema,
                    "schemaType": "PROTOBUF",
                    "references": [
                        {
                            "name": "schema_proto.proto",
                            "subject": base_subject,  # QUALIFIED - explicit context
                            "version": 1,
                        }
                    ],
                }
            ),
        )
        self.assert_equal(result.status_code, 200)

    @cluster(num_nodes=1)
    def test_context_list_delete(self):
        """Test GET /contexts and DELETE /contexts/{context} endpoints."""

        # Initially, only the default context should be listed
        result = self.sr_client.get_contexts()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json(), ["."])

        # Register a schema in a custom context
        ctx = ".test-ctx"
        ctx_subject = f":{ctx}:test-sub"

        result = self.sr_client.post_subjects_subject_versions(
            subject=ctx_subject, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Now both contexts should be listed
        result = self.sr_client.get_contexts()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_in(".", result.json())
        self.assert_in(ctx, result.json())

        # Try to delete the custom context (should fail - not empty)
        result = self.sr_client.delete_context(ctx)
        self.assert_equal(result.status_code, 422)
        self.assert_equal(result.json()["error_code"], 42211)

        # Soft-delete the subject
        result = self.sr_client.delete_subject(subject=ctx_subject)
        self.assert_equal(result.status_code, requests.codes.ok)

        # Try to delete context again (should still fail - has soft-deleted subjects)
        result = self.sr_client.delete_context(ctx)
        self.assert_equal(result.status_code, 422)
        self.assert_equal(result.json()["error_code"], 42211)

        # Permanently delete the subject
        result = self.sr_client.delete_subject(subject=ctx_subject, permanent=True)
        self.assert_equal(result.status_code, requests.codes.ok)

        # Now delete the context (should succeed)
        result = self.sr_client.delete_context(ctx)
        self.assert_equal(result.status_code, 204)

        # Only default context should remain
        result = self.sr_client.get_contexts()
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json(), ["."])

        # Try to delete the default context (should fail with 422)
        # Use URL-encoded dot (%2E) since "." has special meaning in URL paths
        result = self.sr_client.delete_context("%2E")
        self.assert_equal(result.status_code, 422)

        # Try to delete a non-existent context (should fail with 404)
        result = self.sr_client.delete_context(".nonexistent")
        self.assert_equal(result.status_code, 404)

    @cluster(num_nodes=1)
    def test_get_schema_by_id_with_subject(self):
        """Test GET /schemas/ids/{id} with subject query parameter for context lookup."""

        # === SETUP ===
        # Register in default context
        result = self.sr_client.post_subjects_subject_versions(
            subject="sub1", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        default_id1 = result.json()["id"]  # ID 1

        # Register another schema in default context
        result = self.sr_client.post_subjects_subject_versions(
            subject="sub2", data=json.dumps({"schema": schema2_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Register in ctx1 context (same subject name, different context)
        result = self.sr_client.post_subjects_subject_versions(
            subject=":.ctx1:sub1", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        ctx1_id1 = result.json()["id"]  # ID 1 in ctx1

        # Register unique subject only in ctx1 (for cross-context search test)
        # This subject name does NOT exist in default context
        result = self.sr_client.post_subjects_subject_versions(
            subject=":.ctx1:unique-sub", data=json.dumps({"schema": schema3_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        ctx1_unique_id = result.json()["id"]  # ID 2 in ctx1

        # Register a third schema in ctx1 to create an ID that doesn't exist in default
        # (for testing "ID only exists in non-default context")
        result = self.sr_client.post_subjects_subject_versions(
            subject=":.ctx1:ctx-only-sub",
            data=json.dumps({"schema": simple_proto_def, "schemaType": "PROTOBUF"}),
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        ctx1_only_id = result.json()["id"]  # ID 3 in ctx1, no ID 3 in default

        # === Test: Subject portion empty (sub().empty()) ===
        self.logger.info("Testing: Subject portion empty")

        # 1a. No subject param at all - uses default context
        result = self.sr_client.get_schemas_ids_id(id=default_id1)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["schema"], schema1_def)

        # 1b. Context-only param ":.ctx1:" - uses ctx1, no subject restriction
        result = self.sr_client.get_schemas_ids_id(id=ctx1_id1, subject=":.ctx1:")
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["schema"], schema1_def)

        # 1c. Explicit default context-only ":.:" - uses default, no subject restriction
        result = self.sr_client.get_schemas_ids_id(id=default_id1, subject=":.:")
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["schema"], schema1_def)

        # === Test: Non-default context (qualified, no cross-context search) ===
        self.logger.info("Testing: Non-default context")

        # 2a. Non-default context - schema found
        result = self.sr_client.get_schemas_ids_id(id=ctx1_id1, subject=":.ctx1:sub1")
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["schema"], schema1_def)

        # 2b. Non-default context - wrong subject for ID (no fallback for non-default ctx)
        result = self.sr_client.get_schemas_ids_id(
            id=ctx1_id1, subject=":.ctx1:wrong-sub"
        )
        self.assert_equal(result.status_code, requests.codes.not_found)

        # 2c. Non-default context - context doesn't exist
        result = self.sr_client.get_schemas_ids_id(
            id=default_id1, subject=":.nonexistent:sub1"
        )
        self.assert_equal(result.status_code, requests.codes.not_found)

        # === Test: Default context (implicit or explicit), schema found ===
        self.logger.info("Testing: Default context, schema found")

        # 3a. Unqualified subject exists in default context
        result = self.sr_client.get_schemas_ids_id(id=default_id1, subject="sub1")
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["schema"], schema1_def)

        # 3b. Explicit default context (:.:) - same behavior as unqualified
        result = self.sr_client.get_schemas_ids_id(id=default_id1, subject=":.:sub1")
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["schema"], schema1_def)

        # === Test: Cross-context search ===
        self.logger.info("Testing: Cross-context search")

        # 4a. Unqualified subject "unique-sub" not in default, but exists in ctx1
        # Should find it via cross-context search
        result = self.sr_client.get_schemas_ids_id(
            id=ctx1_unique_id, subject="unique-sub"
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["schema"], schema3_def)

        # 4b. Explicit default context (:.:) also triggers cross-context search
        result = self.sr_client.get_schemas_ids_id(
            id=ctx1_unique_id, subject=":.:unique-sub"
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["schema"], schema3_def)

        # === Test: Fallback without subject restriction ===
        self.logger.info("Testing: Fallback without subject restriction")

        # 5a. Subject "nonexistent-sub" doesn't exist anywhere, but ID exists in default
        # Should fallback to returning schema without subject check
        result = self.sr_client.get_schemas_ids_id(
            id=default_id1, subject="nonexistent-sub"
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["schema"], schema1_def)

        # === ERROR CASES ===
        self.logger.info("Testing error cases")

        # 6a. Schema ID doesn't exist at all
        result = self.sr_client.get_schemas_ids_id(id=99999)
        self.assert_equal(result.status_code, requests.codes.not_found)

        # 6b. Schema ID exists only in ctx1, no subject param (looks in default only)
        # ctx1_only_id (ID 3) only exists in ctx1, not in default context (which only has IDs 1-2)
        result = self.sr_client.get_schemas_ids_id(id=ctx1_only_id)
        self.assert_equal(result.status_code, requests.codes.not_found)

    @cluster(num_nodes=1)
    def test_get_schema_by_id_schema_endpoint(self):
        """Test GET /schemas/ids/{id}/schema returns the same schema as GET /schemas/ids/{id}["schema"]."""

        # Register a schema
        result = self.sr_client.post_subjects_subject_versions(
            subject="test-sub", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        schema_id = result.json()["id"]

        # Verify /schema endpoint returns raw schema matching the nested ["schema"] field
        result_full = self.sr_client.get_schemas_ids_id(id=schema_id)
        result_schema = self.sr_client.get_schemas_ids_id_schema(id=schema_id)

        self.assert_equal(result_full.status_code, requests.codes.ok)
        self.assert_equal(result_schema.status_code, requests.codes.ok)
        self.assert_equal(
            result_schema.json(), json.loads(result_full.json()["schema"])
        )

        # Test with subject parameter as well
        result_full = self.sr_client.get_schemas_ids_id(
            id=schema_id, subject="test-sub"
        )
        result_schema = self.sr_client.get_schemas_ids_id_schema(
            id=schema_id, subject="test-sub"
        )

        self.assert_equal(result_full.status_code, requests.codes.ok)
        self.assert_equal(result_schema.status_code, requests.codes.ok)
        self.assert_equal(
            result_schema.json(), json.loads(result_full.json()["schema"])
        )

        # Test error case - schema not found
        result_schema = self.sr_client.get_schemas_ids_id_schema(id=99999)
        self.assert_equal(result_schema.status_code, requests.codes.not_found)

    @cluster(num_nodes=1)
    def test_get_schema_versions_with_subject(self):
        """Test GET /schemas/ids/{id}/versions with subject query parameter for context lookup.

        Extended search (resolve_schema_id_extended) is already covered by
        test_get_schema_by_id_with_subject; here we only verify the param
        is wired up and the response format is correct.
        """

        # Register schema1 in default context under sub1 and sub2
        result = self.sr_client.post_subjects_subject_versions(
            subject="sub1", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        default_schema1_id = result.json()["id"]

        result = self.sr_client.post_subjects_subject_versions(
            subject="sub2", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Register schema1 in ctx1 context
        result = self.sr_client.post_subjects_subject_versions(
            subject=":.ctx1:sub1", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        ctx1_schema1_id = result.json()["id"]

        # Without subject param - returns versions from default context
        result = self.sr_client.get_schemas_ids_id_versions(id=default_schema1_id)
        self.assert_equal(result.status_code, requests.codes.ok)
        versions = result.json()
        self.assert_equal(len(versions), 2)
        subject_versions = {(v["subject"], v["version"]) for v in versions}
        self.assert_equal(subject_versions, {("sub1", 1), ("sub2", 1)})

        # With context-only param ":.ctx1:" - returns versions from ctx1
        result = self.sr_client.get_schemas_ids_id_versions(
            id=ctx1_schema1_id, subject=":.ctx1:"
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        versions = result.json()
        self.assert_equal(len(versions), 1)
        self.assert_equal(versions[0]["subject"], ":.ctx1:sub1")
        self.assert_equal(versions[0]["version"], 1)

        # Error case - schema ID not found in specified context
        result = self.sr_client.get_schemas_ids_id_versions(
            id=ctx1_schema1_id, subject=":.nonexistent:"
        )
        self.assert_equal(result.status_code, requests.codes.not_found)

    @cluster(num_nodes=1)
    def test_get_schema_subjects_with_subject(self):
        """Test GET /schemas/ids/{id}/subjects with subject query parameter for context lookup.

        Extended search (resolve_schema_id_extended) is already covered by
        test_get_schema_versions_with_subject; here we only verify the param
        is wired up and the response format is correct.
        """

        # Register schema1 in default context under sub1 and sub2
        result = self.sr_client.post_subjects_subject_versions(
            subject="sub1", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        default_schema1_id = result.json()["id"]

        result = self.sr_client.post_subjects_subject_versions(
            subject="sub2", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Register schema1 in ctx1 context
        result = self.sr_client.post_subjects_subject_versions(
            subject=":.ctx1:sub1", data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        ctx1_schema1_id = result.json()["id"]

        # Without subject param - returns subjects from default context
        result = self.sr_client.get_schemas_ids_id_subjects(id=default_schema1_id)
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(set(result.json()), {"sub1", "sub2"})

        # With context-only param ":.ctx1:" - returns subjects from ctx1
        result = self.sr_client.get_schemas_ids_id_subjects(
            id=ctx1_schema1_id, subject=":.ctx1:"
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json(), [":.ctx1:sub1"])

        # Error case - schema ID not found in specified context
        result = self.sr_client.get_schemas_ids_id_subjects(
            id=ctx1_schema1_id, subject=":.nonexistent:"
        )
        self.assert_equal(result.status_code, requests.codes.not_found)

    def _get_schema_count_by_context(self):
        """
        Query schema_count metrics and return a dict of context -> count.
        Only reports contexts that have schemas (count > 0).
        """
        samples = self.redpanda.metrics_samples(
            sample_patterns=["schema_count"],
            metrics_endpoint=MetricsEndpoint.METRICS,
            nodes=[random.choice(self.redpanda.nodes)],
        )

        self.logger.info(f"Got metrics samples: {samples}")

        if "schema_count" not in samples:
            return {}

        metrics = samples["schema_count"]

        context_counts = {}

        for sample in metrics.samples:
            if "context" in sample.labels:
                context = sample.labels["context"]
                context_counts[context] = context_counts.get(context, 0) + sample.value

        return context_counts

    def _refresh_cache(self):
        for n in self.redpanda.nodes:
            self.sr_client.get_subjects(hostname=n.account.hostname)

    def _schemas_in_context(self, context: str, expected_count: int):
        counts = self._get_schema_count_by_context()
        self.logger.info(
            f'Counts in context "{context}": {counts.get(context, 0)}, expected: {expected_count}'
        )
        return expected_count == counts.get(context, 0)

    @cluster(num_nodes=3)
    def test_schema_count_context_labels(self):
        """
        Test that schema_count metric includes context labels and correctly
        tracks schemas per context.
        """
        # Check metrics - should see 2 schemas in default context "."
        self.logger.info("Testing schema_count metric with context labels")

        # Initially should have no schemas in any context
        counts = self._get_schema_count_by_context()
        self.logger.info(f"Initial counts: {counts}")
        assert len(counts) == 0 or all(c == 0 for c in counts.values()), (
            f"Expected no schemas initially, got {counts}"
        )

        # Create schemas in default context (unqualified subjects)
        schema1 = json.dumps({"schema": schema1_def})
        schema2 = json.dumps({"schema": schema2_def})

        result = self.sr_client.post_subjects_subject_versions(
            subject="default-subject-1", data=schema1
        )
        assert result.status_code == requests.codes.ok

        result = self.sr_client.post_subjects_subject_versions(
            subject="default-subject-2", data=schema2
        )
        assert result.status_code == requests.codes.ok

        self._refresh_cache()

        wait_until(
            lambda: self._schemas_in_context(".", 2),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for two schemas in default context",
        )

        # Create schemas in custom context
        result = self.sr_client.post_subjects_subject_versions(
            subject=":.prod:my-subject-1", data=schema1
        )
        assert result.status_code == requests.codes.ok

        result = self.sr_client.post_subjects_subject_versions(
            subject=":.prod:my-subject-2", data=schema2
        )
        assert result.status_code == requests.codes.ok

        self._refresh_cache()

        wait_until(
            lambda: self._schemas_in_context(".prod", 2),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for two schemas in .prod context",
        )

        # Check metrics - should see schemas in both contexts
        counts = self._get_schema_count_by_context()
        self.logger.info(f"Counts after prod context: {counts}")
        assert "." in counts, f"Default context '.' not found in {counts}"
        assert counts["."] == 2, (
            f"Expected 2 schemas in default context, got {counts['.']}"
        )
        assert ".prod" in counts, f"Context '.prod' not found in {counts}"
        assert counts[".prod"] == 2, (
            f"Expected 2 schemas in .prod context, got {counts['.prod']}"
        )

        # Create schema in another context
        result = self.sr_client.post_subjects_subject_versions(
            subject=":.dev:test-subject", data=schema1
        )
        assert result.status_code == requests.codes.ok

        self._refresh_cache()

        wait_until(
            lambda: self._schemas_in_context(".dev", 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for one schema in .dev context",
        )

        # Check metrics - should see schemas in all three contexts
        counts = self._get_schema_count_by_context()
        self.logger.info(f"Counts after dev context: {counts}")
        assert counts["."] == 2
        assert counts[".prod"] == 2
        assert ".dev" in counts, f"Context '.dev' not found in {counts}"
        assert counts[".dev"] == 1, (
            f"Expected 1 schema in .dev context, got {counts['.dev']}"
        )

    @cluster(num_nodes=3)
    def test_schema_count_context_labels_empty_contexts(self):
        """
        Test that empty contexts (no schemas) are not reported in metrics.
        """
        self.logger.info("Testing empty context behavior")

        # Create a context by creating and then deleting all schemas
        schema1 = json.dumps({"schema": schema1_def})

        # Create schema in a context
        result = self.sr_client.post_subjects_subject_versions(
            subject=":.temp:subject-1", data=schema1
        )
        assert result.status_code == requests.codes.ok

        self._refresh_cache()

        # Verify it shows up in metrics
        wait_until(
            lambda: self._schemas_in_context(".temp", 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for one schema in .temp context",
        )

        # Delete the subject
        result = self.sr_client.delete_subject(subject=":.temp:subject-1")
        assert result.status_code == requests.codes.ok

        # Permanently delete it
        result = self.sr_client.delete_subject(
            subject=":.temp:subject-1", params={"permanent": "true"}
        )
        assert result.status_code == requests.codes.ok

        self._refresh_cache()

        # Context should now have 0 schemas
        wait_until(
            lambda: self._schemas_in_context(".temp", 0),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for one schema in .temp context",
        )

    @cluster(num_nodes=3)
    def test_schema_count_context_labels_after_restart(self):
        """
        Test that schema_count metrics are correctly recomputed after restart.
        """
        self.logger.info("Testing schema_count metrics after restart")

        # Create schemas in multiple contexts
        schema1 = json.dumps({"schema": schema1_def})
        schema2 = json.dumps({"schema": schema2_def})

        # Default context
        self.sr_client.post_subjects_subject_versions(
            subject="default-subject", data=schema1
        )

        # Custom contexts
        self.sr_client.post_subjects_subject_versions(
            subject=":.prod:prod-subject-1", data=schema1
        )
        self.sr_client.post_subjects_subject_versions(
            subject=":.prod:prod-subject-2", data=schema2
        )
        self.sr_client.post_subjects_subject_versions(
            subject=":.dev:dev-subject", data=schema1
        )

        self._refresh_cache()

        wait_until(
            lambda: self._schemas_in_context(".", 1)
            and self._schemas_in_context(".prod", 2)
            and self._schemas_in_context(".dev", 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for schemas to be registered in all contexts",
        )

        # Get counts before restart
        counts_before = self._get_schema_count_by_context()
        self.logger.info(f"Counts before restart: {counts_before}")

        # Restart all nodes
        self.logger.info("Restarting all nodes")
        self.redpanda.restart_nodes(self.redpanda.nodes)

        self._refresh_cache()

        wait_until(
            lambda: self._schemas_in_context(".", 1)
            and self._schemas_in_context(".prod", 2)
            and self._schemas_in_context(".dev", 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for schemas to be registered in all contexts post restart",
        )

        # Get counts after restart
        counts_after = self._get_schema_count_by_context()
        self.logger.info(f"Counts after restart: {counts_after}")

        # Verify counts are the same
        assert counts_after["."] == 1, (
            f"Default context count changed after restart: {counts_before['.']} -> {counts_after['.']}"
        )
        assert counts_after[".prod"] == 2, (
            f".prod context count changed after restart: {counts_before['.prod']} -> {counts_after['.prod']}"
        )
        assert counts_after[".dev"] == 1, (
            f".dev context count changed after restart: {counts_before['.dev']} -> {counts_after['.dev']}"
        )

    def _get_subject_count_by_context(self):
        """
        Query subject_count metrics and return a dict of (context, deleted) -> count.
        """
        samples = self.redpanda.metrics_samples(
            sample_patterns=["subject_count"],
            metrics_endpoint=MetricsEndpoint.METRICS,
            nodes=[random.choice(self.redpanda.nodes)],
        )

        self.logger.info(f"Subject count samples: {samples}")

        if "subject_count" not in samples:
            return {}

        metrics = samples["subject_count"]

        context_counts = {}
        for sample in metrics.samples:
            if "context" in sample.labels:
                context = sample.labels["context"]
                deleted = sample.labels["deleted"]
                key = (context, deleted)
                context_counts[key] = context_counts.get(key, 0) + sample.value

        return context_counts

    def _subjects_in_context(self, context: str, deleted: bool, expected_count: int):
        counts = self._get_subject_count_by_context()
        key = (context, "true" if deleted else "false")
        self.logger.info(
            f'Counts for context="{context}", deleted="{deleted}": {counts.get(key, 0)}, expected: {expected_count}'
        )
        return expected_count == counts.get(key, 0)

    @cluster(num_nodes=3)
    def test_subject_count_context_labels(self):
        """
        Test that subject_count metric includes both context and deleted labels.
        """
        self.logger.info("Testing subject_count metric with context and deleted labels")

        # Initially should have no subjects
        counts = self._get_subject_count_by_context()
        self.logger.info(f"Initial counts: {counts}")

        # Create subjects in default context
        schema1 = json.dumps({"schema": schema1_def})
        schema2 = json.dumps({"schema": schema2_def})

        self.sr_client.post_subjects_subject_versions(
            subject="default-subject-1", data=schema1
        )
        self.sr_client.post_subjects_subject_versions(
            subject="default-subject-2", data=schema2
        )

        self._refresh_cache()

        # Check metrics - should see 2 not-deleted subjects in default context
        wait_until(
            lambda: self._subjects_in_context(".", False, 2),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for two not-deleted subjects in default context",
        )

        # Create subjects in custom contexts
        self.sr_client.post_subjects_subject_versions(
            subject=":.prod:subject-1", data=schema1
        )
        self.sr_client.post_subjects_subject_versions(
            subject=":.prod:subject-2", data=schema2
        )
        self.sr_client.post_subjects_subject_versions(
            subject=":.dev:subject-1", data=schema1
        )

        self._refresh_cache()

        wait_until(
            lambda: self._subjects_in_context(".prod", False, 2)
            and self._subjects_in_context(".dev", False, 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for subjects in .prod and .dev contexts",
        )

        # Soft delete a subject in default context
        self.sr_client.delete_subject(subject="default-subject-1")

        self._refresh_cache()

        # Check metrics - should move from not-deleted to deleted
        wait_until(
            lambda: self._subjects_in_context(".", False, 1)
            and self._subjects_in_context(".", True, 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for one deleted and one not-deleted subject in default context",
        )

        # Revive the deleted subject
        self.sr_client.post_subjects_subject_versions(
            subject="default-subject-1", data=schema1
        )

        self._refresh_cache()
        wait_until(
            lambda: self._subjects_in_context(".", False, 2),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for two not-deleted subjects in default context after reviving",
        )

    @cluster(num_nodes=3)
    def test_subject_count_permanent_delete(self):
        """
        Test that permanent delete decrements the subject count correctly.
        """
        self.logger.info("Testing subject_count with permanent delete")

        schema1 = json.dumps({"schema": schema1_def})

        # Create a subject
        self.sr_client.post_subjects_subject_versions(
            subject=":.temp:subject-1", data=schema1
        )

        self._refresh_cache()

        wait_until(
            lambda: self._subjects_in_context(".temp", False, 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for one not-deleted subject in .temp context",
        )

        # Soft delete it
        self.sr_client.delete_subject(subject=":.temp:subject-1")

        wait_until(
            lambda: self._subjects_in_context(".temp", False, 0)
            and self._subjects_in_context(".temp", True, 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for one deleted subject in .temp context",
        )

        # Permanently delete it
        self.sr_client.delete_subject(
            subject=":.temp:subject-1", params={"permanent": "true"}
        )

        wait_until(
            lambda: self._subjects_in_context(".temp", False, 0)
            and self._subjects_in_context(".temp", True, 0),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for zero subjects in .temp context after permanent delete",
        )

    @cluster(num_nodes=3)
    def test_subject_soft_delete_versions(self):
        """
        Test that verifies that a subject shows up in deleted after
        all of the versions are soft deleted
        """

        schema1 = json.dumps({"schema": schema1_def})
        schema2 = json.dumps({"schema": schema2_def})

        self.sr_client.post_subjects_subject_versions(
            subject=":.temp:subject-1", data=schema1
        )

        self.sr_client.post_subjects_subject_versions(
            subject=":.temp:subject-1", data=schema2
        )

        version_schema_1 = self.sr_client.post_subjects_subject(
            subject=":.temp:subject-1", data=schema1
        ).json()["version"]
        version_schema_2 = self.sr_client.post_subjects_subject(
            subject=":.temp:subject-1", data=schema2
        ).json()["version"]

        self.logger.info(
            f"Registered schema1 with version {version_schema_1} and schema2 with version {version_schema_2}"
        )

        self._refresh_cache()

        wait_until(
            lambda: self._subjects_in_context(".temp", False, 1)
            and self._subjects_in_context(".temp", True, 0),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for one not-deleted subjects in .temp context",
        )

        # Now delete version 1 and verify that we don't see the subject as deleted
        self.sr_client.delete_subject_version(
            subject=":.temp:subject-1", version=version_schema_1
        )

        self._refresh_cache()

        subjects = self.sr_client.get_subjects(deleted=False).json()
        self.logger.info(f"Non-deleted Subjects: {subjects}")
        self.assert_in(":.temp:subject-1", subjects)

        wait_until(
            lambda: self._subjects_in_context(".temp", False, 1)
            and self._subjects_in_context(".temp", True, 0),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for one not-deleted subjects in .temp context post soft delete",
        )

        # Now delete version 2 and verify that the subject is now reported as deleted
        self.sr_client.delete_subject_version(
            subject=":.temp:subject-1", version=version_schema_2
        )
        self._refresh_cache()

        wait_until(
            lambda: self._subjects_in_context(".temp", False, 0)
            and self._subjects_in_context(".temp", True, 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for one deleted subjects in .temp context",
        )

        # Now resurrect verison 2 and verify we now see the subject back in the context
        self.sr_client.post_subjects_subject_versions(
            subject=":.temp:subject-1", data=schema2
        )
        self._refresh_cache()

        wait_until(
            lambda: self._subjects_in_context(".temp", False, 1)
            and self._subjects_in_context(".temp", True, 0),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for one not-deleted subjects in .temp context post resurrection",
        )

    @cluster(num_nodes=3)
    def test_subject_count_context_after_restart(self):
        """
        Verifies that after restart that the subject count metrics are correct
        for both deleted and undeleted subjects across contexts.
        """
        self.logger.info("Testing subject_count metrics after restart")

        schema1 = json.dumps({"schema": schema1_def})
        schema2 = json.dumps({"schema": schema2_def})

        # Create subjects in default context
        self.sr_client.post_subjects_subject_versions(
            subject="default-subject-1", data=schema1
        )
        self.sr_client.post_subjects_subject_versions(
            subject="default-subject-2", data=schema2
        )

        # Create subjects in .prod context
        self.sr_client.post_subjects_subject_versions(
            subject=":.prod:prod-subject-1", data=schema1
        )
        self.sr_client.post_subjects_subject_versions(
            subject=":.prod:prod-subject-2", data=schema2
        )

        # Create subjects in .dev context
        self.sr_client.post_subjects_subject_versions(
            subject=":.dev:dev-subject-1", data=schema1
        )

        self._refresh_cache()

        # Verify initial state - all subjects should be undeleted
        wait_until(
            lambda: self._subjects_in_context(".", False, 2)
            and self._subjects_in_context(".prod", False, 2)
            and self._subjects_in_context(".dev", False, 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for subjects to be registered in all contexts",
        )

        # Soft delete one subject in each context
        self.sr_client.delete_subject(subject="default-subject-1")
        self.sr_client.delete_subject(subject=":.prod:prod-subject-1")

        self._refresh_cache()

        # Verify we have both deleted and undeleted subjects
        wait_until(
            lambda: self._subjects_in_context(".", False, 1)
            and self._subjects_in_context(".", True, 1)
            and self._subjects_in_context(".prod", False, 1)
            and self._subjects_in_context(".prod", True, 1)
            and self._subjects_in_context(".dev", False, 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for deleted subjects in contexts",
        )

        # Get counts before restart
        counts_before = self._get_subject_count_by_context()
        self.logger.info(f"Counts before restart: {counts_before}")

        # Restart all nodes
        self.logger.info("Restarting all nodes")
        self.redpanda.restart_nodes(self.redpanda.nodes)

        self._refresh_cache()

        # Verify counts are restored after restart
        wait_until(
            lambda: self._subjects_in_context(".", False, 1)
            and self._subjects_in_context(".", True, 1)
            and self._subjects_in_context(".prod", False, 1)
            and self._subjects_in_context(".prod", True, 1)
            and self._subjects_in_context(".dev", False, 1),
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Timed out waiting for subjects to be restored after restart",
        )

        # Get counts after restart
        counts_after = self._get_subject_count_by_context()
        self.logger.info(f"Counts after restart: {counts_after}")

        # Verify counts match before and after restart
        assert counts_after[(".", "false")] == 1, (
            f"Default context undeleted count changed after restart: "
            f"{counts_before.get(('.', 'false'), 0)} -> {counts_after.get(('.', 'false'), 0)}"
        )
        assert counts_after[(".", "true")] == 1, (
            f"Default context deleted count changed after restart: "
            f"{counts_before.get(('.', 'true'), 0)} -> {counts_after.get(('.', 'true'), 0)}"
        )
        assert counts_after[(".prod", "false")] == 1, (
            f".prod context undeleted count changed after restart: "
            f"{counts_before.get(('.prod', 'false'), 0)} -> {counts_after.get(('.prod', 'false'), 0)}"
        )
        assert counts_after[(".prod", "true")] == 1, (
            f".prod context deleted count changed after restart: "
            f"{counts_before.get(('.prod', 'true'), 0)} -> {counts_after.get(('.prod', 'true'), 0)}"
        )
        assert counts_after[(".dev", "false")] == 1, (
            f".dev context undeleted count changed after restart: "
            f"{counts_before.get(('.dev', 'false'), 0)} -> {counts_after.get(('.dev', 'false'), 0)}"
        )

    def _post_new_schema(self, subject: str, schema: str, context: str | None = None):
        subject = subject if context is None else f":.{context}:{subject}"

        result = self.sr_client.post_subjects_subject_versions(
            subject=subject, data=schema
        )

        self.logger.info(f"result: {result}, {result.text}")

        self.assert_equal(result.status_code, requests.codes.ok)

    @cluster(num_nodes=1)
    def test_context_reference_format(self):
        """
        This test verifies the behavior of the new paramater to
        GET /subjects/{subject}/versions/{version} that will make unqualified references
        qualified.
        """
        schema_type = "proto"

        base = base_schemas[schema_type]
        base2 = base2_schemas[schema_type]
        multi_dependent = multi_dependent_schemas[schema_type]

        ctx = f"ctx-{schema_type}"
        base_subject = "base"
        base2_subject = "base2"
        multi_dependent_subject = "multi-dependent"

        base_data = json.dumps({"schema": base["schema"], "schemaType": base["type"]})
        base2_data = json.dumps(
            {"schema": base2["schema"], "schemaType": base2["type"]}
        )

        self._post_new_schema(subject=base_subject, schema=base_data)
        self._post_new_schema(subject=base2_subject, schema=base2_data)

        self._post_new_schema(subject=base_subject, schema=base_data, context=ctx)
        self._post_new_schema(subject=base2_subject, schema=base2_data, context=ctx)

        multi_dependent_default = {
            "schema": multi_dependent["schema"],
            "schemaType": multi_dependent["type"],
            "references": copy.deepcopy(multi_dependent["references"]),
        }
        multi_dependent_default["references"][0]["subject"] = base_subject
        multi_dependent_default["references"][1]["subject"] = base2_subject

        multi_dependent_default_json = json.dumps(multi_dependent_default)

        multi_dependent_context = {
            "schema": multi_dependent["schema"],
            "schemaType": multi_dependent["type"],
            "references": copy.deepcopy(multi_dependent["references"]),
        }
        multi_dependent_context["references"][0]["subject"] = f":.{ctx}:{base_subject}"
        multi_dependent_context["references"][1]["subject"] = base2_subject

        multi_dependent_context_json = json.dumps(multi_dependent_context)

        self._post_new_schema(
            subject=multi_dependent_subject, schema=multi_dependent_default_json
        )
        self._post_new_schema(
            subject=multi_dependent_subject,
            schema=multi_dependent_context_json,
            context=ctx,
        )

        result = self.sr_client.get_subjects_subject_versions_version(
            subject=multi_dependent_subject,
            version=1,
            reference_format=ReferenceFormat.NONE,
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        result_json = result.json()
        self.logger.info(f"result_json: {result_json}")
        self.assert_equal(len(result_json["references"]), 2)
        self.assert_equal(result_json["references"][0]["subject"], base_subject)
        self.assert_equal(result_json["references"][1]["subject"], base2_subject)

        result = self.sr_client.get_subjects_subject_versions_version(
            subject=multi_dependent_subject,
            version=1,
            reference_format=ReferenceFormat.QUALIFIED,
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        result_json = result.json()
        self.logger.info(f"result_json: {result_json}")
        self.assert_equal(len(result_json["references"]), 2)
        self.assert_equal(result_json["references"][0]["subject"], base_subject)
        self.assert_equal(result_json["references"][1]["subject"], base2_subject)

        result = self.sr_client.get_subjects_subject_versions_version(
            subject=f":.{ctx}:{multi_dependent_subject}",
            version=1,
            reference_format=ReferenceFormat.NONE,
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        result_json = result.json()
        self.logger.info(f"result_json: {result_json}")
        self.assert_equal(len(result_json["references"]), 2)
        self.assert_equal(
            result_json["references"][0]["subject"], f":.{ctx}:{base_subject}"
        )
        self.assert_equal(result_json["references"][1]["subject"], base2_subject)

        result = self.sr_client.get_subjects_subject_versions_version(
            subject=f":.{ctx}:{multi_dependent_subject}",
            version=1,
            reference_format=ReferenceFormat.QUALIFIED,
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        result_json = result.json()
        self.logger.info(f"result_json: {result_json}")
        self.assert_equal(len(result_json["references"]), 2)
        self.assert_equal(
            result_json["references"][0]["subject"], f":.{ctx}:{base_subject}"
        )
        self.assert_equal(
            result_json["references"][1]["subject"], f":.{ctx}:{base2_subject}"
        )

    @cluster(num_nodes=1)
    def test_reregister_schema_returns_correct_context_definition(self):
        """
        Regression test for context-aware schema definition lookup.

        When re-registering an existing schema, the response should contain
        the schema definition from the correct context, not from the schema
        with the same schema ID in the default context.
        """
        # Register schema A in .ctx1 - gets ID 1
        ctx1_subject = ":.ctx1:test-subject"
        result = self.sr_client.post_subjects_subject_versions(
            subject=ctx1_subject, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["id"], 1)

        # Register different schema B in default context - also gets ID 1
        default_subject = "test-subject"
        result = self.sr_client.post_subjects_subject_versions(
            subject=default_subject, data=json.dumps({"schema": schema2_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        self.assert_equal(result.json()["id"], 1)  # Same numeric ID, different context

        # Re-register schema A in .ctx1 (should return existing)
        result = self.sr_client.post_subjects_subject_versions(
            subject=ctx1_subject, data=json.dumps({"schema": schema1_def})
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # The returned schema should be schema1_def (from .ctx1), NOT schema2_def
        self.assert_equal(result.json()["schema"], schema1_def)

    @cluster(num_nodes=1)
    def test_context_reference_detection_blocks_deletion(self):
        """
        Regression test for context-aware reference detection.

        Schema references within a non-default context should be correctly
        detected by both the referenced_by API and deletion prevention logic.
        """
        ctx = "reftest"
        base_subject = f":.{ctx}:base"
        dependent_subject = f":.{ctx}:dependent"

        # Register base schema in context
        base_data = json.dumps({"schema": simple_proto_def, "schemaType": "PROTOBUF"})
        result = self.sr_client.post_subjects_subject_versions(
            subject=base_subject, data=base_data
        )
        self.assert_equal(result.status_code, requests.codes.ok)

        # Register dependent schema that references base
        dependent_data = json.dumps(
            {
                "schema": imported_proto_def,
                "schemaType": "PROTOBUF",
                "references": [
                    {"name": "simple", "subject": base_subject, "version": 1}
                ],
            }
        )
        result = self.sr_client.post_subjects_subject_versions(
            subject=dependent_subject, data=dependent_data
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        dependent_id = result.json()["id"]

        # Part A: referenced_by API should return the dependent schema's ID
        result = self.sr_client.get_subjects_subject_versions_version_referenced_by(
            subject=base_subject, version=1
        )
        self.assert_equal(result.status_code, requests.codes.ok)
        referenced_by = result.json()
        self.assert_equal(
            len(referenced_by),
            1,
            f"Expected 1 reference, got {len(referenced_by)}",
        )
        self.assert_equal(referenced_by[0], dependent_id)

        # Part B: Deletion of base should be blocked (schema is referenced)
        result = self.sr_client.delete_subject(subject=base_subject)
        self.assert_equal(
            result.status_code,
            requests.codes.unprocessable_entity,
            "Expected deletion to be blocked due to reference",
        )
        self.assert_equal(result.json()["error_code"], 42206)

        # Part C: Delete dependent first, then base should succeed
        result = self.sr_client.delete_subject(subject=dependent_subject)
        self.assert_equal(result.status_code, requests.codes.ok)

        # Now base can be deleted
        result = self.sr_client.delete_subject(subject=base_subject)
        self.assert_equal(result.status_code, requests.codes.ok)

    @cluster(num_nodes=1)
    def test_reserved_subject_names_rejected(self):
        """
        Verify that reserved subject names (__GLOBAL, __EMPTY) and the
        reserved context (.__GLOBAL) are rejected on post_subject_versions,
        put_config_subject, & put_mode_subject.

        The .__GLOBAL context is allowed on config/mode PUT endpoints but
        rejected on the register endpoint.
        """
        schema_data = json.dumps({"schema": schema1_def})
        config_data = json.dumps({"compatibility": "BACKWARD"})
        mode_data = json.dumps({"mode": "READWRITE"})

        global_subject = "__GLOBAL"
        empty_subject = "__EMPTY"
        global_context = ":.__GLOBAL:"
        global_context_subject = f"{global_context}subject"

        # Reserved subject names are rejected on all endpoints
        for subject in [global_subject, empty_subject]:
            result = self.sr_client.post_subjects_subject_versions(
                subject=subject, data=schema_data
            )
            self.assert_equal(result.status_code, 422)
            self.assert_equal(result.json()["error_code"], 42208)

            result = self.sr_client.set_config_subject(
                subject=subject, data=config_data
            )
            self.assert_equal(result.status_code, 422)
            self.assert_equal(result.json()["error_code"], 42208)

            result = self.sr_client.set_mode_subject(subject=subject, data=mode_data)
            self.assert_equal(result.status_code, 422)
            self.assert_equal(result.json()["error_code"], 42208)

        # .__GLOBAL context is rejected on register but allowed on
        # config/mode PUT endpoints (is_config_or_mode::yes)
        for subject in [global_context, global_context_subject]:
            result = self.sr_client.post_subjects_subject_versions(
                subject=subject, data=schema_data
            )
            self.assert_equal(result.status_code, 422)
            self.assert_equal(result.json()["error_code"], 42208)

            result = self.sr_client.set_config_subject(
                subject=subject, data=config_data
            )
            self.assert_not_equal(result.status_code, 422)

            result = self.sr_client.set_mode_subject(subject=subject, data=mode_data)
            self.assert_not_equal(result.status_code, 422)

    @cluster(num_nodes=4)
    def test_context_name_strategy(self):
        """
        Verify that a Confluent Java serializer configured with
        context.name.strategy registers schemas under the correct
        context-qualified subject and can round-trip produce/consume.
        """
        topic = "serde-topic-context-strategy"
        context = "myctx"
        self._create_topic(topic=topic)

        client = self._get_serde_client(
            SchemaType.AVRO,
            SerdeClientType.Java,
            topic,
            5,
            context_name_strategy="com.redpanda.TopicContextNameStrategy",
            context_name=context,
        )
        client.start()
        client.wait()

        # Verify the schema was registered under the context-qualified subject
        result = self.sr_client.get_subjects()
        subjects = result.json()
        expected_subject = f":.{context}:{topic}-value"
        assert expected_subject in subjects, (
            f"Expected subject {expected_subject} not found in {subjects}"
        )


class SchemaRegistryBasicAuthTest(SchemaRegistryEndpoints):
    """
    Test schema registry against a redpanda cluster with HTTP Basic Auth enabled.
    """

    def __init__(self, context):
        security = SecurityConfig()
        security.enable_sasl = True
        security.endpoint_authn_method = "sasl"

        schema_registry_config = SchemaRegistryConfig()
        schema_registry_config.authn_method = "http_basic"
        schema_registry_config.mode_mutability = True

        super(SchemaRegistryBasicAuthTest, self).__init__(
            context, security=security, schema_registry_config=schema_registry_config
        )

        superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.user = SaslCredentials("user", "panda012345678", superuser.mechanism)
        public_user = SaslCredentials("red", "panda012345678", superuser.mechanism)

        self.super_auth = (superuser.username, superuser.password)
        self.user_auth = (self.user.username, self.user.password)
        self.public_auth = (public_user.username, public_user.password)

    def _init_users(self):
        admin = Admin(self.redpanda)
        admin.create_user(
            username=self.user.username,
            password=self.user.password,
            algorithm=self.user.mechanism,
            await_exists=True,
        )

    @cluster(num_nodes=3)
    def test_schemas_types(self):
        """
        Verify the schema registry returns the supported types
        """
        self._init_users()

        result_raw = self.sr_client.get_schemas_types(auth=self.public_auth)
        assert result_raw.json()["error_code"] == 40101

        self.logger.debug("Request schema types with default accept header")
        result_raw = self.sr_client.get_schemas_types(auth=self.super_auth)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert set(result) == {"JSON", "PROTOBUF", "AVRO"}

    @cluster(num_nodes=3)
    def test_get_schema_id_versions(self):
        """
        Verify schema versions
        """
        self._init_users()

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"

        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject, data=schema_1_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Checking schema 1 versions")
        result_raw = self.sr_client.get_schemas_ids_id_versions(
            id=1, auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.get_schemas_ids_id_versions(
            id=1, auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [{"subject": subject, "version": 1}]

    @cluster(num_nodes=3)
    def test_get_subjects(self):
        """
        Verify getting subjects
        """
        self._init_users()

        topics = ["a", "aa", "b", "ab", "bb"]

        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Posting schemas 1 as subject keys")

        def post(topic):
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=f"{topic}-key", data=schema_1_data, auth=self.super_auth
            )
            self.logger.debug(result_raw)
            assert result_raw.status_code == requests.codes.ok

        for t in topics:
            post(t)

        def get_subjects(prefix: Optional[str]):
            result_raw = self.sr_client.get_subjects(
                subject_prefix=prefix, auth=self.super_auth
            )
            assert result_raw.status_code == requests.codes.ok

            return result_raw.json()

        assert len(get_subjects(prefix=None)) == 5
        assert len(get_subjects(prefix="")) == 5
        assert len(get_subjects(prefix="a")) == 3
        assert len(get_subjects(prefix="aa")) == 1
        assert len(get_subjects(prefix="aaa")) == 0
        assert len(get_subjects(prefix="b")) == 2
        assert len(get_subjects(prefix="bb")) == 1

    @cluster(num_nodes=3)
    def test_post_subjects_subject_versions(self):
        """
        Verify posting a schema
        """
        self._init_users()

        topic = create_topic_names(1)[0]

        schema_1_data = json.dumps({"schema": schema1_def})

        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data, auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.debug("Get subjects")
        result_raw = self.sr_client.get_subjects(auth=self.public_auth)
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.get_subjects(auth=self.super_auth)
        assert result_raw.json() == [f"{topic}-key"]

        self.logger.debug("Get schema versions for subject key")
        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-key", auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-key", auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1]

        self.logger.debug("Get latest schema version for subject key")
        result_raw = self.sr_client.get_subjects_subject_versions_version(
            subject=f"{topic}-key", version="latest", auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.get_subjects_subject_versions_version(
            subject=f"{topic}-key", version="latest", auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == f"{topic}-key"
        assert result["version"] == 1

        self.logger.debug("Get latest (-1) schema version for subject key")
        result_raw = self.sr_client.get_subjects_subject_versions_version(
            subject=f"{topic}-key", version="-1", auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["subject"] == f"{topic}-key"
        assert result["version"] == 1

        self.logger.debug("Get schema version 1")
        result_raw = self.sr_client.get_schemas_ids_id(id=1, auth=self.public_auth)
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.get_schemas_ids_id(id=1, auth=self.super_auth)
        assert result_raw.status_code == requests.codes.ok

    @cluster(num_nodes=3)
    def test_post_subjects_subject(self):
        """
        Verify posting a schema
        """
        self._init_users()

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"

        self.logger.info("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject,
            data=json.dumps({"schema": schema1_def}),
            auth=self.super_auth,
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        result_raw = self.sr_client.post_subjects_subject(
            subject=subject,
            data=json.dumps({"schema": schema1_def}),
            auth=self.public_auth,
        )
        assert result_raw.json()["error_code"] == 40101

        self.logger.info("Posting existing schema should be success")
        result_raw = self.sr_client.post_subjects_subject(
            subject=subject,
            data=json.dumps({"schema": schema1_def}),
            auth=self.super_auth,
        )
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
        self._init_users()

        self.logger.debug("Get initial global config")
        result_raw = self.sr_client.get_config(auth=self.public_auth)
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.get_config(auth=self.super_auth)
        assert result_raw.json()["compatibilityLevel"] == "BACKWARD"

        self.logger.debug("Set global config")
        result_raw = self.sr_client.set_config(
            data=json.dumps({"compatibility": "FULL"}), auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.set_config(
            data=json.dumps({"compatibility": "FULL"}), auth=self.super_auth
        )
        assert result_raw.json()["compatibility"] == "FULL"

        schema_1_data = json.dumps({"schema": schema1_def})

        topic = create_topic_names(1)[0]

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data, auth=self.super_auth
        )

        self.logger.debug("Set subject config")
        self.logger.debug("Set subject config")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "BACKWARD_TRANSITIVE"}),
            auth=self.public_auth,
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "BACKWARD_TRANSITIVE"}),
            auth=self.super_auth,
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["compatibility"] == "BACKWARD_TRANSITIVE"

        self.logger.debug("Get subject config - should be overriden")
        result_raw = self.sr_client.get_config_subject(
            subject=f"{topic}-key", auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.get_config_subject(
            subject=f"{topic}-key", auth=self.super_auth
        )
        assert result_raw.json()["compatibilityLevel"] == "BACKWARD_TRANSITIVE"

        global_config = self.sr_client.get_config(auth=self.super_auth).json()

        old_config = result_raw.json()

        result_raw = self.sr_client.delete_config_subject(
            subject=f"{topic}-key", auth=self.super_auth
        )
        assert (
            result_raw.json()["compatibilityLevel"] == old_config["compatibilityLevel"]
        )
        # , f"{json.dumps(result_raw.json(), indent=1)}, {json.dumps(global_config, indent=1)}"

        result_raw = self.sr_client.get_config_subject(
            subject=f"{topic}-key", fallback=True, auth=self.super_auth
        )
        assert (
            result_raw.json()["compatibilityLevel"]
            == global_config["compatibilityLevel"]
        )

    @cluster(num_nodes=3)
    def test_mode(self):
        """
        Smoketest mode endpoints
        """
        self._init_users()

        self.logger.debug("Get initial global mode")
        result_raw = self.sr_client.get_mode(auth=self.public_auth)
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.get_mode(auth=self.user_auth)
        assert result_raw.json()["mode"] == "READWRITE"

        result_raw = self.sr_client.get_mode(auth=self.super_auth)
        assert result_raw.json()["mode"] == "READWRITE"

        self.logger.debug("Set global mode")
        result_raw = self.sr_client.set_mode(
            data=json.dumps({"mode": "READONLY"}), auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.set_mode(
            data=json.dumps({"mode": "READONLY"}), auth=self.user_auth
        )
        assert result_raw.json()["error_code"] == 403

        result_raw = self.sr_client.set_mode(
            data=json.dumps({"mode": "READONLY"}), auth=self.super_auth
        )
        assert result_raw.json()["mode"] == "READONLY"

        sub = "test-sub"
        self.logger.debug("Set subject mode")
        result_raw = self.sr_client.set_mode_subject(
            subject=sub, data=json.dumps({"mode": "READONLY"}), auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.set_mode_subject(
            subject=sub, data=json.dumps({"mode": "READONLY"}), auth=self.user_auth
        )
        assert result_raw.json()["error_code"] == 403

        result_raw = self.sr_client.set_mode_subject(
            subject=sub, data=json.dumps({"mode": "READONLY"}), auth=self.super_auth
        )
        assert result_raw.json()["mode"] == "READONLY"

        self.logger.debug("Delete subject mode")
        result_raw = self.sr_client.delete_mode_subject(
            subject=sub, auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.delete_mode_subject(
            subject=sub, auth=self.user_auth
        )
        assert result_raw.json()["error_code"] == 403

        result_raw = self.sr_client.delete_mode_subject(
            subject=sub, auth=self.super_auth
        )
        assert result_raw.json()["mode"] == "READONLY"

    @cluster(num_nodes=3)
    def test_post_compatibility_subject_version(self):
        """
        Verify compatibility
        """
        self._init_users()

        topic = create_topic_names(1)[0]

        self.logger.debug("Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set subject config - NONE")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "NONE"}),
            auth=self.super_auth,
        )
        assert result_raw.status_code == requests.codes.ok

        result_raw = self.sr_client.post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=schema_1_data, auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        self.logger.debug("Check compatibility none, no default")
        result_raw = self.sr_client.post_compatibility_subject_version(
            subject=f"{topic}-key", version=1, data=schema_1_data, auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["is_compatible"] == True

    @cluster(num_nodes=3)
    def test_delete_subject(self):
        """
        Verify delete subject
        """
        self._init_users()

        topic = create_topic_names(1)[0]

        self.logger.debug("Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Soft delete subject")
        result_raw = self.sr_client.delete_subject(
            subject=f"{topic}-key", auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.delete_subject(
            subject=f"{topic}-key", auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [1]

        self.logger.debug("Permanently delete subject")
        result_raw = self.sr_client.delete_subject(
            subject=f"{topic}-key", permanent=True, auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.delete_subject(
            subject=f"{topic}-key", permanent=True, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

    @cluster(num_nodes=3)
    def test_delete_subject_version(self):
        """
        Verify delete subject version
        """
        self._init_users()

        topic = create_topic_names(1)[0]

        self.logger.debug("Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Set subject config - NONE")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "NONE"}),
            auth=self.super_auth,
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Soft delete version 1")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=1, auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=1, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Permanently delete version 1")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=1, permanent=True, auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=1, permanent=True, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

    @cluster(num_nodes=3)
    def test_protobuf(self):
        """
        Verify basic protobuf functionality
        """
        self._init_users()

        self.logger.info("Posting failed schema should be 422")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject="imported",
            data=json.dumps({"schema": imported_proto_def, "schemaType": "PROTOBUF"}),
            auth=self.super_auth,
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.unprocessable_entity

        self.logger.info("Posting simple as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject="simple",
            data=json.dumps({"schema": simple_proto_def, "schemaType": "PROTOBUF"}),
            auth=self.super_auth,
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1

        self.logger.info("Posting imported as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject="imported",
            data=json.dumps(
                {
                    "schema": imported_proto_def,
                    "schemaType": "PROTOBUF",
                    "references": [
                        {"name": "simple", "subject": "simple", "version": 1}
                    ],
                }
            ),
            auth=self.super_auth,
        )
        self.logger.info(result_raw)
        self.logger.info(result_raw.content)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 2

        result_raw = self.sr_client.request(
            "GET",
            "subjects/simple/versions/1/schema",
            headers=HTTP_GET_HEADERS,
            auth=self.super_auth,
        )
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.text.strip() == simple_proto_def.strip()

        result_raw = self.sr_client.request(
            "GET", "schemas/ids/1", headers=HTTP_GET_HEADERS, auth=self.super_auth
        )
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert result["schemaType"] == "PROTOBUF"
        assert result["schema"].strip() == simple_proto_def.strip()

        # Regular user should fail
        result_raw = self.sr_client.get_subjects_subject_versions_version_referenced_by(
            "simple", 1, auth=self.public_auth
        )
        assert result_raw.json()["error_code"] == 40101

        result_raw = self.sr_client.get_subjects_subject_versions_version_referenced_by(
            "simple", 1, auth=self.super_auth
        )
        self.logger.info(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json() == [2]

    @cluster(num_nodes=3)
    def test_delete_subject_bug(self):
        topic = "foo"
        self.logger.debug("Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Register a schema against a subject")
        schema_2_data = json.dumps({"schema": schema2_def})

        self.logger.debug("Posting schema 2 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_2_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Soft delete subject 1 version 1")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=1, auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Code: {result_raw.status_code}"
        )
        assert result_raw.json() == 1, f"Json: {result_raw.json()}"

        self.logger.debug("Soft delete subject 1 version 2")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=2, auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Code: {result_raw.status_code}"
        )
        assert result_raw.json() == 2, f"Json: {result_raw.json()}"

        self.logger.debug("Posting schema 1 - again - as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 1, f"Json: {result_raw.json()}"

        self.logger.debug("Get subject versions")
        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-key", auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Code: {result_raw.status_code}"
        )
        assert result_raw.json() == [3], f"Json: {result_raw.json()}"

        self.logger.debug("Posting schema 2 - again - as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_2_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["id"] == 2, f"Json: {result_raw.json()}"

        self.logger.debug("Get subject versions")
        result_raw = self.sr_client.get_subjects_subject_versions(
            subject=f"{topic}-key", auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Code: {result_raw.status_code}"
        )
        assert result_raw.json() == [3, 4], f"Json: {result_raw.json()}"

    @cluster(num_nodes=3)
    def test_delete_subject_last_clears_config(self):
        topic = "foo"

        self.logger.debug("Set subject config - NONE")
        result_raw = self.sr_client.set_config_subject(
            subject=f"{topic}-key",
            data=json.dumps({"compatibility": "NONE"}),
            auth=self.super_auth,
        )
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Register a schema against a subject")
        schema_1_data = json.dumps({"schema": schema1_def})
        schema_3_data = json.dumps({"schema": schema3_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug("Get subject config - should be overriden")
        result_raw = self.sr_client.get_config_subject(
            subject=f"{topic}-key", auth=self.super_auth
        )
        assert result_raw.json()["compatibilityLevel"] == "NONE"

        self.logger.debug("Soft delete subject 1 version 1")
        result_raw = self.sr_client.delete_subject_version(
            subject=f"{topic}-key", version=1, auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.ok, (
            f"Code: {result_raw.status_code}"
        )
        assert result_raw.json() == 1, f"Json: {result_raw.json()}"

        self.logger.debug("Get subject config - should fail")
        result_raw = self.sr_client.get_config_subject(
            subject=f"{topic}-key", auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40408

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_1_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok

        self.logger.debug(
            "Posting incompatible schema 3 as a subject key - expect conflict"
        )
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=f"{topic}-key", data=schema_3_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.conflict

        self.logger.debug("Get subject config - should fail")
        result_raw = self.sr_client.get_config_subject(
            subject=f"{topic}-key", auth=self.super_auth
        )
        assert result_raw.status_code == requests.codes.not_found
        assert result_raw.json()["error_code"] == 40408

    @cluster(num_nodes=3)
    def test_hard_delete_subject_deletes_schema(self):
        subject = "example_topic-key"
        schema_1_data = json.dumps({"schema": schema1_def})

        self.logger.debug("Posting schema 1 as a subject key")
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=subject, data=schema_1_data, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok, (
            f"Code: {result_raw.status_code}"
        )
        assert result_raw.json()["id"] == 1, f"Json: {result_raw.json()}"

        self.logger.debug("Soft delete subject")
        result_raw = self.sr_client.delete_subject(
            subject=subject, permanent=False, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok, (
            f"Code: {result_raw.status_code}"
        )

        self.logger.debug("Then hard delete subject")
        result_raw = self.sr_client.delete_subject(
            subject=subject, permanent=True, auth=self.super_auth
        )
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok, (
            f"Code: {result_raw.status_code}"
        )

        def schema_no_longer_present():
            self.logger.debug("Sending get schema 1")
            result_raw = self.sr_client.get_schemas_ids_id(id=1, auth=self.super_auth)
            self.logger.debug(result_raw)
            assert result_raw.status_code == requests.codes.not_found, (
                f"Code: {result_raw.status_code}"
            )
            assert result_raw.json()["error_code"] == 40403, (
                f"Json: {result_raw.json()}"
            )
            return True

        self.logger.debug("Wait until get schema 1 now eventually fails")
        wait_until(
            schema_no_longer_present,
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Failed to delete schema 1 in time",
        )


class SchemaRegistryTest(SchemaRegistryTestMethods):
    """
    Test schema registry against a redpanda cluster without auth.

    This derived class inherits all the tests from SchemaRegistryTestMethods.
    """

    def __init__(self, context):
        super(SchemaRegistryTest, self).__init__(context)

    @cluster(num_nodes=3)
    def test_nodejs_serde_client(self):
        brokers = self.redpanda.brokers(limit=1)
        sr = self.redpanda.schema_reg(limit=1)
        node = "/opt/nodejs/bin/node"
        protobuf_serde = "/opt/redpanda-tests/nodejs/protobuf-serde/src/index.js"
        cmd = f"{node} {protobuf_serde} --brokers={brokers} --sr={sr}"
        self.logger.info(f"running: {cmd}")
        exit_code = self.redpanda.nodes[0].account.ssh(cmd)
        assert exit_code == 0, (
            "expected exit code 0 from nodejs serde client, got {exit_code}"
        )


class SchemaRegistryAutoAuthTest(SchemaRegistryTestMethods):
    """
    Test schema registry against a redpanda cluster with Auto Auth enabled.

    This derived class inherits all the tests from SchemaRegistryTestMethods.
    """

    def __init__(self, context):
        security = SecurityConfig()
        security.kafka_enable_authorization = True
        security.endpoint_authn_method = "sasl"
        security.auto_auth = True

        super(SchemaRegistryAutoAuthTest, self).__init__(context, security=security)


class SchemaRegistryMTLSBase(SchemaRegistryEndpoints):
    topics = [
        TopicSpec(),
    ]

    def __init__(self, *args, **kwargs):
        super(SchemaRegistryMTLSBase, self).__init__(*args, **kwargs)

        self.security = SecurityConfig()

        super_username, super_password, super_algorithm = (
            self.redpanda.SUPERUSER_CREDENTIALS
        )
        self.admin_user = User(0)
        self.admin_user.username = super_username
        self.admin_user.password = super_password
        self.admin_user.algorithm = super_algorithm

        self.schema_registry_config = SchemaRegistryConfig()
        self.schema_registry_config.require_client_auth = True

    def setup_cluster(self, basic_auth_enabled: bool = False):
        tls_manager = tls.TLSCertManager(self.logger)
        self.security.require_client_auth = True
        self.security.principal_mapping_rules = "RULE:.*CN=(.*).*/$1/"

        if basic_auth_enabled:
            self.security.kafka_enable_authorization = True
            self.security.endpoint_authn_method = "sasl"
            self.schema_registry_config.authn_method = "http_basic"
        else:
            self.security.endpoint_authn_method = "mtls_identity"

        # cert for principal with no explicitly granted permissions
        self.admin_user.certificate = tls_manager.create_cert(
            socket.gethostname(),
            common_name=self.admin_user.username,
            name="test_admin_client",
        )

        self.security.tls_provider = PandaProxyTLSProvider(tls_manager)

        self.schema_registry_config.client_key = self.admin_user.certificate.key
        self.schema_registry_config.client_crt = self.admin_user.certificate.crt

        self.redpanda.set_security_settings(self.security)
        self.redpanda.set_schema_registry_settings(self.schema_registry_config)
        self.redpanda.start()

        admin = Admin(self.redpanda)

        # Create the users
        admin.create_user(
            self.admin_user.username,
            self.admin_user.password,
            self.admin_user.algorithm,
            await_exists=True,
        )

        # Create topic with rpk instead of KafkaCLITool because rpk is configured to use TLS certs
        self.super_client(basic_auth_enabled).create_topic(self.topic)

    def super_client(self, basic_auth_enabled: bool = False):
        if basic_auth_enabled:
            return RpkTool(
                self.redpanda,
                username=self.admin_user.username,
                password=self.admin_user.password,
                sasl_mechanism=self.admin_user.algorithm,
                tls_cert=self.admin_user.certificate,
            )
        else:
            return RpkTool(self.redpanda, tls_cert=self.admin_user.certificate)


class SchemaRegistryMTLSTest(SchemaRegistryMTLSBase):
    def __init__(self, *args, **kwargs):
        super(SchemaRegistryMTLSTest, self).__init__(*args, **kwargs)

    def setUp(self):
        self.setup_cluster()

    @cluster(num_nodes=3)
    def test_mtls(self):
        result_raw = self.sr_client.get_schemas_types(
            tls_enabled=True,
            verify=self.admin_user.certificate.ca.crt,
            cert=(self.admin_user.certificate.crt, self.admin_user.certificate.key),
        )
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert set(result) == {"JSON", "PROTOBUF", "AVRO"}


class SchemaRegistryMTLSAndBasicAuthTest(SchemaRegistryMTLSBase):
    def __init__(self, *args, **kwargs):
        super(SchemaRegistryMTLSAndBasicAuthTest, self).__init__(*args, **kwargs)

    def setUp(self):
        self.setup_cluster(basic_auth_enabled=True)

    @cluster(num_nodes=3)
    def test_mtls_and_basic_auth(self):
        result_raw = self.sr_client.get_schemas_types(
            tls_enabled=True,
            auth=(self.admin_user.username, self.admin_user.password),
            verify=self.admin_user.certificate.ca.crt,
            cert=(self.admin_user.certificate.crt, self.admin_user.certificate.key),
        )
        assert result_raw.status_code == requests.codes.ok
        result = result_raw.json()
        assert set(result) == {"JSON", "PROTOBUF", "AVRO"}


class SchemaValidationEnableWithoutSchemaRegistry(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super(SchemaValidationEnableWithoutSchemaRegistry, self).__init__(
            *args, schema_registry_config=None, **kwargs
        )
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=1)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_enable_schema_id_validation(self, mode):
        try:
            self.redpanda.set_cluster_config({"enable_schema_id_validation": mode})
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
        super(SchemaValidationWithoutSchemaRegistry, self).__init__(
            *args,
            extra_rp_conf={
                "enable_schema_id_validation": SchemaIdValidationMode.REDPANDA.value
            },
            schema_registry_config=None,
            **kwargs,
        )

    @cluster(num_nodes=1, log_allow_list=INVALID_CONFIG_LOG_ALLOW_LIST)
    def test_disabled_schema_registry(self):
        rpk = RpkTool(self.redpanda)
        topic = "no_schema_registry"
        rpk.create_topic(
            topic,
            config={
                TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION: "true",
            },
        )
        try:
            rpk.produce(topic, "key", "value")
            assert False, "expected INVALID_RECORD"
        except RpkException as e:
            print(e)
            assert "INVALID_RECORD" in e.stderr

        wait_until(
            lambda: self.redpanda.search_log_all(
                "enable_schema_id_validation requires schema_registry to be enabled in redpanda.yaml"
            ),
            timeout_sec=5,
        )


class SchemaValidationTopicPropertiesTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super(SchemaValidationTopicPropertiesTest, self).__init__(
            *args,
            extra_rp_conf={
                "enable_schema_id_validation": SchemaIdValidationMode.COMPAT.value
            },
            schema_registry_config=SchemaRegistryConfig(),
            **kwargs,
        )
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def _get_topic_properties(
        self,
        mode: Optional[SchemaIdValidationMode],
        enable: Optional[bool],
        strategy: TopicSpec.SubjectNameStrategy,
    ):
        enable_str = f"{enable}".lower()
        config = {}
        if mode == SchemaIdValidationMode.REDPANDA:
            if enable is not None:
                config.update(
                    {
                        TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION: enable_str,
                        TopicSpec.PROPERTY_RECORD_VALUE_SCHEMA_ID_VALIDATION: enable_str,
                    }
                )
            if strategy is not None:
                config.update(
                    {
                        TopicSpec.PROPERTY_RECORD_KEY_SUBJECT_NAME_STRATEGY: strategy.value,
                        TopicSpec.PROPERTY_RECORD_VALUE_SUBJECT_NAME_STRATEGY: strategy.value,
                    }
                )

        if mode == SchemaIdValidationMode.COMPAT:
            if enable is not None:
                config.update(
                    {
                        TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION_COMPAT: enable_str,
                        TopicSpec.PROPERTY_RECORD_VALUE_SCHEMA_ID_VALIDATION_COMPAT: enable_str,
                    }
                )
            if strategy is not None:
                if strategy == TopicSpec.SubjectNameStrategy.TOPIC_NAME:
                    strategy_compat = TopicSpec.SubjectNameStrategyCompat.TOPIC_NAME
                elif strategy == TopicSpec.SubjectNameStrategy.RECORD_NAME:
                    strategy_compat = TopicSpec.SubjectNameStrategyCompat.RECORD_NAME
                elif strategy == TopicSpec.SubjectNameStrategy.TOPIC_RECORD_NAME:
                    strategy_compat = (
                        TopicSpec.SubjectNameStrategyCompat.TOPIC_RECORD_NAME
                    )

                config.update(
                    {
                        TopicSpec.PROPERTY_RECORD_KEY_SUBJECT_NAME_STRATEGY_COMPAT: strategy_compat.value,
                        TopicSpec.PROPERTY_RECORD_VALUE_SUBJECT_NAME_STRATEGY_COMPAT: strategy_compat.value,
                    }
                )
        return config

    @cluster(num_nodes=1)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_schema_id_validation_disabled_config(self, mode):
        """
        When the feature is disabled, the configs should not appear
        """

        self.redpanda.set_cluster_config(
            {"enable_schema_id_validation": SchemaIdValidationMode.NONE}
        )

        topic = "default-topic"
        self.rpk.create_topic(topic)
        desc = self.rpk.describe_topic_configs(topic)

        all_config = self._get_topic_properties(
            mode, False, TopicSpec.SubjectNameStrategy.TOPIC_NAME
        )

        for k in all_config.items():
            assert k not in desc

    @cluster(num_nodes=1)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_schema_id_validation_active_config(self, mode):
        """
        When the feature is active, the configs should be default
        """

        self.redpanda.set_cluster_config({"enable_schema_id_validation": mode})

        topic = "default-topic"
        self.rpk.create_topic(topic)
        desc = self.rpk.describe_topic_configs(topic)

        config = self._get_topic_properties(
            mode, False, TopicSpec.SubjectNameStrategy.TOPIC_NAME
        )

        for k, v in config.items():
            assert desc[k] == (v, "DEFAULT_CONFIG")

    @cluster(num_nodes=1)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_schema_id_validation_active_explicit_default_config(self, mode):
        """
        If the configuration is explicitly set to default, pretend it isn't
        dynamic, so that tools with a reconcialiation loop aren't confused
        """

        self.redpanda.set_cluster_config({"enable_schema_id_validation": mode})

        topic = "default-topic"

        config = self._get_topic_properties(
            mode, False, TopicSpec.SubjectNameStrategy.TOPIC_NAME
        )

        self.rpk.create_topic(topic, config=config)
        desc = self.rpk.describe_topic_configs(topic)

        for k, v in config.items():
            assert desc[k] == (v, "DEFAULT_CONFIG")

    @cluster(num_nodes=1)
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_schema_id_validation_active_nondefault_config(self, mode):
        """
        If the configuration is explicitly set to non-default, it should show
        as dyamic
        """

        self.redpanda.set_cluster_config({"enable_schema_id_validation": mode})

        topic = "default-topic"

        config = self._get_topic_properties(
            mode, True, TopicSpec.SubjectNameStrategy.RECORD_NAME
        )

        self.rpk.create_topic(topic, config=config)

        desc = self.rpk.describe_topic_configs(topic)

        for k, v in config.items():
            assert desc[k] == (v, "DYNAMIC_TOPIC_CONFIG")

    @cluster(num_nodes=1)
    def test_schema_id_validation_create_collision(self):
        """
        Test creating a topic where Redpanda and compat modes are incompatible
        """

        topic = "default-topic"
        try:
            self.rpk.create_topic(
                topic,
                config={
                    TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION: "true",
                    TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION_COMPAT: "false",
                },
            )
            assert False, "Expected failure"
        except RpkException:
            pass

    @cluster(num_nodes=1)
    def test_schema_id_validation_alter_collision(self):
        """
        Test altering a topic where Redpanda and compat modes are incompatible
        """

        topic = "default-topic"
        self.rpk.create_topic(
            topic,
            config={
                TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION: "true",
            },
        )
        try:
            self.rpk.alter_topic_config(
                topic=topic,
                set_key=TopicSpec.PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION_COMPAT,
                set_value="false",
            )
            assert False, "Expected failure"
        except RpkException:
            pass


class SchemaRegistryLicenseTest(RedpandaTest):
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, *args, **kwargs):
        super(SchemaRegistryLicenseTest, self).__init__(
            *args,
            extra_rp_conf={
                "enable_schema_id_validation": SchemaIdValidationMode.NONE.value
            },
            schema_registry_config=SchemaRegistryConfig(),
            **kwargs,
        )
        self.redpanda.set_environment(
            {
                "__REDPANDA_PERIODIC_REMINDER_INTERVAL_SEC": f"{self.LICENSE_CHECK_INTERVAL_SEC}",
            }
        )

    @cluster(num_nodes=3)
    @skip_fips_mode  # See NOTE below
    @parametrize(mode=SchemaIdValidationMode.REDPANDA)
    @parametrize(mode=SchemaIdValidationMode.COMPAT)
    def test_license_nag(self, mode):
        wait_until_nag_is_set(
            redpanda=self.redpanda, check_interval_sec=self.LICENSE_CHECK_INTERVAL_SEC
        )

        self.logger.debug("Ensuring no license nag")
        time.sleep(self.LICENSE_CHECK_INTERVAL_SEC * 2)
        # NOTE: This assertion will FAIL if running in FIPS mode because
        # being in FIPS mode will trigger the license nag
        assert not self.redpanda.has_license_nag()

        self.logger.debug("Setting cluster config")
        self.redpanda.set_cluster_config({"enable_schema_id_validation": mode})

        self.redpanda.set_environment({"__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE": "1"})
        self.redpanda.rolling_restart_nodes(
            self.redpanda.nodes, use_maintenance_mode=False
        )

        wait_until_nag_is_set(
            redpanda=self.redpanda, check_interval_sec=self.LICENSE_CHECK_INTERVAL_SEC
        )

        self.logger.debug("Waiting for license nag")
        wait_until(
            self.redpanda.has_license_nag,
            timeout_sec=self.LICENSE_CHECK_INTERVAL_SEC * 2,
            err_msg="License nag failed to appear",
        )


class SchemaRegistryConfluentClient(SchemaRegistryEndpoints):
    """
    Test schema registry with the confluent python client
    """

    def __init__(self, context, **kwargs):
        super(SchemaRegistryConfluentClient, self).__init__(context, **kwargs)

        # Replace the Redpanda SR client.
        self._base_uri = self.sr_client.base_uri()
        self.sr_client = SchemaRegistryClient({"url": self._base_uri})

    @cluster(num_nodes=3)
    @matrix(normalize_schemas=[True, False])
    def test_register_get_lookup_schema(self, normalize_schemas):
        """
        Verify that the register, get and lookup schema methods work
        """

        test_subject = "topic_1-key"
        schema1 = Schema(schema1_def, "AVRO")

        result = self.sr_client.register_schema(
            test_subject, schema1, normalize_schemas=normalize_schemas
        )
        assert result == 1, f"Result: {result}"

        result = self.sr_client.get_schema(1)
        assert result == schema1, f"Result: {result}"

        result = self.sr_client.lookup_schema(
            test_subject, schema1, normalize_schemas=normalize_schemas
        )
        assert result.schema_id == 1, f"Result: {result}"

        # TODO: we currently always normalize schemas. Once redpanda supports
        # the normize flag, this test should pass
        # with expect_exception(SchemaRegistryError, lambda e: True):
        #     self.sr_client.lookup_schema(
        #         test_subject, schema1, normalize_schemas=not normalize_schemas)

    @cluster(num_nodes=3)
    def test_versions(self):
        """
        Verify that the version endpoints work with the confluent client
        """

        test_subject = "topic_1-key"
        schema1 = Schema(schema1_def, "AVRO")
        schema2 = Schema(schema2_def, "AVRO")

        result = self.sr_client.register_schema(test_subject, schema1)
        assert result == 1, f"Result: {result}"

        result = self.sr_client.get_latest_version(test_subject)
        assert result.schema_id == 1, f"Result: {result}"

        result = self.sr_client.register_schema(test_subject, schema2)
        assert result == 2, f"Result: {result}"

        result = self.sr_client.get_latest_version(test_subject)
        assert result.schema_id == 2, f"Result: {result}"

        result = self.sr_client.get_version(test_subject, 1)
        assert result.schema == schema1, f"Result: {result}"

        result = self.sr_client.get_version(test_subject, 2)
        assert result.schema == schema2, f"Result: {result}"

        result = self.sr_client.get_versions(test_subject)
        assert result == [1, 2], f"Result: {result}"

        result = self.sr_client.delete_version(test_subject, 2)
        assert result == 2, f"Result: {result}"

        result = self.sr_client.get_latest_version(test_subject)
        assert result.schema_id == 1, f"Result: {result}"

        result = self.sr_client.get_versions(test_subject)
        assert result == [1], f"Result: {result}"

        # reinitialize client to drop the cache
        self.sr_client = SchemaRegistryClient({"url": self._base_uri})
        with expect_exception(SchemaRegistryError, lambda e: True):
            self.sr_client.get_version(test_subject, 2)

    @cluster(num_nodes=3)
    def test_set_get_compatibility(self):
        """
        Verify that setting and getting the compatibility level works with the confluent client
        """

        levels = [
            "BACKWARD",
            "BACKWARD_TRANSITIVE",
            "FORWARD",
            "FORWARD_TRANSITIVE",
            "FULL",
            "FULL_TRANSITIVE",
            "NONE",
        ]

        for level in levels:
            result = self.sr_client.set_compatibility(level=level)
            assert result["compatibility"] == level, f"Result: {result}"

            result = self.sr_client.get_compatibility()
            assert result == level, f"Result: {result}"

            test_subject = "topic_1-key"
            schema1 = Schema(schema1_def, "AVRO")

            result = self.sr_client.register_schema(test_subject, schema1)
            assert result == 1, f"Result: {result}"

            result = self.sr_client.set_compatibility(test_subject, level=level)
            assert result["compatibility"] == level, f"Result: {result}"

            result = self.sr_client.get_compatibility(test_subject)
            assert result == level, f"Result: {result}"

    @cluster(num_nodes=3)
    @matrix(permanent=[True, False])
    def test_delete_subject(self, permanent):
        """
        Verify that soft and hard deleting a subject works with the confluent client
        """

        test_subject = "topic_1-key"
        schema1 = Schema(schema1_def, "AVRO")

        result = self.sr_client.register_schema(test_subject, schema1)
        assert result == 1, f"Result: {result}"

        result = self.sr_client.get_schema(1)
        assert result == schema1, f"Result: {result}"

        result = self.sr_client.delete_subject(test_subject, permanent=permanent)
        assert result == [1], f"Result: {result}"

        result = self.sr_client.get_subjects()
        assert len(result) == 0, f"Result: {result}"

    @cluster(num_nodes=3)
    def test_test_compatible(self):
        """
        Verify that the test_compatible method of the confluent client works
        """

        test_subject = "topic_1-key"
        schema1 = Schema(schema1_def, "AVRO")
        schema2 = Schema(schema2_def, "AVRO")
        schema3 = Schema(schema3_def, "AVRO")

        result = self.sr_client.register_schema(test_subject, schema1)
        assert result == 1, f"Result: {result}"

        result = self.sr_client.test_compatibility(test_subject, schema2)
        assert result == True, f"Result: {result}"

        result = self.sr_client.test_compatibility(test_subject, schema3)
        assert result == False, f"Result: {result}"

    @cluster(num_nodes=3)
    def test_references(self):
        """
        Verify that reference handling works with the confluent client
        """

        simple_subject = "topic_1-key"
        simple_schema = Schema(simple_proto_def, "PROTOBUF")

        result = self.sr_client.register_schema(simple_subject, simple_schema)
        assert result == 1, f"Result: {result}"

        imported_subject = "topic_2-key"
        imported_schema = Schema(
            imported_proto_def,
            "PROTOBUF",
            references=[SchemaReference("simple", simple_subject, 1)],
        )

        result = self.sr_client.register_schema(imported_subject, imported_schema)
        assert result == 2, f"Result: {result}"

        well_known_subject = "topic_3-key"
        well_known_schema = Schema(well_known_proto_def, "PROTOBUF")

        result = self.sr_client.register_schema(well_known_subject, well_known_schema)
        assert result == 3, f"Result: {result}"

        validate_subject = "topic_4-key"
        validate_schema = Schema(validate_proto_def, "PROTOBUF")

        result = self.sr_client.register_schema(validate_subject, validate_schema)
        assert result == 4, f"Result: {result}"

        result = self.sr_client.get_schema(1)
        assert result == simple_schema, f"Result: {result}"

        result = self.sr_client.get_schema(2)
        assert result == imported_schema, f"Result: {result}"

        result = self.sr_client.get_schema(3)
        assert result == well_known_schema, f"Result: {result}"

        result = self.sr_client.get_schema(4)
        assert result == validate_schema, f"Result: {result}"


# dataset for SchemaRegistryCompatibilityModes: schemas is a list of 3 schemas compatible for `mode`, `antimode` is a suitable mode that will make the compat check for schemas fail
CompatDataset = NamedTuple(
    "CompatDataset", [("schemas", list[str]), ("antimode", str | None)]
)


class SchemaRegistryCompatibilityModes(SchemaRegistryEndpoints):
    def __init__(self, test_context, **kwargs):
        super().__init__(test_context, num_brokers=1, **kwargs)
        self._csr_client = SchemaRegistryClient({"url": self.sr_client.base_uri()})
        self._topic = "test-topic"

    def _register_schema(
        self, schema: str, type: str, mode: None | str, expect_invalid_schema=False
    ):
        """
        utility to register a `schema` of `type` with compatibility set to `mode`.
        if `expect_invalid_schema` is True, then a SchemaRegistryError is expected, for schema incompatibility
        """
        if mode is not None:
            self._csr_client.set_compatibility(subject_name=self._topic, level=mode)
        try:
            self._csr_client.register_schema(
                subject_name=self._topic,
                schema=Schema(schema_str=schema, schema_type=type),
            )
            assert not expect_invalid_schema, "expected invalid schema"
        except SchemaRegistryError as e:
            assert expect_invalid_schema, f"{schema=} was expected valid, got {e=}"
            assert e.error_code == 409, (
                f"expected SchemaRegistryError 409/'Schema being registered is incompatible with an earlier schema [...]',  got {e=}"
            )

    @staticmethod
    def _get_dataset_for_schema_type_mode(schema_type: str, mode: str) -> CompatDataset:
        if schema_type == "JSON":
            if mode == "NONE":
                # some non-compatible schemas
                return CompatDataset(
                    schemas=[
                        json.dumps({"type": "integer"}),
                        json.dumps({"type": "boolean"}),
                        json.dumps({"type": "array"}),
                    ],
                    antimode="BACKWARD",
                )

            if mode == "FULL" or "FULL_TRANSITIVE":
                # full or full transitive requires that schemas are backward and forward compatible, basically can change only the non-functional parts
                return CompatDataset(
                    schemas=[
                        json.dumps(
                            {
                                "$id": "id-1",
                                "type": "integer",
                            }
                        ),
                        json.dumps(
                            {
                                "$id": "id-2",
                                "type": "integer",
                            }
                        ),
                        json.dumps(
                            {
                                "$id": "id-3",
                                "type": "integer",
                            }
                        ),
                    ],
                    antimode=None,
                )

            # a series backward-transitive-compatibles schemas, each one more relaxed than the one before
            json_schema_set = [
                json.dumps(
                    {
                        "type": "integer",
                        "minimum": 10,
                    }
                ),
                json.dumps(
                    {
                        "type": "integer",
                        "minimum": 5,
                    }
                ),
                json.dumps(
                    {
                        "type": "number",
                        "minimum": 5,
                    }
                ),
            ]
            if mode == "BACKWARD" or mode == "BACKWARD_TRANSITIVE":
                return CompatDataset(schemas=json_schema_set, antimode="FORWARD")
            elif mode == "FORWARD" or mode == "FORWARD_TRANSITIVE":
                # forward can use the reversed json_schema_set
                return CompatDataset(
                    schemas=list(reversed(json_schema_set)), antimode="BACKWARD"
                )

        elif schema_type == "AVRO":
            if mode == "NONE":
                return CompatDataset(
                    schemas=[
                        json.dumps(
                            {
                                "type": "record",
                                "name": "myrecord",
                                "fields": [{"name": "f1", "type": "string"}],
                            }
                        ),
                        json.dumps(
                            {
                                "type": "record",
                                "name": "myrecord",
                                "fields": [{"name": "f1", "type": "int"}],
                            }
                        ),
                        json.dumps(
                            {
                                "type": "record",
                                "name": "myrecord",
                                "fields": [{"name": "f1", "type": "null"}],
                            }
                        ),
                    ],
                    antimode="BACKWARD",
                )

            if mode == "FULL" or mode == "FULL_TRANSITIVE":
                return CompatDataset(
                    schemas=[
                        json.dumps(
                            {
                                "type": "record",
                                "name": "myrecord",
                                "doc": "doc1",
                                "fields": [{"name": "f1", "type": "string"}],
                            }
                        ),
                        json.dumps(
                            {
                                "type": "record",
                                "name": "myrecord",
                                "doc": "doc2",
                                "fields": [{"name": "f1", "type": "string"}],
                            }
                        ),
                        json.dumps(
                            {
                                "type": "record",
                                "name": "myrecord",
                                "doc": "doc3",
                                "fields": [{"name": "f1", "type": "string"}],
                            }
                        ),
                    ],
                    antimode=None,
                )

            # a series backward-transitive-compatibles schemas
            avro_schema_set = [
                json.dumps(
                    {
                        "type": "record",
                        "name": "myrecord",
                        "fields": [{"name": "f1", "type": "string"}],
                    }
                ),
                json.dumps(
                    {
                        "type": "record",
                        "name": "myrecord",
                        "fields": [{"name": "f1", "type": ["null", "string"]}],
                    }
                ),
                json.dumps(
                    {
                        "type": "record",
                        "name": "myrecord",
                        "fields": [{"name": "f1", "type": ["int", "null", "string"]}],
                    }
                ),
            ]
            if mode == "BACKWARD" or mode == "BACKWARD_TRANSITIVE":
                return CompatDataset(schemas=avro_schema_set, antimode="FORWARD")
            elif mode == "FORWARD" or mode == "FORWARD_TRANSITIVE":
                # forward can use the reversed json_schema_set
                return CompatDataset(
                    schemas=list(reversed(avro_schema_set)), antimode="BACKWARD"
                )
        assert False, f"not implemented for {schema_type=} and {mode=}"

    @cluster(num_nodes=1)
    @matrix(
        schema_type=["JSON", "AVRO"],
        mode=[
            "BACKWARD",
            "BACKWARD_TRANSITIVE",
            "FORWARD",
            "FORWARD_TRANSITIVE",
            "NONE",
            "FULL",
            "FULL_TRANSITIVE",
        ],
    )
    def test_compatible_schemas(self, schema_type: str, mode: str):
        """
        register a base schema, and then for each remaining schema, checks that it's rejects with compatibility=`antimode` and accepted with compatibility=`mode`
        e.g: mode=BACKWARD, antimode=FORWARD, schemas is a list where schemas[N+1] is backward compatible with schemas[N]
        """
        schemas, antimode = (
            SchemaRegistryCompatibilityModes._get_dataset_for_schema_type_mode(
                schema_type, mode
            )
        )

        self.logger.debug(f"register base {schemas[0]=}")
        self._register_schema(schema=schemas[0], type=schema_type, mode=None)

        for s in schemas[1:]:
            if antimode is not None:
                self.logger.debug(
                    f"try register schema={s} with {antimode} compatibility and expect an exception"
                )
                self._register_schema(
                    schema=s,
                    type=schema_type,
                    mode=antimode,
                    expect_invalid_schema=True,
                )

            self.logger.debug(f"register schema={s} with {mode} compatibility")
            self._register_schema(schema=s, type=schema_type, mode=mode)


class SchemaRegistryACLTest(SchemaRegistryEndpoints):
    """
    Test schema registry ACL CRUD endpoints.
    """

    VALID_OPERATIONS = [
        "ALL",
        "READ",
        "WRITE",
        "DELETE",
        "DESCRIBE",
        "DESCRIBE_CONFIGS",
        "ALTER_CONFIGS",
    ]
    DISALLOWED_OPERATIONS = ["CREATE", "ALTER", "CLUSTER_ACTION", "IDEMPOTENT_WRITE"]

    VALID_PATTERN_TYPES = ["LITERAL", "PREFIXED"]

    def __init__(self, context, **kwargs):
        super(SchemaRegistryACLTest, self).__init__(context, **kwargs)

    def _create_test_acl(
        self,
        principal="User:alice",
        resource="test-subject",
        resource_type="SUBJECT",
        pattern_type="LITERAL",
        host="*",
        operation="READ",
        permission="ALLOW",
    ):
        """Helper method to create a test ACL with (overridable) default values."""
        return {
            "principal": principal,
            "resource": resource,
            "resource_type": resource_type,
            "pattern_type": pattern_type,
            "host": host,
            "operation": operation,
            "permission": permission,
        }

    def await_acl_count(self, count):
        """Wait for all nodes to see the expected ACL count. Useful as a linearizable barrier after post/delete requests."""

        def all_nodes_see_acls():
            for node in self.redpanda.nodes:
                response = self.sr_client.get_security_acls(
                    hostname=node.account.hostname
                )
                self.assert_equal(response.status_code, 200)
                self.assert_equal(len(response.json()), count)
            return True

        wait_until(
            all_nodes_see_acls,
            timeout_sec=15,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg=f"Timeout waiting for ACLs count to reach {count} on all nodes",
        )

    def _check_filtered_acls(self, filters, expected_acls):
        """Helper to check if listing ACLs with the given `filters` leads to a response of `expected_acls`"""
        response = self.sr_client.get_security_acls(params=filters)
        self.assert_equal(response.status_code, 200)

        # Sort both lists for consistent comparison
        sorted_response = sorted(response.json(), key=str)
        sorted_expected = sorted(expected_acls, key=str)

        self.assert_equal(
            sorted_response,
            sorted_expected,
            f"Expected {sorted_expected} ACLs for filters {filters}, got {sorted_response}",
        )
        return sorted_response

    @cluster(num_nodes=3)
    @matrix(scale=[1, 1000])
    def test_basic_acl_operations(self, scale: int):
        """Test basic CRUD operations for ACLs"""
        # Define the ACLs
        acls = [
            self._create_test_acl(
                principal="User:alice",
                resource=f"test-subject{i}",
                resource_type="SUBJECT",
                operation="READ",
            )
            for i in range(scale)
        ] + [
            self._create_test_acl(
                principal="User:bob",
                resource="*",
                resource_type="REGISTRY",
                operation="WRITE",
                permission="DENY",
            )
        ]

        # Create ACLs and verify 201 status
        resp = self.sr_client.post_security_acls(acls)
        self.assert_equal(resp.status_code, 201)

        # Get ACLs and verify they exist
        def acls_exist():
            resp = self.sr_client.get_security_acls()
            self.assert_equal(resp.status_code, 200)
            created_acls = resp.json()
            self.assert_equal(len(created_acls), scale + 1)
            return True

        wait_until(
            acls_exist,
            timeout_sec=15,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="Timeout waiting for ACLs to be created",
        )

        # Delete ACLs
        resp = self.sr_client.delete_security_acls(acls)
        self.assert_equal(resp.status_code, 200)
        deleted_acls = resp.json()
        self.assert_equal(len(deleted_acls), scale + 1)

        # Verify ACLs are gone
        def acls_removed():
            resp = self.sr_client.get_security_acls()
            self.assert_equal(resp.status_code, 200)
            self.assert_equal(len(resp.json()), 0)
            return True

        wait_until(
            acls_removed,
            timeout_sec=15,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="Timeout waiting for ACLs to be removed",
        )

    @cluster(num_nodes=3)
    def test_acl_validation(self):
        """Test comprehensive ACL validation including control characters, invalid input values and invalid value combinations."""

        # Test control characters in ACL fields
        control_chars = ["\n", "\t", "\r"]
        for control_char in control_chars:
            # Test in POST request body
            acl_with_control = self._create_test_acl(
                principal=f"User:user{control_char}1",
                resource=f"test{control_char}subject",
            )
            resp = self.sr_client.post_security_acls([acl_with_control])
            self.assert_equal(resp.status_code, 400)
            self.assert_in("Control characters not allowed", resp.text)

            # Test in DELETE request body
            resp = self.sr_client.delete_security_acls([acl_with_control])
            self.assert_equal(resp.status_code, 400)
            self.assert_in("Control characters not allowed", resp.text)

            # Test in GET query parameters
            resp = self.sr_client.get_security_acls(
                params={"principal": f"User:user{control_char}1"}
            )
            self.assert_equal(resp.status_code, 400)
            self.assert_in("Invalid parameter", resp.text)

        # Test invalid resource types
        invalid_resource_acl = [self._create_test_acl(resource_type="TOPIC")]
        resp = self.sr_client.post_security_acls(invalid_resource_acl)
        self.assert_equal(resp.status_code, 400)

        # Test missing required fields
        missing_field_acl = [self._create_test_acl()]
        del missing_field_acl[0]["operation"]
        resp = self.sr_client.post_security_acls(missing_field_acl)
        self.assert_equal(resp.status_code, 400)

        # Test invalid host format
        invalid_host_acl = [self._create_test_acl(host="invalid:host:format")]
        resp = self.sr_client.post_security_acls(invalid_host_acl)
        self.assert_equal(resp.status_code, 400)

        # Test PREFIXED pattern type not allowed for REGISTRY resource
        invalid_registry_acl = [
            self._create_test_acl(
                resource="*", resource_type="REGISTRY", pattern_type="PREFIXED"
            )
        ]
        resp = self.sr_client.post_security_acls(invalid_registry_acl)
        self.assert_equal(resp.status_code, 400)

        # Wildcard is only valid for users, not roles
        invalid_wildcard_role = [self._create_test_acl(principal="RedpandaRole:*")]
        resp = self.sr_client.post_security_acls(invalid_wildcard_role)
        self.assert_equal(resp.status_code, 400)

    @cluster(num_nodes=3)
    def test_field_variations(self):
        """Test the range of field values for pattern types and operations."""

        # Test valid pattern types
        for pattern_type in self.VALID_PATTERN_TYPES:
            acl = [self._create_test_acl(pattern_type=pattern_type)]
            resp = self.sr_client.post_security_acls(acl)
            self.assert_equal(resp.status_code, 201)

        # Test valid operations
        for operation in self.VALID_OPERATIONS:
            acl = [
                self._create_test_acl(
                    resource=f"test-{operation.lower()}", operation=operation
                )
            ]
            resp = self.sr_client.post_security_acls(acl)
            self.assert_equal(resp.status_code, 201)

        # Test invalid values
        invalid_pattern_acl = [self._create_test_acl(pattern_type="INVALID")]
        resp = self.sr_client.post_security_acls(invalid_pattern_acl)
        self.assert_equal(resp.status_code, 400)

        for operation in self.DISALLOWED_OPERATIONS + ["INVALID_OP"]:
            invalid_operation_acl = [self._create_test_acl(operation=operation)]
            resp = self.sr_client.post_security_acls(invalid_operation_acl)
            self.assert_equal(resp.status_code, 400)

    @cluster(num_nodes=3)
    def test_case_handling(self):
        """Test case insensitive inputs and case sensitivity in operations."""

        # Test lowercase inputs are accepted and normalized to uppercase
        acl = [
            self._create_test_acl(
                operation="ReAd", resource_type="subject", pattern_type="LITERAL"
            )
        ]

        resp = self.sr_client.post_security_acls(acl)
        self.assert_equal(resp.status_code, 201)

        # Wait for ACLs to propagate
        self.await_acl_count(1)

        # Verify normalization to uppercase in response
        resp = self.sr_client.get_security_acls()
        self.assert_equal(resp.status_code, 200)
        created_acls = resp.json()
        self.assert_equal(len(created_acls), 1)
        self.assert_equal(created_acls[0]["resource_type"], "SUBJECT")
        self.assert_equal(created_acls[0]["pattern_type"], "LITERAL")

        # Test case-insensitive filtering
        resp = self.sr_client.get_security_acls(
            params={"resource_type": "subject", "permission": "ALLOW"}
        )
        self.assert_equal(resp.status_code, 200)
        self.assert_equal(len(resp.json()), 1)

        # Test case-insensitive deletion
        acl_lower = [
            {
                k: v.lower() if k in ["resource_type", "pattern_type"] else v
                for k, v in acl[0].items()
            }
        ]
        resp = self.sr_client.delete_security_acls(acl_lower)
        self.assert_equal(resp.status_code, 200)
        self.assert_equal(len(resp.json()), 1)

    @cluster(num_nodes=3)
    def test_acl_delete_filters(self):
        """
        Test ACL deletion with similar ACLs
        """
        # Create some ACLs
        first = self._create_test_acl(principal="User:alice", operation="READ")
        second = self._create_test_acl(principal="User:bob", operation="WRITE")
        acls = [first, second]

        resp = self.sr_client.post_security_acls(acls)
        self.assert_equal(resp.status_code, 201)

        # Test deletion
        resp = self.sr_client.delete_security_acls([first])
        self.assert_equal(resp.status_code, 200)
        deleted_acls = resp.json()
        self.assert_equal(len(deleted_acls), 1)

        # Verify first ACL is gone, only the second remains
        def only_first_acl_deleted():
            resp = self.sr_client.get_security_acls()
            self.assert_equal(resp.status_code, 200)
            remaining_acls = resp.json()
            self.assert_equal(len(remaining_acls), 1)
            self.assert_equal(remaining_acls[0]["principal"], "User:bob")
            return True

        wait_until(
            only_first_acl_deleted,
            timeout_sec=15,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="Timeout waiting for only the first ACL to be deleted",
        )

    @cluster(num_nodes=3)
    def test_acl_get_filters(self):
        """Test filtering ACLs using query parameters on GET /security/acls endpoint."""
        # Define test ACLs with clear names for verification
        subject_read_acl = self._create_test_acl(
            principal="User:user1", resource="test-subject-1", operation="READ"
        )
        subject_write_acl = self._create_test_acl(
            principal="User:user2",
            resource="test-subject-2",
            pattern_type="PREFIXED",
            host="192.168.1.1",
            operation="WRITE",
            permission="DENY",
        )
        registry_admin_acl = self._create_test_acl(
            principal="RedpandaRole:admin",
            resource="*",
            resource_type="REGISTRY",
            operation="ALL",
        )

        test_acls = [subject_read_acl, subject_write_acl, registry_admin_acl]
        resp = self.sr_client.post_security_acls(test_acls)
        self.assert_equal(resp.status_code, 201)

        # Wait until ACLs propagate to all nodes
        self.await_acl_count(3)

        # Test individual field filters
        self._check_filtered_acls({}, test_acls)
        self._check_filtered_acls({"principal": "User:user1"}, [subject_read_acl])
        self._check_filtered_acls(
            {"principal": "RedpandaRole:admin"}, [registry_admin_acl]
        )
        self._check_filtered_acls({"resource": "test-subject-1"}, [subject_read_acl])
        self._check_filtered_acls({"resource_type": "REGISTRY"}, [registry_admin_acl])
        self._check_filtered_acls({"pattern_type": "PREFIXED"}, [subject_write_acl])
        self._check_filtered_acls({"host": "192.168.1.1"}, [subject_write_acl])
        self._check_filtered_acls({"operation": "ALL"}, [registry_admin_acl])
        self._check_filtered_acls({"permission": "DENY"}, [subject_write_acl])

        # Test wildcard matches
        self._check_filtered_acls({"host": "*"}, [subject_read_acl, registry_admin_acl])
        self._check_filtered_acls({"resource": "*"}, [registry_admin_acl])

        # Test combination of filters
        self._check_filtered_acls(
            {
                "resource_type": "SUBJECT",
                "pattern_type": "LITERAL",
                "permission": "ALLOW",
            },
            [subject_read_acl],
        )

        self._check_filtered_acls(
            {"principal": "User:user1", "resource_type": "SUBJECT"}, [subject_read_acl]
        )

        # Test filters that should return empty results
        self._check_filtered_acls(
            {
                "principal": "User:nonexistent",
            },
            [],
        )
        self._check_filtered_acls({"principal": "User:user1", "permission": "DENY"}, [])

        # Test case sensitivity
        response_upper = self._check_filtered_acls(
            {"permission": "ALLOW"}, [subject_read_acl, registry_admin_acl]
        )
        response_lower = self._check_filtered_acls(
            {"permission": "allow"}, [subject_read_acl, registry_admin_acl]
        )
        self.assert_equal(
            response_upper,
            response_lower,
            f"Case-insensitive filtering should return same results. {response_upper} != {response_lower}",
        )

    @cluster(num_nodes=3)
    def test_acl_idempotency(self):
        """Test idempotency of ACL operations and partial deletions."""
        test_acl = self._create_test_acl()

        # Create initial ACL
        resp = self.sr_client.post_security_acls([test_acl])
        self.assert_equal(resp.status_code, 201)

        # Wait until ACLs propagate to all nodes
        self.await_acl_count(1)

        # Create same ACL again - should be idempotent
        resp = self.sr_client.post_security_acls([test_acl])
        self.assert_equal(resp.status_code, 201)

        # Verify only one exists
        resp = self.sr_client.get_security_acls(params={"principal": "User:alice"})
        self.assert_equal(resp.status_code, 200)
        self.assert_equal(len(resp.json()), 1)

        # Test partial deletion
        non_existent_acl = self._create_test_acl(resource="non-existent")
        resp = self.sr_client.delete_security_acls([test_acl, non_existent_acl])
        self.assert_equal(resp.status_code, 200)
        deleted_acls = resp.json()
        self.assert_equal(len(deleted_acls), 1)
        self.assert_equal(deleted_acls[0]["resource"], "test-subject")

        # Verify ACL no longer exists eventually
        self.await_acl_count(0)

    @cluster(num_nodes=3)
    def test_group_acl_creation(self):
        """
        Test that Group principal ACLs can be created
        """
        # Create Group ACLs with various configurations
        group_acls = [
            self._create_test_acl(
                principal="Group:developers",
                resource="dev-subject",
                resource_type="SUBJECT",
                operation="READ",
                permission="ALLOW",
            ),
            self._create_test_acl(
                principal="Group:admins",
                resource="*",
                resource_type="REGISTRY",
                operation="ALL",
                permission="ALLOW",
            ),
            self._create_test_acl(
                principal="Group:blocked",
                resource="secret-subject",
                resource_type="SUBJECT",
                operation="WRITE",
                permission="DENY",
            ),
        ]

        # POST should succeed
        response = self.sr_client.post_security_acls(group_acls)
        self.assert_equal(
            response.status_code, 201, f"Failed to create Group ACLs: {response.text}"
        )

        # Verify ACLs were created
        self.await_acl_count(3)

        # GET and verify principals
        response = self.sr_client.get_security_acls()
        self.assert_equal(response.status_code, 200)
        acls = response.json()

        principals = {acl["principal"] for acl in acls}
        self.assert_in("Group:developers", principals)
        self.assert_in("Group:admins", principals)
        self.assert_in("Group:blocked", principals)

        # Verify ACL details
        for acl in acls:
            if acl["principal"] == "Group:developers":
                self.assert_equal(acl["resource"], "dev-subject")
                self.assert_equal(acl["resource_type"], "SUBJECT")
                self.assert_equal(acl["operation"], "READ")
                self.assert_equal(acl["permission"], "ALLOW")
            elif acl["principal"] == "Group:admins":
                self.assert_equal(acl["resource"], "*")
                self.assert_equal(acl["resource_type"], "REGISTRY")
                self.assert_equal(acl["operation"], "ALL")
            elif acl["principal"] == "Group:blocked":
                self.assert_equal(acl["resource"], "secret-subject")
                self.assert_equal(acl["permission"], "DENY")

    @cluster(num_nodes=3)
    def test_group_acl_query_filtering(self):
        """Test querying ACLs with Group principal filters"""

        # Create mix of User and Group ACLs
        acls = [
            self._create_test_acl(
                principal="User:alice", resource="alice-subject", operation="WRITE"
            ),
            self._create_test_acl(
                principal="Group:developers",
                resource="dev-subject",
                operation="READ",
            ),
            self._create_test_acl(
                principal="Group:developers",
                resource="dev-registry",
                resource_type="REGISTRY",
                operation="DESCRIBE",
            ),
            self._create_test_acl(
                principal="Group:admins",
                resource="*",
                resource_type="REGISTRY",
                operation="ALL",
            ),
        ]

        response = self.sr_client.post_security_acls(acls)
        self.assert_equal(response.status_code, 201)
        self.await_acl_count(4)

        # Query for Group:developers ACLs only
        response = self.sr_client.get_security_acls(
            params={"principal": "Group:developers"}
        )
        self.assert_equal(response.status_code, 200)
        filtered_acls = response.json()
        self.assert_equal(len(filtered_acls), 2)

        for acl in filtered_acls:
            self.assert_equal(acl["principal"], "Group:developers")

        # Query for all Group principals
        response = self.sr_client.get_security_acls()
        self.assert_equal(response.status_code, 200)
        all_acls = response.json()

        group_acls = [acl for acl in all_acls if acl["principal"].startswith("Group:")]
        self.assert_equal(len(group_acls), 3)

        # Query for specific resource
        response = self.sr_client.get_security_acls(params={"resource": "dev-subject"})
        self.assert_equal(response.status_code, 200)
        resource_acls = response.json()
        self.assert_equal(len(resource_acls), 1)  # Group:developers

    @cluster(num_nodes=3)
    def test_group_acl_deletion(self):
        """Test deleting Group ACLs with various filter combinations"""

        # Create multiple Group ACLs
        acls = [
            self._create_test_acl(
                principal="Group:team_a", resource="subject_a", operation="READ"
            ),
            self._create_test_acl(
                principal="Group:team_a", resource="subject_b", operation="WRITE"
            ),
            self._create_test_acl(
                principal="Group:team_b", resource="subject_a", operation="READ"
            ),
        ]

        response = self.sr_client.post_security_acls(acls)
        self.assert_equal(response.status_code, 201)
        self.await_acl_count(3)

        # Delete all team_a ACLs using principal filter
        delete_filter = [{"principal": "Group:team_a"}]

        response = self.sr_client.delete_security_acls(delete_filter)
        self.assert_equal(response.status_code, 200)
        deleted = response.json()
        self.assert_equal(len(deleted), 2)

        # Verify only team_b ACL remains

        self.await_acl_count(1)

        response = self.sr_client.get_security_acls()
        self.assert_equal(response.status_code, 200)
        remaining = response.json()
        self.logger.info(f"Remaining: {remaining}")
        self.assert_equal(remaining[0]["principal"], "Group:team_b")

    @cluster(num_nodes=3)
    def test_mixed_user_and_group_acls(self):
        """Test Schema Registry with both User and Group ACLs"""

        mixed_acls = [
            self._create_test_acl(
                principal="User:alice", resource="user-subject", operation="WRITE"
            ),
            self._create_test_acl(
                principal="Group:readers",
                resource="shared-subject",
                operation="READ",
            ),
            self._create_test_acl(
                principal="User:bob", resource="shared-subject", operation="WRITE"
            ),
            self._create_test_acl(
                principal="Group:admins",
                resource="*",
                resource_type="REGISTRY",
                operation="ALL",
            ),
        ]

        # Create all ACLs
        response = self.sr_client.post_security_acls(mixed_acls)
        self.assert_equal(response.status_code, 201)
        self.await_acl_count(4)

        # Verify all ACLs exist
        response = self.sr_client.get_security_acls()
        self.assert_equal(response.status_code, 200)
        acls = response.json()

        principals = {acl["principal"] for acl in acls}
        self.assert_equal(len(principals), 4)
        self.assert_in("User:alice", principals)
        self.assert_in("User:bob", principals)
        self.assert_in("Group:readers", principals)
        self.assert_in("Group:admins", principals)

        # Verify resource distribution
        subject_acls = [acl for acl in acls if acl["resource_type"] == "SUBJECT"]
        registry_acls = [acl for acl in acls if acl["resource_type"] == "REGISTRY"]
        self.assert_equal(len(subject_acls), 3)
        self.assert_equal(len(registry_acls), 1)

    @cluster(num_nodes=3)
    def test_group_acl_validation(self):
        """Test validation rules specific to Group principals"""

        # Test wildcard group (should fail)
        wildcard_group_acl = [self._create_test_acl(principal="Group:*")]
        response = self.sr_client.post_security_acls(wildcard_group_acl)
        self.assert_equal(
            response.status_code,
            400,
            f"Wildcard group should fail but got {response.status_code}: {response.text}",
        )
        self.assert_in("wildcard", response.text.lower())

        # Test empty group name (should fail)
        empty_group_acl = [self._create_test_acl(principal="Group:")]
        response = self.sr_client.post_security_acls(empty_group_acl)
        self.assert_equal(
            response.status_code,
            400,
            f"Empty group should fail but got {response.status_code}",
        )

        # Test valid group names with special characters
        valid_groups = [
            "Group:my-team",
            "Group:team_123",
            "Group:UPPERCASE_GROUP",
            "Group:mixed_Case-123",
        ]

        for group in valid_groups:
            acl = [
                self._create_test_acl(
                    principal=group, resource=f"test-{group.replace(':', '-')}"
                )
            ]
            response = self.sr_client.post_security_acls(acl)
            self.assert_equal(
                response.status_code,
                201,
                f"Valid group {group} should be accepted but got {response.status_code}: {response.text}",
            )

    @cluster(num_nodes=3)
    @matrix(scale=[1, 100])
    def test_group_acl_scale(self, scale):
        """Test Group ACL operations at scale"""

        # Create N Group ACLs
        acls = [
            self._create_test_acl(
                principal=f"Group:team_{i}",
                resource=f"subject_{i}",
                operation="READ",
            )
            for i in range(scale)
        ]

        # Test creation performance
        response = self.sr_client.post_security_acls(acls)
        self.assert_equal(
            response.status_code,
            201,
            f"Failed to create {scale} Group ACLs: {response.text}",
        )

        # Verify all created
        self.await_acl_count(scale)

        # Test query performance
        response = self.sr_client.get_security_acls()
        self.assert_equal(response.status_code, 200)
        all_acls = response.json()
        self.assert_equal(len(all_acls), scale)

        # Verify all are Group principals
        for acl in all_acls:
            assert acl["principal"].startswith("Group:team_"), (
                f"Unexpected principal: {acl['principal']}"
            )

        # Test deletion performance
        response = self.sr_client.delete_security_acls(acls)
        self.assert_equal(response.status_code, 200)
        deleted = response.json()
        self.assert_equal(len(deleted), scale)


class ACLTestEndpoint:
    """Base class for ACL-protected endpoints"""

    def __init__(self, test_instance: "SchemaRegistryAclAuthzTest"):
        self.test = test_instance
        self.sr_client = test_instance.sr_client

    @property
    def name(self) -> str:
        """Endpoint identifier"""
        raise NotImplementedError

    @property
    def path(self) -> str:
        """Endpoint path"""
        raise NotImplementedError

    def setup(self) -> None:
        """Any pre-test setup"""
        pass

    def make_request(self, auth) -> requests.Response:
        """Execute the actual HTTP request"""
        raise NotImplementedError

    def requests_per_request(self) -> int:
        """The number of requests made per make_request call"""
        return 1

    def create_acl(self) -> dict:
        """Create the ACL required for this endpoint"""
        raise NotImplementedError

    def resource(self) -> dict:
        """Return the resource required by the request"""
        raise NotImplementedError


class GetConfigEndpoint(ACLTestEndpoint):
    name = "GET_CONFIG"

    @property
    def path(self) -> str:
        return "config"

    def make_request(self, auth):
        return self.sr_client.get_config(auth=auth)

    def create_acl(self):
        return self.test._create_acl(
            resource="*",
            resource_type="REGISTRY",
            pattern_type="LITERAL",
            operation="DESCRIBE_CONFIGS",
        )

    def resource(self) -> dict:
        return {"name": "", "type": "registry"}


class PutConfigEndpoint(ACLTestEndpoint):
    name = "PUT_CONFIG"

    @property
    def path(self) -> str:
        return "config"

    def make_request(self, auth):
        return self.sr_client.set_config(
            data=json.dumps({"compatibility": "FULL"}), auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(
            resource="*",
            resource_type="REGISTRY",
            pattern_type="LITERAL",
            operation="ALTER_CONFIGS",
        )

    def resource(self) -> dict:
        return {"name": "", "type": "registry"}


class GetConfigSubjectEndpoint(ACLTestEndpoint):
    name = "GET_CONFIG_SUBJECT"

    @property
    def path(self) -> str:
        return f"config/{self.test.subject}"

    def setup(self) -> None:
        res = self.sr_client.set_config_subject(
            self.test.subject,
            data=json.dumps({"compatibility": "FULL"}),
            auth=self.test.super_auth,
        )
        self.test.assert_equal(res.status_code, 200)

    def make_request(self, auth):
        return self.sr_client.get_config_subject(self.test.subject, auth=auth)

    def create_acl(self):
        return self.test._create_acl(
            resource=self.test.subject,
            resource_type="SUBJECT",
            pattern_type="LITERAL",
            operation="DESCRIBE_CONFIGS",
        )

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class PutConfigSubjectEndpoint(ACLTestEndpoint):
    name = "PUT_CONFIG_SUBJECT"

    @property
    def path(self) -> str:
        return f"config/{self.test.subject}"

    def make_request(self, auth):
        return self.sr_client.set_config_subject(
            self.test.subject, data=json.dumps({"compatibility": "FULL"}), auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(
            resource=self.test.subject,
            resource_type="SUBJECT",
            pattern_type="LITERAL",
            operation="ALTER_CONFIGS",
        )

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class DeleteConfigSubject(ACLTestEndpoint):
    name = "DELETE_CONFIG_SUBJECT"

    @property
    def path(self) -> str:
        return f"config/{self.test.subject}"

    def setup(self) -> None:
        res = self.sr_client.set_config_subject(
            self.test.subject,
            data=json.dumps({"compatibility": "FULL"}),
            auth=self.test.super_auth,
        )
        self.test.assert_equal(res.status_code, 200)

    def make_request(self, auth):
        return self.sr_client.delete_config_subject(self.test.subject, auth=auth)

    def create_acl(self):
        return self.test._create_acl(
            resource=self.test.subject,
            resource_type="SUBJECT",
            pattern_type="LITERAL",
            operation="ALTER_CONFIGS",
        )

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class GetMode(ACLTestEndpoint):
    name = "GET_MODE"

    @property
    def path(self) -> str:
        return "mode"

    def make_request(self, auth):
        return self.sr_client.get_mode(auth=auth)

    def create_acl(self):
        return self.test._create_acl(
            resource="*",
            resource_type="REGISTRY",
            pattern_type="LITERAL",
            operation="DESCRIBE_CONFIGS",
        )

    def resource(self) -> dict:
        return {"name": "", "type": "registry"}


class PutMode(ACLTestEndpoint):
    name = "PUT_MODE"

    @property
    def path(self) -> str:
        return "mode"

    def make_request(self, auth):
        return self.sr_client.set_mode(
            data=json.dumps({"mode": "READWRITE"}), auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(
            resource="*",
            resource_type="REGISTRY",
            pattern_type="LITERAL",
            operation="ALTER_CONFIGS",
        )

    def resource(self) -> dict:
        return {"name": "", "type": "registry"}


class GetModeSubject(ACLTestEndpoint):
    name = "GET_MODE_SUBJECT"

    @property
    def path(self) -> str:
        return f"mode/{self.test.subject}"

    def setup(self) -> None:
        res = self.sr_client.set_mode_subject(
            self.test.subject,
            data=json.dumps({"mode": "READWRITE"}),
            auth=self.test.super_auth,
        )
        self.test.assert_equal(res.status_code, 200)

    def make_request(self, auth):
        return self.sr_client.get_mode_subject(self.test.subject, auth=auth)

    def create_acl(self):
        return self.test._create_acl(
            resource=self.test.subject,
            resource_type="SUBJECT",
            pattern_type="LITERAL",
            operation="DESCRIBE_CONFIGS",
        )

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class PutModeSubject(ACLTestEndpoint):
    name = "PUT_MODE_SUBJECT"

    @property
    def path(self) -> str:
        return f"mode/{self.test.subject}"

    def make_request(self, auth):
        return self.sr_client.set_mode_subject(
            self.test.subject, data=json.dumps({"mode": "READWRITE"}), auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(
            resource=self.test.subject,
            resource_type="SUBJECT",
            pattern_type="LITERAL",
            operation="ALTER_CONFIGS",
        )

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class DeleteModeSubject(ACLTestEndpoint):
    name = "DELETE_MODE_SUBJECT"

    @property
    def path(self) -> str:
        return f"mode/{self.test.subject}"

    def setup(self) -> None:
        res = self.sr_client.set_mode_subject(
            self.test.subject,
            data=json.dumps({"mode": "READWRITE"}),
            auth=self.test.super_auth,
        )
        self.test.assert_equal(res.status_code, 200)

    def make_request(self, auth):
        return self.sr_client.delete_mode_subject(self.test.subject, auth=auth)

    def create_acl(self):
        return self.test._create_acl(
            resource=self.test.subject,
            resource_type="SUBJECT",
            pattern_type="LITERAL",
            operation="ALTER_CONFIGS",
        )

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class PostSubjectVersions(ACLTestEndpoint):
    name = "POST_SUBJECT_VERSIONS"

    @property
    def path(self) -> str:
        return f"subjects/{self.test.subject}/versions"

    def make_request(self, auth):
        return self.sr_client.post_subjects_subject_versions(
            self.test.subject, data=self.test.schema_data_1, auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(
            resource=self.test.subject,
            resource_type="SUBJECT",
            pattern_type="LITERAL",
            operation="WRITE",
        )

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class GetSchemasIdsIdVersions(ACLTestEndpoint):
    name = "GET_SCHEMAS_IDS_ID_VERSIONS"

    @property
    def path(self) -> str:
        return f"schemas/ids/{self.schema_id}/versions"

    def setup(self) -> None:
        self.schema_id = self.test._create_schema(self.test.subject)

    def make_request(self, auth):
        return self.sr_client.get_schemas_ids_id_versions(self.schema_id, auth=auth)

    def create_acl(self):
        return self.test._create_acl("*", "REGISTRY", "LITERAL", "DESCRIBE")

    def resource(self) -> dict:
        return {"name": "", "type": "registry"}


class GetSchemasIdsIdSubjects(ACLTestEndpoint):
    name = "GET_SCHEMAS_IDS_ID_SUBJECTS"

    @property
    def path(self) -> str:
        return f"schemas/ids/{self.schema_id}/subjects"

    def setup(self) -> None:
        self.schema_id = self.test._create_schema(self.test.subject)

    def make_request(self, auth):
        return self.sr_client.get_schemas_ids_id_subjects(self.schema_id, auth=auth)

    def create_acl(self):
        return self.test._create_acl("*", "REGISTRY", "LITERAL", "DESCRIBE")

    def resource(self) -> dict:
        return {"name": "", "type": "registry"}


class GetSubjectVersions(ACLTestEndpoint):
    name = "GET_SUBJECT_VERSIONS"

    @property
    def path(self) -> str:
        return f"subjects/{self.test.subject}/versions"

    def setup(self) -> None:
        self.test._create_schema(self.test.subject)

    def make_request(self, auth):
        return self.sr_client.get_subjects_subject_versions(
            self.test.subject, auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(
            self.test.subject, "SUBJECT", "LITERAL", "DESCRIBE"
        )

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class PostSubject(ACLTestEndpoint):
    name = "POST_SUBJECT"

    @property
    def path(self) -> str:
        return f"subjects/{self.test.subject}"

    def setup(self) -> None:
        self.test._create_schema(self.test.subject)

    def make_request(self, auth):
        return self.sr_client.post_subjects_subject(
            self.test.subject, data=self.test.schema_data_1, auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(self.test.subject, "SUBJECT", "LITERAL", "READ")

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class GetSubjectVersionsVersion(ACLTestEndpoint):
    name = "GET_SUBJECT_VERSIONS_VERSION"

    @property
    def path(self) -> str:
        return f"subjects/{self.test.subject}/versions/1"

    def setup(self) -> None:
        self.test._create_schema(self.test.subject)

    def make_request(self, auth):
        return self.sr_client.get_subjects_subject_versions_version(
            self.test.subject, version=1, auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(self.test.subject, "SUBJECT", "LITERAL", "READ")

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class GetSubjectVersionsVersionSchema(ACLTestEndpoint):
    name = "GET_SUBJECT_VERSIONS_VERSION_SCHEMA"

    @property
    def path(self) -> str:
        return f"subjects/{self.test.subject}/versions/1/schema"

    def setup(self) -> None:
        self.test._create_schema(self.test.subject)

    def make_request(self, auth):
        return self.sr_client.get_subjects_subject_versions_version_schema(
            self.test.subject, version=1, auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(self.test.subject, "SUBJECT", "LITERAL", "READ")

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class GetSubjectVersionsVersionReferencedBy(ACLTestEndpoint):
    name = "GET_SUBJECT_VERSIONS_VERSION_REFERENCED_BY"

    @property
    def path(self) -> str:
        return f"subjects/{self.test.subject}/versions/1/referencedby"

    def setup(self) -> None:
        self.test._create_schema(self.test.subject)

    def make_request(self, auth):
        return self.sr_client.get_subjects_subject_versions_version_referenced_by(
            self.test.subject, version=1, auth=auth
        )

    def create_acl(self):
        return self.test._create_acl("*", "REGISTRY", "LITERAL", "DESCRIBE")

    def requests_per_request(self) -> int:
        """
        sr_client.get_subjects_subject_versions_version_referenced_by makes 2 requests:
        /subjects/{self.test.subject}/versions/1/referencedby
        /subjects/{self.test.subject}/versions/1/referencedBy (deprecated)
        """
        return 2

    def resource(self) -> dict:
        return {"name": "", "type": "registry"}


class DeleteSubject(ACLTestEndpoint):
    name = "DELETE_SUBJECT"

    @property
    def path(self) -> str:
        return f"subjects/{self.test.subject}"

    def setup(self) -> None:
        self.test._create_schema(self.test.subject)

    def make_request(self, auth):
        return self.sr_client.delete_subject(self.test.subject, auth=auth)

    def create_acl(self):
        return self.test._create_acl(self.test.subject, "SUBJECT", "LITERAL", "DELETE")

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class DeleteSubjectVersion(ACLTestEndpoint):
    name = "DELETE_SUBJECT_VERSION"

    @property
    def path(self) -> str:
        return f"subjects/{self.test.subject}/versions/1"

    def setup(self) -> None:
        self.test._create_schema(self.test.subject)

    def make_request(self, auth):
        return self.sr_client.delete_subject_version(
            self.test.subject, version=1, auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(self.test.subject, "SUBJECT", "LITERAL", "DELETE")

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class CompatibilitySubjectVersion(ACLTestEndpoint):
    name = "COMPATIBILITY_SUBJECT_VERSION"

    @property
    def path(self) -> str:
        return f"compatibility/subjects/{self.test.subject}/versions/1"

    def setup(self) -> None:
        self.test._create_schema(self.test.subject)

    def make_request(self, auth):
        return self.sr_client.post_compatibility_subject_version(
            self.test.subject, version=1, data=self.test.schema_data_1, auth=auth
        )

    def create_acl(self):
        return self.test._create_acl(self.test.subject, "SUBJECT", "LITERAL", "READ")

    def resource(self) -> dict:
        return {"name": self.test.subject, "type": "subject"}


class GetSchemasTypes(ACLTestEndpoint):
    name = "GET_SCHEMAS_TYPES"

    @property
    def path(self) -> str:
        return "schemas/types"

    def setup(self) -> None:
        pass

    def make_request(self, auth):
        return self.sr_client.get_schemas_types()

    def create_acl(self):
        pass

    def resource(self) -> dict:
        return {"name": "", "type": "registry"}


class GetStatusReady(ACLTestEndpoint):
    name = "SCHEMA_REGISTRY_STATUS_READY"

    @property
    def path(self) -> str:
        return "status/ready"

    def setup(self) -> None:
        pass

    def make_request(self, auth):
        return self.sr_client.get_status_ready()

    def create_acl(self):
        pass

    def resource(self) -> dict:
        return {"name": "", "type": "registry"}


class SchemaRegistryAclAuthzTestBase(SchemaRegistryEndpoints):
    """
    Base class providing shared ACL test infrastructure (setup, helpers) without test methods.
    """

    def __init__(self, context, extra_rp_conf: dict | None = None, **kwargs):
        security = SecurityConfig()
        security.enable_sasl = True
        security.endpoint_authn_method = "sasl"

        schema_registry_config = SchemaRegistryConfig()
        schema_registry_config.authn_method = "http_basic"
        schema_registry_config.mode_mutability = True

        super().__init__(
            context,
            security=security,
            num_brokers=1,
            schema_registry_config=schema_registry_config,
            extra_rp_conf=extra_rp_conf,
            **kwargs,
        )

        superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.user = SaslCredentials("user", "panda012345678", superuser.mechanism)

        self.super_auth = (superuser.username, superuser.password)
        self.user_auth = (self.user.username, self.user.password)

        self.subject = "test-subject"
        self.schema_data_1 = json.dumps({"schema": schema1_def})
        self.schema_data_2 = json.dumps({"schema": schema2_def})

        self.rpk = RpkTool(
            self.redpanda,
            username=superuser.username,
            password=superuser.password,
            sasl_mechanism=superuser.algorithm,
        )

    def _init_users(self):
        admin = Admin(self.redpanda)
        admin.create_user(
            username=self.user.username,
            password=self.user.password,
            algorithm=self.user.mechanism,
            await_exists=True,
        )

    def _create_acl(
        self, resource, resource_type, pattern_type, operation, permission="ALLOW"
    ):
        return self.sr_client.create_acl(
            self.user.username,
            resource,
            resource_type,
            pattern_type,
            "*",
            operation,
            permission,
        )

    def _post_acl(self, acl):
        """Grant one or more ACLs to the regular user."""
        acl_list = [acl] if isinstance(acl, dict) else acl

        resp = self.sr_client.post_security_acls(acl_list, auth=self.super_auth)
        self.assert_equal(resp.status_code, 201, f"Failed to create ACL: {acl=}")

        # Wait until the ACLs are propagated to all nodes
        def acl_all_observable():
            for node in self.redpanda.nodes:
                resp = self.sr_client.get_security_acls(
                    hostname=node.account.hostname, auth=self.super_auth
                )
                self.assert_equal(resp.status_code, 200)

                response_acls = resp.json()
                for a in acl_list:
                    self.redpanda.logger.debug(
                        f"Checking if {a} in response from {node.account.hostname}: {response_acls}"
                    )
                    self.assert_in(a, response_acls)

            return True

        wait_until(
            acl_all_observable,
            timeout_sec=30,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg=f"Failed to propagate ACLs to all nodes: {acl_list}",
        )

    def _create_schema(self, subject: str) -> int:
        response = self.sr_client.post_subjects_subject_versions(
            subject, data=self.schema_data_1, auth=self.super_auth
        )
        self.assert_equal(response.status_code, 200, "Failed to create schema")
        return response.json()["id"]

    def setUp(self):
        super().setUp()
        self._init_users()
        self.redpanda.set_cluster_config(
            {"schema_registry_enable_authorization": "True"}
        )


class SchemaRegistryAclAuthzTest(SchemaRegistryAclAuthzTestBase):
    """
    Verify that schema registry endpoints are protected by the correct ACL resource and operation.
    """

    ENDPOINTS = [
        GetConfigEndpoint,
        PutConfigEndpoint,
        GetConfigSubjectEndpoint,
        PutConfigSubjectEndpoint,
        DeleteConfigSubject,
        GetMode,
        PutMode,
        GetModeSubject,
        PutModeSubject,
        DeleteModeSubject,
        PostSubjectVersions,
        GetSchemasIdsIdVersions,
        GetSchemasIdsIdSubjects,
        GetSubjectVersions,
        PostSubject,
        GetSubjectVersionsVersion,
        GetSubjectVersionsVersionSchema,
        GetSubjectVersionsVersionReferencedBy,
        DeleteSubject,
        DeleteSubjectVersion,
        CompatibilitySubjectVersion,
        # Tested separately:
        # GET_SCHEMAS_TYPES             - no ACLs required
        # SCHEMA_REGISTRY_STATUS_READY  - no ACLs required
        # GET_SCHEMAS_IDS_ID            - custom ACL handling
        # GET_SUBJECTS                  - custom ACL handling
        # GET_SECURITY_ACLS             - kafka cluster ACL required
        # POST_SECURITY_ACLS            - kafka cluster ACL required
        # DELETE_SECURITY_ACLS          - kafka cluster ACL required
    ]

    def _get_endpoint_by_name(self, name: str) -> ACLTestEndpoint:
        for endpoint in self.ENDPOINTS:
            if endpoint.name == name:
                return endpoint(self)
        raise ValueError(f"Endpoint {name} not found")

    @cluster(num_nodes=1)
    @matrix(endpoint_name=[e.name for e in ENDPOINTS])
    def test_acl_protection(self, endpoint_name: str):
        """
        Verify that schema registry endpoints are protected by the correct ACL resource and operation.
        """
        endpoint = self._get_endpoint_by_name(endpoint_name)

        # Setup any prerequisites
        endpoint.setup()

        # No ACL — should be denied
        result = endpoint.make_request(self.user_auth)
        self.assert_equal(result.status_code, 403)

        # Grant correct ACL
        acl = endpoint.create_acl()
        self._post_acl(acl)

        # Try again — should now succeed
        result = endpoint.make_request(self.user_auth)
        self.assert_equal(result.status_code, 200)

    @cluster(num_nodes=1)
    def test_unauthenticated(self):
        """Test the behaviour of endpoints when they are requested unauthenticated"""

        # Test public endpoints - GET_SCHEMAS_TYPES and SCHEMA_REGISTRY_STATUS_READY
        result = self.sr_client.get_schemas_types()
        self.assert_equal(result.status_code, 200)

        result = self.sr_client.get_status_ready()
        self.assert_equal(result.status_code, 200)

        # Test non-public endpoints - should return 401
        result = self.sr_client.get_config()
        self.assert_equal(result.status_code, 401)

        result = self.sr_client.post_subjects_subject_versions(
            "test-subject", data=self.schema_data_1
        )
        self.assert_equal(result.status_code, 401)

        result = self.sr_client.get_security_acls()
        self.assert_equal(result.status_code, 401)

        result = self.sr_client.get_schemas_ids_id(1)
        self.assert_equal(result.status_code, 401)

        result = self.sr_client.get_subjects()
        self.assert_equal(result.status_code, 401)

    @cluster(num_nodes=1)
    def test_acl_endpoints(self):
        """Test the ACL GET/POST/DELETE endpoints which are protected by the kafka cluster resource"""

        def check_acl_endpoints(expected_success):
            acl = self._create_acl("*", "SUBJECT", "LITERAL", "WRITE")

            result = self.sr_client.get_security_acls(auth=self.user_auth)
            self.assert_equal(result.status_code, 200 if expected_success else 403)

            result = self.sr_client.post_security_acls([acl], auth=self.user_auth)
            self.assert_equal(result.status_code, 201 if expected_success else 403)

            result = self.sr_client.delete_security_acls([acl], auth=self.user_auth)
            self.assert_equal(result.status_code, 200 if expected_success else 403)

        def grant_cluster_acl():
            def try_create_acl():
                self.rpk.acl_create_allow_cluster(self.user.username, "all")

                def present(acl):
                    return all(
                        part in acl for part in [self.user.username, "CLUSTER", "ALL"]
                    )

                return all(
                    any(
                        present(acl)
                        for acl in self.rpk.acl_list(node=node).splitlines()
                    )
                    for node in self.redpanda.nodes
                )

            # Retry to handle NOT_CONTROLLER errors
            wait_until(
                try_create_acl,
                timeout_sec=20,
                backoff_sec=1,
                retry_on_exc=True,
                err_msg="ACL not created",
            )

        check_acl_endpoints(expected_success=False)

        grant_cluster_acl()

        check_acl_endpoints(expected_success=True)

    @cluster(num_nodes=1)
    def test_superuser_access(self):
        """Test that superusers have access to all ACL-protected endpoints"""
        # Check a global endpoint
        result = self.sr_client.set_config(
            data=json.dumps({"compatibility": "FULL"}), auth=self.super_auth
        )
        self.assert_equal(result.status_code, 200)

        # Check a subject-level endpoint
        subject = "test-subject"
        result = self.sr_client.post_subjects_subject_versions(
            subject, data=self.schema_data_1, auth=self.super_auth
        )
        self.assert_equal(result.status_code, 200)

        # Check deferred endpoints
        schema_id = result.json()["id"]
        result = self.sr_client.get_schemas_ids_id(schema_id, auth=self.super_auth)
        self.assert_equal(result.status_code, 200)

        result = self.sr_client.get_subjects(auth=self.super_auth)
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json(), [subject])

    @cluster(num_nodes=1)
    def test_resource_patterns(self):
        """Test that prefixed and global pattern matching of resources works"""

        def check_post_schemas(can_post_1, can_post_2):
            result = self.sr_client.post_subjects_subject_versions(
                "test-subject-1", data=self.schema_data_1, auth=self.user_auth
            )
            self.assert_equal(result.status_code, 200 if can_post_1 else 403)

            result = self.sr_client.post_subjects_subject_versions(
                "test-subject-2", data=self.schema_data_2, auth=self.user_auth
            )
            self.assert_equal(result.status_code, 200 if can_post_2 else 403)

        # Check prefix matching works
        acl_1 = self._create_acl("test-subject-", "SUBJECT", "PREFIXED", "WRITE")
        self._post_acl(acl_1)

        check_post_schemas(can_post_1=True, can_post_2=True)

        # Check denying overwrites a prefixed allow
        acl_2 = self._create_acl(
            "test-subject-2", "SUBJECT", "LITERAL", "WRITE", "DENY"
        )
        self._post_acl(acl_2)

        check_post_schemas(can_post_1=True, can_post_2=False)

        # Check * matching works
        acl_3 = self._create_acl("*", "SUBJECT", "LITERAL", "WRITE", "DENY")
        self._post_acl(acl_3)

        check_post_schemas(can_post_1=False, can_post_2=False)

    @cluster(num_nodes=1)
    def test_get_schemas_ids_id_authorization(self):
        """
        Test GET /schemas/ids/{id} endpoint authorization logic.
        This endpoint allows access if the user has READ permission on ANY subject
        that references the schema.
        """

        # Create test subjects referencing the same schema
        subject_1 = "test-subject-1"
        subject_2 = "test-subject-2"
        subject_3 = "test-subject-3"

        schema_id = self._create_schema(subject_1)

        response = self.sr_client.post_subjects_subject_versions(
            subject_2, data=self.schema_data_1, auth=self.super_auth
        )
        self.assert_equal(response.status_code, 200)
        self.assert_equal(response.json()["id"], schema_id)

        response = self.sr_client.post_subjects_subject_versions(
            subject_3, data=self.schema_data_1, auth=self.super_auth
        )
        self.assert_equal(response.status_code, 200)
        self.assert_equal(response.json()["id"], schema_id)

        # Unknown schema id - should be 403 (don't leak presence info)
        result = self.sr_client.get_schemas_ids_id(99999, auth=self.user_auth)
        self.assert_equal(result.status_code, 403)

        # No ACLs - should be denied
        result = self.sr_client.get_schemas_ids_id(schema_id, auth=self.user_auth)
        self.assert_equal(result.status_code, 403)

        # Grant READ to subject_1 - should succeed
        self._post_acl(self._create_acl(subject_1, "SUBJECT", "LITERAL", "READ"))
        result = self.sr_client.get_schemas_ids_id(schema_id, auth=self.user_auth)
        self.assert_equal(result.status_code, 200)

        # Switch access to subject_2 - should still work (any subject access sufficient)
        self._post_acl(
            self._create_acl(subject_1, "SUBJECT", "LITERAL", "READ", "DENY")
        )
        self._post_acl(self._create_acl(subject_2, "SUBJECT", "LITERAL", "READ"))
        result = self.sr_client.get_schemas_ids_id(schema_id, auth=self.user_auth)
        self.assert_equal(result.status_code, 200)

        # Remove all access - should be denied
        self._post_acl(
            self._create_acl(subject_2, "SUBJECT", "LITERAL", "READ", "DENY")
        )
        result = self.sr_client.get_schemas_ids_id(schema_id, auth=self.user_auth)
        self.assert_equal(result.status_code, 403)

        # Grant access to subject 3 using a prefixed ACL - should succeed
        self._post_acl(self._create_acl("test-subject-", "SUBJECT", "PREFIXED", "READ"))
        result = self.sr_client.get_schemas_ids_id(schema_id, auth=self.user_auth)
        self.assert_equal(result.status_code, 200)

        # Delete the only subject that granted access to the endpoint
        # Should still succeed since soft-deleted subjects also count
        result = self.sr_client.delete_subject(subject_3, auth=self.super_auth)
        self.assert_equal(result.status_code, 200)

        result = self.sr_client.get_schemas_ids_id(schema_id, auth=self.user_auth)
        self.assert_equal(result.status_code, 200)

        # Remove access to all subjects - should be denied
        self._post_acl(self._create_acl("*", "SUBJECT", "LITERAL", "READ", "DENY"))
        result = self.sr_client.get_schemas_ids_id(schema_id, auth=self.user_auth)
        self.assert_equal(result.status_code, 403)

    @cluster(num_nodes=1)
    def test_get_schemas_ids_no_match(self):
        """
        Test that access is denied when no subject referencing the schema allows access.

        Even with a wildcard (*) ALLOW rule, if all specific subjects that reference
        the schema are explicitly denied, the endpoint should return 403.
        """
        subject_1 = "test-subject-1"
        schema_id = self._create_schema(subject_1)

        # Verify wildcard (*) ALLOW grants access
        self._post_acl(self._create_acl("*", "SUBJECT", "LITERAL", "READ"))
        result = self.sr_client.get_schemas_ids_id(schema_id, auth=self.user_auth)
        self.assert_equal(result.status_code, 200)

        # Add specific DENY to override wildcard ALLOW - should be denied (no subject grants access)
        self._post_acl(
            self._create_acl(subject_1, "SUBJECT", "LITERAL", "READ", "DENY")
        )
        result = self.sr_client.get_schemas_ids_id(schema_id, auth=self.user_auth)
        self.assert_equal(result.status_code, 403)

    @cluster(num_nodes=1)
    def test_get_subjects_authorization(self):
        """
        Test GET /subjects endpoint authorization logic.
        This endpoint filters subjects based on individual READ permissions,
        only returning subjects the user is authorized to access.
        """

        # Create multiple test subjects with different schemas
        subject_1 = "test-subject-1"
        subject_2 = "test-subject-2"

        # Create subjects with schemas (using superuser)
        self._create_schema(subject_1)
        self._create_schema(subject_2)

        # No ACLs - should return empty list
        result = self.sr_client.get_subjects(auth=self.user_auth)
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json(), [])

        # Grant READ to subject_1 only - should only return subject_1
        self._post_acl(self._create_acl(subject_1, "SUBJECT", "LITERAL", "DESCRIBE"))
        result = self.sr_client.get_subjects(auth=self.user_auth)
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json(), [subject_1])

        # Grant wildcard (*) access - should return all subjects
        self._post_acl(self._create_acl("*", "SUBJECT", "LITERAL", "DESCRIBE"))
        result = self.sr_client.get_subjects(auth=self.user_auth)
        self.assert_equal(result.status_code, 200)
        self.assert_equal(set(result.json()), {subject_1, subject_2})

        # Deny all access - should return no subjects
        self._post_acl(self._create_acl("*", "SUBJECT", "LITERAL", "DESCRIBE", "DENY"))
        result = self.sr_client.get_subjects(auth=self.user_auth)
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json(), [])

    @cluster(num_nodes=1)
    def test_context_acl_prefix_authorization(self):
        """
        Test that prefix-based ACLs can authorize access to all subjects
        within a context. Verifies that ACL on ':.staging:' (prefix) grants
        access to all subjects in the .staging context.
        """
        schema_data = json.dumps({"schema": schema1_def})

        # Create subjects in different contexts
        staging_subject_1 = ":.staging:topic-1"
        staging_subject_2 = ":.staging:topic-2"
        prod_subject = ":.prod:topic-1"
        default_subject = "topic-1"

        # Register schemas in contexts (using superuser)
        for subject in [
            staging_subject_1,
            staging_subject_2,
            prod_subject,
            default_subject,
        ]:
            result = self.sr_client.post_subjects_subject_versions(
                subject=subject, data=schema_data, auth=self.super_auth
            )
            self.assert_equal(result.status_code, 200)

        # No ACLs - should deny access to all subjects
        result = self.sr_client.get_subjects_subject_versions(
            subject=staging_subject_1, auth=self.user_auth
        )
        self.assert_equal(result.status_code, 403)

        # Grant prefix ACL on .staging context - should allow all .staging subjects
        self._post_acl(self._create_acl(":.staging:", "SUBJECT", "PREFIXED", "READ"))

        # Should allow access to .staging subjects
        result = self.sr_client.get_subjects_subject_versions(
            subject=staging_subject_1, auth=self.user_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json(), [1])

        result = self.sr_client.get_subjects_subject_versions(
            subject=staging_subject_2, auth=self.user_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json(), [1])

        # Should deny access to .prod subjects
        result = self.sr_client.get_subjects_subject_versions(
            subject=prod_subject, auth=self.user_auth
        )
        self.assert_equal(result.status_code, 403)

        # Should deny access to default context subjects
        result = self.sr_client.get_subjects_subject_versions(
            subject=default_subject, auth=self.user_auth
        )
        self.assert_equal(result.status_code, 403)

        # GET /subjects should filter correctly by context ACL
        result = self.sr_client.get_subjects(auth=self.user_auth)
        self.assert_equal(result.status_code, 200)
        # Should return qualified subjects for .staging context
        result_subjects = set(result.json())
        self.assert_in(":.staging:topic-1", result_subjects)
        self.assert_in(":.staging:topic-2", result_subjects)
        self.assert_not_in(":.prod:topic-1", result_subjects)
        self.assert_not_in("topic-1", result_subjects)

    @cluster(num_nodes=3)
    def test_enterprise_sanctions(self):
        """
        Test sanctions when the license is invalid.

        1. schema_registry_enable_authorization cannot be enabled
        2. ACLs cannot be modified via POST/DELETE /security/acls
        3. Existing ACLs are honoured
        """
        get_config_sub = GetConfigSubjectEndpoint(self)

        # Setup GetConfigSubject
        get_config_sub.setup()
        get_config_sub_acl = get_config_sub.create_acl()
        self._post_acl(get_config_sub_acl)

        result = get_config_sub.make_request(self.user_auth)
        self.assert_equal(
            result.status_code, 200, f"Failed to licensed get_config_sub: {result.text}"
        )

        # Disable the license and restart
        self.redpanda.set_environment(
            {"__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE": True}
        )
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Verify ACLs still work
        result = get_config_sub.make_request(self.user_auth)
        self.assert_equal(
            result.status_code,
            200,
            f"Failed to unlicensed get_config_sub: {result.text}",
        )

        # Verify ACLs can be requested
        result = self.sr_client.get_security_acls(auth=self.super_auth)
        self.assert_equal(
            result.status_code,
            200,
            f"Failed to unlicensed get_security_acls: {result.text}",
        )

        # Verify ACLs cannot be created or deleted
        post_config_sub = PutConfigSubjectEndpoint(self)
        post_config_sub_acl = post_config_sub.create_acl()
        for endpoint, acl in [
            (self.sr_client.post_security_acls, [post_config_sub_acl]),
            (self.sr_client.delete_security_acls, [get_config_sub_acl]),
        ]:
            resp = endpoint(acl, auth=self.super_auth)
            self.assert_equal(
                resp.status_code,
                403,
                f"ACL action {endpoint.__name__} should be forbidden after sanctions",
            )

        # Verify schema registry authorization can be disabled
        self.redpanda.set_cluster_config(
            {"schema_registry_enable_authorization": False}
        )

        # Verify schema registry authorization cannot be enabled
        with expect_exception(requests.exceptions.HTTPError, lambda e: True):
            self.redpanda.set_cluster_config(
                {"schema_registry_enable_authorization": True}
            )


class SchemaRegistryContextAuthzTest(SchemaRegistryAclAuthzTestBase):
    """
    Authorization tests for context-qualified subject functionality.

    These tests verify that Schema Registry correctly enforces ACL authorization
    when using context-qualified subjects and the subject query parameter.
    """

    def __init__(self, context: TestContext, **kwargs: Any):
        super().__init__(
            context,
            extra_rp_conf={"schema_registry_enable_qualified_subjects": True},
            **kwargs,
        )

    def _setup_test_schemas(self):
        """Create schemas used by all authorization tests."""
        schema_data = json.dumps({"schema": schema1_def})
        schema_data_2 = json.dumps({"schema": schema2_def})

        # sub1 and sub2 in default context share the same schema
        result = self.sr_client.post_subjects_subject_versions(
            "sub1", data=schema_data, auth=self.super_auth
        )
        self.assert_equal(result.status_code, 200)
        self.schema_id_default_ctx = result.json()["id"]

        result = self.sr_client.post_subjects_subject_versions(
            "sub2", data=schema_data, auth=self.super_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json()["id"], self.schema_id_default_ctx)

        # :.ctx1:sub1 also has the same schema
        result = self.sr_client.post_subjects_subject_versions(
            ":.ctx1:sub1", data=schema_data, auth=self.super_auth
        )
        self.assert_equal(result.status_code, 200)
        self.schema_id_ctx1 = result.json()["id"]

        # :.ctx1:sub2 also has the same schema
        result = self.sr_client.post_subjects_subject_versions(
            ":.ctx1:sub2", data=schema_data, auth=self.super_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json()["id"], self.schema_id_ctx1)

        # :.ctx1:unique-sub has a different schema (only exists in ctx1)
        result = self.sr_client.post_subjects_subject_versions(
            ":.ctx1:unique-sub", data=schema_data_2, auth=self.super_auth
        )
        self.assert_equal(result.status_code, 200)
        self.unique_id_ctx1 = result.json()["id"]

    def setUp(self):
        super().setUp()
        self._setup_test_schemas()

    @cluster(num_nodes=1)
    def test_subject_param_authorized_and_unauthorized(self):
        """
        GET /schemas/ids/{id}?subject=sub2 fails when user only has READ on sub1,
        even though both subjects reference the same schema.
        Authorization checks only the specified subject.
        GET /schemas/ids/{id}?subject=sub1 succeeds when user has READ on sub1.
        """
        self._post_acl(self._create_acl("sub1", "SUBJECT", "LITERAL", "READ"))
        result = self.sr_client.get_schemas_ids_id(
            self.schema_id_default_ctx, subject="sub2", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 403)

        result = self.sr_client.get_schemas_ids_id(
            self.schema_id_default_ctx, subject="sub1", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json()["schema"], schema1_def)

    @cluster(num_nodes=1)
    def test_context_qualified_subject_with_matching_acl(self):
        """
        GET /schemas/ids/{id}?subject=:.ctx1:sub1 succeeds when user has READ
        on the context-qualified subject :.ctx1:sub1.
        """
        self._post_acl(self._create_acl(":.ctx1:sub1", "SUBJECT", "LITERAL", "READ"))
        result = self.sr_client.get_schemas_ids_id(
            self.schema_id_ctx1, subject=":.ctx1:sub1", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json()["schema"], schema1_def)

    @cluster(num_nodes=1)
    def test_context_qualified_subject_without_acl_on_that_context(self):
        """
        GET /schemas/ids/{id}?subject=:.ctx1:sub2 fails when user has READ on
        sub1 (default context) but not on :.ctx1:sub2.
        """
        self._post_acl(self._create_acl("sub1", "SUBJECT", "LITERAL", "READ"))
        result = self.sr_client.get_schemas_ids_id(
            self.schema_id_ctx1, subject=":.ctx1:sub2", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 403)

    @cluster(num_nodes=1)
    def test_cross_context_search_finds_subject_in_non_default_context(self):
        """
        GET /schemas/ids/{id}?subject=unique-sub succeeds when the subject only
        exists in ctx1 as :.ctx1:unique-sub and user has READ on :.ctx1:unique-sub.
        Cross-context search finds the subject in the non-default context.
        """
        self._post_acl(
            self._create_acl(":.ctx1:unique-sub", "SUBJECT", "LITERAL", "READ")
        )
        result = self.sr_client.get_schemas_ids_id(
            self.unique_id_ctx1, subject="unique-sub", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json()["schema"], schema2_def)

    @cluster(num_nodes=1)
    def test_cross_context_search_no_auth_on_found_context(self):
        """
        GET /schemas/ids/{id}?subject=unique-sub fails when the subject is found
        via cross-context search in ctx1 but user has DENY on :.ctx1:unique-sub.
        """
        self._post_acl(
            self._create_acl(":.ctx1:unique-sub", "SUBJECT", "LITERAL", "READ", "DENY")
        )
        result = self.sr_client.get_schemas_ids_id(
            self.unique_id_ctx1, subject="unique-sub", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 403)

    @cluster(num_nodes=1)
    def test_context_only_param_with_auth_on_ctx_subject(self):
        """
        GET /schemas/ids/{id}?subject=:.ctx1: succeeds when user has READ on
        at least one subject in ctx1 (:.ctx1:sub1).
        Context-only param checks all subjects in that context.
        """
        self._post_acl(self._create_acl(":.ctx1:sub1", "SUBJECT", "LITERAL", "READ"))
        result = self.sr_client.get_schemas_ids_id(
            self.schema_id_ctx1, subject=":.ctx1:", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json()["schema"], schema1_def)

    @cluster(num_nodes=1)
    def test_context_only_param_without_auth_on_any_ctx_subject(self):
        """
        GET /schemas/ids/{id}?subject=:.ctx1: fails when user has no READ
        permission on any subject in ctx1.
        """
        # Only grant access to default context subject, not ctx1
        self._post_acl(self._create_acl("sub1", "SUBJECT", "LITERAL", "READ"))
        result = self.sr_client.get_schemas_ids_id(
            self.schema_id_ctx1, subject=":.ctx1:", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 403)

    @cluster(num_nodes=1)
    def test_prefix_acl_covers_context_qualified_subject(self):
        """
        GET /schemas/ids/{id}?subject=:.ctx1:sub1 succeeds when user has a
        PREFIX ACL on :.ctx1: which covers all subjects in that context.
        """
        self._post_acl(self._create_acl(":.ctx1:", "SUBJECT", "PREFIXED", "READ"))
        result = self.sr_client.get_schemas_ids_id(
            self.schema_id_ctx1, subject=":.ctx1:sub1", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json()["schema"], schema1_def)

        result = self.sr_client.get_schemas_ids_id(
            self.schema_id_ctx1, subject=":.ctx1:sub2", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json()["schema"], schema1_def)

        result = self.sr_client.get_schemas_ids_id(
            self.unique_id_ctx1, subject=":.ctx1:unique-sub", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 200)
        self.assert_equal(result.json()["schema"], schema2_def)

    @cluster(num_nodes=1)
    def test_nonexistent_schema_id_returns_403_not_404(self):
        """
        GET /schemas/ids/{id}?subject=sub1 returns 403 (not 404) for non-existent
        schema ID when user lacks authorization. This prevents information leakage
        about whether a schema ID exists.
        """
        result = self.sr_client.get_schemas_ids_id(
            99999, subject="sub1", auth=self.user_auth
        )
        self.assert_equal(result.status_code, 403)
