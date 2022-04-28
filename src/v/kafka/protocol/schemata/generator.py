# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

#
# Code generator for kafka messages
# =================================
#
# Message schemas are taken from the 2.4 branch.
#
# Kafka reference on schema:
#     https://github.com/apache/kafka/blob/2.4/clients/src/main/resources/common/message/README.md
#
# TODO:
#   - It has become clear that for the vast majority of cases where are using the
#   path_type_map to override types, it would be more efficient to specify the
#   same mapping using the field_name_type_map + a whitelist of request types.
#
#   - Handle ignorable fields. Currently we handle nullable fields properly. The
#   ignorable flag on a field doesn't change the wire protocol, but gives
#   instruction on how things should behave when there is missing data.
#
import io
import json
import functools
import pathlib
import re
import sys
import textwrap
import jsonschema
import jinja2
import enum

# Type overrides
# ==============
#
# The following four mappings:
#
#    - path_type_map
#    - entity_type_map
#    - field_name_type_map
#    - basic_type_map
#
# control how the types in a kafka message schema translate to redpanda types.
# the mappings are in order of preference. that is, a match in path_type_map
# will override a match in the entity_type_map.

# nested dictionary path within a json document
path_type_map = {
    "OffsetFetchRequestData": {
        "Topics": {
            "PartitionIndexes": ("model::partition_id", "int32"),
        }
    },
    "OffsetFetchResponseData": {
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "CommittedOffset": ("model::offset", "int64"),
                "CommittedLeaderEpoch": ("kafka::leader_epoch", "int32"),
            },
        }
    },
    "OffsetCommitRequestData": {
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "CommittedOffset": ("model::offset", "int64"),
                "CommittedLeaderEpoch": ("kafka::leader_epoch", "int32"),
            },
        },
        "MemberId": ("kafka::member_id", "string"),
        "GroupInstanceId": ("kafka::group_instance_id", "string"),
    },
    "AddPartitionsToTxnRequestData": {
        "Topics": {
            "Partitions": ("model::partition_id", "int32")
        }
    },
    "AddPartitionsToTxnResponseData": {
        "Results": {
            "Results": {
                "PartitionIndex": ("model::partition_id", "int32")
            }
        }
    },
    "TxnOffsetCommitRequestData": {
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "CommittedOffset": ("model::offset", "int64"),
                "CommittedLeaderEpoch": ("kafka::leader_epoch", "int32"),
            },
        }
    },
    "JoinGroupRequestData": {
        "MemberId": ("kafka::member_id", "string"),
        "GroupInstanceId": ("kafka::group_instance_id", "string"),
        "ProtocolType": ("kafka::protocol_type", "string"),
        "Protocols": {
            "Name": ("kafka::protocol_name", "string"),
        },
    },
    "JoinGroupResponseData": {
        "GenerationId": ("kafka::generation_id", "int32"),
        "ProtocolName": ("kafka::protocol_name", "string"),
        "Leader": ("kafka::member_id", "string"),
        "MemberId": ("kafka::member_id", "string"),
        "Members": {
            "MemberId": ("kafka::member_id", "string"),
            "GroupInstanceId": ("kafka::group_instance_id", "string"),
        },
    },
    "SyncGroupRequestData": {
        "GenerationId": ("kafka::generation_id", "int32"),
        "MemberId": ("kafka::member_id", "string"),
        "GroupInstanceId": ("kafka::group_instance_id", "string"),
        "Assignments": {
            "MemberId": ("kafka::member_id", "string"),
        },
    },
    "HeartbeatRequestData": {
        "GenerationId": ("kafka::generation_id", "int32"),
        "MemberId": ("kafka::member_id", "string"),
        "GroupInstanceId": ("kafka::group_instance_id", "string"),
    },
    "LeaveGroupRequestData": {
        "MemberId": ("kafka::member_id", "string"),
        "Members": {
            "MemberId": ("kafka::member_id", "string"),
            "GroupInstanceId": ("kafka::group_instance_id", "string"),
        },
    },
    "LeaveGroupResponseData": {
        "Members": {
            "MemberId": ("kafka::member_id", "string"),
            "GroupInstanceId": ("kafka::group_instance_id", "string"),
        },
    },
    "DeleteTopicsRequestData": {
        "TimeoutMs": ("std::chrono::milliseconds", "int32"),
    },
    "CreateTopicsRequestData": {
        "timeoutMs": ("std::chrono::milliseconds", "int32"),
        "Topics": {
            "Assignments": {
                "PartitionIndex": ("model::partition_id", "int32"),
            },
        },
    },
    "FindCoordinatorRequestData": {
        "KeyType": ("kafka::coordinator_type", "int8"),
    },
    "ListOffsetRequestData": {
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "Timestamp": ("model::timestamp", "int64"),
                "CurrentLeaderEpoch": ("kafka::leader_epoch", "int32"),
            },
        },
    },
    "ListOffsetResponseData": {
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "Timestamp": ("model::timestamp", "int64"),
                "Offset": ("model::offset", "int64"),
                "LeaderEpoch": ("kafka::leader_epoch", "int32"),
            },
        },
    },
    "DescribeGroupsResponseData": {
        "Groups": {
            "ProtocolType": ("kafka::protocol_type", "string"),
            "Members": {
                "MemberId": ("kafka::member_id", "string"),
                "GroupInstanceId": ("kafka::group_instance_id", "string"),
            },
        },
    },
    "DescribeConfigsRequestData": {
        "Resources": {
            "ResourceType": ("kafka::config_resource_type", "int8"),
        },
    },
    "DescribeConfigsResponseData": {
        "Results": {
            "ResourceType": ("kafka::config_resource_type", "int8"),
            "Configs": {
                "ConfigSource": ("kafka::describe_configs_source", "int8"),
            },
        },
    },
    "ProduceRequestData": {
        "TimeoutMs": ("std::chrono::milliseconds", "int32"),
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "Records": ("kafka::produce_request_record_data", "iobuf"),
            },
        },
    },
    "ProduceResponseData": {
        "Responses": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "BaseOffset": ("model::offset", "int64"),
                "LogAppendTimeMs": ("model::timestamp", "int64"),
                "LogStartOffset": ("model::offset", "int64"),
            },
        },
    },
    "MetadataResponseData": {
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "IsrNodes": ("model::node_id", "int32"),
                "LeaderEpoch": ("kafka::leader_epoch", "int32"),
            },
        },
    },
    "FetchRequestData": {
        "MaxWaitMs": ("std::chrono::milliseconds", "int32"),
        "IsolationLevel": ("model::isolation_level", "int8"),
        "Topics": {
            "FetchPartitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "FetchOffset": ("model::offset", "int64"),
                "CurrentLeaderEpoch": ("kafka::leader_epoch", "int32"),
            },
        },
    },
    "FetchResponseData": {
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "HighWatermark": ("model::offset", "int64"),
                "LastStableOffset": ("model::offset", "int64"),
                "LogStartOffset": ("model::offset", "int64"),
                "Records": ("kafka::batch_reader", "fetch_record_set"),
            },
        },
    },
    "InitProducerIdRequestData": {
        "TransactionTimeoutMs": ("std::chrono::milliseconds", "int32")
    },
    "CreatePartitionsRequestData": {
        "TimeoutMs": ("std::chrono::milliseconds", "int32")
    },
    "OffsetForLeaderEpochRequestData": {
        "Topics": {
            "Partitions": {
                "Partition": ("model::partition_id", "int32"),
                "CurrentLeaderEpoch": ("kafka::leader_epoch", "int32"),
                "LeaderEpoch": ("kafka::leader_epoch", "int32"),
            }
        }
    },
    "OffsetForLeaderEpochResponseData": {
        "Topics": {
            "Partitions": {
                "Partition": ("model::partition_id", "int32"),
                "LeaderEpoch": ("kafka::leader_epoch", "int32"),
                "EndOffset": ("model::offset", "int64"),
            }
        }
    }
}

# a few kafka field types specify an entity type
entity_type_map = dict(
    groupId=("kafka::group_id", "string"),
    transactionalId=("kafka::transactional_id", "string"),
    topicName=("model::topic", "string"),
    brokerId=("model::node_id", "int32"),
    producerId=("kafka::producer_id", "int64"),
)

# mapping specified as a combination of native type and field name
field_name_type_map = {
    ("int16", "ErrorCode"): ("kafka::error_code", None),
    ("int32", "ThrottleTimeMs"): ("std::chrono::milliseconds", 0),
    ("int32", "SessionTimeoutMs"): ("std::chrono::milliseconds", None),
    ("int32", "RebalanceTimeoutMs"): ("std::chrono::milliseconds", None),
}

# primitive types
basic_type_map = dict(
    string=("ss::sstring", "read_string()", "read_nullable_string()",
            "read_flex_string()", "read_nullable_flex_string()"),
    bytes=("bytes", "read_bytes()", None, "read_flex_bytes()", None),
    bool=("bool", "read_bool()"),
    int8=("int8_t", "read_int8()"),
    int16=("int16_t", "read_int16()"),
    int32=("int32_t", "read_int32()"),
    int64=("int64_t", "read_int64()"),
    iobuf=("iobuf", None, "read_fragmented_nullable_bytes()", None,
           "read_fragmented_nullable_flex_bytes()"),
    fetch_record_set=("batch_reader", None, "read_nullable_batch_reader()",
                      None, "read_nullable_flex_batch_reader()"),
)

# apply a rename to a struct. this is useful when there is a type name conflict
# between two request types. since we generate types in a flat namespace this
# feature is important for resolving naming conflicts.
#
# the format here is the field name path terminating with the expected type name
# mapping to the new type name.
# yapf: disable
struct_renames = {
    ("IncrementalAlterConfigsRequestData", "Resources"):
        ("AlterConfigsResource", "IncrementalAlterConfigsResource"),

    ("IncrementalAlterConfigsRequestData", "Resources", "Configs"):
        ("AlterableConfig", "IncrementalAlterableConfig"),

    ("IncrementalAlterConfigsResponseData", "Responses"):
        ("AlterConfigsResourceResponse", "IncrementalAlterConfigsResourceResponse"),
}
# yapf: enable


def make_context_field(path):
    """
    For a given path return a special field to be added to a generated
    structure. This structure will not be encoded/decoded on the wire and is
    used to add some extra context.
    """
    if path == ("FetchResponseData", "Topics", "Partitions"):
        return ("bool", "has_to_be_included{true}")


# a listing of expected struct types
STRUCT_TYPES = [
    "ApiVersionsRequestKey",
    "ApiVersionsResponseKey",
    "OffsetFetchRequestTopic",
    "OffsetFetchResponseTopic",
    "OffsetFetchResponsePartition",
    "OffsetCommitRequestTopic",
    "OffsetCommitRequestPartition",
    "OffsetCommitResponseTopic",
    "OffsetCommitResponsePartition",
    "JoinGroupRequestProtocol",
    "JoinGroupResponseMember",
    "SyncGroupRequestAssignment",
    "MemberIdentity",
    "MemberResponse",
    "DeletableTopicResult",
    "DescribeConfigsResult",
    "DescribeConfigsResource",
    "DescribeConfigsResourceResult",
    "DescribeConfigsSynonym",
    "ListOffsetTopic",
    "ListOffsetTopicResponse",
    "ListOffsetPartitionResponse",
    "ListOffsetPartition",
    "AlterConfigsResource",
    "AlterableConfig",
    "AlterConfigsResourceResponse",
    "ListedGroup",
    "DescribedGroup",
    "DescribedGroupMember",
    "CreatableTopic",
    "CreatableTopicResult",
    "CreatableReplicaAssignment",
    "CreateableTopicConfig",
    "CreatableTopicConfigs",
    "DeletableGroupResult",
    "DescribeAclsResource",
    "AclDescription",
    "DescribeLogDirsTopic",
    "DescribableLogDirTopic",
    "DescribeLogDirsResult",
    "DescribeLogDirsPartition",
    "CreatableAcl",
    "CreatableAclResult",
    "DeleteAclsFilter",
    "DeleteAclsFilterResult",
    "DeleteAclsMatchingAcl",
    "TopicProduceResponse",
    "PartitionProduceResponse",
    "BatchIndexAndErrorMessage",
    "TopicProduceData",
    "PartitionProduceData",
    "MetadataResponseBroker",
    "MetadataRequestTopic",
    "MetadataResponseTopic",
    "MetadataResponsePartition",
    "AddPartitionsToTxnTopic",
    "AddPartitionsToTxnTopicResult",
    "AddPartitionsToTxnPartitionResult",
    "TxnOffsetCommitRequestTopic",
    "TxnOffsetCommitResponseTopic",
    "TxnOffsetCommitResponsePartition",
    "TxnOffsetCommitRequestPartition",
    "FetchTopic",
    "ForgottenTopic",
    "FetchPartition",
    "FetchableTopicResponse",
    "FetchablePartitionResponse",
    "AbortedTransaction",
    "CreatePartitionsTopic",
    "CreatePartitionsTopicResult",
    "CreatePartitionsAssignment",
    "OffsetForLeaderTopic",
    "OffsetForLeaderPartition",
    "OffsetForLeaderTopicResult",
    "EpochEndOffset",
]

SCALAR_TYPES = list(basic_type_map.keys())
ENTITY_TYPES = list(entity_type_map.keys())


def apply_struct_renames(path, type_name):
    rename = struct_renames.get(path, None)
    if rename is None:
        return type_name
    assert rename[0] == type_name
    return rename[1]


class VersionRange:
    """
    A version range is fundamentally a range [min, max] but there are several
    different ways in the kafka schema format to specify the bounds.
    """
    def __init__(self, spec):
        self.min, self.max = self._parse(spec)

    def _parse(self, spec):
        match = re.match("^(?P<min>\d+)$", spec)
        if match:
            min = int(match.group("min"))
            return min, min

        match = re.match("^(?P<min>\d+)\+$", spec)
        if match:
            min = int(match.group("min"))
            return min, None

        match = re.match("^(?P<min>\d+)\-(?P<max>\d+)$", spec)
        if match:
            min = int(match.group("min"))
            max = int(match.group("max"))
            return min, max

    guard_modes = enum.Enum('guard_modes', 'GUARD, NO_GUARD, NO_SOURCE')

    @property
    def guard_enum(self):
        return VersionRange.guard_modes

    def _guard(self):
        """
        Generate the C++ bounds check.
        """
        if self.min == self.max:
            cond = f"version == api_version({self.min})"
        else:
            cond = []
            if self.min > 0:
                cond.append(f"version >= api_version({self.min})")
            if self.max != None:
                cond.append(f"version <= api_version({self.max})")
            cond = " && ".join(cond)

        return (self.guard_enum.NO_GUARD,
                None) if cond == "" else (self.guard_enum.GUARD, cond)

    def guard(self, flex, first_flex):
        """
        Optimize generated code by either omitting a guard or source itself
        """
        if first_flex < 0:
            return self._guard()
        elif not flex:
            if self.min >= first_flex:
                return self.guard_enum.NO_SOURCE, None
        else:
            if self.max == None:
                if self.min <= first_flex:
                    return self.guard_enum.NO_GUARD, None
            elif self.max < first_flex:
                return self.guard_enum.NO_SOURCE, None
            elif self.max == first_flex:
                return self.guard_enum.NO_GUARD, None

        return self._guard()

    def __repr__(self):
        max = "+inf)" if self.max is None else f"{self.max}]"
        return f"[{self.min}, {max}"


def snake_case(name):
    """Convert camel to snake case"""
    return name[0].lower() + "".join(
        [f"_{c.lower()}" if c.isupper() else c for c in name[1:]])


class FieldType:
    ARRAY_RE = re.compile("^\[\](?P<type>.+)$")

    def __init__(self, name):
        self._name = name

    @staticmethod
    def create(field, path):
        """
        FieldType factory based on Kafka field type name:

            int32 -> Int32Type
            []int32 -> ArrayType(Int32Type)
            []FooType -> ArrayType(StructType)

        Verifies that structs are only stored in arrays and that there are no array
        of array types like [][]FooType.
        """
        type_name = field["type"]

        match = FieldType.ARRAY_RE.match(type_name)
        is_array = match is not None
        if is_array:
            type_name = match.group("type")
            # we do not assume 2d arrays
            assert FieldType.ARRAY_RE.match(type_name) is None

        if type_name in SCALAR_TYPES:
            t = ScalarType(type_name)
        else:
            assert is_array
            path = path + (field["name"], )
            type_name = apply_struct_renames(path, type_name)
            t = StructType(type_name, field["fields"], path)

        if is_array:
            return ArrayType(t)

        return t

    @property
    def is_struct(self):
        return False

    @property
    def name(self):
        return self._name


class ScalarType(FieldType):
    def __init__(self, name):
        super().__init__(name)

    @property
    def potentially_flexible_type(self):
        """Evaluates to true if the scalar type would be parsed as flex
        if the version is high enough"""
        return self.name == "string" or self.name == "bytes" or self.name == "iobuf"


class StructType(FieldType):
    def __init__(self, name, fields, path=()):
        super().__init__(snake_case(name))
        self.fields = []
        self.tags = []
        for f in fields:
            new_field = Field.create(f, path)
            if new_field.is_tag:
                self.tags.append(new_field)
            else:
                self.fields.append(new_field)
        self.tags.sort(key=lambda x: x._tag)
        self.context_field = make_context_field(path)

    @property
    def is_struct(self):
        return True

    @property
    def format(self):
        """Format string for output operator"""
        return " ".join(map(lambda f: f"{f.name}={{}}", self.fields))

    def structs(self):
        """
        Return all struct types reachable from this struct.
        """
        res = []
        for field in self.fields:
            t = field.type()
            if isinstance(t, ArrayType):
                t = t.value_type()  # unwrap value type
            if isinstance(t, StructType):
                res += t.structs()
                res.append(t)
        return res


class ArrayType(FieldType):
    def __init__(self, value_type):
        # the name of the ArrayType is its value type
        super().__init__(value_type._name)
        self._value_type = value_type

    def value_type(self):
        return self._value_type


class Field:
    def __init__(self, field, field_type, path):
        self._field = field
        self._type = field_type
        self._path = path + (self._field["name"], )
        self._versions = VersionRange(self._field["versions"])
        self._nullable_versions = self._field.get("nullableVersions", None)
        if self._nullable_versions is not None:
            self._nullable_versions = VersionRange(self._nullable_versions)
        self._default_value = self._field.get("default", "")
        if self._default_value == "null":
            self._default_value = ""
        self._tag = self._field.get("tag", None)
        assert len(self._path)

    @staticmethod
    def create(field, path):
        field_type = FieldType.create(field, path)
        return Field(field, field_type, path)

    def type(self):
        return self._type

    def nullable(self):
        return self._nullable_versions is not None

    def versions(self):
        return self._versions

    def default_value(self):
        return self._default_value

    def about(self):
        return self._field.get("about", "<no description>")

    def _redpanda_path_type(self):
        """
        Resolve a redpanda field path type override.
        """
        d = path_type_map
        for p in self._path:
            d = d.get(p, None)
            if d is None:
                break
        if isinstance(d, tuple):
            return d
        return None

    def _redpanda_type(self):
        """
        Resolve a redpanda type override.
        Lookup occurs from most to least specific.
        """
        # path overrides
        path_type = self._redpanda_path_type()
        if path_type:
            return path_type[0], None

        # entity type overrides
        et = self._field.get("entityType", None)
        if et in entity_type_map:
            return entity_type_map[et][0], None

        tn = self._type.name
        fn = self._field["name"]

        # type/name overrides
        if (tn, fn) in field_name_type_map:
            return field_name_type_map[(tn, fn)]

        # fundamental type overrides
        if tn in basic_type_map:
            return basic_type_map[tn][0], None

        return tn, None

    def _redpanda_decoder(self):
        """
        Resolve a redpanda type override.
        Lookup occurs from most to least specific.
        """
        # path overrides
        path_type = self._redpanda_path_type()
        if path_type:
            return basic_type_map[path_type[1]], path_type[0]

        # entity type overrides
        et = self._field.get("entityType", None)
        if et in entity_type_map:
            m = entity_type_map[et]
            return basic_type_map[m[1]], m[0]

        tn = self._type.name
        fn = self._field["name"]

        # type/name overrides
        if (tn, fn) in field_name_type_map:
            return basic_type_map[tn], field_name_type_map[(tn, fn)][0]

        # fundamental type overrides
        if tn in basic_type_map:
            return basic_type_map[tn], None

        raise Exception(f"No decoder for {(tn, fn)}")

    def decoder(self, flex):
        """
        There are two cases:

            1a. plain native: read_int32()
            1b. nullable native: read_nullable_string()
            1c. named types: named_type(read_int32())
            1d. flexible types: read_flex_string()
            1e. flexible nullable native: read_nullable_flex_string()

            2a. optional named types:
                auto tmp = read_nullable_string()
                if (tmp) {
                  named_type(*tmp)
                }
        """
        plain_decoder, named_type = self._redpanda_decoder()
        if self.is_array:
            # array fields never contain nullable types. so if this is an array
            # field then choose the non-nullable decoder for its element type.
            assert plain_decoder[1]
            return plain_decoder[1], named_type
        if self.potentially_flexible_type:
            if flex is True:
                if self.nullable():
                    assert plain_decoder[4]
                    return plain_decoder[4], named_type
                else:
                    assert plain_decoder[3]
                    return plain_decoder[3], named_type
        if self.nullable():
            assert plain_decoder[2]
            return plain_decoder[2], named_type
        assert plain_decoder[1]
        return plain_decoder[1], named_type

    @property
    def is_tag(self):
        return self._tag is not None

    @property
    def is_array(self):
        return isinstance(self._type, ArrayType)

    @property
    def potentially_flexible_type(self):
        return isinstance(self._type,
                          ScalarType) and self._type.potentially_flexible_type

    @property
    def type_name(self):
        name, default_value = self._redpanda_type()
        if isinstance(self._type, ArrayType):
            assert default_value is None  # not supported
            name = f"std::vector<{name}>"
        if self.nullable():
            assert default_value is None  # not supported
            return f"std::optional<{name}>", None
        return name, default_value

    @property
    def value_type(self):
        assert self.is_array
        return self._redpanda_type()[0]

    @property
    def name(self):
        return snake_case(self._field["name"])


HEADER_TEMPLATE = """
#pragma once
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/errors.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>

{% macro render_struct(struct) %}
{{ render_struct_comment(struct) }}
struct {{ struct.name }} {
{%- for field in struct.fields %}
    {%- set info = field.type_name %}
    {%- if info[1] != None %}
    {{ info[0] }} {{ field.name }}{{'{'}}{{info[1]}}{{'}'}};
    {%- else %}
    {{ info[0] }} {{ field.name }}{ {{- field.default_value() -}} };
    {%- endif %}
{%- endfor %}
{%- if struct.context_field %}

    // extra context not part of kafka protocol.
    // added by redpanda. see generator.py:make_context_field.
    {{ struct.context_field[0] }} {{ struct.context_field[1] -}};
{%- endif %}
{% endmacro %}

namespace kafka {

class request_reader;
class response_writer;
class response;

{% for struct in struct.structs() %}
{{ render_struct(struct) }}
    friend std::ostream& operator<<(std::ostream&, const {{ struct.name }}&);
};

{% endfor %}

{{ render_struct(struct) }}
    void encode(response_writer&, api_version);
{%- if op_type == "request" %}
    void decode(request_reader&, api_version);
{%- else %}
    void decode(iobuf, api_version);
{%- endif %}

    friend std::ostream& operator<<(std::ostream&, const {{ struct.name }}&);
{%- if first_flex > 0 %}
private:
    void encode_flex(response_writer&, api_version);
    void encode_standard(response_writer&, api_version);
{%- if op_type == "request" %}
    void decode_flex(request_reader&, api_version);
    void decode_standard(request_reader&, api_version);
{%- else %}
    void decode_flex(iobuf, api_version);
    void decode_standard(iobuf, api_version);
{%- endif %}
{%- endif %}
};

}
"""

SOURCE_TEMPLATE = """
#include "kafka/protocol/schemata/{{ header }}"

#include "cluster/types.h"
#include "kafka/protocol/response_writer.h"
#include "kafka/protocol/request_reader.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

{% macro version_guard(field, flex) %}
{%- set guard_enum = field.versions().guard_enum %}
{%- set e, cond = field.versions().guard(flex, first_flex) %}
{%- if e == guard_enum.NO_GUARD %}
{{- caller() }}
{%- elif e == guard_enum.NO_SOURCE %}
{%- elif e == guard_enum.GUARD %}
if ({{ cond }}) {
{{- caller() | indent }}
}
{%- else %}
#error unexpected enumeration case encountered
{%- endif %}
{%- endmacro %}

{% macro field_encoder(field, obj, flex) %}
{%- if obj %}
{%- set fname = obj + "." + field.name %}
{%- else %}
{%- set fname = field.name %}
{%- endif %}
{%- if field.is_array %}
{%- if field.nullable() %}
{%- if flex %}
writer.write_nullable_flex_array({{ fname }}, [version]({{ field.value_type }}& v, response_writer& writer) {
{%- else %}
writer.write_nullable_array({{ fname }}, [version]({{ field.value_type }}& v, response_writer& writer) {
{%- endif %}
{%- else %}
{%- if flex %}
writer.write_flex_array({{ fname }}, [version]({{ field.value_type }}& v, response_writer& writer) {
{%- else %}
writer.write_array({{ fname }}, [version]({{ field.value_type }}& v, response_writer& writer) {
{%- endif %}
    (void)version;
{%- endif %}
{%- if field.type().value_type().is_struct %}
{{- struct_serde(field.type().value_type(), field_encoder, flex, "v") | indent }}
{%- else %}
    writer.write(v);
{%- endif %}
});
{%- elif flex and field.type().potentially_flexible_type %}
writer.write_flex({{ fname }});
{%- else %}
writer.write({{ fname }});
{%- endif %}
{%- endmacro %}

{% macro field_decoder(field, obj, flex) %}
{%- if obj %}
{%- set fname = obj + "." + field.name %}
{%- else %}
{%- set fname = field.name %}
{%- endif %}
{%- if field.is_array %}
{%- if field.nullable() %}
{%- if flex %}
{{ fname }} = reader.read_nullable_flex_array([version](request_reader& reader) {
{%- else %}
{{ fname }} = reader.read_nullable_array([version](request_reader& reader) {
{%- endif %}
{%- else %}
{%- if flex %}
{{ fname }} = reader.read_flex_array([version](request_reader& reader) {
{%- else %}
{{ fname }} = reader.read_array([version](request_reader& reader) {
{%- endif %}
    (void)version;
{%- endif %}
{%- if field.type().value_type().is_struct %}
    {{ field.type().value_type().name }} v;
{{- struct_serde(field.type().value_type(), field_decoder, flex, "v") | indent }}
    return v;
{%- else %}
{%- set decoder, named_type = field.decoder(flex) %}
{%- if named_type == None %}
    return reader.{{ decoder }};
{%- elif field.nullable() %}
    {
        auto tmp = reader.{{ decoder }};
        if (tmp) {
            return {{ named_type }}(std::move(*tmp));
        }
        return std::nullopt;
    }
{%- else %}
    return {{ named_type }}(reader.{{ decoder }});
{%- endif %}
{%- endif %}
});
{%- else %}
{%- set decoder, named_type = field.decoder(flex) %}
{%- if named_type == None %}
{{ fname }} = reader.{{ decoder }};
{%- elif field.nullable() %}
{
    auto tmp = reader.{{ decoder }};
    if (tmp) {
{%- if named_type == "kafka::produce_request_record_data" %}
        {{ fname }} = {{ named_type }}(std::move(*tmp), version);
{%- else %}
        {{ fname }} = {{ named_type }}(std::move(*tmp));
{%- endif %}
    }
}
{%- else %}
{{ fname }} = {{ named_type }}(reader.{{ decoder }});
{%- endif %}
{%- endif %}
{%- endmacro %}

{% macro struct_serde(struct, field_serde, flex, obj = "") %}
{%- for field in struct.fields %}
{%- call version_guard(field, flex) %}
{{- field_serde(field, obj, flex) }}
{%- endcall %}
{%- endfor %}
{%- if flex %}
{%- if field_serde.name == "field_decoder" %}
reader.consume_tags();
{%- elif field_serde.name == "field_encoder" %}
writer.write_tags();
{%- endif %}
{%- endif %}
{%- endmacro %}

namespace kafka {

{%- if struct.fields %}
{%- if first_flex > 0 %}
void {{ struct.name }}::encode(response_writer& writer, api_version version) {
    if (version >= api_version({{ first_flex }})) {
        encode_flex(writer, version);
    } else {
        encode_standard(writer, version);
    }
}

void {{ struct.name }}::encode_flex(response_writer& writer, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, field_encoder, True) | indent }}
}

void {{ struct.name }}::encode_standard([[maybe_unused]] response_writer& writer, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, field_encoder, False) | indent }}
}

{%- elif first_flex < 0 %}
void {{ struct.name }}::encode(response_writer& writer, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, field_encoder, False) | indent }}
}
{%- else %}
void {{ struct.name }}::encode(response_writer& writer, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, field_encoder, True) | indent }}
}
{%- endif %}


{%- if op_type == "request" %}
{%- if first_flex > 0 %}
void {{ struct.name }}::decode(request_reader& reader, api_version version) {
    if (version >= api_version({{ first_flex }})) {
        decode_flex(reader, version);
    } else {
        decode_standard(reader, version);
    }
}
void {{ struct.name }}::decode_flex(request_reader& reader, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, field_decoder, True) | indent }}
}

void {{ struct.name }}::decode_standard([[maybe_unused]] request_reader& reader, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, field_decoder, False) | indent }}
}
{%- elif first_flex < 0 %}
void {{ struct.name }}::decode(request_reader& reader, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, field_decoder, False) | indent }}
}
{% else %}
void {{ struct.name }}::decode(request_reader& reader, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, field_decoder, True) | indent }}
}
{%- endif %}
{%- else %}

{%- if first_flex > 0 %}
void {{ struct.name }}::decode(iobuf buf, api_version version) {
    if (version >= api_version({{ first_flex }})) {
        decode_flex(std::move(buf), version);
    } else {
        decode_standard(std::move(buf), version);
    }
}
void {{ struct.name }}::decode_flex(iobuf buf, [[maybe_unused]] api_version version) {
    request_reader reader(std::move(buf));

{{- struct_serde(struct, field_decoder, True) | indent }}
}
void {{ struct.name }}::decode_standard(iobuf buf, [[maybe_unused]] api_version version) {
    request_reader reader(std::move(buf));

{{- struct_serde(struct, field_decoder, False) | indent }}
}

{%- elif first_flex < 0 %}
void {{ struct.name }}::decode(iobuf buf, [[maybe_unused]] api_version version) {
    request_reader reader(std::move(buf));

{{- struct_serde(struct, field_decoder, False) | indent }}
}
{%- else %}
void {{ struct.name }}::decode(iobuf buf, [[maybe_unused]] api_version version) {
    request_reader reader(std::move(buf));

{{- struct_serde(struct, field_decoder, True) | indent }}
}
{%- endif %}


{%- endif %}
{%- else %}
{%- if op_type == "request" %}
void {{ struct.name }}::encode(response_writer&, api_version) {}
void {{ struct.name }}::decode(request_reader&, api_version) {}
{%- else %}
void {{ struct.name }}::encode(response_writer&, api_version&) {}
void {{ struct.name }}::decode(iobuf, api_version) {}
{%- endif %}
{%- endif %}

{% set structs = struct.structs() + [struct] %}
{% for struct in structs %}
{%- if struct.fields %}
std::ostream& operator<<(std::ostream& o, const {{ struct.name }}& v) {
    fmt::print(o,
      "{{'{{' + struct.format + '}}'}}",
      {%- for field in struct.fields %}
      v.{{ field.name }}{% if not loop.last %},{% endif %}
      {%- endfor %}
    );
    return o;
}
{%- else %}
std::ostream& operator<<(std::ostream& o, const {{ struct.name }}&) {
    return o << "{}";
}
{%- endif %}
{% endfor %}
}
"""

# This is the schema of the json files from the kafka tree. This isn't strictly
# necessary for the code generator, but it is useful. The schema verification
# performed on our input files from kafka is _very_ strict. Since the json files
# in kafka do not seem to have any sort of formalized structure, verification
# is a check on our assumptions. If verification fails, it should be taken as an
# indication that the generator may need to be updated.
#
# remove scalar type `iobuf` from the set of types used to validate schema. the
# type is not a native kafka type, but is still represented in the code
# generator for some scenarios involving overloads / customizing output.
ALLOWED_SCALAR_TYPES = list(set(SCALAR_TYPES) - set(["iobuf"]))
ALLOWED_TYPES = \
    ALLOWED_SCALAR_TYPES + \
    [f"[]{t}" for t in ALLOWED_SCALAR_TYPES + STRUCT_TYPES]

# yapf: disable
SCHEMA = {
    "definitions": {
        "versions": {
            "oneOf": [
                {
                    "type": "string",
                    "pattern": "^\d+$"
                },
                {
                    "type": "string",
                    "pattern": "^\d+\-\d+$"
                },
                {
                    "type": "string",
                    "pattern": "^\d+\+$"
                },
            ],
        },
        "field": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "type": {
                    "type": "string",
                    "enum": ALLOWED_TYPES,
                },
                "versions": {"$ref": "#/definitions/versions"},
                "taggedVersions": {"$ref": "#/definitions/versions"},
                "tag": {"type": "integer"},
                "nullableVersions": {"$ref": "#/definitions/versions"},
                "entityType": {
                    "type": "string",
                    "enum": ENTITY_TYPES,
                },
                "about": {"type": "string"},
                "default": {
                    "oneOf": [
                        {"type": "integer"},
                        {"type": "string"},
                    ],
                },
                "mapKey": {"type": "boolean"},
                "ignorable": {"type": "boolean"},
                "fields": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/field"},
                },
            },
            "required": [
                "name",
                "type",
                "versions",
            ],
            "additionalProperties": False,
        },
        "fields": {
            "type": "array",
            "items": {"$ref": "#/definitions/field"},
        },
    },
    "type": "object",
    "properties": {
        "apiKey": {"type": "integer"},
        "type": {
            "type": "string",
            "enum": ["request", "response"],
        },
        "name": {"type": "string"},
        "validVersions": {"$ref": "#/definitions/versions"},
        "flexibleVersions": {
            "oneOf": [
                {
                    "type": "string",
                    "enum": ["none"],
                },
                {"$ref": "#/definitions/versions"},
            ],
        },
        "fields": {"$ref": "#/definitions/fields"},
    },
    "required": [
        "apiKey",
        "type",
        "name",
        "validVersions",
        "flexibleVersions",
        "fields",
    ],
    "additionalProperties": False,
}
# yapf: enable


# helper called from template to render a nice struct comment
def render_struct_comment(struct):
    indent = " * "
    wrapper = textwrap.TextWrapper(initial_indent=indent,
                                   subsequent_indent=indent,
                                   width=80)
    comment = wrapper.fill(f"The {struct.name} message.") + "\n"
    comment += indent + "\n"

    max_width = 0
    for field in struct.fields:
        max_width = max(len(field.name), max_width)

    for field in struct.fields:
        field_indent = indent + f"{field.name:>{max_width}}: "
        wrapper = textwrap.TextWrapper(initial_indent=field_indent,
                                       subsequent_indent=indent + " " *
                                       (2 + max_width),
                                       width=80)
        about = field.about() + f" Supported versions: {field.versions()}"
        comment += wrapper.fill(about) + "\n"

    return f"/*\n{comment} */"


def parse_flexible_versions(flex_version):
    # A first_flex value of 0 will produce code that is optimized to do no flex
    # version branching, since all versions since inception were always flexible
    #
    # A first_flex value of -1 indicates the value of flexibleVersions was set to 'none'.
    # These requests will always be interpreted as non-flex and produce no conditional to
    # branch and parse a flex request.
    #
    # All other types of requests will produce code that checks for the flex version once
    # (at the beginning of encode/decode) and will not add redundent checks i.e. check
    # if its ok to serialize something at version 7 if it already branched due to flex and
    # that flex version is already greater than 7.
    if flex_version == "none":
        return -1
    r = VersionRange(flex_version)
    return r.min


if __name__ == "__main__":
    assert len(sys.argv) == 3
    outdir = pathlib.Path(sys.argv[1])
    schema_path = pathlib.Path(sys.argv[2])
    src = (outdir / schema_path.name).with_suffix(".cc")
    hdr = (outdir / schema_path.name).with_suffix(".h")

    # remove comments from the json file. comments are a non-standard json
    # extension that is not supported by the python json parser.
    schema = io.StringIO()
    with open(schema_path, "r") as f:
        for line in f.readlines():
            line = re.sub("\/\/.*", "", line)
            if line.strip():
                schema.write(line)

    # parse json and verify its schema.
    msg = json.loads(schema.getvalue())
    jsonschema.validate(instance=msg, schema=SCHEMA)

    # the root struct in the schema corresponds to the root type in redpanda.
    # but its naming in snake case will conflict with our high level request and
    # response types so arrange for a "_data" suffix to be generated.
    type_name = f"{msg['name']}Data"
    struct = StructType(type_name, msg["fields"], (type_name, ))

    # request or response
    op_type = msg["type"]

    # either 'none' or 'VersionRange'
    first_flex = parse_flexible_versions(msg["flexibleVersions"])

    with open(hdr, 'w') as f:
        f.write(
            jinja2.Template(HEADER_TEMPLATE).render(
                struct=struct,
                render_struct_comment=render_struct_comment,
                op_type=op_type,
                first_flex=first_flex))

    with open(src, 'w') as f:
        f.write(
            jinja2.Template(SOURCE_TEMPLATE).render(struct=struct,
                                                    header=hdr.name,
                                                    op_type=op_type,
                                                    first_flex=first_flex))
