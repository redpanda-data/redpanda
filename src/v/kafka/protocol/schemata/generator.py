#!/usr/bin/env python3
#
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
#   - Build a more robust way to define messages and their contents as
#   sensitive. The current approach relies on a list of root structs to
#   generate stream operators that don't contain sensitive information. We
#   should strongly consider leveraging the C++ type system to generate code
#   that forces users to explicitly "unwrap" sensitive fields before use, to
#   make it easier to audit where sensitive information is used.
import io
import json
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
    "OffsetDeleteRequestData": {
        "GroupId": ("kafka::group_id", "string"),
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32")
            }
        }
    },
    "OffsetDeleteResponseData": {
        "ErrorCode": ("kafka::error_code", "int16"),
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
                "ErrorCode": ("kafka::error_code", "int16"),
            }
        }
    },
    "TxnOffsetCommitRequestData": {
        "MemberId": ("kafka::member_id", "string"),
        "GroupInstanceId": ("kafka::group_instance_id", "string"),
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
    "CreateTopicsResponseData": {
        "Topics": {
            "Configs": {
                "ConfigSource": ("kafka::describe_configs_source", "int8"),
            },
            "TopicConfigErrorCode": ("kafka::error_code", "int16"),
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
                "ConfigType": ("kafka::describe_configs_type", "int8"),
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
    },
    "AlterPartitionReassignmentsRequestData": {
        "TimeoutMs": ("std::chrono::milliseconds", "int32"),
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
            },
        }
    },
    "AlterPartitionReassignmentsResponseData": {
        "ThrottleTimeMs": ("std::chrono::milliseconds", "int32"),
        "Responses": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
            },
        }
    },
    "ListPartitionReassignmentsRequestData": {
        "TimeoutMs": ("std::chrono::milliseconds", "int32"),
        "Topics": {
            "PartitionIndexes": ("model::partition_id", "int32"),
        }
    },
    "ListPartitionReassignmentsResponseData": {
        "ThrottleTimeMs": ("std::chrono::milliseconds", "int32"),
        "Topics": {
            "Partitions": {
                "PartitionIndex": ("model::partition_id", "int32"),
            },
        }
    },
    "DescribeProducersRequestData": {
        "Topics": {
            "PartitionIndexes": ("model::partition_id", "int32"),
        }
    },
    "DescribeTransactionsResponseData": {
        "Topics": {
            "Partitions": ("model::partition_id", "int32"),
        }
    }
}

# a few kafka field types specify an entity type
entity_type_map = dict(
    groupId=("kafka::group_id", "string"),
    transactionalId=("kafka::transactional_id", "string"),
    topicName=("model::topic", "string"),
    uuid=("kafka::uuid", "uuid"),
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
    string=("ss::sstring", "read_string_with_control_check()",
            "read_nullable_string_with_control_check()",
            "read_flex_string_with_control_check()",
            "read_nullable_flex_string_with_control_check()"),
    bytes=("bytes", "read_bytes()", None, "read_flex_bytes()", None),
    bool=("bool", "read_bool()"),
    int8=("int8_t", "read_int8()"),
    int16=("int16_t", "read_int16()"),
    int32=("int32_t", "read_int32()"),
    int64=("int64_t", "read_int64()"),
    uuid=("uuid", "read_uuid()"),
    iobuf=("iobuf", None, "read_fragmented_nullable_bytes()", None,
           "read_fragmented_nullable_flex_bytes()"),
    fetch_record_set=("batch_reader", None, "read_nullable_batch_reader()",
                      None, "read_nullable_flex_batch_reader()"),
)

# Declare some fields as sensitive. Utmost care should be taken to ensure the
# contents of these fields are never made available over unsecure channels
# (sent over an unencrypted connection, printed in logs, etc.).
sensitive_map = {
    "SaslAuthenticateRequestData": {
        "AuthBytes": True,
    },
    "SaslAuthenticateResponseData": {
        "AuthBytes": True,
    },
}

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

# extra header per type name
extra_headers = {
    "std::optional": dict(
        header = ("<optional>",),
        source = "utils/to_string.h"
    ),
    "std::vector": dict(
        header = "<vector>",
    ),
    "kafka::produce_request_record_data": dict(
        header = "kafka/protocol/kafka_batch_adapter.h",
    ),
    "kafka::batch_reader": dict(
        header = "kafka/protocol/batch_reader.h",
    ),
    "model::timestamp": dict(
        header = "model/timestamp.h",
    ),
    "std::chrono::milliseconds": dict(
        source = "utils/to_string.h",
    ),
}
# yapf: enable

# These types, when they appear as the member type of an array, will use
# a vector implementation which resists fragmentation.
enable_fragmentation_resistance = {'metadata_response_partition'}


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
    "OffsetDeleteRequestTopic",
    "OffsetDeleteRequestPartition",
    "OffsetDeleteResponseTopic",
    "OffsetDeleteResponsePartition",
    "OffsetForLeaderTopic",
    "OffsetForLeaderPartition",
    "OffsetForLeaderTopicResult",
    "EpochEndOffset",
    "SupportedFeatureKey",
    "FinalizedFeatureKey",
    "DeleteTopicState",
    "ReassignableTopic",
    "ReassignablePartition",
    "ReassignableTopicResponse",
    "ReassignablePartitionResponse",
    "ListPartitionReassignmentsTopics",
    "OngoingTopicReassignment",
    "OngoingPartitionReassignment",
    "TopicRequest",
    "TopicResponse",
    "PartitionResponse",
    "ProducerState",
    "DescribeTransactionState",
    "TopicData",
    "ListTransactionState",
]

# a list of struct types which are ineligible to have default-generated
# `operator==()`, because one or more of its member variables are not
# comparable
WITHOUT_DEFAULT_EQUALITY_OPERATOR = {
    'kafka::batch_reader', 'kafka::produce_request_record_data'
}

# The following is a list of tag types which contain fields where their
# respective types are not prefixed with []. The generator special cases these
# as ArrayTypes
TAGGED_WITH_FIELDS = []

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
            # Its possible for tagged types to contain fields where the type is not
            # prefixed with [], these types are listed in the TAGGED_WITH_FIELDS map
            is_array = is_array or (type_name in TAGGED_WITH_FIELDS)
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
        all_fields = self.fields + self.tags
        for field in all_fields:
            t = field.type()
            if isinstance(t, ArrayType):
                t = t.value_type()  # unwrap value type
            if isinstance(t, StructType):
                res += t.structs()
                res.append(t)
        return res

    def headers(self, which):
        """
        calculate extra headers needed to support this struct
        """
        whiches = set(("header", "source"))
        assert which in whiches

        def type_iterator(fields):
            for field in fields:
                yield from field.type_name_parts()
                t = field.type()
                if isinstance(t, ArrayType):
                    t = t.value_type()  # unwrap value type
                if isinstance(t, StructType):
                    yield from type_iterator(t.fields)

        types = set(type_iterator(self.fields))

        def maybe_strings(s):
            if isinstance(s, str):
                yield s
            else:
                assert isinstance(s, tuple)
                yield from s

        def type_headers(t):
            h = extra_headers.get(t, None)
            if h is None:
                return
            assert isinstance(h, dict)
            assert set(h.keys()) <= whiches
            h = h.get(which, ())
            yield from maybe_strings(h)

        return set(h for t in types for h in type_headers(t))

    @property
    def is_default_comparable(self):
        return all(field.is_default_comparable for field in self.fields)


class ArrayType(FieldType):
    def __init__(self, value_type):
        # the name of the ArrayType is its value type
        super().__init__(value_type._name)
        self._value_type = value_type

    def value_type(self):
        return self._value_type

    @property
    def potentially_flexible_type(self):
        assert isinstance(self._value_type, ScalarType)
        return self._value_type.potentially_flexible_type


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
        self._tagged_versions = self._field.get("taggedVersions", None)
        if self._tagged_versions is not None:
            self._tagged_versions = VersionRange(self._tagged_versions)
        assert len(self._path)

    @staticmethod
    def create(field, path):
        field_type = FieldType.create(field, path)
        return Field(field, field_type, path)

    def type(self):
        return self._type

    def tag(self):
        return self._tag

    def nullable(self):
        return self._nullable_versions is not None

    def versions(self):
        return self._versions

    def tagged_versions(self):
        return self._tagged_versions

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
            if self.potentially_flexible_type and flex:
                assert plain_decoder[3]
                return plain_decoder[3], named_type
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
    def is_sensitive(self):
        d = sensitive_map
        for p in self._path:
            d = d.get(p, None)
            if d is None:
                break
        if type(d) is dict:
            # We got to the end of this field's path and `sensitive_map` has
            # sensitive decendents defined. This field is an ancestor of a
            # sensitive field, but it itself isn't sensitive.
            return False
        assert d is None or d is True, \
            "expected field '{}' to be missing or True; field path: {}, remaining path: {}" \
            .format(self._field["name"], self._path, d)
        return d

    @property
    def is_tag(self):
        return self._tag is not None

    @property
    def is_array(self):
        return isinstance(self._type, ArrayType)

    @property
    def potentially_flexible_type(self):
        return self._type.potentially_flexible_type

    @property
    def type_name(self):
        name, default_value = self._redpanda_type()
        if isinstance(self._type, ArrayType):
            assert default_value is None  # not supported
            if name in enable_fragmentation_resistance:
                name = f'large_fragment_vector<{name}>'
            else:
                name = f'std::vector<{name}>'
        if self.nullable():
            assert default_value is None  # not supported
            return f"std::optional<{name}>", None
        return name, default_value

    def type_name_parts(self):
        """
        yield normalized types required by this field
        """
        name, default_value = self._redpanda_type()
        yield name
        if isinstance(self._type, ArrayType):
            assert default_value is None  # not supported
            yield "std::vector"
        if self.nullable():
            assert default_value is None  # not supported
            yield "std::optional"

    @property
    def value_type(self):
        assert self.is_array
        return self._redpanda_type()[0]

    @property
    def name(self):
        return snake_case(self._field["name"])

    @property
    def is_default_comparable(self):
        type_name, _ = self._redpanda_type()
        return type_name not in WITHOUT_DEFAULT_EQUALITY_OPERATOR


HEADER_TEMPLATE = """
#pragma once
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "kafka/protocol/errors.h"
#include "seastarx.h"
#include "utils/fragmented_vector.h"

{%- for header in struct.headers("header") %}
{%- if header.startswith("<") %}
#include {{ header }}
{%- else %}
#include "{{ header }}"
{%- endif %}
{%- endfor %}

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

{%- if struct.tags|length > 0 %}

    // Tagged fields
{%- for tag in struct.tags %}
    {%- set info = tag.type_name %}
    {%- if info[1] != None %}
    {{ info[0] }} {{ tag.name }}{{'{'}}{{info[1]}}{{'}'}};
    {%- else %}
    {{ info[0] }} {{ tag.name }}{ {{- tag.default_value() -}} };
    {%- endif %}
{%- endfor %}
{%- endif %}
    tagged_fields unknown_tags;

{%- if struct.context_field %}
    // extra context not part of kafka protocol.
    // added by redpanda. see generator.py:make_context_field.
    {{ struct.context_field[0] }} {{ struct.context_field[1] -}};
{%- endif %}
{%- if struct.is_default_comparable %}
    friend bool operator==(const {{ struct.name }}&, const {{ struct.name }}&) = default;
{%- endif %}
{% endmacro %}

namespace kafka {

namespace protocol {
class decoder;
class encoder;
}
class response;

{% for struct in struct.structs() %}
{{ render_struct(struct) }}
    friend std::ostream& operator<<(std::ostream&, const {{ struct.name }}&);
};

{% endfor %}

{{ render_struct(struct) }}
    void encode(protocol::encoder&, api_version);
{%- if op_type == "request" %}
    void decode(protocol::decoder&, api_version);
{%- else %}
    void decode(iobuf, api_version);
{%- endif %}

    friend std::ostream& operator<<(std::ostream&, const {{ struct.name }}&);
{%- if first_flex > 0 %}
private:
    void encode_flex(protocol::encoder&, api_version);
    void encode_standard(protocol::encoder&, api_version);
{%- if op_type == "request" %}
    void decode_flex(protocol::decoder&, api_version);
    void decode_standard(protocol::decoder&, api_version);
{%- else %}
    void decode_flex(iobuf, api_version);
    void decode_standard(iobuf, api_version);
{%- endif %}
{%- endif %}
};

{%- if op_type == "request" %}

struct {{ request_name }}_request;
struct {{ request_name }}_response;

struct {{ request_name }}_api final {
    using request_type = {{ request_name }}_request;
    using response_type = {{ request_name }}_response;

    static constexpr const char* name = "{{ request_name }}";
    static constexpr api_key key = api_key({{ api_key }});
    static constexpr api_version min_flexible = {% if first_flex == -1 %}never_flexible{% else %}api_version({{ first_flex }}){% endif %};
};
{%- endif %}
}
"""

COMBINED_SOURCE_TEMPLATE = """
{%- for header in schema_headers %}
#include "kafka/protocol/schemata/{{ header }}"
{%- endfor %}

#include "kafka/protocol/wire.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

{%- for header in extra_headers %}
{%- if header.startswith("<") %}
#include {{ header }}
{%- else %}
#include "{{ header }}"
{%- endif %}
{%- endfor %}

{%- for name, source in sources %}
/*
 * begin: {{ name }}
 */
{{ source }}
{%- endfor %}
"""

SOURCE_TEMPLATE = """
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
{{ fail("Unexpected enumeration encountered, type: VersionRange.guard_modes") }}
{%- endif %}
{%- endmacro %}

{% macro tag_version_guard(tag) %}
{%- set guard_enum = tag.versions().guard_enum %}
{%- set e, cond = tag.tagged_versions()._guard() %}
{%- if e == guard_enum.GUARD %}
if ({{ cond }}) {
{{- caller() | indent }}
}
{%- elif e == guard_enum.NO_GUARD %}
{{- caller() }}
{%- else %}
{{ fail("Unexpected condition reached NO_SOURCE, in tag_version_guard()") }}
{%- endif %}
{% endmacro %}

{% macro field_encoder(field, methods, obj, writer = "writer") %}
{%- set flex = methods|length > 1 %}
{%- if obj %}
{%- set fname = obj + "." + field.name %}
{%- else %}
{%- set fname = field.name %}
{%- endif %}
{%- if field.is_array %}
{%- if field.nullable() %}
{%- if flex %}
{{ writer }}.write_nullable_flex_array({{ fname }}, [version]({{ field.value_type }}& v, protocol::encoder& writer) {
{%- else %}
{{ writer }}.write_nullable_array({{ fname }}, [version]({{ field.value_type }}& v, protocol::encoder& writer) {
{%- endif %}
{%- else %}
{%- if flex %}
{{ writer }}.write_flex_array({{ fname }}, [version]({{ field.value_type }}& v, protocol::encoder& writer) {
{%- else %}
{{ writer }}.write_array({{ fname }}, [version]({{ field.value_type }}& v, protocol::encoder& writer) {
{%- endif %}
{%- endif %}
    (void)version;
{%- if field.type().value_type().is_struct %}
{{- struct_serde(field.type().value_type(), methods, "v") | indent }}
{%- elif flex and field.type().value_type().potentially_flexible_type %}
    {{ writer }}.write_flex(v);
{%- else %}
    {{ writer }}.write(v);
{%- endif %}
});
{%- elif flex and field.type().potentially_flexible_type %}
{{ writer }}.write_flex({{ fname }});
{%- else %}
{{ writer }}.write({{ fname }});
{%- endif %}
{%- endmacro %}

{% macro field_decoder(field, methods, obj) %}
{%- set flex = methods|length > 1 %}
{%- if obj %}
{%- set fname = obj + "." + field.name %}
{%- else %}
{%- set fname = field.name %}
{%- endif %}
{%- if field.is_array %}
{%- if field.nullable() %}
{%- if flex %}
{{ fname }} = reader.read_nullable_flex_array([version](protocol::decoder& reader) {
{%- else %}
{{ fname }} = reader.read_nullable_array([version](protocol::decoder& reader) {
{%- endif %}
{%- else %}
{%- if flex %}
{{ fname }} = reader.read_flex_array([version](protocol::decoder& reader) {
{%- else %}
{{ fname }} = reader.read_array([version](protocol::decoder& reader) {
{%- endif %}
{%- endif %}
    (void)version;
{%- if field.type().value_type().is_struct %}
    {{ field.type().value_type().name }} v;
{{- struct_serde(field.type().value_type(), methods, "v") | indent }}
    return v;
{%- else %}
{%- set decoder, named_type = field.decoder(flex) %}
{%- if named_type == None %}
    return reader.{{ decoder }};
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

{% macro tag_decoder_impl(tag_definitions, obj = "") %}
/// Tags decoding section
auto num_tags = reader.read_unsigned_varint();
while(num_tags-- > 0) {
    auto tag = reader.read_unsigned_varint();
    auto sz = reader.read_unsigned_varint(); // size
    switch(tag){
{%- for tdef in tag_definitions %}
    case {{ tdef.tag() }}:
{{- field_decoder(tdef, (field_decoder, tag_decoder), obj) | indent | indent }}
        break;
{%- endfor %}
    default:
{%- set tf = "unknown_tags" %}
{%- if obj != "" %}
{%- set tf = obj + '.unknown_tags' %}
{%- endif %}
        reader.consume_unknown_tag({{ tf }}, tag, sz);
    }
}
{%- endmacro %}

{% macro tag_decoder(tag_definitions, obj = "") %}
{%- if tag_definitions|length == 0 %}
{%- set tf = "unknown_tags" %}
{%- if obj != "" %}
{%- set tf = obj + '.unknown_tags' %}
{%- endif %}
{{ tf }} = reader.read_tags();
{%- else %}
{
{{- tag_decoder_impl(tag_definitions, obj) | indent }}
}
{%- endif %}
{%- endmacro %}

{% macro conditional_tag_encode(tdef, vec) %}
{%- if tdef.nullable() %}
if ({{ tdef.name }}) {
    {{ vec }}.push_back({{ tdef.tag() }});
}
{%- elif tdef.is_array %}
if (!{{ tdef.name }}.empty()) {
    {{ vec }}.push_back({{ tdef.tag() }});
}
{%- elif tdef.default_value() != "" %}
if ({{ tdef.name }} != {{ tdef.default_value() }}) {
    {{ vec }}.push_back({{ tdef.tag() }});
}
{%- else %}
{{ vec }}.push_back({{ tdef.tag() }});
{%- endif %}
{%- endmacro %}

{% macro tag_encoder_impl(tag_definitions, obj = "") %}
/// Tags encoding section
std::vector<uint32_t> to_encode;
{%- for tdef in tag_definitions -%}
{%- call tag_version_guard(tdef) %}
{{- conditional_tag_encode(tdef, "to_encode") }}
{%- endcall %}
{%- endfor %}
{%- set tf = "unknown_tags" %}
{%- if obj != "" %}
{%- set tf = obj + '.unknown_tags' %}
{%- endif %}
tagged_fields::type tags_to_encode{std::move({{ tf }})};
for(uint32_t tag : to_encode) {
    iobuf b;
    protocol::encoder rw(b);
    switch(tag){
{%- for tdef in tag_definitions %}
    case {{ tdef.tag() }}:
{{- field_encoder(tdef, (field_encoder, tag_encoder), obj, "rw") | indent | indent }}
        break;
{%- endfor %}
    default:
        __builtin_unreachable();
    }
    tags_to_encode.emplace(tag_id(tag), iobuf_to_bytes(b));
}
writer.write_tags(tagged_fields(std::move(tags_to_encode)));
{%- endmacro %}

{% macro tag_encoder(tag_definitions, obj = "") %}
{%- if tag_definitions|length == 0 %}
{%- set tf = "unknown_tags" %}
{%- if obj != "" %}
{%- set tf = obj + '.unknown_tags' %}
{%- endif %}
writer.write_tags(std::move({{ tf }}));
{%- else %}
{
{{- tag_encoder_impl(tag_definitions, obj) | indent }}
}
{%- endif %}
{%- endmacro %}

{% set encoder = (field_encoder,) %}
{% set decoder = (field_decoder,) %}
{% set flex_encoder = (field_encoder, tag_encoder) %}
{% set flex_decoder = (field_decoder, tag_decoder) %}

{% macro struct_serde(struct, serde_methods, obj = "") %}
{%- set flex = serde_methods|length > 1 %}
{%- for field in struct.fields %}
{%- call version_guard(field, flex) %}
{{- serde_methods[0](field, serde_methods, obj) }}
{%- endcall %}
{%- endfor %}
{%- if flex %}
{{- serde_methods[1](struct.tags, obj) }}
{%- endif %}
{%- endmacro %}

namespace kafka {

{%- if struct.fields %}
{%- if first_flex > 0 %}
void {{ struct.name }}::encode(protocol::encoder& writer, api_version version) {
    if (version >= api_version({{ first_flex }})) {
        encode_flex(writer, version);
    } else {
        encode_standard(writer, version);
    }
}

void {{ struct.name }}::encode_flex(protocol::encoder& writer, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, flex_encoder) | indent }}
}

void {{ struct.name }}::encode_standard([[maybe_unused]] protocol::encoder& writer, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, encoder) | indent }}
}

{%- elif first_flex < 0 %}
void {{ struct.name }}::encode(protocol::encoder& writer, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, encoder) | indent }}
}
{%- else %}
void {{ struct.name }}::encode(protocol::encoder& writer, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, flex_encoder) | indent }}
}
{%- endif %}


{%- if op_type == "request" %}
{%- if first_flex > 0 %}
void {{ struct.name }}::decode(protocol::decoder& reader, api_version version) {
    if (version >= api_version({{ first_flex }})) {
        decode_flex(reader, version);
    } else {
        decode_standard(reader, version);
    }
}
void {{ struct.name }}::decode_flex(protocol::decoder& reader, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, flex_decoder) | indent }}
}

void {{ struct.name }}::decode_standard([[maybe_unused]] protocol::decoder& reader, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, decoder) | indent }}
}
{%- elif first_flex < 0 %}
void {{ struct.name }}::decode(protocol::decoder& reader, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, decoder) | indent }}
}
{% else %}
void {{ struct.name }}::decode(protocol::decoder& reader, [[maybe_unused]] api_version version) {
{{- struct_serde(struct, flex_decoder) | indent }}
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
    protocol::decoder reader(std::move(buf));

{{- struct_serde(struct, flex_decoder) | indent }}
}
void {{ struct.name }}::decode_standard(iobuf buf, [[maybe_unused]] api_version version) {
    protocol::decoder reader(std::move(buf));

{{- struct_serde(struct, decoder) | indent }}
}

{%- elif first_flex < 0 %}
void {{ struct.name }}::decode(iobuf buf, [[maybe_unused]] api_version version) {
    protocol::decoder reader(std::move(buf));

{{- struct_serde(struct, decoder) | indent }}
}
{%- else %}
void {{ struct.name }}::decode(iobuf buf, [[maybe_unused]] api_version version) {
    protocol::decoder reader(std::move(buf));

{{- struct_serde(struct, flex_decoder) | indent }}
}
{%- endif %}


{%- endif %}
{%- else %}
{%- if op_type == "request" %}
{%- if first_flex > 0 %}
void {{ struct.name }}::encode(protocol::encoder& writer, api_version version) {
    if (version >= api_version({{ first_flex }})) {
        writer.write_tags(std::move(unknown_tags));
    }
}
void {{ struct.name }}::decode(protocol::decoder& reader, api_version version) {
    if (version >= api_version({{ first_flex }})) {
        unknown_tags = reader.read_tags();
    }
}
{%- else %}
void {{ struct.name }}::encode(protocol::encoder&, api_version) {}
void {{ struct.name }}::decode(protocol::decoder&, api_version) {}
{%- endif %}
{%- else %}
{%- if first_flex > 0 %}
void {{ struct.name }}::encode(protocol::encoder& writer, api_version version) {
    if (version >= api_version({{ first_flex }})) {
        write.write_tags(std::move(unknown_tags));
    }
}
void {{ struct.name }}::decode(iobuf buf, api_version version) {
    if (version >= api_version({{ first_flex }})) {
        protocol::decoder reader(std::move(buf));
        unknown_tags = reader.read_tags();
    }
}
{%- else %}
void {{ struct.name }}::encode(protocol::encoder&, api_version) {}
void {{ struct.name }}::decode(iobuf, api_version) {}
{%- endif %}
{%- endif %}
{%- endif %}

{% set structs = struct.structs() + [struct] %}
{% for struct in structs %}
{%- if struct.fields %}
std::ostream& operator<<(std::ostream& o, [[maybe_unused]] const {{ struct.name }}& v) {
    fmt::print(o,
      "{{'{{' + struct.format + '}}'}}",
      {%- for field in struct.fields %}
      {%- if field.is_sensitive %}"****"{% else %}v.{{ field.name }}{% endif %}{% if not loop.last %},{% endif %}

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
    [f"[]{t}" for t in ALLOWED_SCALAR_TYPES +
        STRUCT_TYPES] + TAGGED_WITH_FIELDS

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
        "listeners": {"type": "array", "optional": True}
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


def codegen(schema_path):
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

    # request id
    api_key = msg["apiKey"]

    def parse_name(name):
        name = name.removesuffix("Request")
        name = name.removesuffix("Response")
        name = snake_case(name)
        # Special case to sidestep an otherwise large rename in the source
        return "list_offsets" if name == "list_offset" else name

    request_name = parse_name(msg["name"])

    # either 'none' or 'VersionRange'
    first_flex = parse_flexible_versions(msg["flexibleVersions"])

    def fail(msg):
        assert False, msg

    hdr = jinja2.Template(HEADER_TEMPLATE).render(
        struct=struct,
        render_struct_comment=render_struct_comment,
        op_type=op_type,
        fail=fail,
        api_key=api_key,
        request_name=request_name,
        first_flex=first_flex)

    src = jinja2.Template(SOURCE_TEMPLATE).render(struct=struct,
                                                  op_type=op_type,
                                                  fail=fail,
                                                  first_flex=first_flex)

    return hdr, src, struct.headers("source")


if __name__ == "__main__":
    schemata = []
    headers = []
    source = None

    for arg in sys.argv[1:]:
        path = pathlib.Path(arg)
        if path.suffix == ".json":
            schemata.append(path)
        elif path.suffix == ".h":
            headers.append(path)
        elif path.suffix == ".cc":
            assert source is None
            source = path
        else:
            assert False, f"unknown arg {arg}"

    assert len(schemata) == len(headers)
    assert source is not None

    sources = []
    extra_schema_headers = set()
    for schema, hdr_path in zip(schemata, headers):
        hdr, src, extra = codegen(schema)
        sources.append((schema.name, src))
        extra_schema_headers.update(extra)
        with open(hdr_path, 'w') as f:
            f.write(hdr)

    src = jinja2.Template(COMBINED_SOURCE_TEMPLATE).render(
        schema_headers=map(lambda p: p.name, headers),
        extra_headers=extra_schema_headers,
        sources=sources)

    with open(source, 'w') as f:
        f.write(src)
