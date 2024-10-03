load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel/thirdparty:seastar.bzl", "seastar_cc_swagger_library")

seastar_cc_swagger_library(
    name = "rest_swagger",
    src = "api/api-doc/rest.json",
)

seastar_cc_swagger_library(
    name = "schema_registry_swagger",
    src = "api/api-doc/schema_registry.json",
)

proto_library(
    name = "schema_registry_protos",
    srcs = [
        "schema_registry/protobuf/confluent/meta.proto",
        "schema_registry/protobuf/confluent/types/decimal.proto",
        "schema_registry/protobuf/google/type/calendar_period.proto",
        "schema_registry/protobuf/google/type/color.proto",
        "schema_registry/protobuf/google/type/date.proto",
        "schema_registry/protobuf/google/type/datetime.proto",
        "schema_registry/protobuf/google/type/dayofweek.proto",
        "schema_registry/protobuf/google/type/decimal.proto",
        "schema_registry/protobuf/google/type/expr.proto",
        "schema_registry/protobuf/google/type/fraction.proto",
        "schema_registry/protobuf/google/type/interval.proto",
        "schema_registry/protobuf/google/type/latlng.proto",
        "schema_registry/protobuf/google/type/localized_text.proto",
        "schema_registry/protobuf/google/type/money.proto",
        "schema_registry/protobuf/google/type/month.proto",
        "schema_registry/protobuf/google/type/phone_number.proto",
        "schema_registry/protobuf/google/type/postal_address.proto",
        "schema_registry/protobuf/google/type/quaternion.proto",
        "schema_registry/protobuf/google/type/timeofday.proto",
    ],
    deps = [
        "@protobuf//:any_proto",
        "@protobuf//:descriptor_proto",
        "@protobuf//:duration_proto",
        "@protobuf//:timestamp_proto",
        "@protobuf//:wrappers_proto",
    ],
)

cc_proto_library(
    name = "descriptor_cc_proto",
    deps = [
        "@protobuf//:descriptor_proto",
    ],
)

cc_proto_library(
    name = "struct_cc_proto",
    deps = [
        "@protobuf//:struct_proto",
    ],
)

cc_proto_library(
    name = "field_mask_cc_proto",
    deps = [
        "@protobuf//:field_mask_proto",
    ],
)

cc_proto_library(
    name = "source_context_cc_proto",
    deps = [
        "@protobuf//:source_context_proto",
    ],
)

cc_proto_library(
    name = "timestamp_cc_proto",
    deps = [
        "@protobuf//:timestamp_proto",
    ],
)

cc_proto_library(
    name = "type_cc_proto",
    deps = [
        "@protobuf//:type_proto",
    ],
)

cc_proto_library(
    name = "wrappers_cc_proto",
    deps = [
        "@protobuf//:wrappers_proto",
    ],
)

cc_proto_library(
    name = "api_cc_proto",
    deps = [
        "@protobuf//:api_proto",
    ],
)

cc_proto_library(
    name = "duration_cc_proto",
    deps = [
        "@protobuf//:duration_proto",
    ],
)

cc_proto_library(
    name = "empty_cc_proto",
    deps = [
        "@protobuf//:empty_proto",
    ],
)

cc_proto_library(
    name = "any_cc_proto",
    deps = [
        "@protobuf//:any_proto",
    ],
)

cc_proto_library(
    name = "schema_registry_cc_proto",
    deps = [":schema_registry_protos"],
)

redpanda_cc_library(
    name = "pandaproxy",
    srcs = [
        "error.cc",
        "json/error.cc",
        "kafka_client_cache.cc",
        "logger.cc",
        "parsing/error.cc",
        "probe.cc",
        "rest/api.cc",
        "rest/configuration.cc",
        "rest/handlers.cc",
        "rest/proxy.cc",
        "schema_registry/api.cc",
        "schema_registry/avro.cc",
        "schema_registry/compatibility.cc",
        "schema_registry/configuration.cc",
        "schema_registry/error.cc",
        "schema_registry/handlers.cc",
        "schema_registry/json.cc",
        "schema_registry/protobuf.cc",
        "schema_registry/seq_writer.cc",
        "schema_registry/service.cc",
        "schema_registry/sharded_store.cc",
        "schema_registry/types.cc",
        "schema_registry/validation.cc",
        "server.cc",
    ],
    hdrs = [
        "api/api-doc/rest.json.hh",
        "api/api-doc/schema_registry.json.hh",
        "error.h",
        "fwd.h",
        "json/error.h",
        "json/exceptions.h",
        "json/iobuf.h",
        "json/requests/brokers.h",
        "json/requests/create_consumer.h",
        "json/requests/error_reply.h",
        "json/requests/fetch.h",
        "json/requests/offset_commit.h",
        "json/requests/offset_fetch.h",
        "json/requests/partition_offsets.h",
        "json/requests/partitions.h",
        "json/requests/produce.h",
        "json/requests/subscribe_consumer.h",
        "json/rjson_parse.h",
        "json/rjson_util.h",
        "json/types.h",
        "kafka_client_cache.h",
        "logger.h",
        "parsing/error.h",
        "parsing/exceptions.h",
        "parsing/from_chars.h",
        "parsing/httpd.h",
        "probe.h",
        "reply.h",
        "rest/api.h",
        "rest/configuration.h",
        "rest/fwd.h",
        "rest/handlers.h",
        "rest/proxy.h",
        "schema_registry/api.h",
        "schema_registry/avro.h",
        "schema_registry/compatibility.h",
        "schema_registry/configuration.h",
        "schema_registry/error.h",
        "schema_registry/errors.h",
        "schema_registry/exceptions.h",
        "schema_registry/fwd.h",
        "schema_registry/handlers.h",
        "schema_registry/json.h",
        "schema_registry/protobuf.h",
        "schema_registry/requests/compatibility.h",
        "schema_registry/requests/config.h",
        "schema_registry/requests/get_schemas_ids_id.h",
        "schema_registry/requests/get_schemas_ids_id_versions.h",
        "schema_registry/requests/get_subject_versions_version.h",
        "schema_registry/requests/mode.h",
        "schema_registry/requests/post_subject_versions.h",
        "schema_registry/schema_id_cache.h",
        "schema_registry/seq_writer.h",
        "schema_registry/service.h",
        "schema_registry/sharded_store.h",
        "schema_registry/storage.h",
        "schema_registry/store.h",
        "schema_registry/types.h",
        "schema_registry/util.h",
        "schema_registry/validation.h",
        "schema_registry/validation_metrics.h",
        "server.h",
        "types.h",
        "util.h",
    ],
    include_prefix = "pandaproxy",
    visibility = ["//visibility:public"],
    deps = [
        ":any_cc_proto",
        ":api_cc_proto",
        ":config",
        ":descriptor_cc_proto",
        ":duration_cc_proto",
        ":empty_cc_proto",
        ":field_mask_cc_proto",
        ":rest_swagger",
        ":schema_registry_cc_proto",
        ":schema_registry_swagger",
        ":source_context_cc_proto",
        ":struct_cc_proto",
        ":subject_name_strategy",
        ":timestamp_cc_proto",
        ":type_cc_proto",
        ":wrappers_cc_proto",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:fragmented_vector",
        "//src/v/container:json",
        "//src/v/hashing:jump_consistent",
        "//src/v/hashing:xx",
        "//src/v/json",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol:join_group",
        "//src/v/kafka/protocol:list_offset",
        "//src/v/kafka/protocol:offset_commit",
        "//src/v/kafka/protocol:offset_fetch",
        "//src/v/kafka/protocol:produce",
        "//src/v/kafka/protocol/schemata:offset_commit_request",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/net",
        "//src/v/random:time_jitter",
        "//src/v/reflection:type_traits",
        "//src/v/rpc",
        "//src/v/security",
        "//src/v/security:request_auth",
        "//src/v/security/audit",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:sformat",
        "//src/v/storage:parser_utils",
        "//src/v/storage:record_batch_builder",
        "//src/v/strings:string_switch",
        "//src/v/strings:utf8",
        "//src/v/thirdparty/protobuf:descriptor",
        "//src/v/utils:absl_sstring_hash",
        "//src/v/utils:adjustable_semaphore",
        "//src/v/utils:base64",
        "//src/v/utils:log_hist",
        "//src/v/utils:mutex",
        "//src/v/utils:named_type",
        "//src/v/utils:retry",
        "//src/v/utils:tristate",
        "@abseil-cpp//absl/algorithm:container",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/container:inlined_vector",
        "@abseil-cpp//absl/container:node_hash_map",
        "@avro",
        "@boost//:algorithm",
        "@boost//:graph",
        "@boost//:lexical_cast",
        "@boost//:math",
        "@boost//:multi_index",
        "@boost//:outcome",
        "@boost//:range",
        "@fmt",
        "@jsoncons",
        "@protobuf",
        "@protobuf//src/google/protobuf/compiler:importer",
        "@protobuf//src/google/protobuf/io",
        "@protobuf//src/google/protobuf/io:tokenizer",
        "@rapidjson",
        "@re2",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "config",
    hdrs = [
        "schema_registry/schema_id_validation.h",
    ],
    include_prefix = "pandaproxy",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/strings:string_switch",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "subject_name_strategy",
    hdrs = [
        "schema_registry/subject_name_strategy.h",
    ],
    include_prefix = "pandaproxy",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/strings:string_switch",
    ],
)
