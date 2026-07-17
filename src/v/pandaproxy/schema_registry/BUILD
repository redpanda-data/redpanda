load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/schema/protobuf:well_known.bzl", "well_known_cc_protos")

redpanda_cc_library(
    name = "exceptions",
    hdrs = [
        "exceptions.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "rjson",
    hdrs = [
        "rjson.h",
    ],
    implementation_deps = [
        "@rapidjson",
    ],
    visibility = [":__subpackages__"],
    deps = [
        ":types",
        "//src/v/base",
        "//src/v/container:json",
        "//src/v/json",
        "//src/v/pandaproxy:json",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = [
        "error.cc",
        "types.cc",
    ],
    hdrs = [
        "error.h",
        "errors.h",
        "schema_getter.h",
        "types.h",
        "util.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/config",
        "//src/v/pandaproxy:error",
        "//src/v/strings:string_switch",
        "//src/v/utils:to_string",
        "@fmt",
        "@rapidjson",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":exceptions",
        ":fwd",
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/json",
        "//src/v/model",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/container:btree",
        "@boost//:outcome",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "transport",
    hdrs = [
        "transport.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster:errc",
        "//src/v/cluster:topic_configuration",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "kafka_client_transport",
    srcs = [
        "kafka_client_transport.cc",
    ],
    hdrs = [
        "kafka_client_transport.h",
    ],
    implementation_deps = [
        ":exceptions",
        "//src/v/cluster",
        "//src/v/cluster:ephemeral_credential_frontend",
        "//src/v/cluster:members_table",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client:config_utils",
        "//src/v/kafka/client:exceptions",
        "//src/v/kafka/data/rpc",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/model",
        "//src/v/pandaproxy:logger",
        "//src/v/security",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":transport",
        "//src/v/cluster:fwd",
        "//src/v/kafka/client",
        "//src/v/kafka/client:configuration",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "configuration",
    srcs = ["configuration.cc"],
    hdrs = ["configuration.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "//src/v/net",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rpc_transport",
    srcs = [
        "rpc_transport.cc",
    ],
    hdrs = [
        "rpc_transport.h",
    ],
    implementation_deps = [
        ":exceptions",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "@seastar",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":transport",
        "//src/v/kafka/data/rpc",
    ],
)

redpanda_cc_library(
    name = "compatibility",
    srcs = ["compatibility.cc"],
    hdrs = ["compatibility.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/base",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "avro",
    srcs = ["avro.cc"],
    hdrs = ["avro.h"],
    implementation_deps = [
        ":compatibility",
        "//src/v/bytes:streambuf",
        "//src/v/json",
        "//src/v/pandaproxy:logger",
        "//src/v/strings:string_switch",
        "//src/v/utils:to_string",
        "@avro",
        "@boost//:outcome",
        "@fmt",
        "@rapidjson",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "json_schema",
    srcs = ["json.cc"],
    hdrs = ["json.h"],
    implementation_deps = [
        ":compatibility",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/utils:to_string",
        "@boost//:graph",
        "@boost//:math",
        "@fmt",
        "@jsoncons",
        "@rapidjson",
        "@re2",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":types",
        "//src/v/json",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "protobuf_schema",
    srcs = ["protobuf.cc"],
    hdrs = ["protobuf.h"],
    implementation_deps = [
        ":compatibility",
        "//src/v/base",
        "//src/v/bytes:streambuf",
        "//src/v/pandaproxy:logger",
        "//src/v/schema/protobuf:confluent_type_cc_proto",
        "//src/v/schema/protobuf:google_type_cc_proto",
        "//src/v/ssx:sformat",
        "//src/v/utils:base64",
        "//src/v/utils:to_string",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:algorithm",
        "@fmt",
        "@protobuf//src/google/protobuf/compiler:importer",
        "@protobuf//src/google/protobuf/io",
        "@protobuf//src/google/protobuf/io:tokenizer",
        "@protovalidate//proto/protovalidate/buf/validate:validate_proto_cc",
    ] + well_known_cc_protos,
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":types",
        "@protobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "store",
    srcs = ["sharded_store.cc"],
    hdrs = [
        "schema_id_cache.h",
        "sharded_store.h",
        "store.h",
    ],
    implementation_deps = [
        ":avro",
        ":compatibility",
        ":exceptions",
        ":json_schema",
        ":protobuf_schema",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/hashing:jump_consistent",
        "//src/v/hashing:xx",
        "//src/v/pandaproxy:json",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/utils:to_string",
        "@abseil-cpp//absl/algorithm:container",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:node_hash_map",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":subject_name_strategy",
        ":types",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/pandaproxy:logger",
        "@abseil-cpp//absl/container:btree",
        "@boost//:multi_index",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "seq_writer",
    srcs = ["seq_writer.cc"],
    hdrs = [
        "seq_writer.h",
        "storage.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/pandaproxy:logger",
        "//src/v/storage:record_batch_builder",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":exceptions",
        ":fwd",
        ":rjson",
        ":store",
        ":transport",
        ":types",
        "//src/v/json",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/pandaproxy:json",
        "//src/v/pandaproxy:parsing",
        "//src/v/random:time_jitter",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:sformat",
        "//src/v/strings:string_switch",
        "//src/v/utils:retry",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "validation",
    srcs = ["validation.cc"],
    hdrs = ["validation.h"],
    implementation_deps = [
        ":api",
        ":avro",
        ":compatibility",
        ":exceptions",
        ":json_schema",
        ":protobuf_schema",
        ":seq_writer",
        ":store",
        ":subject_name_strategy",
        "//src/v/bytes:iobuf_parser",
        "//src/v/model:batch_compression",
        "//src/v/pandaproxy:logger",
        "@avro",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":config",
        ":fwd",
        ":types",
        "//src/v/base",
        "//src/v/cluster:topic_configuration",
        "//src/v/config",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "api",
    srcs = ["api.cc"],
    hdrs = [
        "api.h",
        "validation_metrics.h",
    ],
    implementation_deps = [
        ":configuration",
        ":exceptions",
        ":kafka_client_transport",
        ":rpc_transport",
        ":seq_writer",
        ":server",
        ":store",
        "//src/v/cluster:ephemeral_credential_frontend",
        "//src/v/cluster:members_table",
        "//src/v/cluster:node_status_table",
        "//src/v/config",
        "//src/v/features",
        "//src/v/kafka/client:configuration",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/metrics",
        "//src/v/pandaproxy:core",
        "//src/v/pandaproxy:logger",
        "//src/v/pandaproxy:parsing",
        "//src/v/security/audit",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":types",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/kafka/data/rpc",
        "//src/v/model",
        "//src/v/security",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "context_router",
    hdrs = ["context_router.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/base",
        "//src/v/pandaproxy:parsing",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "server",
    srcs = [
        "auth.cc",
        "authorization.cc",
        "handlers.cc",
        "requests/acls.cc",
        "service.cc",
    ],
    hdrs = [
        "auth.h",
        "authorization.h",
        "handlers.h",
        "requests/acls.h",
        "requests/compatibility.h",
        "requests/config.h",
        "requests/get_schemas_ids_id.h",
        "requests/get_schemas_ids_id_versions.h",
        "requests/get_subject_versions_version.h",
        "requests/mode.h",
        "requests/post_subject_versions.h",
        "service.h",
    ],
    implementation_deps = [
        ":avro",
        ":context_router",
        ":json_schema",
        ":protobuf_schema",
        ":seq_writer",
        ":store",
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cluster",
        "//src/v/cluster:data_migration_group_proxy",
        "//src/v/cluster:ephemeral_credential_frontend",
        "//src/v/cluster:feature_backend",
        "//src/v/cluster:leader_balancer_strategy",
        "//src/v/cluster:members_table",
        "//src/v/cluster:node_status_table",
        "//src/v/cluster:plugin_backend",
        "//src/v/cluster:shard_table",
        "//src/v/cluster:topic_configuration",
        "//src/v/cluster:topic_metrics_watcher",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/container:json",
        "//src/v/json",
        "//src/v/kafka/client:configuration",
        "//src/v/kafka/data/rpc",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:list_offset",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/pandaproxy:core",
        "//src/v/pandaproxy:json",
        "//src/v/pandaproxy:logger",
        "//src/v/pandaproxy:parsing",
        "//src/v/pandaproxy:schema_registry_swagger_definitions",
        "//src/v/security",
        "//src/v/security:request_auth",
        "//src/v/security/audit",
        "//src/v/security/audit:types",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:sformat",
        "//src/v/strings:string_switch",
        "//src/v/strings:utf8",
        "//src/v/utils:adjustable_semaphore",
        "//src/v/utils:named_type",
        "//src/v/utils:tristate",
        "@abseil-cpp//absl/algorithm:container",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/strings",
        "@avro",
        "@boost//:algorithm",
        "@seastar",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":config",
        ":configuration",
        ":exceptions",
        ":fwd",
        ":rjson",
        ":subject_name_strategy",
        ":transport",
        ":types",
        "//src/v/cluster:client_quota_backend",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster:cluster_link_types",
        "//src/v/cluster:cluster_recovery_table",
        "//src/v/cluster:controller_log_limiter",
        "//src/v/cluster:data_migration_table",
        "//src/v/cluster:fwd",
        "//src/v/cluster:plugin_table",
        "//src/v/cluster:rpc_utils",
        "//src/v/pandaproxy:core",
        "//src/v/utils:variant",
    ],
)

redpanda_cc_library(
    name = "config",
    hdrs = [
        "schema_id_validation.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/strings:string_switch",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "subject_name_strategy",
    hdrs = [
        "subject_name_strategy.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/strings:string_switch",
    ],
)
