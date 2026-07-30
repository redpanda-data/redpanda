load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "source_reader",
    srcs = [
        "unavailable_source_reader.cc",
    ],
    hdrs = [
        "source_reader.h",
        "unavailable_source_reader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster_link/model",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "tail_reader",
    hdrs = [
        "tail_reader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":source_reader",
        "//src/v/base",
        "//src/v/cluster_link:fwd",
        "//src/v/container:chunked_hash_map",
        "//src/v/pandaproxy/schema_registry:types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schemas_topic_tail_reader",
    srcs = [
        "schemas_topic_tail_reader.cc",
    ],
    hdrs = [
        "schemas_topic_tail_reader.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link:logger",
        "//src/v/cluster_link:utils",
        "//src/v/container:chunked_vector",
        "//src/v/json",
        "//src/v/kafka/client:cluster",
        "//src/v/kafka/client/direct_consumer",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/pandaproxy/schema_registry:seq_writer",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "@rapidjson",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":tail_reader",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "http_source_reader",
    srcs = [
        "http_source_reader.cc",
    ],
    hdrs = [
        "http_source_reader.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cluster_link:utils",
        "//src/v/config",
        "//src/v/http",
        "//src/v/net",
        "//src/v/pandaproxy/schema_registry/rest_client:error",
        "//src/v/pandaproxy/schema_registry/rest_client:pooled_client",
        "//src/v/pandaproxy/schema_registry/rest_client:rate_limited_client",
        "//src/v/utils:retry_chain_node",
        "@ada",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":source_reader",
        "//src/v/cluster_link/model",
        "//src/v/container:chunked_vector",
        "//src/v/net:tls",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/pandaproxy/schema_registry/rest_client:client",
        "//src/v/pandaproxy/schema_registry/rest_client:credentials",
        "//src/v/ssx:semaphore",
        "//src/v/utils:unresolved_address",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "mirroring_task",
    srcs = [
        "mirroring_task.cc",
        "probe.cc",
        "reconciler.cc",
        "scope.cc",
    ],
    hdrs = [
        "mirroring_task.h",
        "probe.h",
        "reconciler.h",
        "scope.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cluster_link:logger",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:exceptions",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":source_reader",
        ":tail_reader",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link/model",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/schema:registry",
        "//src/v/ssx:mutex",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)
