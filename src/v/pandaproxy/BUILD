load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel/thirdparty:seastar.bzl", "seastar_cc_swagger_library")

seastar_cc_swagger_library(
    name = "rest_swagger",
    src = "api/api-doc/rest.json",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "rest_swagger_definitions",
    hdrs = [
        "api/api-doc/rest.json.hh",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":rest_swagger",
    ],
)

seastar_cc_swagger_library(
    name = "schema_registry_swagger",
    src = "api/api-doc/schema_registry.json",
)

redpanda_cc_library(
    name = "schema_registry_swagger_definitions",
    hdrs = [
        "api/api-doc/schema_registry.json.hh",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":schema_registry_swagger",
    ],
)

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/utils:truncating_logger",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "json",
    srcs = [
        "json/error.cc",
    ],
    hdrs = [
        "json/error.h",
        "json/exceptions.h",
        "json/iobuf.h",
        "json/rjson_parse.h",
        "json/rjson_util.h",
        "json/types.h",
        "json/util.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/json",
        "//src/v/kafka/protocol",
        "//src/v/strings:utf8",
    ],
)

redpanda_cc_library(
    name = "parsing",
    srcs = [
        "parsing/error.cc",
    ],
    hdrs = [
        "parsing/error.h",
        "parsing/exceptions.h",
        "parsing/from_chars.h",
        "parsing/httpd.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":json",
        "//src/v/base",
        "//src/v/strings:utf8",
    ],
)

redpanda_cc_library(
    name = "error",
    srcs = [
        "error.cc",
    ],
    hdrs = [
        "error.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":json",
        ":parsing",
        "//src/v/base",
        "//src/v/kafka/protocol",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "core",
    srcs = [
        "kafka_client_cache.cc",
        "probe.cc",
        "server.cc",
    ],
    hdrs = [
        "fwd.h",
        "kafka_client_cache.h",
        "probe.h",
        "reply.h",
        "server.h",
        "types.h",
        "util.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":error",
        ":error_reply",
        ":json",
        ":logger",
        ":parsing",
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/config",
        "//src/v/hashing:jump_consistent",
        "//src/v/hashing:xx",
        "//src/v/kafka/client",
        "//src/v/kafka/client:exceptions",
        "//src/v/kafka/protocol",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/net",
        "//src/v/pandaproxy/schema_registry:exceptions",
        "//src/v/rpc",
        "//src/v/security:request_auth",
        "//src/v/ssx:future_util",
        "//src/v/ssx:mutex",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:sformat",
        "//src/v/utils:adjustable_semaphore",
        "//src/v/utils:log_hist",
        "//src/v/utils:truncating_logger",
        "@boost//:multi_index",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "error_reply",
    srcs = [],
    hdrs = [
        "json/requests/error_reply.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":json",
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "rest_requests",
    hdrs = [
        "json/requests/brokers.h",
        "json/requests/create_consumer.h",
        "json/requests/fetch.h",
        "json/requests/offset_commit.h",
        "json/requests/offset_fetch.h",
        "json/requests/partition_offsets.h",
        "json/requests/partitions.h",
        "json/requests/produce.h",
        "json/requests/subscribe_consumer.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":json",
        ":parsing",
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol:join_group",
        "//src/v/kafka/protocol:offset_commit",
        "//src/v/kafka/protocol:offset_fetch",
        "//src/v/model",
        "//src/v/model:batch_compression",
    ],
)

redpanda_cc_library(
    name = "pandaproxy",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/pandaproxy/rest:server",
        "//src/v/pandaproxy/schema_registry:server",
    ],
)
