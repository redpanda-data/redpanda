load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "fixture",
    hdrs = [
        "fixture.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "kafka/client/test",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/kafka/client",
        "//src/v/redpanda/tests:fixture_btest",
    ],
)

redpanda_test_cc_library(
    name = "utils",
    hdrs = [
        "utils.h",
    ],
    implementation_deps = [
        "//src/v/random:generators",
    ],
    include_prefix = "kafka/client/test",
    deps = [
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
    ],
)

redpanda_cc_btest(
    name = "partitioners_test",
    timeout = "short",
    srcs = [
        "partitioners.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/hashing:murmur",
        "//src/v/kafka/client",
        "//src/v/model",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "fetch_session_test",
    timeout = "short",
    srcs = [
        "fetch_session.cc",
    ],
    deps = [
        ":utils",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:fetch",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "produce_batcher_test",
    timeout = "short",
    srcs = [
        "produce_batcher.cc",
    ],
    deps = [
        ":utils",
        "//src/v/cluster",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:produce",
        "//src/v/model",
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "produce_partition_test",
    timeout = "short",
    srcs = [
        "produce_partition.cc",
    ],
    deps = [
        ":utils",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:produce",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "retry_with_mitigation_test",
    timeout = "short",
    srcs = [
        "retry_with_mitigation.cc",
    ],
    deps = [
        "//src/v/kafka/client",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "consumer_group",
    timeout = "short",
    srcs = [
        "consumer_group.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/kafka/client",
        "//src/v/kafka/client/test:fixture",
        "//src/v/kafka/client/test:utils",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:describe_groups",
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol:find_coordinator",
        "//src/v/kafka/protocol:heartbeat",
        "//src/v/kafka/protocol:join_group",
        "//src/v/kafka/protocol:leave_group",
        "//src/v/kafka/protocol:list_groups",
        "//src/v/kafka/protocol:list_offset",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol:offset_fetch",
        "//src/v/kafka/protocol:sync_group",
        "//src/v/kafka/protocol/schemata:join_group_request",
        "//src/v/kafka/protocol/schemata:join_group_response",
        "//src/v/kafka/protocol/schemata:offset_fetch_response",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "fetch",
    timeout = "short",
    srcs = [
        "fetch.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/cluster",
        "//src/v/kafka/client",
        "//src/v/kafka/client/test:fixture",
        "//src/v/kafka/client/test:utils",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol:metadata",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "produce",
    timeout = "short",
    srcs = [
        "produce.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/kafka/client",
        "//src/v/kafka/client/test:fixture",
        "//src/v/kafka/client/test:utils",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol:produce",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "reconnect",
    timeout = "short",
    srcs = [
        "reconnect.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/http",
        "//src/v/kafka/client",
        "//src/v/kafka/client/test:fixture",
        "//src/v/kafka/client/test:utils",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol:produce",
        "//src/v/model",
        "//src/v/pandaproxy/test:utils",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "retry",
    timeout = "short",
    srcs = [
        "retry.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/kafka/client",
        "//src/v/kafka/client/test:fixture",
        "//src/v/kafka/client/test:utils",
        "//src/v/kafka/protocol",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
