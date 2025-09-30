load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "fixture",
    srcs = [
        "fixture.cc",
    ],
    hdrs = [
        "fixture.h",
    ],
    implementation_deps = [
        "//src/v/cloud_roles:types",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster",
        "//src/v/cluster:offsets_lookup",
        "//src/v/config",
        "//src/v/kafka/client:transport",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol:sasl_authenticate",
        "//src/v/kafka/protocol:sasl_handshake",
        "//src/v/kafka/protocol/schemata:fetch_request",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/model",
        "//src/v/model:timeout_clock",
        "//src/v/net",
        "//src/v/pandaproxy/rest:server",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/pandaproxy/schema_registry:registry",
        "//src/v/pandaproxy/schema_registry:server",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:cpu_scheduling",
        "//src/v/security",
        "//src/v/ssx:thread_worker",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:async",
        "//src/v/test_utils:test_env",
        "//src/v/utils:unresolved_address",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/redpanda:application",
        "//src/v/test_utils:async",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "cli_parser",
    timeout = "short",
    srcs = [
        "cli_parser_tests.cc",
    ],
    deps = [
        "//src/v/redpanda:cli_parser",
        "@boost//:algorithm",
        "@boost//:program_options",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
