load(
    "//bazel:test.bzl",
    "redpanda_cc_gtest",
    "redpanda_test_cc_library",
)

redpanda_test_cc_library(
    name = "common",
    srcs = [
        "common.cc",
    ],
    hdrs = [
        "common.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "cloud_storage/inventory/tests",
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/compression",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "create_inventory_config_test",
    timeout = "short",
    srcs = [
        "create_inventory_config_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/inventory/tests:common",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "fetch_report_tests",
    timeout = "short",
    srcs = [
        "fetch_report_tests.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/inventory/tests:common",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "report_parser_tests",
    timeout = "short",
    srcs = [
        "report_parser_tests.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/inventory/tests:common",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "//src/v/utils:base64",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "inv_consumer_tests",
    timeout = "short",
    srcs = [
        "inv_consumer_tests.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/inventory/tests:common",
        "//src/v/test_utils:gtest",
        "//src/v/utils:base64",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "inv_svc_tests",
    timeout = "short",
    srcs = [
        "inv_svc_tests.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/inventory/tests:common",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/utils:base64",
        "@abseil-cpp//absl/container:node_hash_set",
        "@boost//:algorithm",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)
