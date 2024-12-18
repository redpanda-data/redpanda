load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_btest(
    name = "bounded_property_test",
    timeout = "short",
    srcs = [
        "bounded_property_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "enum_property_test",
    timeout = "short",
    srcs = [
        "enum_property_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "retention_property_test",
    timeout = "short",
    srcs = [
        "retention_property_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "config_store_test",
    timeout = "short",
    srcs = [
        "config_store_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/json",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "socket_address_convert_test",
    timeout = "short",
    srcs = [
        "socket_address_convert_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
        "@yaml-cpp",
    ],
)

redpanda_cc_btest(
    name = "tls_config_convert_test",
    timeout = "short",
    srcs = [
        "tls_config_convert_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@seastar",
        "@seastar//:testing",
        "@yaml-cpp",
    ],
)

redpanda_cc_btest(
    name = "scoped_config_test",
    timeout = "short",
    srcs = [
        "scoped_config_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
        "@yaml-cpp",
    ],
)

redpanda_cc_btest(
    name = "advertised_kafka_api_test",
    timeout = "short",
    srcs = [
        "advertised_kafka_api_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "seed_server_property_test",
    timeout = "short",
    srcs = [
        "seed_server_property_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "cloud_credentials_source_test",
    timeout = "short",
    srcs = [
        "cloud_credentials_source_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
        "@yaml-cpp",
    ],
)

redpanda_cc_btest(
    name = "validator_test",
    timeout = "short",
    srcs = [
        "validator_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "throughput_control_group_test",
    timeout = "short",
    srcs = [
        "throughput_control_group_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
        "@yaml-cpp",
    ],
)

redpanda_cc_btest(
    name = "node_override_test",
    timeout = "short",
    srcs = [
        "node_override_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:uuid",
        "@seastar",
        "@seastar//:testing",
        "@yaml-cpp",
    ],
)

redpanda_cc_btest(
    name = "leaders_preference_test",
    timeout = "short",
    srcs = [
        "leaders_preference_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "enterprise_property_test",
    timeout = "short",
    srcs = [
        "enterprise_property_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
