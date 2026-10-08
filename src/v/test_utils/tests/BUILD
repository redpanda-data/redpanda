load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "gtest_test",
    timeout = "short",
    srcs = [
        "gtest_test.cc",
    ],
    deps = [
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "reactor_stall_probe_test",
    timeout = "short",
    srcs = [
        "reactor_stall_probe_test.cc",
    ],
    deps = [
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:reactor_stall_probe",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "test_random_bytes_test",
    timeout = "short",
    srcs = [
        "random_bytes_test.cc",
    ],
    deps = [
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "test_utils_test",
    timeout = "short",
    srcs = [
        "test_utils_test.cc",
    ],
    deps = [
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "gtest_listener_test",
    timeout = "short",
    srcs = [
        "gtest_listener_test.cc",
    ],
    args = [
        "--gtest_output=xml:/dev/null",
    ],
    deps = [
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "test_env_test",
    timeout = "short",
    srcs = [
        "test_env_test.cc",
    ],
    deps = [
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:test_env",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "container_ostream_test",
    timeout = "short",
    srcs = [
        "container_ostream_test.cc",
    ],
    deps = [
        "//src/v/test_utils:container_ostream",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
