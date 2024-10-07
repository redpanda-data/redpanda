load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "state_update_test",
    timeout = "short",
    srcs = [
        "state_update_test.cc",
    ],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/datalake:writer",
        "//src/v/datalake/coordinator:state",
        "//src/v/datalake/coordinator:state_update",
        "//src/v/datalake/coordinator:translated_offset_range",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
