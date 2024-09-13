load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "types_test",
    timeout = "short",
    srcs = [
        "types_test.cc",
    ],
    deps = [
        "//src/v/debug_bundle",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "debug_bundle_service_test",
    timeout = "short",
    srcs = [
        "debug_bundle_service_test.cc",
    ],
    data = [":rpk-shim.sh"],
    env = {
        "RPK_SHIM": "$(rootpath :rpk-shim.sh)",
    },
    deps = [
        "//src/v/config",
        "//src/v/debug_bundle",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
