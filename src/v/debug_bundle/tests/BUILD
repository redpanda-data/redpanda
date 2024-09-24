load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "types_test",
    timeout = "short",
    srcs = [
        "json_test.cc",
        "types_test.cc",
    ],
    deps = [
        "//src/v/debug_bundle",
        "//src/v/debug_bundle:json",
        "//src/v/json",
        "//src/v/model",
        "//src/v/security",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@fmt",
        "@googletest//:gtest",
        "@rapidjson",
        "@seastar",
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
