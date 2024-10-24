load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "types_test",
    timeout = "short",
    srcs = [
        "json_test.cc",
        "metadata_test.cc",
        "types_test.cc",
    ],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/debug_bundle",
        "//src/v/debug_bundle:json",
        "//src/v/debug_bundle:metadata",
        "//src/v/debug_bundle:types",
        "//src/v/json",
        "//src/v/model",
        "//src/v/security",
        "//src/v/serde",
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
    timeout = "moderate",
    srcs = [
        "debug_bundle_service_test.cc",
    ],
    data = [":rpk-shim.sh"],
    env = {
        "RPK_SHIM": "$(rootpath :rpk-shim.sh)",
    },
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/config",
        "//src/v/debug_bundle",
        "//src/v/debug_bundle:error",
        "//src/v/debug_bundle:metadata",
        "//src/v/debug_bundle:types",
        "//src/v/debug_bundle:utils",
        "//src/v/features",
        "//src/v/random:generators",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/storage:resources",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
