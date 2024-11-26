load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest")

redpanda_cc_bench(
    name = "serde_bench",
    timeout = "short",
    srcs = [
        "bench.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/serde",
        "@boost//:container_hash",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_btest(
    name = "serde_test",
    timeout = "short",
    srcs = [
        "serde_test.cc",
    ],
    defines = [
        "SERDE_TEST",
    ],
    deps = [
        "//src/v/bytes:random",
        "//src/v/container:fragmented_vector",
        "//src/v/hashing:crc32c",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/serde:array",
        "//src/v/serde:bool_class",
        "//src/v/serde:bytes",
        "//src/v/serde:chrono",
        "//src/v/serde:enum",
        "//src/v/serde:inet_address",
        "//src/v/serde:iobuf",
        "//src/v/serde:map",
        "//src/v/serde:set",
        "//src/v/serde:sstring",
        "//src/v/serde:variant",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:tristate",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/container:node_hash_set",
        "@boost//:container_hash",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
