load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "arity_test",
    timeout = "short",
    srcs = [
        "arity_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/reflection:arity",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "async_adl_test",
    timeout = "short",
    srcs = [
        "async_adl_test.cc",
    ],
    deps = [
        "//src/v/bytes:hash",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:node_hash_map",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "collections_interop",
    timeout = "short",
    srcs = [
        "collections_interop.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)
