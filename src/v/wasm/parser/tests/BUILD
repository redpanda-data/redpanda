load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "leb128_test",
    timeout = "short",
    srcs = [
        "leb128_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/test_utils:gtest",
        "//src/v/wasm/parser:leb128",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "parser_test",
    timeout = "short",
    srcs = [
        "parser_test.cc",
    ],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:fragmented_vector",
        "//src/v/test_utils:gtest",
        "//src/v/wasm/parser",
        "@crates//:wasmtime_c",
        "@googletest//:gtest",
        "@seastar",
    ],
)
