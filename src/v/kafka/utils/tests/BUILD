load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "txn_reader_test",
    timeout = "short",
    srcs = [
        "txn_reader_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/kafka/utils:txn_reader",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/algorithm:container",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)
