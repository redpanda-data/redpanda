load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_btest_no_seastar", "redpanda_cc_gtest")

redpanda_cc_btest_no_seastar(
    name = "tristate_test",
    timeout = "short",
    srcs = [
        "tristate_test.cc",
    ],
    defines = [
        "BOOST_TEST_MODULE=tristate",
    ],
    deps = [
        "//src/v/utils:tristate",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "named_type_test",
    timeout = "short",
    srcs = [
        "named_type_tests.cc",
    ],
    defines = [
        "BOOST_TEST_MODULE=named_type",
    ],
    deps = [
        "//src/v/base",
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_btest(
    name = "object_pool_test",
    timeout = "short",
    srcs = [
        "object_pool_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:object_pool",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "vint_test",
    timeout = "short",
    srcs = [
        "vint_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:vint",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "uuid_test",
    timeout = "short",
    srcs = [
        "uuid_test.cc",
    ],
    deps = [
        "//src/v/serde",
        "//src/v/serde:map",
        "//src/v/serde:optional",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/hash",
        "@boost//:container_hash",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "xid_test",
    timeout = "short",
    srcs = [
        "xid_test.cc",
    ],
    deps = [
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "//src/v/utils:xid",
        "@abseil-cpp//absl/container:node_hash_map",
        "@googletest//:gtest",
    ],
)

redpanda_cc_bench(
    name = "vint_bench",
    timeout = "short",
    srcs = [
        "vint_bench.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/random:generators",
        "//src/v/utils:vint",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "human_test",
    timeout = "short",
    srcs = [
        "human_test.cc",
    ],
    defines = [
        "BOOST_TEST_MODULE=human",
    ],
    deps = [
        "//src/v/utils:human",
        "@boost//:test",
        "@fmt",
    ],
)

redpanda_cc_btest(
    name = "base64_test",
    timeout = "short",
    srcs = [
        "base64_test.cc",
    ],
    deps = [
        "//src/v/bytes:random",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:base64",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "directory_walker_test",
    timeout = "short",
    srcs = [
        "directory_walker_test.cc",
    ],
    deps = [
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:directory_walker",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "expiring_promise_test",
    timeout = "short",
    srcs = [
        "expiring_promise_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:expiring_promise",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "retry_chain_node_test",
    timeout = "short",
    srcs = [
        "retry_chain_node_test.cc",
    ],
    deps = [
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:retry_chain_node",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "bottomless_token_bucket_test",
    timeout = "short",
    srcs = [
        "bottomless_token_bucket_test.cc",
    ],
    deps = [
        "//src/v/utils:bottomless_token_bucket",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "move_canary_test",
    timeout = "short",
    srcs = [
        "move_canary_test.cc",
    ],
    defines = [
        "BOOST_TEST_MODULE=move_canary",
    ],
    deps = [
        "//src/v/utils:move_canary",
        "@boost//:test",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "stable_iterator_test",
    timeout = "short",
    srcs = [
        "stable_iterator_test.cc",
    ],
    defines = [
        "BOOST_TEST_MODULE=stable_iterator_test",
    ],
    deps = [
        "//src/v/utils:stable_iterator",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "tracking_allocator_test",
    timeout = "short",
    srcs = [
        "tracking_allocator_tests.cc",
    ],
    deps = [
        "//src/v/json",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:tracking_allocator",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
    ],
)

redpanda_cc_bench(
    name = "coro_bench",
    timeout = "short",
    srcs = [
        "coro_bench.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:future_util",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "seastar_histogram_bench",
    timeout = "short",
    srcs = [
        "seastar_histogram_bench.cc",
    ],
    deps = [
        "//src/v/utils:hdr_hist",
        "//src/v/utils:log_hist",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
