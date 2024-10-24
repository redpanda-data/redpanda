load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_btest_no_seastar", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "random",
    hdrs = [
        "randoms.h",
    ],
    include_prefix = "utils/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/random:generators",
        "//src/v/utils:unresolved_address",
    ],
)

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
        "//src/v/bytes:iobuf_parser",
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
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/container:node_hash_set",
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

redpanda_cc_btest_no_seastar(
    name = "auto_fmt_test",
    timeout = "short",
    srcs = [
        "auto_fmt_test.cc",
    ],
    defines = [
        "BOOST_TEST_MODULE=auto_fmt",
    ],
    deps = [
        "//src/v/utils:auto_fmt",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "moving_average_test",
    timeout = "short",
    srcs = [
        "moving_average_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:moving_average",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "rwlock_test",
    timeout = "short",
    srcs = [
        "rwlock_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:semaphore",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:rwlock",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "token_bucket_test",
    timeout = "short",
    srcs = [
        "token_bucket_test.cc",
    ],
    deps = [
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:token_bucket",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "waiter_queue_test",
    timeout = "short",
    srcs = [
        "waiter_queue_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:waiter_queue",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "timed_mutex_test",
    timeout = "short",
    srcs = [
        "timed_mutex_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:semaphore",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:timed_mutex",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "input_stream_fanout_test",
    timeout = "short",
    srcs = [
        "input_stream_fanout_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/bytes:random",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:stream_utils",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "seastar_histogram_test",
    timeout = "short",
    srcs = [
        "seastar_histogram_test.cc",
    ],
    deps = [
        "//src/v/metrics",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:hdr_hist",
        "//src/v/utils:log_hist",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "remote_test",
    timeout = "short",
    srcs = [
        "remote_test.cc",
    ],
    deps = [
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:remote",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "retry_test",
    timeout = "short",
    srcs = [
        "retry_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:retry",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "filtered_lower_bound_test",
    timeout = "short",
    srcs = [
        "filtered_lower_bound_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:filtered_lower_bound",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "delta_for_test",
    timeout = "short",
    srcs = [
        "delta_for_characterization_data.cc",
        "delta_for_characterization_data.h",
        "delta_for_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:delta_for",
        "@boost//:numeric_conversion",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "delta_for_bench",
    timeout = "short",
    srcs = [
        "delta_for_bench.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "//src/v/utils:delta_for",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_gtest(
    name = "external_process_test",
    timeout = "short",
    srcs = [
        "external_process_test.cc",
    ],
    data = [":handle-sigterm.sh"],
    env = {
        "HANDLE_SIGTERM_SCRIPT": "$(location :handle-sigterm.sh)",
    },
    deps = [
        "//src/v/test_utils:gtest",
        "//src/v/utils:external_process",
        "@googletest//:gtest",
        "@seastar",
    ],
)
