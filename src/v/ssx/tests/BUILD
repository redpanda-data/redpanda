load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_btest(
    name = "future-util_test",
    timeout = "short",
    srcs = [
        "future_util.cc",
    ],
    deps = [
        "//src/v/ssx:future_util",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:functional",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "thread_worker_test",
    timeout = "short",
    srcs = [
        "thread_worker.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:thread_worker",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/algorithm:container",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "sformat_test",
    timeout = "short",
    srcs = [
        "sformat.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "abort_source_test",
    timeout = "short",
    srcs = [
        "abort_source_test.cc",
    ],
    cpu = 2,
    deps = [
        "//src/v/base",
        "//src/v/ssx:abort_source",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "sleep_abortable_test",
    timeout = "short",
    srcs = [
        "sleep_abortable_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:sleep_abortable",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "watchdog_test",
    timeout = "short",
    srcs = [
        "watchdog_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:watchdog",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "task_local_ptr_test",
    timeout = "short",
    srcs = [
        "task_local_ptr_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:task_local",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/algorithm:container",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "sharded_ptr_test",
    timeout = "short",
    srcs = [
        "sharded_ptr_test.cc",
    ],
    cpu = 2,
    deps = [
        "//src/v/base",
        "//src/v/ssx:sharded_ptr",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "single_sharded_test",
    timeout = "short",
    srcs = [
        "single_sharded_test.cc",
    ],
    cpu = 2,
    deps = [
        "//src/v/base",
        "//src/v/ssx:future_util",
        "//src/v/ssx:single_sharded",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "when_all_test",
    timeout = "short",
    srcs = [
        "when_all_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/ssx:when_all",
        "//src/v/test_utils:gtest",
        "@boost//:container",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "work_queue_test",
    timeout = "short",
    srcs = [
        "work_queue_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:work_queue",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "event_test",
    timeout = "short",
    srcs = [
        "event_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:event",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "async_algorithm_test",
    timeout = "short",
    srcs = [
        "async_algorithm_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:async_clear",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:gtest",
        "//src/v/utils:move_canary",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "thread_worker_bench",
    timeout = "short",
    srcs = [
        "thread_worker_bench.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:thread_worker",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "sformat_bench",
    timeout = "short",
    srcs = [
        "sformat_bench.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:sformat",
        "@boost//:uuid",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
