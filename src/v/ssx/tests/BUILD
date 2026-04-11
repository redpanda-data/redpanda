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

redpanda_cc_btest(
    name = "async_transforms",
    timeout = "short",
    srcs = [
        "async_transforms.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "async_merge_top_k",
    timeout = "short",
    srcs = [
        "async_merge_top_k_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/ssx:async_merge_top_k",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
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
        "//src/v/container:chunked_vector",
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
    name = "actor_test",
    timeout = "short",
    srcs = [
        "actor_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:actor",
        "//src/v/test_utils:async",
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
    name = "ssx_bench_rpbench",
    srcs = [
        "sformat_bench.cc",
        "thread_worker_bench.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:sformat",
        "//src/v/ssx:thread_worker",
        "@boost//:uuid",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "async_algorithm_rpbench",
    srcs = [
        "async_algorithm_bench.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:async_algorithm",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_gtest(
    name = "rate_limited_function_test",
    timeout = "short",
    srcs = [
        "rate_limited_function_test.cc",
    ],
    deps = [
        "//src/v/ssx:rate_limited_function",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "checkpoint_mutex_test",
    timeout = "short",
    srcs = [
        "checkpoint_mutex_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:checkpoint_mutex",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "condition_variable_test",
    timeout = "short",
    srcs = [
        "condition_variable_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:condition_variable",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "single_fiber_executor_test",
    timeout = "short",
    srcs = [
        "single_fiber_executor_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:single_fiber_executor",
        "//src/v/test_utils:async",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "time_test",
    timeout = "short",
    srcs = [
        "time_test.cc",
    ],
    deps = [
        "//src/v/ssx:time",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "clock_test",
    timeout = "short",
    srcs = [
        "clock_test.cc",
    ],
    deps = [
        "//src/v/ssx:clock",
        "//src/v/ssx:time",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "minimum_interval_timer_test",
    timeout = "short",
    srcs = [
        "minimum_interval_timer_test.cc",
    ],
    deps = [
        "//src/v/ssx:minimum_interval_timer",
        "//src/v/test_utils:async",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)
