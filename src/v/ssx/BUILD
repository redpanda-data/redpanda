load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "condition_variable",
    srcs = ["condition_variable.cc"],
    hdrs = ["condition_variable.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:intrusive",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "thread_worker",
    hdrs = [
        "thread_worker.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@boost//:lockfree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "sformat",
    hdrs = [
        "sformat.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "sharded_service_container",
    hdrs = [
        "sharded_service_container.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":watchdog",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "future_util",
    hdrs = [
        "future-util.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/ssx:when_all",
        "//src/v/utils:functional",
    ],
)

redpanda_cc_library(
    name = "semaphore",
    hdrs = [
        "semaphore.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "abort_source",
    hdrs = [
        "abort_source.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "async_algorithm",
    hdrs = [
        "async_algorithm.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "async_merge_top_k",
    hdrs = [
        "async_merge_top_k.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":async_algorithm",
        "//src/v/base",
        "//src/v/container:priority_queue",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "async_clear",
    hdrs = [
        "async-clear.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "watchdog",
    hdrs = [
        "watchdog.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":future_util",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "checkpoint_mutex",
    hdrs = [
        "checkpoint_mutex.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":sformat",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "task_local",
    hdrs = [
        "task_local_ptr.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "single_sharded",
    hdrs = [
        "single_sharded.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "event",
    hdrs = [
        "event.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":semaphore",
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "sleep_abortable",
    hdrs = [
        "sleep_abortable.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "sharded_ptr",
    hdrs = [
        "sharded_ptr.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":mutex",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "aligned",
    hdrs = [
        "aligned.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "sharded_value",
    hdrs = [
        "sharded_value.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":aligned",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "work_queue",
    srcs = [
        "work_queue.cc",
    ],
    hdrs = [
        "work_queue.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":future_util",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "when_all",
    hdrs = [
        "when_all.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rate_limited_function",
    hdrs = [
        "rate_limited_function.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "single_fiber_executor",
    hdrs = [
        "single_fiber_executor.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":future_util",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "mutex",
    hdrs = [
        "mutex.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":semaphore",
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "actor",
    hdrs = [
        "actor.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":future_util",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "time",
    srcs = [
        "time.cc",
    ],
    hdrs = [
        "time.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@abseil-cpp//absl/time",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "clock",
    srcs = [
        "clock.cc",
    ],
    hdrs = [
        "clock.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":time",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "minimum_interval_timer",
    srcs = [
        "minimum_interval_timer.cc",
    ],
    hdrs = [
        "minimum_interval_timer.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":future_util",
        "//src/v/base",
        "@seastar",
    ],
)
