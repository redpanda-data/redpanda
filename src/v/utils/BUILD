load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//visibility:public"])

redpanda_cc_library(
    name = "arch",
    hdrs = [
        "arch.h",
    ],
)

redpanda_cc_library(
    name = "auto_fmt",
    hdrs = [
        "auto_fmt.h",
    ],
    deps = [
        "//src/v/reflection:to_tuple",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "absl_sstring_hash",
    hdrs = [
        "absl_sstring_hash.h",
    ],
    deps = [
        "//src/v/base",
        "@abseil-cpp//absl/hash",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "moving_average",
    hdrs = [
        "moving_average.h",
    ],
    deps = [
        ":named_type",
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "stop_signal",
    hdrs = [
        "stop_signal.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "vint",
    srcs = [
        "vint.cc",
    ],
    hdrs = [
        "vint.h",
        "vint_iostream.h",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "functional",
    hdrs = [
        "functional.h",
    ],
)

redpanda_cc_library(
    name = "inet_address",
    hdrs = [
        "inet_address_wrapper.h",
    ],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "tristate",
    hdrs = [
        "tristate.h",
    ],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "token_bucket",
    hdrs = [
        "token_bucket.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memory_data_source",
    hdrs = [
        "memory_data_source.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "static_deleter_fn",
    hdrs = [
        "static_deleter_fn.h",
    ],
)

redpanda_cc_library(
    name = "named_type",
    hdrs = [
        "named_type.h",
    ],
    deps = [
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "waiter_queue",
    hdrs = [
        "waiter_queue.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "to_string",
    srcs = ["to_string.cc"],
    hdrs = ["to_string.h"],
    deps = [
        "//src/v/base",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/container:node_hash_map",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "remote",
    hdrs = [
        "remote.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "object_pool",
    hdrs = [
        "object_pool.h",
    ],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "human",
    srcs = [
        "human.cc",
    ],
    hdrs = [
        "human.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "retry",
    hdrs = [
        "retry.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "filtered_lower_bound",
    hdrs = [
        "filtered_lower_bound.h",
    ],
)

redpanda_cc_library(
    name = "timed_mutex",
    hdrs = [
        "timed_mutex.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "file_io",
    srcs = [
        "file_io.cc",
    ],
    hdrs = [
        "file_io.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "tracking_allocator",
    srcs = [
        "tracking_allocator.cc",
    ],
    hdrs = [
        "tracking_allocator.h",
    ],
    deps = [
        ":human",
        "//src/v/container:intrusive",
        "//src/v/json",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "stream_provider",
    hdrs = [
        "stream_provider.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "stream_utils",
    hdrs = [
        "stream_utils.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "unresolved_address",
    srcs = [
        "unresolved_address.cc",
    ],
    hdrs = [
        "unresolved_address.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/serde",
        "@abseil-cpp//absl/strings",
        "@boost//:container_hash",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "hdr_hist",
    srcs = [
        "hdr_hist.cc",
    ],
    hdrs = [
        "hdr_hist.h",
    ],
    deps = [
        ":human",
        ":static_deleter_fn",
        "//src/v/base",
        "@boost//:intrusive",
        "@fmt",
        "@hdrhistogram",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "log_hist",
    srcs = [
        "log_hist.cc",
    ],
    hdrs = [
        "log_hist.h",
    ],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/reflection:type_traits",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "uuid",
    srcs = [
        "uuid.cc",
    ],
    hdrs = [
        "uuid.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "@abseil-cpp//absl/hash",
        "@boost//:uuid",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "xid",
    srcs = [
        "xid.cc",
    ],
    hdrs = [
        "xid.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde",
        "//src/v/serde:array",
        "//src/v/ssx:sformat",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "base64",
    srcs = [
        "base64.cc",
    ],
    hdrs = [
        "base64.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/thirdparty/base64",
        "@abseil-cpp//absl/strings",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "directory_walker",
    srcs = [
        "directory_walker.cc",
    ],
    hdrs = [
        "directory_walker.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "expiring_promise",
    hdrs = [
        "expiring_promise.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "retry_chain_node",
    srcs = [
        "retry_chain_node.cc",
    ],
    hdrs = [
        "retry_chain_node.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "//src/v/ssx:sformat",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "move_canary",
    hdrs = [
        "move_canary.h",
    ],
)

redpanda_cc_library(
    name = "exceptions",
    hdrs = [
        "exceptions.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "stable_iterator",
    hdrs = [
        "stable_iterator_adaptor.h",
    ],
    deps = [
        ":exceptions",
        "//src/v/base",
        "@boost//:iterator",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "delta_for",
    hdrs = [
        "delta_for.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde",
        "//src/v/serde:array",
        "//src/v/serde:iobuf",
        "//src/v/serde:optional",
        "//src/v/ssx:sformat",
    ],
)

redpanda_cc_library(
    name = "prefix_logger",
    hdrs = [
        "prefix_logger.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:sformat",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "notification_list",
    hdrs = [
        "notification_list.h",
    ],
)

redpanda_cc_library(
    name = "adjustable_semaphore",
    hdrs = [
        "adjustable_semaphore.h",
    ],
    deps = [
        ":named_type",
        "//src/v/base",
        "//src/v/ssx:semaphore",
    ],
)

redpanda_cc_library(
    name = "truncating_logger",
    hdrs = [
        "truncating_logger.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:sformat",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "available_promise",
    hdrs = [
        "available_promise.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ema",
    hdrs = [
        "ema.h",
    ],
)

redpanda_cc_library(
    name = "lazy_abort_source",
    hdrs = [
        "lazy_abort_source.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "queue_depth_control",
    hdrs = [
        "queue_depth_control.h",
    ],
    deps = [
        ":ema",
        "//src/v/base",
        "//src/v/ssx:semaphore",
    ],
)

redpanda_cc_library(
    name = "fixed_string",
    hdrs = [
        "fixed_string.h",
    ],
)

redpanda_cc_library(
    name = "external_process",
    srcs = [
        "external_process.cc",
    ],
    hdrs = [
        "external_process.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:mutex",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "copy_range",
    hdrs = [
        "copy_range.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "s3_fifo",
    hdrs = [
        "s3_fifo.h",
    ],
    deps = [
        "@boost//:intrusive",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "chunked_kv_cache",
    hdrs = [
        "chunked_kv_cache.h",
    ],
    deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/utils:s3_fifo",
        "@boost//:intrusive",
    ],
)

redpanda_cc_library(
    name = "null_output_stream",
    srcs = [
        "null_output_stream.cc",
    ],
    hdrs = [
        "null_output_stream.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "variant",
    hdrs = [
        "variant.h",
    ],
)

redpanda_cc_library(
    name = "backoff_policy",
    hdrs = [
        "backoff_policy.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "execution_monitor",
    hdrs = [
        "execution_monitor.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "windowed_sum_tracker",
    hdrs = [
        "windowed_sum_tracker.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "detailed_error",
    hdrs = [
        "detailed_error.h",
    ],
    deps = [
        "//src/v/base",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "device_utils",
    srcs = [
        "device_utils.cc",
    ],
    hdrs = [
        "device_utils.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:sformat",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "offset_monitor",
    hdrs = [
        "offset_monitor.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
