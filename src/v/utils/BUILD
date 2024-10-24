load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//visibility:public"])

redpanda_cc_library(
    name = "auto_fmt",
    hdrs = [
        "auto_fmt.h",
    ],
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rwlock",
    hdrs = [
        "rwlock.h",
    ],
    include_prefix = "utils",
    deps = [
        "//src/v/base",
        "//src/v/ssx:semaphore",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
)

redpanda_cc_library(
    name = "inet_address",
    hdrs = [
        "inet_address_wrapper.h",
    ],
    include_prefix = "utils",
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "tristate",
    hdrs = [
        "tristate.h",
    ],
    include_prefix = "utils",
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "token_bucket",
    hdrs = [
        "token_bucket.h",
    ],
    include_prefix = "utils",
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
    include_prefix = "utils",
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "mutex",
    hdrs = [
        "mutex.h",
    ],
    include_prefix = "utils",
    deps = [
        "//src/v/base",
        "//src/v/ssx:semaphore",
    ],
)

redpanda_cc_library(
    name = "static_deleter_fn",
    hdrs = [
        "static_deleter_fn.h",
    ],
    include_prefix = "utils",
)

redpanda_cc_library(
    name = "named_type",
    hdrs = [
        "named_type.h",
    ],
    include_prefix = "utils",
    deps = [
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "waiter_queue",
    hdrs = [
        "waiter_queue.h",
    ],
    include_prefix = "utils",
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "to_string",
    hdrs = [
        "to_string.h",
    ],
    include_prefix = "utils",
    deps = [
        "//src/v/base",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
)

redpanda_cc_library(
    name = "timed_mutex",
    hdrs = [
        "timed_mutex.h",
    ],
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
    deps = [
        "//src/v/base",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "unresolved_address",
    hdrs = [
        "unresolved_address.h",
    ],
    include_prefix = "utils",
    deps = [
        "//src/v/serde",
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
    include_prefix = "utils",
    deps = [
        ":human",
        ":static_deleter_fn",
        "//src/v/base",
        "//src/v/thirdparty/hdrhistogram",
        "@boost//:intrusive",
        "@fmt",
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
    include_prefix = "utils",
    deps = [
        "//src/v/container:fragmented_vector",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
    deps = [
        "//src/v/base",
        "//src/v/random:fast_prng",
        "//src/v/ssx:sformat",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "bottomless_token_bucket",
    srcs = [
        "bottomless_token_bucket.cc",
    ],
    hdrs = [
        "bottomless_token_bucket.h",
    ],
    include_prefix = "utils",
    deps = [
        "//src/v/base",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "move_canary",
    hdrs = [
        "move_canary.h",
    ],
    include_prefix = "utils",
)

redpanda_cc_library(
    name = "exceptions",
    hdrs = [
        "exceptions.h",
    ],
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
)

redpanda_cc_library(
    name = "adjustable_semaphore",
    hdrs = [
        "adjustable_semaphore.h",
    ],
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
)

redpanda_cc_library(
    name = "lazy_abort_source",
    hdrs = [
        "lazy_abort_source.h",
    ],
    include_prefix = "utils",
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
    include_prefix = "utils",
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
    include_prefix = "utils",
)

redpanda_cc_library(
    name = "external_process",
    srcs = [
        "external_process.cc",
    ],
    hdrs = [
        "external_process.h",
    ],
    include_prefix = "utils",
    deps = [
        "//src/v/base",
        "//src/v/utils:mutex",
        "@seastar",
    ],
)
