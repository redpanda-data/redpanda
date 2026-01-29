load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "tls",
    srcs = [
        "tls.cc",
    ],
    hdrs = [
        "tls.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "types",
    hdrs = [
        "types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "net",
    srcs = [
        "batched_output_stream.cc",
        "conn_quota.cc",
        "connection.cc",
        "dns.cc",
        "probes.cc",
        "server.cc",
        "transport.cc",
    ],
    hdrs = [
        "batched_output_stream.h",
        "client_probe.h",
        "conn_quota.h",
        "connection.h",
        "connection_rate.h",
        "connection_rate_counter.h",
        "dns.h",
        "exceptions.h",
        "fwd.h",
        "server.h",
        "server_probe.h",
        "tls_certificate_probe.h",
        "transport.h",
    ],
    implementation_deps = [
        "//src/v/strings:utf8",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":tls",
        ":types",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/config",
        "//src/v/hashing:crc32c",
        "//src/v/hashing:xx",
        "//src/v/metrics",
        "//src/v/ssx:abort_source",
        "//src/v/ssx:future_util",
        "//src/v/ssx:mutex",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:sformat",
        "//src/v/utils:inet_address",
        "//src/v/utils:log_hist",
        "//src/v/utils:named_type",
        "//src/v/utils:unresolved_address",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/hash",
        "@boost//:intrusive",
        "@boost//:lexical_cast",
        "@fmt",
        "@seastar",
    ],
)
