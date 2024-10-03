load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "model",
    hdrs = [
        "types.h",
    ],
    include_prefix = "datalake/translation",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "stm",
    srcs = [
        "state_machine.cc",
    ],
    hdrs = [
        "state_machine.h",
    ],
    include_prefix = "datalake/translation",
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        "//src/v/cluster",
        "//src/v/datalake:logger",
        "//src/v/raft",
        "//src/v/serde",
    ],
)

# TODO(iceberg): depends on batching_parquet_writer
# which depends on arrow which is bazel compatible.
# uncomment once the issue is resolved.
#redpanda_cc_library(
#    name = "partition_translator",
#    srcs = [
#        "partition_translator.cc",
#    ],
#    hdrs = [
#        "partition_translator.h",
#    ],
#    include_prefix = "datalake/translation",
#    visibility = ["//visibility:public"],
#    deps = [
#        ":model",
#        ":stm",
#        "//src/v/base",
#        "//src/v/cluster",
#        "//src/v/datalake:cloud_data_io",
#        "//src/v/datalake:logger",
#        "//src/v/datalake:types",
#        "//src/v/datalake:writer",
#        "//src/v/datalake/coordinator:frontend",
#        "//src/v/features",
#        "//src/v/kafka/data:partition_proxy",
#        "//src/v/kafka/utils:txn_reader",
#        "//src/v/model",
#        "//src/v/random:time_jitter",
#        "//src/v/resource_mgmt:io_priority",
#        "//src/v/ssx:future_util",
#        "//src/v/ssx:semaphore",
#        "//src/v/utils:prefix_logger",
#        "//src/v/utils:retry_chain_node",
#        "@seastar",
#   ],
#)
