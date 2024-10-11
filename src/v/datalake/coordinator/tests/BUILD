load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "state_test_utils",
    srcs = [
        "state_test_utils.cc",
    ],
    hdrs = [
        "state_test_utils.h",
    ],
    include_prefix = "datalake/coordinator/tests",
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/datalake/coordinator:state",
        "//src/v/datalake/coordinator:translated_offset_range",
        "//src/v/model",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "coordinator_test",
    timeout = "short",
    srcs = [
        "coordinator_test.cc",
    ],
    cpu = 1,
    deps = [
        ":state_test_utils",
        "//src/v/datalake:logger",
        "//src/v/datalake/coordinator",
        "//src/v/datalake/coordinator:state",
        "//src/v/datalake/coordinator:stm",
        "//src/v/datalake/coordinator:translated_offset_range",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "iceberg_file_committer_test",
    timeout = "short",
    srcs = [
        "iceberg_file_committer_test.cc",
    ],
    cpu = 1,
    deps = [
        ":state_test_utils",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/datalake:logger",
        "//src/v/datalake/coordinator:iceberg_file_committer",
        "//src/v/iceberg:filesystem_catalog",
        "//src/v/iceberg:manifest_io",
        "//src/v/iceberg:metadata_query",
        "//src/v/iceberg:values_bytes",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "state_update_test",
    timeout = "short",
    srcs = [
        "state_update_test.cc",
    ],
    deps = [
        ":state_test_utils",
        "//src/v/container:fragmented_vector",
        "//src/v/datalake/coordinator:state",
        "//src/v/datalake/coordinator:state_update",
        "//src/v/datalake/coordinator:translated_offset_range",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
