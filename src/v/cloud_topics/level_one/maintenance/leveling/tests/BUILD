load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "leveling_reducer_test",
    timeout = "short",
    srcs = ["leveling_reducer_test.cc"],
    deps = [
        "//src/v/cloud_topics/level_one/frontend_reader/tests:l1_reader_fixture",
        "//src/v/cloud_topics/level_one/maintenance:meta",
        "//src/v/cloud_topics/level_one/maintenance:worker_probe",
        "//src/v/cloud_topics/level_one/maintenance/leveling:leveling_source_and_sink",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:offset_interval_set",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/compaction:reducer",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
