load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "random",
    srcs = [
        "random_batch.cc",
    ],
    hdrs = [
        "random_batch.h",
        "randoms.h",
    ],
    include_prefix = "model/tests",
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:random",
        "//src/v/compression",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:random",
        "//src/v/utils:vint",
        "//src/v/utils/tests:random",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "ktp_test",
    timeout = "short",
    srcs = [
        "ktp_test.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:move_canary",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "lexical_cast_test",
    timeout = "short",
    srcs = [
        "lexical_cast_tests.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:move_canary",
        "@boost//:lexical_cast",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "ntp_path_test",
    timeout = "short",
    srcs = [
        "ntp_path_test.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:move_canary",
        "@boost//:lexical_cast",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "topic_view_test",
    timeout = "short",
    srcs = [
        "topic_view_tests.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:move_canary",
        "@boost//:lexical_cast",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "record_batch_reader_test",
    timeout = "short",
    srcs = [
        "record_batch_reader_test.cc",
    ],
    deps = [
        ":random",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)
