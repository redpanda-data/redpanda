load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_btest_no_seastar", "redpanda_cc_gtest")

redpanda_cc_btest(
    name = "random_test",
    timeout = "short",
    srcs = [
        "random_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "random_seeding_btest_seastar_test",
    timeout = "short",
    srcs = [
        "random_seeding_test.cc",
    ],
    defines = [
        "IS_SEASTAR_TEST=1",
    ],
    deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "//src/v/random:test_seeding",
        "//src/v/test_utils:seastar_boost",
        "//src/v/test_utils:test_macros",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "random_seeding_btest_noseastar_test",
    timeout = "short",
    srcs = [
        "random_seeding_test.cc",
    ],
    defines = [
        "IS_SEASTAR_TEST=0",
    ],
    deps = [
        "//src/v/random:generators",
        "//src/v/random:test_seeding",
        "//src/v/test_utils:test_macros",
        "@boost//:test",
    ],
)

redpanda_cc_gtest(
    name = "random_seeding_gtest_test",
    timeout = "short",
    srcs = [
        "random_seeding_test.cc",
    ],
    defines = [
        "IS_SEASTAR_TEST=1",
    ],
    deps = [
        "//src/v/random:generators",
        "//src/v/random:test_seeding",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:test_macros",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "random_rpbench",
    srcs = ["random_bench.cc"],
    deps = [
        "//src/v/random:generators",
        "@abseil-cpp//absl/random",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
