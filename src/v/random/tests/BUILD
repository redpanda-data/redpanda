load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "random_test",
    timeout = "short",
    srcs = [
        "random_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/random:fast_prng",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
    ],
)
