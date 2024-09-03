load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "crypto",
    srcs = [
        "crypto.cc",
        "digest.cc",
        "hmac.cc",
        "internal.h",
        "key.cc",
        "key.h",
        "ossl_context_service.cc",
        "random.cc",
        "signature.cc",
        "ssl_utils.cc",
        "ssl_utils.h",
    ],
    hdrs = [
        "crypto.h",
        "exceptions.h",
        "ossl_context_service.h",
        "types.h",
    ],
    implementation_deps = [
        "//src/v/ssx:thread_worker",
        "//src/v/thirdparty/openssl",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/container:node_hash_map",
    ],
    include_prefix = "crypto",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "@seastar",
    ],
)
