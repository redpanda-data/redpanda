load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("//bazel:internal.bzl", "redpanda_copts")

# convenience wrapper around a filegroup to satisfy layering checks.
cc_library(
    name = "libclang_hdrs",
    hdrs = ["@current_llvm_toolchain_llvm//:all_includes"],
    tags = ["no-clang-tidy-headers"],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "plugins.so",
    srcs = [
        "redpanda_lambda_coroutine_deduces_this_check.cc",
        "redpanda_lambda_coroutine_deduces_this_check.h",
        "redpanda_noop_check.h",
        "redpanda_tidy_module.cc",
    ],
    copts = redpanda_copts() + [
        "-Iexternal/toolchains_llvm++llvm+current_llvm_toolchain_llvm/include",
        "-Wno-deprecated-declarations",
        "-Wno-unused-parameter",
    ],
    features = [
        "layering_check",
    ],
    linkshared = True,
    visibility = [
        "//bazel/clang_tidy:__subpackages__",
        "//tools:__subpackages__",
    ],
    deps = [
        ":libclang_hdrs",
    ],
)
