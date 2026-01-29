load("@rules_python//python:defs.bzl", "py_binary")
load("//bazel:build.bzl", "redpanda_cc_binary")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_fuzz_test", "redpanda_test_cc_library")

redpanda_cc_bench(
    name = "serde_rpbench",
    srcs = [
        "bench.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/serde",
        "//src/v/serde:iobuf",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_btest(
    name = "serde_test",
    timeout = "short",
    srcs = [
        "serde_test.cc",
    ],
    defines = [
        "SERDE_TEST",
    ],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/hashing:crc32c",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/serde:array",
        "//src/v/serde:bool_class",
        "//src/v/serde:bytes",
        "//src/v/serde:chrono",
        "//src/v/serde:enum",
        "//src/v/serde:inet_address",
        "//src/v/serde:iobuf",
        "//src/v/serde:map",
        "//src/v/serde:pair",
        "//src/v/serde:set",
        "//src/v/serde:sstring",
        "//src/v/serde:variant",
        "//src/v/test_utils:random",
        "//src/v/test_utils:random_bytes",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:tristate",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/container:node_hash_set",
        "@boost//:container_hash",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

py_binary(
    name = "struct_gen",
    srcs = ["struct_gen.py"],
    main = "struct_gen.py",
    visibility = ["//visibility:public"],
    deps = ["@python_deps//jinja2"],
)

genrule(
    name = "generated_structs_genrule",
    outs = ["generated_structs.h"],
    cmd = "$(execpath :struct_gen) $@",
    toolchains = ["@rules_python//python:current_py_toolchain"],
    tools = [":struct_gen"],
)

redpanda_test_cc_library(
    name = "generated_structs",
    hdrs = [":generated_structs.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde",
        "//src/v/serde:iobuf",
        "//src/v/serde:optional",
        "//src/v/serde:sstring",
        "//src/v/serde:vector",
    ],
)

redpanda_cc_fuzz_test(
    name = "serde_fuzz_test",
    timeout = "moderate",
    srcs = ["fuzz.cc"],
    args = [
        "-max_total_time=60",
        "-rss_limit_mb=8192",
    ],
    deps = [
        ":generated_structs",
        "//src/v/reflection:type_traits",
        "//src/v/serde",
        "//src/v/serde:vector",
    ],
)

redpanda_cc_binary(
    name = "serde_fuzz_main",
    testonly = True,
    srcs = ["fuzz.cc"],
    defines = ["MAIN=1"],
    deps = [
        ":generated_structs",
        "//src/v/reflection:type_traits",
        "//src/v/serde",
    ],
)
