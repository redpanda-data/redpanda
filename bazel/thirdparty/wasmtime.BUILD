# This file is injected into the crate BUILD file
load("@bazel_skylib//rules:expand_template.bzl", "expand_template")

expand_template(
    name = "gen_conf",
    out = "include/wasmtime/conf.h",
    substitutions = {
        "#cmakedefine WASMTIME_FEATURE_ADDR2LINE": "#define WASMTIME_FEATURE_ADDR2LINE",
        "#cmakedefine WASMTIME_FEATURE_ASYNC": "#define WASMTIME_FEATURE_ASYNC",
        "#cmakedefine WASMTIME_FEATURE_CACHE": "",
        "#cmakedefine WASMTIME_FEATURE_COREDUMP": "",
        "#cmakedefine WASMTIME_FEATURE_CRANELIFT": "#define WASMTIME_FEATURE_CRANELIFT",
        "#cmakedefine WASMTIME_FEATURE_DEMANGLE": "",
        "#cmakedefine WASMTIME_FEATURE_DISABLE_LOGGING": "",
        "#cmakedefine WASMTIME_FEATURE_GC": "",
        "#cmakedefine WASMTIME_FEATURE_LOGGING": "",
        "#cmakedefine WASMTIME_FEATURE_PARALLEL_COMPILATION": "",
        "#cmakedefine WASMTIME_FEATURE_PROFILING": "",
        "#cmakedefine WASMTIME_FEATURE_THREADS": "",
        "#cmakedefine WASMTIME_FEATURE_WASI": "",
        "#cmakedefine WASMTIME_FEATURE_WAT": "#define WASMTIME_FEATURE_WAT",
        "#cmakedefine WASMTIME_FEATURE_WINCH": "",
    },
    template = "include/wasmtime/conf.h.in",
)

cc_library(
    name = "wasmtime_c",
    hdrs = glob(["include/**"]) + [":gen_conf"],
    strip_include_prefix = "include",
    # include_prefix = "wasmtime",
    deps = [":wasmtime_c_api"],
)
