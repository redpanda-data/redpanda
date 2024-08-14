load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/wasm:__pkg__",
        "//src/v/wasm/tests:__pkg__",
    ],
)

redpanda_cc_library(
    name = "allocator",
    srcs = [
        "allocator.cc",
    ],
    hdrs = [
        "allocator.h",
    ],
    include_prefix = "wasm",
    deps = [
        ":logger",
        "//src/v/base",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "wasm",
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ffi",
    srcs = [
        "ffi.cc",
    ],
    hdrs = [
        "ffi.h",
    ],
    include_prefix = "wasm",
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/reflection:type_traits",
        "//src/v/utils:named_type",
        "//src/v/utils:vint",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema_registry",
    srcs = [
        "schema_registry.cc",
    ],
    hdrs = [
        "schema_registry.h",
    ],
    include_prefix = "wasm",
    deps = [
        "//src/v/base",
        "//src/v/pandaproxy",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema_registry_module",
    srcs = [
        "schema_registry_module.cc",
    ],
    hdrs = [
        "schema_registry_module.h",
    ],
    include_prefix = "wasm",
    deps = [
        ":ffi",
        ":logger",
        ":schema_registry",
        "//src/v/base",
        "//src/v/pandaproxy",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "wasi_logger",
    hdrs = [
        "wasi_logger.h",
    ],
    include_prefix = "wasm",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "wasi",
    srcs = [
        "wasi.cc",
    ],
    hdrs = [
        "wasi.h",
    ],
    include_prefix = "wasm",
    deps = [
        ":ffi",
        ":logger",
        ":wasi_logger",
        "//src/v/base",
        "//src/v/model",
        "//src/v/strings:utf8",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/strings",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "transform_module",
    srcs = [
        "transform_module.cc",
    ],
    hdrs = [
        "transform_module.h",
    ],
    include_prefix = "wasm",
    deps = [
        ":api",
        ":ffi",
        ":logger",
        ":wasi",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "transform_probe",
    srcs = [
        "transform_probe.cc",
    ],
    hdrs = [
        "transform_probe.h",
    ],
    include_prefix = "wasm",
    visibility = ["//visibility:public"],
    deps = [
        ":ffi",
        ":logger",
        ":wasi",
        "//src/v/base",
        "//src/v/metrics",
        "//src/v/utils:log_hist",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "engine_probe",
    srcs = [
        "engine_probe.cc",
    ],
    hdrs = [
        "engine_probe.h",
    ],
    include_prefix = "wasm",
    visibility = ["//visibility:public"],
    deps = [
        ":ffi",
        ":logger",
        ":wasi",
        "//src/v/base",
        "//src/v/metrics",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "api",
    hdrs = [
        "engine.h",
        "errc.h",
        "fwd.h",
    ],
    include_prefix = "wasm",
    visibility = ["//visibility:public"],
    deps = [
        ":transform_probe",
        ":wasi_logger",
        "//src/v/base",
        "//src/v/model",
        "//src/v/pandaproxy",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "wasmtime",
    srcs = [
        "wasmtime.cc",
    ],
    hdrs = [
        "wasmtime.h",
    ],
    include_prefix = "wasm",
    deps = [
        ":allocator",
        ":api",
        ":engine_probe",
        ":ffi",
        ":logger",
        ":schema_registry",
        ":schema_registry_module",
        ":transform_module",
        ":transform_probe",
        ":wasi",
        ":wasi_logger",
        "//src/v/base",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/pandaproxy",
        "//src/v/reflection:type_traits",
        "//src/v/ssx:thread_worker",
        "//src/v/storage:parser_utils",
        "//src/v/thirdparty/wasmtime",
        "//src/v/utils:human",
        "//src/v/utils:mutex",
        "//src/v/utils:named_type",
        "//src/v/utils:to_string",
        "//src/v/wasm/parser",
        "@abseil-cpp//absl/algorithm:container",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/strings",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cache",
    srcs = [
        "cache.cc",
    ],
    hdrs = [
        "cache.h",
    ],
    include_prefix = "wasm",
    visibility = ["//visibility:public"],
    deps = [
        ":api",
        ":engine_probe",
        ":logger",
        ":transform_probe",
        ":wasi_logger",
        "//src/v/base",
        "//src/v/model",
        "//src/v/utils:mutex",
        "//src/v/utils:named_type",
        "//src/v/utils:to_string",
        "//src/v/wasm/parser",
        "@abseil-cpp//absl/algorithm:container",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "impl",
    srcs = [
        "impl.cc",
    ],
    hdrs = [
        "impl.h",
    ],
    include_prefix = "wasm",
    visibility = ["//visibility:public"],
    deps = [
        ":api",
        ":schema_registry",
        ":wasmtime",
        "//src/v/base",
        "//src/v/pandaproxy",
        "@seastar",
    ],
)
