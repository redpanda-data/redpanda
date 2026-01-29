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
    deps = [
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
    implementation_deps = [
        "//src/v/utils:vint",
    ],
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/reflection:type_traits",
        "//src/v/utils:named_type",
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
    implementation_deps = [
        ":logger",
        "//src/v/utils:named_type",
    ],
    deps = [
        ":ffi",
        "//src/v/base",
        "//src/v/pandaproxy/schema_registry:registry",
        "//src/v/schema:registry",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "wasi_logger",
    hdrs = [
        "wasi_logger.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "wasi",
    srcs = [
        "wasi.cc",
    ],
    hdrs = [
        "wasi.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/strings:utf8",
        "@abseil-cpp//absl/strings",
    ],
    deps = [
        ":ffi",
        ":wasi_logger",
        "//src/v/base",
        "//src/v/model",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/container:flat_hash_map",
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
    implementation_deps = [
        ":logger",
        "//src/v/bytes:iobuf_parser",
    ],
    deps = [
        ":api",
        ":ffi",
        ":wasi",
        "//src/v/base",
        "//src/v/bytes:iobuf",
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
    visibility = ["//visibility:public"],
    deps = [
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
    visibility = ["//visibility:public"],
    deps = [
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
    visibility = ["//visibility:public"],
    deps = [
        ":transform_probe",
        ":wasi_logger",
        "//src/v/base",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:server",
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
    implementation_deps = [
        ":allocator",
        ":engine_probe",
        ":ffi",
        ":logger",
        ":schema_registry_module",
        ":transform_module",
        ":transform_probe",
        ":wasi",
        ":wasi_logger",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/reflection:type_traits",
        "//src/v/schema:registry",
        "//src/v/ssx:mutex",
        "//src/v/ssx:thread_worker",
        "//src/v/utils:human",
        "//src/v/utils:named_type",
        "//src/v/utils:to_string",
        "//src/v/wasm/parser",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/strings",
        "@crates//:wasmtime_c",
        "@fmt",
    ],
    deps = [
        ":api",
        "//src/v/base",
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
    implementation_deps = [
        ":logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":api",
        ":engine_probe",
        ":transform_probe",
        ":wasi_logger",
        "//src/v/base",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/ssx:mutex",
        "@abseil-cpp//absl/container:btree",
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
    implementation_deps = [
        ":wasmtime",
        "//src/v/schema:registry",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":api",
        "//src/v/base",
        "//src/v/pandaproxy/schema_registry:types",
        "@seastar",
    ],
)
