load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/lsm:__subpackages__"])

redpanda_cc_library(
    name = "table_builder",
    srcs = [
        "table_builder.cc",
    ],
    hdrs = [
        "table_builder.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/io:persistence",
        "//src/v/lsm/sst:builder",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "iter",
    srcs = [
        "iter.cc",
    ],
    hdrs = [
        "iter.h",
    ],
    implementation_deps = [
        "//src/v/random:generators",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
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
        ":compaction_task",
        ":flush_task",
        ":iter",
        ":table_builder",
        ":version_edit",
        "//src/v/lsm/core/internal:logger",
        "//src/v/lsm/core/internal:merging_iterator",
        "//src/v/lsm/sst:builder",
        "//src/v/ssx:future_util",
        "@fmt",
    ],
    deps = [
        ":gc_actor",
        ":memtable",
        ":snapshot",
        ":table_cache",
        ":version_set",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_hash_map",
        "//src/v/lsm:stats",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/io:persistence",
        "//src/v/lsm/sst:block_cache",
        "//src/v/ssx:clock",
        "//src/v/ssx:condition_variable",
        "//src/v/ssx:mutex",
        "//src/v/ssx:time",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memtable",
    srcs = [
        "memtable.cc",
    ],
    hdrs = [
        "memtable.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:lookup_result",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "table_cache",
    srcs = [
        "table_cache.cc",
    ],
    hdrs = [
        "table_cache.h",
    ],
    implementation_deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:logger",
        "//src/v/lsm/sst:reader",
        "//src/v/ssx:mutex",
        "//src/v/ssx:work_queue",
        "//src/v/utils:s3_fifo",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core:probe",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/io:persistence",
        "//src/v/lsm/sst:block_cache",
        "@abseil-cpp//absl/functional:function_ref",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "version_edit",
    srcs = [
        "version_edit.cc",
    ],
    hdrs = [
        "version_edit.h",
    ],
    implementation_deps = [
        "//src/v/utils:to_string",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:intrusive",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "@abseil-cpp//absl/container:fixed_array",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "file_utils",
    srcs = [
        "file_utils.cc",
    ],
    hdrs = [
        "file_utils.h",
    ],
    deps = [
        ":version_edit",
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:keys",
        "@boost//:range",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "version_set",
    srcs = [
        "version_set.cc",
    ],
    hdrs = [
        "version_set.h",
    ],
    implementation_deps = [
        ":file_utils",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:logger",
        "//src/v/lsm/core/internal:merging_iterator",
        "//src/v/lsm/core/internal:two_level_iterator",
        "//src/v/lsm/proto:manifest_redpanda_proto",
        "//src/v/utils:to_string",
        "@abseil-cpp//absl/container:btree",
    ],
    deps = [
        ":table_cache",
        ":version_edit",
        ":weak_intrusive_list",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/lsm/core:lookup_result",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/io:persistence",
        "@abseil-cpp//absl/container:fixed_array",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "snapshot",
    srcs = [
        "snapshot.cc",
    ],
    hdrs = [
        "snapshot.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:keys",
    ],
)

redpanda_cc_library(
    name = "weak_intrusive_list",
    hdrs = ["weak_intrusive_list.h"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "gc_actor",
    srcs = [
        "gc_actor.cc",
    ],
    hdrs = [
        "gc_actor.h",
    ],
    implementation_deps = [
        "//src/v/lsm/core/internal:logger",
    ],
    deps = [
        ":table_cache",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/io:persistence",
        "//src/v/ssx:actor",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "flush_task",
    srcs = [
        "flush_task.cc",
    ],
    hdrs = [
        "flush_task.h",
    ],
    implementation_deps = [
        "//src/v/lsm/core/internal:logger",
    ],
    deps = [
        ":memtable",
        ":table_builder",
        ":version_set",
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/io:persistence",
        "//src/v/ssx:actor",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "compaction_task",
    srcs = [
        "compaction_task.cc",
    ],
    hdrs = [
        "compaction_task.h",
    ],
    implementation_deps = [
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:logger",
        "//src/v/lsm/sst:builder",
        "//src/v/ssx:when_all",
    ],
    deps = [
        ":snapshot",
        ":version_set",
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/io:persistence",
        "@seastar",
    ],
)
