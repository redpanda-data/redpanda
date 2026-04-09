load("//bazel:build.bzl", "redpanda_cc_binary")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_binary(
    name = "db_bench",
    testonly = True,
    srcs = [
        "db_bench.cc",
    ],
    deps = [
        ":immutable_tree",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:compression",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/db:impl",
        "//src/v/lsm/db:memtable",
        "//src/v/lsm/io:disk_persistence",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "@abseil-cpp//absl/strings",
        "@boost//:program_options",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "memtable_test",
    timeout = "short",
    srcs = [
        "memtable_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:lookup_result",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal/tests:iterator_test_harness",
        "//src/v/lsm/db:memtable",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "snapshot_test",
    timeout = "short",
    srcs = [
        "snapshot_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/db:snapshot",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "weak_intrusive_list_test",
    timeout = "short",
    srcs = [
        "weak_intrusive_list_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/db:weak_intrusive_list",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "table_cache_test",
    timeout = "short",
    srcs = [
        "table_cache_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/db:table_cache",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/lsm/sst:block_cache",
        "//src/v/lsm/sst:builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "file_utils_test",
    timeout = "short",
    srcs = [
        "file_utils_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/db:file_utils",
        "//src/v/lsm/db:version_edit",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "version_set_test",
    timeout = "short",
    srcs = [
        "version_set_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/db:table_cache",
        "//src/v/lsm/db:version_edit",
        "//src/v/lsm/db:version_set",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/lsm/sst:block_cache",
        "//src/v/lsm/sst:builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "iter_test",
    timeout = "short",
    srcs = [
        "iter_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/db:iter",
        "//src/v/lsm/db:memtable",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "impl_test",
    timeout = "short",
    srcs = [
        "impl_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/db:impl",
        "//src/v/lsm/db:memtable",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/lsm/sst:builder",
        "//src/v/random:generators",
        "//src/v/ssx:clock",
        "//src/v/ssx:time",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "gc_actor_test",
    timeout = "short",
    srcs = [
        "gc_actor_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/db:gc_actor",
        "//src/v/lsm/db:table_cache",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/lsm/sst:block_cache",
        "//src/v/lsm/sst:builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "immutable_tree",
    hdrs = ["immutable_tree.h"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "immutable_tree_test",
    timeout = "short",
    srcs = [
        "immutable_tree_test.cc",
    ],
    deps = [
        ":immutable_tree",
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "flush_task_rpbench",
    srcs = ["flush_task_bench.cc"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/db:flush_task",
        "//src/v/lsm/db:memtable",
        "//src/v/lsm/db:table_cache",
        "//src/v/lsm/db:version_set",
        "//src/v/lsm/io:disk_persistence",
        "//src/v/lsm/sst:block_cache",
        "//src/v/random:generators",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
