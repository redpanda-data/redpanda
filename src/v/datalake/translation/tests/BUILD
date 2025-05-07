load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "state_machine_test",
    timeout = "short",
    srcs = [
        "state_machine_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/datalake/translation:stm",
        "//src/v/datalake/translation:utils",
        "//src/v/raft/tests:stm_test_fixture",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "scheduler_fixture",
    srcs = [
        "scheduler_fixture.cc",
    ],
    hdrs = [
        "scheduler_fixture.h",
    ],
    include_prefix = "datalake/translation/tests",
    deps = [
        "//src/v/base",
        "//src/v/datalake:logger",
        "//src/v/datalake/translation:scheduler",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "scheduler_test",
    timeout = "moderate",
    srcs = [
        "scheduler_tests.cc",
    ],
    cpu = 1,
    deps = [
        ":scheduler_fixture",
        "//src/v/datalake:logger",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "fair_scheduler_test",
    timeout = "moderate",
    srcs = [
        "fair_scheduling_policy_tests.cc",
    ],
    cpu = 1,
    deps = [
        ":scheduler_fixture",
        "//src/v/datalake:logger",
        "//src/v/datalake/translation:scheduler",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "partition_translator_test",
    timeout = "short",
    srcs = [
        "partition_translator_tests.cc",
    ],
    cpu = 1,
    deps = [
        ":scheduler_fixture",
        "//src/v/datalake/translation:partition_translator",
        "//src/v/datalake/translation:translator_deps",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "record_multiplexer_test",
    timeout = "short",
    srcs = [
        "record_multiplexer_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/cloud_io:provider",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/datalake:location",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake:table_definition",
        "//src/v/datalake:table_id_provider",
        "//src/v/datalake/tests:catalog_and_registry_fixture",
        "//src/v/datalake/tests:record_generator",
        "//src/v/datalake/tests:test_data_writer",
        "//src/v/datalake/tests:test_utils",
        "//src/v/datalake/translation:record_multiplexer",
        "//src/v/datalake/translation:translation_probe",
        "//src/v/iceberg:filesystem_catalog",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/pandaproxy",
        "//src/v/random:generators",
        "//src/v/schema:registry",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@avro",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "record_multiplexer_rpbench",
    timeout = "long",
    srcs = [
        "record_multiplexer_bench.cc",
    ],
    deps = [
        "//src/v/cloud_io:provider",
        "//src/v/container:fragmented_vector",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/datalake:location",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake:table_creator",
        "//src/v/datalake/tests:catalog_and_registry_fixture",
        "//src/v/datalake/tests:record_generator",
        "//src/v/datalake/tests:test_data_writer",
        "//src/v/datalake/tests:test_utils",
        "//src/v/datalake/translation:record_multiplexer",
        "//src/v/model",
        "//src/v/serde/avro/tests:data_generator",
        "//src/v/serde/protobuf/tests:data_generator",
        "//src/v/storage:parser_utils",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
