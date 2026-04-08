/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cloud_io/provider.h"
#include "container/chunked_vector.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/location.h"
#include "datalake/record_multiplexer.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/tests/catalog_and_registry_fixture.h"
#include "datalake/tests/record_generator.h"
#include "datalake/tests/test_data_writer.h"
#include "datalake/tests/test_utils.h"
#include "features/feature_table.h"
#include "model/batch_compression.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "serde/avro/tests/data_generator.h"
#include "serde/protobuf/tests/data_generator.h"

#include <seastar/testing/perf_tests.hh>

#include <optional>
#include <string_view>

namespace {

std::string generate_nested_proto_internal(size_t total_depth) {
    constexpr auto proto_template = R"(
    message Foo{} {{
        {}
        string a{} = {};
        {}
    }})";

    if (total_depth == 0) {
        return "";
    }
    std::string member = "";
    if (total_depth > 1) {
        member = std::format(
          "Foo{} b{} = {}; ",
          total_depth - 1,
          total_depth,
          2 * total_depth + 1);
    }
    return std::format(
      proto_template,
      total_depth,
      generate_nested_proto_internal(total_depth - 1),
      total_depth,
      2 * total_depth,
      member);
}

/**
 * Generates a nested protobuf schema.
 *
 * I.e, if total_depth=3 then the following would be generated;
 *
 * syntax = "proto3";
 * message Foo3 {
 *      message Foo2 {
 *          message Foo1 {
 *              string a1 = 2;
 *          }
 *          string a2 = 4;
 *          Foo1 b2 = 5;
 *      }
 *      string a3 = 6;
 *      Foo2 b3 = 7;
 * }
 */
std::string generate_nested_proto(size_t total_depth) {
    return std::format(
      "syntax = \"proto3\"; {}", generate_nested_proto_internal(total_depth));
}

/**
 * Generates a linear protobuf schema.
 *
 * I.e, if total_fields=3 then the following would be generated;
 *
 * syntax = "proto3";
 * message Linear {
 *      string a1 = 1;
 *      string a2 = 2;
 *      string a3 = 3;
 * }
 */
std::string generate_linear_proto(size_t total_fields) {
    constexpr auto proto_template = R"(
    syntax = "proto3";
    message Linear {{
        {}
    }})";

    std::string fields = "";
    for (size_t i = 1; i <= total_fields; i++) {
        fields += std::format("string a{} = {};\n", i, i);
    }

    return std::format(proto_template, fields);
}

std::string generate_nested_avro_internal(size_t total_depth) {
    constexpr auto avro_template = R"(
    {{
        "name": "nestedval{}",
        "type": {{
            "type": "record",
            "name": "nestedrecord{}",
            "fields": [
                {}
                {}
            ]
        }}
    }})";

    if (total_depth == 0) {
        return "";
    }

    std::string string_field = std::format(
      R"({{ "name": "inval{}", "type": "string" }})", total_depth);
    if (total_depth != 1) {
        string_field += ",";
    };

    return std::format(
      avro_template,
      total_depth,
      total_depth,
      string_field,
      generate_nested_avro_internal(total_depth - 1));
}

/**
 * Generates a nested avro schema;
 *
 * I.e, if total_depth=2 then the following would be generated;
 * {
 *  "name": "base",
 *  "type": "record",
 *  "fields": [
 *	{
 *	  "name": "nestedval2",
 *	  "type": {
 *		"type": "record",
 *		"name": "nestedrecord2",
 *		"fields": [
 *		  {
 *			"name": "inval2",
 *			"type": "string"
 *		  },
 *		  {
 *			"name": "nestedval1",
 *			"type": {
 *			  "type": "record",
 *			  "name": "nestedrecord1",
 *			  "fields": [
 *				{
 *				  "name": "inval1",
 *				  "type": "string"
 *				}
 *			  ]
 *			}
 *		  }
 *		]
 *	  }
 *	}
 *  ]
 *}
 *
 */
std::string generate_nested_avro(size_t total_depth) {
    constexpr auto avro_template = R"({{
    "name": "base",
    "type": "record",
    "fields": [
        {}
    ]}})";

    return std::format(
      avro_template, generate_nested_avro_internal(total_depth));
}

/**
 * Generates a linear avro schema.
 *
 * I.e, if total_fields=3 then the following would be generated;
 * {
 *   "name": "base",
 *   "type": "record",
 *   "fields": [
 *    {
 *      "name": "field0",
 *      "type": "string"
 *    },
 *    {
 *      "name": "field1",
 *      "type": "string"
 *    },
 *    {
 *      "name": "field2",
 *      "type": "string"
 *    }
 *  ]
 * }
 *
 */
std::string generate_linear_avro(size_t total_fields) {
    constexpr auto avro_template = R"({{
    "name": "base",
    "type": "record",
    "fields": [
        {}
    ]}})";
    constexpr auto field_template
      = R"({{ "name": "field{}", "type": "string" }})";
    std::string ret = "";

    for (size_t i = 0; i < total_fields; i++) {
        ret += std::format(field_template, i);
        if (i != total_fields - 1) {
            ret += ",";
        }
    }

    ret = std::format(avro_template, ret);
    return ret;
}

std::string generate_nested_json_internal(size_t total_depth) {
    if (total_depth == 0) {
        return "";
    }

    std::string nested_prop = "";
    if (total_depth > 1) {
        nested_prop = std::format(
          R"(,"nested{}": {{
            "type": "object",
            "properties": {{
                "val{}": {{"type": "string"}}
                {}
            }}
          }})",
          total_depth - 1,
          total_depth - 1,
          generate_nested_json_internal(total_depth - 1));
    }

    return nested_prop;
}

/**
 * Generates a nested JSON schema.
 *
 * I.e, if total_depth=2 then the following would be generated;
 *
 * {
 *   "$schema": "http://json-schema.org/draft-07/schema#",
 *   "type": "object",
 *   "properties": {
 *     "val2": {"type": "string"},
 *     "nested1": {
 *       "type": "object",
 *       "properties": {
 *         "val1": {"type": "string"}
 *       }
 *     }
 *   }
 * }
 */
std::string generate_nested_json(size_t total_depth) {
    if (total_depth == 0) {
        return R"({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {}
        })";
    }

    return std::format(
      R"({{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {{
            "val{}": {{"type": "string"}}
            {}
        }}
    }})",
      total_depth,
      generate_nested_json_internal(total_depth));
}

/**
 * Generates a linear JSON schema.
 *
 * I.e, if total_fields=3 then the following would be generated;
 *
 * {
 *   "$schema": "http://json-schema.org/draft-07/schema#",
 *   "type": "object",
 *   "properties": {
 *     "field0": {"type": "string"},
 *     "field1": {"type": "string"},
 *     "field2": {"type": "string"}
 *   }
 * }
 */
std::string generate_linear_json(size_t total_fields) {
    constexpr auto json_template = R"({{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {{
            {}
        }}
    }})";
    constexpr auto field_template = R"("field{}": {{"type": "string"}})";
    std::string fields = "";

    for (size_t i = 0; i < total_fields; i++) {
        fields += std::format(field_template, i);
        if (i != total_fields - 1) {
            fields += ",";
        }
    }

    return std::format(json_template, fields);
}

chunked_vector<model::record_batch>
share_batches(chunked_vector<model::record_batch>& batches) {
    chunked_vector<model::record_batch> ret;
    for (auto& batch : batches) {
        ret.push_back(batch.share());
    }
    return ret;
}

struct counting_consumer {
    size_t total_records = 0;
    datalake::record_multiplexer mux;
    ss::abort_source& as;
    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        total_records += batch.record_count();
        return mux.do_multiplex(std::move(batch), kafka::offset{}, as);
    }
    ss::future<counting_consumer> end_of_stream() {
        datalake::record_multiplexer::finished_files files;
        auto res = co_await std::move(mux).finish(files);
        if (res.has_error()) [[unlikely]] {
            throw std::runtime_error(
              fmt::format("failed to end stream: {}", res.error()));
        }
        if (!files.dlq_files.empty()) {
            throw std::runtime_error(
              fmt::format(
                "unexpected dlq files created: dlq_files_created={}",
                files.dlq_files.size()));
        }
        co_return std::move(*this);
    }
};

// Specifies how many batches should be in the test dataset.
#ifdef NDEBUG
static constexpr size_t batches = 1000;
#else
static constexpr size_t batches = 1;
#endif
// Specifies how many records should be in each batch of the test dataset.
static constexpr size_t records_per_batch = 10;

static constexpr size_t small_field_size_bytes = 8;
static constexpr size_t large_field_size_bytes = 256;
static constexpr size_t max_nesting_level = 40;

} // namespace

class record_multiplexer_bench_fixture
  : public datalake::tests::catalog_and_registry_fixture {
public:
    record_multiplexer_bench_fixture()
      : _schema_cache({10, 5})
      , _resolved_type_cache({10, 5})
      , _schema_mgr(catalog, &_features)
      , _type_resolver(registry, _schema_cache, _resolved_type_cache)
      , _record_gen(&registry)
      , _table_creator(_type_resolver, _schema_mgr) {
        _features.testing_activate_all();
    }

    template<typename T>
    requires std::same_as<T, ::testing::protobuf_generator_config>
             || std::same_as<T, ::testing::avro_generator_config>
             || std::same_as<
               T,
               iceberg::conversion::json_schema::testing::generator_config>
    ss::future<> configure_bench(
      T gen_config,
      std::string schema,
      size_t batches,
      size_t records_per_batch,
      model::compression compression_type = model::compression::none) {
        if constexpr (std::is_same_v<T, ::testing::protobuf_generator_config>) {
            _batch_data = co_await generate_protobuf_batches(
              records_per_batch,
              batches,
              compression_type,
              "proto_schema",
              schema,
              {0},
              gen_config);
        } else if constexpr (
          std::is_same_v<T, ::testing::avro_generator_config>) {
            _batch_data = co_await generate_avro_batches(
              records_per_batch,
              batches,
              compression_type,
              "avro_schema",
              schema,
              gen_config);
        } else {
            _batch_data = co_await generate_json_batches(
              records_per_batch,
              batches,
              compression_type,
              "json_schema",
              schema,
              gen_config);
        }
    }

    ss::future<size_t> run_bench() {
        auto reader = model::make_chunked_memory_record_batch_reader(
          share_batches(_batch_data));
        auto consumer = counting_consumer{.mux = create_mux(), .as = _as};

        perf_tests::start_measuring_time();
        auto res = co_await reader.consume(
          std::move(consumer), model::no_timeout);
        perf_tests::stop_measuring_time();

        co_return res.total_records;
    }

    ss::future<size_t> run_protobuf_linear_bench(
      size_t num_fields,
      size_t field_size,
      model::compression compression = model::compression::none) {
        co_await configure_bench(
          ::testing::protobuf_generator_config{
            .string_length_range{field_size, field_size}},
          generate_linear_proto(num_fields),
          batches,
          records_per_batch,
          compression);
        co_return co_await run_bench();
    }

    ss::future<size_t> run_protobuf_nested_bench(
      size_t num_levels,
      size_t field_size,
      model::compression compression = model::compression::none) {
        co_await configure_bench(
          ::testing::protobuf_generator_config{
            .string_length_range{field_size, field_size},
            .max_nesting_level = max_nesting_level},
          generate_nested_proto(num_levels),
          batches,
          records_per_batch,
          compression);
        co_return co_await run_bench();
    }

    ss::future<size_t> run_avro_linear_bench(
      size_t num_fields,
      size_t field_size,
      model::compression compression = model::compression::none) {
        co_await configure_bench(
          ::testing::avro_generator_config{
            .string_length_range{field_size, field_size}},
          generate_linear_avro(num_fields),
          batches,
          records_per_batch,
          compression);
        co_return co_await run_bench();
    }

    ss::future<size_t> run_avro_nested_bench(
      size_t num_levels,
      size_t field_size,
      model::compression compression = model::compression::none) {
        co_await configure_bench(
          ::testing::avro_generator_config{
            .string_length_range{field_size, field_size},
            .max_nesting_level = max_nesting_level},
          generate_nested_avro(num_levels),
          batches,
          records_per_batch,
          compression);
        co_return co_await run_bench();
    }

    ss::future<size_t> run_json_linear_bench(
      size_t num_fields,
      size_t field_size,
      model::compression compression = model::compression::none) {
        co_await configure_bench(
          iceberg::conversion::json_schema::testing::generator_config{
            .string_length_range{field_size, field_size}},
          generate_linear_json(num_fields),
          batches,
          records_per_batch,
          compression);
        co_return co_await run_bench();
    }

    ss::future<size_t> run_json_nested_bench(
      size_t num_levels,
      size_t field_size,
      model::compression compression = model::compression::none) {
        co_await configure_bench(
          iceberg::conversion::json_schema::testing::generator_config{
            .string_length_range{field_size, field_size},
            .max_nesting_level = max_nesting_level},
          generate_nested_json(num_levels),
          batches,
          records_per_batch,
          compression);
        co_return co_await run_bench();
    }

private:
    const model::ntp ntp{
      model::ns{"rp"}, model::topic{"t"}, model::partition_id{0}};
    const model::revision_id topic_rev{123};

    std::unordered_set<std::string> _added_names;
    features::feature_table _features;
    datalake::chunked_schema_cache _schema_cache;
    datalake::chunked_resolved_type_cache _resolved_type_cache;
    datalake::catalog_schema_manager _schema_mgr;
    datalake::record_schema_resolver _type_resolver;
    datalake::tests::record_generator _record_gen;
    datalake::default_translator _translator;
    datalake::direct_table_creator _table_creator;
    datalake::translation_probe _translation_probe{ntp};
    chunked_vector<model::record_batch> _batch_data;
    ss::abort_source _as;

    datalake::record_multiplexer create_mux() {
        return datalake::record_multiplexer(
          ntp,
          topic_rev,
          std::make_unique<datalake::test_serde_parquet_writer_factory>(),
          _schema_mgr,
          _type_resolver,
          _translator,
          _table_creator,
          model::iceberg_invalid_record_action::dlq_table,
          datalake::location_provider(
            scoped_remote->remote.local().provider(), bucket_name),
          _translation_probe,
          &_features);
    }

    ss::future<>
    try_add_avro_schema(std::string_view name, std::string_view schema) {
        auto [_, added] = _added_names.emplace(name);
        if (!added) {
            co_return;
        }

        auto reg_res = co_await _record_gen.register_avro_schema(name, schema);
        if (reg_res.has_error()) [[unlikely]] {
            throw std::runtime_error(
              fmt::format(
                "failed to register avro schema: {}", reg_res.error()));
        }
    }

    ss::future<>
    try_add_protobuf_schema(std::string_view name, std::string_view schema) {
        auto [_, added] = _added_names.emplace(name);
        if (!added) {
            co_return;
        }

        auto reg_res = co_await _record_gen.register_protobuf_schema(
          name, schema);
        if (reg_res.has_error()) [[unlikely]] {
            throw std::runtime_error(
              fmt::format(
                "failed to register protobuf schema: {}", reg_res.error()));
        }
    }

    ss::future<>
    try_add_json_schema(std::string_view name, std::string_view schema) {
        auto [_, added] = _added_names.emplace(name);
        if (!added) {
            co_return;
        }

        auto reg_res = co_await _record_gen.register_json_schema(name, schema);
        if (reg_res.has_error()) [[unlikely]] {
            throw std::runtime_error(
              fmt::format(
                "failed to register json schema: {}", reg_res.error()));
        }
    }

    ss::future<chunked_vector<model::record_batch>> generate_batches(
      size_t records_per_batch,
      size_t batches,
      model::compression compression_type,
      std::function<ss::future<
        checked<std::nullopt_t, datalake::tests::record_generator::error>>(
        storage::record_batch_builder&)> add_record) {
        chunked_vector<model::record_batch> ret;
        ret.reserve(batches);

        model::offset o{0};
        for (size_t i = 0; i < batches; ++i) {
            storage::record_batch_builder batch_builder(
              model::record_batch_type::raft_data, o);

            // Add some records per batch.
            for (size_t r = 0; r < records_per_batch; ++r) {
                auto res = co_await add_record(batch_builder);
                ++o;

                if (res.has_error()) [[unlikely]] {
                    throw std::runtime_error(
                      fmt::format("unable to add record: {}", res.error()));
                }
            }

            auto batch = std::move(batch_builder).build();
            if (compression_type != model::compression::none) {
                batch = co_await model::compress_batch(
                  compression_type, std::move(batch));
            }
            ret.emplace_back(std::move(batch));
        }

        co_return ret;
    }

    ss::future<chunked_vector<model::record_batch>> generate_protobuf_batches(
      size_t records_per_batch,
      size_t batches,
      model::compression compression_type,
      std::string schema_name,
      std::string proto_schema,
      std::vector<int32_t> msg_idx,
      ::testing::protobuf_generator_config gen_config) {
        co_await try_add_protobuf_schema(schema_name, proto_schema);
        co_return co_await generate_batches(
          records_per_batch, batches, compression_type, [&](auto& bb) {
              return _record_gen.add_random_protobuf_record(
                bb, schema_name, msg_idx, std::nullopt, gen_config);
          });
    }

    ss::future<chunked_vector<model::record_batch>> generate_avro_batches(
      size_t records_per_batch,
      size_t batches,
      model::compression compression_type,
      std::string schema_name,
      std::string avro_schema,
      ::testing::avro_generator_config gen_config) {
        co_await try_add_avro_schema(schema_name, avro_schema);
        co_return co_await generate_batches(
          records_per_batch, batches, compression_type, [&](auto& bb) {
              return _record_gen.add_random_avro_record(
                bb, schema_name, std::nullopt, gen_config);
          });
    }

    ss::future<chunked_vector<model::record_batch>> generate_json_batches(
      size_t records_per_batch,
      size_t batches,
      model::compression compression_type,
      std::string schema_name,
      std::string json_schema,
      iceberg::conversion::json_schema::testing::generator_config gen_config) {
        co_await try_add_json_schema(schema_name, json_schema);
        co_return co_await generate_batches(
          records_per_batch, batches, compression_type, [&](auto& bb) {
              return _record_gen.add_random_json_record(
                bb, schema_name, std::nullopt, gen_config);
          });
    }
};

PERF_TEST_CN(record_multiplexer_bench_fixture, protobuf_linear_1_field_small) {
    co_return co_await run_protobuf_linear_bench(1, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, protobuf_linear_1_field_large) {
    co_return co_await run_protobuf_linear_bench(1, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_linear_1_field_small_zstd) {
    co_return co_await run_protobuf_linear_bench(
      1, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_linear_1_field_large_zstd) {
    co_return co_await run_protobuf_linear_bench(
      1, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_linear_40_fields_small) {
    co_return co_await run_protobuf_linear_bench(40, small_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_linear_40_fields_large) {
    co_return co_await run_protobuf_linear_bench(40, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_linear_40_fields_small_zstd) {
    co_return co_await run_protobuf_linear_bench(
      40, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_linear_40_fields_large_zstd) {
    co_return co_await run_protobuf_linear_bench(
      40, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_linear_80_fields_small) {
    co_return co_await run_protobuf_linear_bench(80, small_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_linear_80_fields_large) {
    co_return co_await run_protobuf_linear_bench(80, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_linear_80_fields_small_zstd) {
    co_return co_await run_protobuf_linear_bench(
      80, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_linear_80_fields_large_zstd) {
    co_return co_await run_protobuf_linear_bench(
      80, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_nested_10_levels_small) {
    co_return co_await run_protobuf_nested_bench(10, small_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_nested_10_levels_large) {
    co_return co_await run_protobuf_nested_bench(10, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_nested_10_levels_small_zstd) {
    co_return co_await run_protobuf_nested_bench(
      10, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_nested_10_levels_large_zstd) {
    co_return co_await run_protobuf_nested_bench(
      10, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_nested_30_levels_small) {
    co_return co_await run_protobuf_nested_bench(30, small_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_nested_30_levels_large) {
    co_return co_await run_protobuf_nested_bench(30, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_nested_30_levels_small_zstd) {
    co_return co_await run_protobuf_nested_bench(
      30, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_nested_30_levels_large_zstd) {
    co_return co_await run_protobuf_nested_bench(
      30, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_linear_1_field_small) {
    co_return co_await run_avro_linear_bench(1, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_linear_1_field_large) {
    co_return co_await run_avro_linear_bench(1, large_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_linear_1_field_small_zstd) {
    co_return co_await run_avro_linear_bench(
      1, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_linear_1_field_large_zstd) {
    co_return co_await run_avro_linear_bench(
      1, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_linear_40_fields_small) {
    co_return co_await run_avro_linear_bench(40, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_linear_40_fields_large) {
    co_return co_await run_avro_linear_bench(40, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_linear_40_fields_small_zstd) {
    co_return co_await run_avro_linear_bench(
      40, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_linear_40_fields_large_zstd) {
    co_return co_await run_avro_linear_bench(
      40, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_linear_80_fields_small) {
    co_return co_await run_avro_linear_bench(80, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_linear_80_fields_large) {
    co_return co_await run_avro_linear_bench(80, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_linear_80_fields_small_zstd) {
    co_return co_await run_avro_linear_bench(
      80, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_linear_80_fields_large_zstd) {
    co_return co_await run_avro_linear_bench(
      80, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_nested_10_levels_small) {
    co_return co_await run_avro_nested_bench(10, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_nested_10_levels_large) {
    co_return co_await run_avro_nested_bench(10, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_nested_10_levels_small_zstd) {
    co_return co_await run_avro_nested_bench(
      10, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_nested_10_levels_large_zstd) {
    co_return co_await run_avro_nested_bench(
      10, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_nested_30_levels_small) {
    co_return co_await run_avro_nested_bench(30, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, avro_nested_30_levels_large) {
    co_return co_await run_avro_nested_bench(30, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_nested_30_levels_small_zstd) {
    co_return co_await run_avro_nested_bench(
      30, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_nested_30_levels_large_zstd) {
    co_return co_await run_avro_nested_bench(
      30, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_linear_1_field_small) {
    co_return co_await run_json_linear_bench(1, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_linear_1_field_large) {
    co_return co_await run_json_linear_bench(1, large_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_linear_1_field_small_zstd) {
    co_return co_await run_json_linear_bench(
      1, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_linear_1_field_large_zstd) {
    co_return co_await run_json_linear_bench(
      1, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_linear_40_fields_small) {
    co_return co_await run_json_linear_bench(40, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_linear_40_fields_large) {
    co_return co_await run_json_linear_bench(40, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, json_linear_40_fields_small_zstd) {
    co_return co_await run_json_linear_bench(
      40, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, json_linear_40_fields_large_zstd) {
    co_return co_await run_json_linear_bench(
      40, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_linear_80_fields_small) {
    co_return co_await run_json_linear_bench(80, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_linear_80_fields_large) {
    co_return co_await run_json_linear_bench(80, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, json_linear_80_fields_small_zstd) {
    co_return co_await run_json_linear_bench(
      80, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, json_linear_80_fields_large_zstd) {
    co_return co_await run_json_linear_bench(
      80, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_nested_10_levels_small) {
    co_return co_await run_json_nested_bench(10, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_nested_10_levels_large) {
    co_return co_await run_json_nested_bench(10, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, json_nested_10_levels_small_zstd) {
    co_return co_await run_json_nested_bench(
      10, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, json_nested_10_levels_large_zstd) {
    co_return co_await run_json_nested_bench(
      10, large_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_nested_30_levels_small) {
    co_return co_await run_json_nested_bench(30, small_field_size_bytes);
}

PERF_TEST_CN(record_multiplexer_bench_fixture, json_nested_30_levels_large) {
    co_return co_await run_json_nested_bench(30, large_field_size_bytes);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, json_nested_30_levels_small_zstd) {
    co_return co_await run_json_nested_bench(
      30, small_field_size_bytes, model::compression::zstd);
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, json_nested_30_levels_large_zstd) {
    co_return co_await run_json_nested_bench(
      30, large_field_size_bytes, model::compression::zstd);
}
