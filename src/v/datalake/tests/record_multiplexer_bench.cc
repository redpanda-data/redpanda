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
#include "container/fragmented_vector.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/record_multiplexer.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/tests/catalog_and_registry_fixture.h"
#include "datalake/tests/record_generator.h"
#include "datalake/tests/test_data_writer.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "serde/avro/tests/data_generator.h"
#include "serde/protobuf/tests/data_generator.h"

#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <optional>
#include <string_view>

namespace {
std::string generate_nested_proto_int(size_t total_depth) {
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
      generate_nested_proto_int(total_depth - 1),
      total_depth,
      2 * total_depth,
      member);
}

std::string generate_nested_proto(size_t total_depth) {
    return std::format(
      "syntax = \"proto3\"; {}", generate_nested_proto_int(total_depth));
}

std::string generate_linear_proto(size_t total_fields) {
    constexpr auto proto_template = R"(
    syntax = "proto3";
    message Linear {{
        {}
    }})";

    std::string fields = "";
    for (int i = 1; i <= total_fields; i++) {
        fields += std::format("string a{} = {};\n", i, i);
    }

    return std::format(proto_template, fields);
}

std::string generate_nested_avro_int(size_t total_depth) {
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
      generate_nested_avro_int(total_depth - 1));
}

std::string generate_nested_avro(size_t total_depth) {
    constexpr auto avro_template = R"({{
    "name": "base",
    "type": "record",
    "fields": [
        {}
    ]}})";

    return std::format(avro_template, generate_nested_avro_int(total_depth));
}

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

chunked_vector<model::record_batch>
share_batches(chunked_vector<model::record_batch>& batches) {
    chunked_vector<model::record_batch> ret;
    for (auto& batch : batches) {
        ret.push_back(batch.share());
    }
    return ret;
}

struct counting_consumer {
    size_t total_bytes = 0;
    datalake::record_multiplexer mux;
    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        total_bytes += batch.size_bytes();
        return mux(std::move(batch));
    }
    ss::future<counting_consumer> end_of_stream() {
        auto res = co_await mux.end_of_stream();
        BOOST_REQUIRE(!res.has_error());
        co_return std::move(*this);
    }
};

const model::ntp
  ntp(model::ns{"rp"}, model::topic{"t"}, model::partition_id{0});

} // namespace

class record_multiplexer_bench_fixture
  : public datalake::tests::catalog_and_registry_fixture {
public:
    record_multiplexer_bench_fixture()
      : _schema_mgr(catalog)
      , _type_resolver(registry)
      , _record_gen(&registry) {}

    ss::future<>
    try_add_avro_schema(std::string_view name, std::string_view schema) {
        auto [_, added] = _added_names.emplace(name);
        if (!added) {
            co_return;
        }

        auto reg_res = co_await _record_gen.register_avro_schema(name, schema);
        BOOST_REQUIRE(reg_res.has_value());
    }

    ss::future<>
    try_add_protobuf_schema(std::string_view name, std::string_view schema) {
        auto [_, added] = _added_names.emplace(name);
        if (!added) {
            co_return;
        }

        auto reg_res = co_await _record_gen.register_protobuf_schema(
          name, schema);
        BOOST_REQUIRE(reg_res.has_value());
    }

    ss::future<chunked_vector<model::record_batch>> generate_batches(
      size_t records_per_batch,
      size_t batches,
      std::function<ss::future<
        checked<std::nullopt_t, datalake::tests::record_generator::error>>(
        storage::record_batch_builder&)> add_batch) {
        chunked_vector<model::record_batch> ret;
        ret.reserve(batches);

        model::offset o{0};
        for (size_t i = 0; i < batches; ++i) {
            storage::record_batch_builder batch_builder(
              model::record_batch_type::raft_data, o);

            // Add some records per batch.
            for (size_t r = 0; r < records_per_batch; ++r) {
                auto res = co_await add_batch(batch_builder);
                ++o;

                BOOST_REQUIRE(!res.has_error());
            }
            auto batch = std::move(batch_builder).build();
            ret.emplace_back(std::move(batch));
        }

        co_return ret;
    }

    ss::future<chunked_vector<model::record_batch>> generate_protobuf_batches(
      size_t records_per_batch,
      size_t batches,
      std::string schema_name,
      std::string proto_schema,
      std::vector<int32_t> msg_idx,
      ::testing::protobuf_generator_config gen_config) {
        co_await try_add_protobuf_schema(schema_name, proto_schema);
        co_return co_await generate_batches(
          records_per_batch, batches, [&](auto& bb) {
              return _record_gen.add_random_protobuf_record(
                bb, schema_name, msg_idx, std::nullopt, gen_config);
          });
    }

    ss::future<chunked_vector<model::record_batch>> generate_avro_batches(
      size_t records_per_batch,
      size_t batches,
      std::string schema_name,
      std::string avro_schema,
      ::testing::avro_generator_config gen_config) {
        co_await try_add_avro_schema(schema_name, avro_schema);
        co_return co_await generate_batches(
          records_per_batch, batches, [&](auto& bb) {
              return _record_gen.add_random_avro_record(
                bb, schema_name, std::nullopt, gen_config);
          });
    }

    datalake::record_multiplexer create_mux() {
        return datalake::record_multiplexer(
          ntp,
          std::make_unique<datalake::test_serde_parquet_writer_factory>(),
          _schema_mgr,
          _type_resolver,
          _translator);
    }
    datalake::tests::record_generator& get_record_gen() { return _record_gen; }

private:
    std::unordered_set<std::string> _added_names;
    datalake::catalog_schema_manager _schema_mgr;
    datalake::record_schema_resolver _type_resolver;
    datalake::tests::record_generator _record_gen;
    datalake::default_translator _translator;
};

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_381_byte_message_linear_1_field) {
    static constexpr size_t records_per_batch = 10;
    static constexpr size_t batches = 1000;
    static constexpr ::testing::protobuf_generator_config gen_config = {
      .string_length_range{302, 302}};

    static std::string proto_schema = generate_linear_proto(1);

    static thread_local chunked_vector<model::record_batch> batch_data
      = co_await generate_protobuf_batches(
        records_per_batch,
        batches,
        "proto_schema",
        proto_schema,
        {0},
        gen_config);

    auto reader = model::make_fragmented_memory_record_batch_reader(
      share_batches(batch_data));

    auto consumer = counting_consumer{.mux = create_mux()};

    perf_tests::start_measuring_time();
    auto res = co_await reader.consume(std::move(consumer), model::no_timeout);
    perf_tests::stop_measuring_time();

    co_return res.total_bytes;
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_40_fields) {
    static constexpr size_t records_per_batch = 10;
    static constexpr size_t batches = 1000;
    static constexpr ::testing::protobuf_generator_config gen_config = {
      .string_length_range{5, 5}};

    // Generate a schema near the nesting limit.
    // (12 payload + 3 header) bytes per field
    static std::string proto_schema = generate_linear_proto(40);

    static thread_local chunked_vector<model::record_batch> batch_data
      = co_await generate_protobuf_batches(
        records_per_batch,
        batches,
        "proto_schema",
        proto_schema,
        {0},
        gen_config);

    auto reader = model::make_fragmented_memory_record_batch_reader(
      share_batches(batch_data));

    auto consumer = counting_consumer{.mux = create_mux()};

    perf_tests::start_measuring_time();
    auto res = co_await reader.consume(std::move(consumer), model::no_timeout);
    perf_tests::stop_measuring_time();

    co_return res.total_bytes;
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_80_fields) {
    static constexpr size_t records_per_batch = 10;
    static constexpr size_t batches = 1000;
    static constexpr ::testing::protobuf_generator_config gen_config = {
      .string_length_range{1, 1}};

    // Generate a schema near the nesting limit.
    // (3 payload + 3 header) bytes per field
    static std::string proto_schema = generate_linear_proto(80);

    static thread_local chunked_vector<model::record_batch> batch_data
      = co_await generate_protobuf_batches(
        records_per_batch,
        batches,
        "proto_schema",
        proto_schema,
        {0},
        gen_config);

    auto reader = model::make_fragmented_memory_record_batch_reader(
      share_batches(batch_data));

    auto consumer = counting_consumer{.mux = create_mux()};

    perf_tests::start_measuring_time();
    auto res = co_await reader.consume(std::move(consumer), model::no_timeout);
    perf_tests::stop_measuring_time();

    co_return res.total_bytes;
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_384_byte_message_nested_24_levels) {
    static constexpr size_t records_per_batch = 10;
    static constexpr size_t batches = 1000;
    static constexpr ::testing::protobuf_generator_config gen_config = {
      .string_length_range{7, 7}, .max_nesting_level = 40};

    // Generate a schema near the nesting limit.
    // 11 bytes per nest
    static std::string proto_schema = generate_nested_proto(24);

    static thread_local chunked_vector<model::record_batch> batch_data
      = co_await generate_protobuf_batches(
        records_per_batch,
        batches,
        "proto_schema",
        proto_schema,
        {0},
        gen_config);

    auto reader = model::make_fragmented_memory_record_batch_reader(
      share_batches(batch_data));

    auto consumer = counting_consumer{.mux = create_mux()};

    perf_tests::start_measuring_time();
    auto res = co_await reader.consume(std::move(consumer), model::no_timeout);
    perf_tests::stop_measuring_time();

    co_return res.total_bytes;
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_386_byte_message_nested_31_levels) {
    static constexpr size_t records_per_batch = 10;
    static constexpr size_t batches = 1000;
    static constexpr ::testing::protobuf_generator_config gen_config = {
      .string_length_range{4, 4}, .max_nesting_level = 40};

    // Generate a schema near the nesting limit.
    static std::string proto_schema = generate_nested_proto(31);

    static thread_local chunked_vector<model::record_batch> batch_data
      = co_await generate_protobuf_batches(
        records_per_batch,
        batches,
        "proto_schema",
        proto_schema,
        {0},
        gen_config);

    auto reader = model::make_fragmented_memory_record_batch_reader(
      share_batches(batch_data));

    auto consumer = counting_consumer{.mux = create_mux()};

    perf_tests::start_measuring_time();
    auto res = co_await reader.consume(std::move(consumer), model::no_timeout);
    perf_tests::stop_measuring_time();

    co_return res.total_bytes;
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_385_byte_message_linear_1_field) {
    static constexpr size_t records_per_batch = 10;
    static constexpr size_t batches = 1000;
    static constexpr ::testing::avro_generator_config gen_config = {
      .string_length_range{308, 308}};

    static std::string avro_schema = generate_linear_avro(1);

    static thread_local chunked_vector<model::record_batch> batch_data
      = co_await generate_avro_batches(
        records_per_batch, batches, "avro_schema", avro_schema, gen_config);

    auto reader = model::make_fragmented_memory_record_batch_reader(
      share_batches(batch_data));

    auto consumer = counting_consumer{.mux = create_mux()};

    perf_tests::start_measuring_time();
    auto res = co_await reader.consume(std::move(consumer), model::no_timeout);
    perf_tests::stop_measuring_time();

    co_return res.total_bytes;
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_385_byte_message_linear_31_fields) {
    static constexpr size_t records_per_batch = 10;
    static constexpr size_t batches = 1000;
    static constexpr ::testing::avro_generator_config gen_config = {
      .string_length_range{9, 9}};

    static std::string avro_schema = generate_linear_avro(31);

    static thread_local chunked_vector<model::record_batch> batch_data
      = co_await generate_avro_batches(
        records_per_batch, batches, "avro_schema", avro_schema, gen_config);

    auto reader = model::make_fragmented_memory_record_batch_reader(
      share_batches(batch_data));

    auto consumer = counting_consumer{.mux = create_mux()};

    perf_tests::start_measuring_time();
    auto res = co_await reader.consume(std::move(consumer), model::no_timeout);
    perf_tests::stop_measuring_time();

    co_return res.total_bytes;
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_385_byte_message_linear_62_fields) {
    static constexpr size_t records_per_batch = 10;
    static constexpr size_t batches = 1000;
    static constexpr ::testing::avro_generator_config gen_config = {
      .string_length_range{4, 4}};

    static std::string avro_schema = generate_linear_avro(62);

    static thread_local chunked_vector<model::record_batch> batch_data
      = co_await generate_avro_batches(
        records_per_batch, batches, "avro_schema", avro_schema, gen_config);

    auto reader = model::make_fragmented_memory_record_batch_reader(
      share_batches(batch_data));

    auto consumer = counting_consumer{.mux = create_mux()};

    perf_tests::start_measuring_time();
    auto res = co_await reader.consume(std::move(consumer), model::no_timeout);
    perf_tests::stop_measuring_time();

    co_return res.total_bytes;
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_385_byte_message_nested_31_levels) {
    static constexpr size_t records_per_batch = 10;
    static constexpr size_t batches = 1000;
    static constexpr ::testing::avro_generator_config gen_config = {
      .string_length_range{9, 9}, .max_nesting_level = 40};

    static std::string avro_schema = generate_nested_avro(31);

    static thread_local chunked_vector<model::record_batch> batch_data
      = co_await generate_avro_batches(
        records_per_batch, batches, "avro_schema", avro_schema, gen_config);

    auto reader = model::make_fragmented_memory_record_batch_reader(
      share_batches(batch_data));

    auto consumer = counting_consumer{.mux = create_mux()};

    perf_tests::start_measuring_time();
    auto res = co_await reader.consume(std::move(consumer), model::no_timeout);
    perf_tests::stop_measuring_time();

    co_return res.total_bytes;
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture, avro_385_byte_message_nested_62_levels) {
    static constexpr size_t records_per_batch = 10;
    static constexpr size_t batches = 1000;
    static constexpr ::testing::avro_generator_config gen_config = {
      .string_length_range{4, 4}, .max_nesting_level = 40};

    static std::string avro_schema = generate_nested_avro(62);

    static thread_local chunked_vector<model::record_batch> batch_data
      = co_await generate_avro_batches(
        records_per_batch, batches, "avro_schema", avro_schema, gen_config);

    auto reader = model::make_fragmented_memory_record_batch_reader(
      share_batches(batch_data));

    auto consumer = counting_consumer{.mux = create_mux()};

    perf_tests::start_measuring_time();
    auto res = co_await reader.consume(std::move(consumer), model::no_timeout);
    perf_tests::stop_measuring_time();

    co_return res.total_bytes;
}
