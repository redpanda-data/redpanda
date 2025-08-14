/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/random.h"
#include "container/chunked_vector.h"
#include "random/generators.h"
#include "serde/parquet/column_writer.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "test_utils/random_bytes.h"

#include <seastar/testing/perf_tests.hh>

#include <cstdint>
#include <type_traits>

using namespace serde::parquet;

namespace {

schema_element leaf_node(
  ss::sstring name,
  field_repetition_type rep_type,
  physical_type ptype,
  logical_type ltype = logical_type{}) {
    return {
      .type = ptype,
      .repetition_type = rep_type,
      .path = {std::move(name)},
      .logical_type = ltype,
    };
}

constexpr size_t num_values = 10'000;

} // namespace

class column_writer_bench {
public:
    ss::future<size_t> bench(
      const schema_element& element,
      bool compressed,
      chunked_vector<value> values) {
        auto writer = column_writer(
          element,
          {
            .compress = compressed,
          });

        perf_tests::start_measuring_time();
        for (size_t i = 0; i < num_values; i++) {
            writer.add(std::move(values[i]), rep_level{0}, def_level{0});
        }

        perf_tests::do_not_optimize(co_await writer.flush_pages());
        perf_tests::stop_measuring_time();

        co_return num_values;
    }

    template<typename T, typename PT, typename VT>
    ss::future<size_t> bench(std::function<T()> rand_fn, bool compressed) {
        chunked_vector<value> values;
        values.reserve(num_values);
        for (size_t i = 0; i < num_values; i++) {
            values.push_back(VT(rand_fn()));
        }

        size_t element_size = sizeof(T);
        if constexpr (std::is_same_v<T, iobuf>) {
            element_size = rand_fn().size_bytes();
        }

        co_return co_await bench(
          leaf_node("leaf", field_repetition_type::required, PT{}),
          compressed,
          std::move(values))
          * element_size;
    }
};

PERF_TEST_CN(column_writer_bench, byte_array_type_4_byte_compressed) {
    co_return co_await bench<iobuf, byte_array_type, byte_array_value>(
      [] { return tests::random_iobuf(4); }, true);
}

PERF_TEST_CN(column_writer_bench, byte_array_type_4_byte_uncompressed) {
    co_return co_await bench<iobuf, byte_array_type, byte_array_value>(
      [] { return tests::random_iobuf(4); }, false);
}

PERF_TEST_CN(column_writer_bench, byte_array_type_40_byte_compressed) {
    co_return co_await bench<iobuf, byte_array_type, byte_array_value>(
      [] { return tests::random_iobuf(40); }, true);
}

PERF_TEST_CN(column_writer_bench, byte_array_type_40_byte_uncompressed) {
    co_return co_await bench<iobuf, byte_array_type, byte_array_value>(
      [] { return tests::random_iobuf(40); }, false);
}

PERF_TEST_CN(column_writer_bench, byte_array_type_400_byte_compressed) {
    co_return co_await bench<iobuf, byte_array_type, byte_array_value>(
      [] { return tests::random_iobuf(400); }, true);
}

PERF_TEST_CN(column_writer_bench, byte_array_type_400_byte_uncompressed) {
    co_return co_await bench<iobuf, byte_array_type, byte_array_value>(
      [] { return tests::random_iobuf(400); }, false);
}

PERF_TEST_CN(column_writer_bench, bool_type_compressed) {
    co_return co_await bench<bool, bool_type, boolean_value>(
      [] { return random_generators::random_choice({true, false}); }, true);
}

PERF_TEST_CN(column_writer_bench, bool_type_uncompressed) {
    co_return co_await bench<bool, bool_type, boolean_value>(
      [] { return random_generators::random_choice({true, false}); }, false);
}

PERF_TEST_CN(column_writer_bench, i32_type_compressed) {
    co_return co_await bench<int32_t, i32_type, int32_value>(
      [] { return random_generators::get_int<int32_t>(); }, true);
}

PERF_TEST_CN(column_writer_bench, i32_type_uncompressed) {
    co_return co_await bench<int32_t, i32_type, int32_value>(
      [] { return random_generators::get_int<int32_t>(); }, false);
}

PERF_TEST_CN(column_writer_bench, i64_type_compressed) {
    co_return co_await bench<int64_t, i64_type, int64_value>(
      [] { return random_generators::get_int<int64_t>(); }, true);
}

PERF_TEST_CN(column_writer_bench, i64_type_uncompressed) {
    co_return co_await bench<int64_t, i64_type, int64_value>(
      [] { return random_generators::get_int<int64_t>(); }, false);
}

PERF_TEST_CN(column_writer_bench, f32_type_compressed) {
    co_return co_await bench<float, f32_type, float32_value>(
      [] { return random_generators::get_real<float>(); }, true);
}

PERF_TEST_CN(column_writer_bench, f32_type_uncompressed) {
    co_return co_await bench<float, f32_type, float32_value>(
      [] { return random_generators::get_real<float>(); }, false);
}

PERF_TEST_CN(column_writer_bench, f64_type_compressed) {
    co_return co_await bench<double, f64_type, float64_value>(
      [] { return random_generators::get_real<double>(); }, true);
}

PERF_TEST_CN(column_writer_bench, f64_type_uncompressed) {
    co_return co_await bench<double, f64_type, float64_value>(
      [] { return random_generators::get_real<double>(); }, false);
}
