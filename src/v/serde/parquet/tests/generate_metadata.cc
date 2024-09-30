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

#include "base/seastarx.h"
#include "bytes/iostream.h"
#include "serde/parquet/metadata.h"
#include "src/v/serde/parquet/metadata.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

#include <limits>

using namespace serde::parquet;

namespace {

template<typename T, typename... Rest>
chunked_vector<T> list(T head, Rest... rest) {
    chunked_vector<T> v;
    v.push_back(std::move(head));
    (v.push_back(std::move(rest)), ...);
    return v;
}

// NOLINTBEGIN(*magic-number*)
iobuf serialize_testcase(size_t test_case) {
    switch (test_case) {
    case 0:
        return encode(page_header{
          .uncompressed_page_size = 42,
          .compressed_page_size = 22,
          .crc = 0xBEEF,
          .type = index_page_header{},
        });
    case 1:
        return encode(page_header{
          .uncompressed_page_size = 2,
          .compressed_page_size = 1,
          .crc = 0xDEAD,
          .type = data_page_header{
            .num_values = 22,
            .num_nulls = 0,
            .num_rows = 1,
            .data_encoding = encoding::plain,
            .definition_levels_byte_length = 21,
            .repetition_levels_byte_length = 1,
            .is_compressed = false,
          },
        });
    case 2:
        return encode(page_header{
          .uncompressed_page_size = 99999,
          .compressed_page_size = 0,
          .crc = 0xEEEE,
          .type = dictionary_page_header{
            .num_values = 44,
            .data_encoding = encoding::rle,
            .is_sorted = true,
          },
        });
    case 3:
        return encode(file_metadata{
          .version = 2,
          .schema = flatten(schema_element{
            .name = "root",
            .children = list(
              schema_element{
                .type = i32_type{},
                .repetition_type = field_repetition_type::optional,
                .name = "foo",
                .field_id = 1,
                .logical_type = time_type{
                  .is_adjusted_to_utc = true,
                  .unit = time_unit::millis,
                },
              },
              schema_element{
                .type = byte_array_type{.fixed_length = 16},
                .repetition_type = field_repetition_type::required,
                .name = "bar",
                .field_id = 2,
                .logical_type = uuid_type{},
              },
              schema_element{
                .type = bool_type{},
                .repetition_type = field_repetition_type::repeated,
                .name = "baz",
                .field_id = 3,
              }
            ),
          }),
          .num_rows = std::numeric_limits<int64_t>::max(),
          .row_groups = list(
             row_group{
                .columns = {
                  column_chunk{
                    .meta_data = {
                      .type = bool_type{},
                      .encodings = {encoding::plain, encoding::rle},
                      .path_in_schema = {"foo", "baz"},
                      .codec = compression_codec::UNCOMPRESSED,
                      .num_values = 888,
                      .total_uncompressed_size = 9999,
                      .total_compressed_size = 9,
                      .key_value_metadata = {{"qux", "thid"}},
                      .data_page_offset = 2,
                      .index_page_offset = 5,
                      .dictionary_page_offset = 9,
                    },
                    .offset_index_offset = 23,
                    .offset_index_length = 43,
                    .column_index_offset = 999,
                    .column_index_length = 876,
                  },
                },
                .total_byte_size = 321,
                .num_rows = 1,
                .file_offset = 1234,
             },
             row_group{
                .total_byte_size = 99999,
                .num_rows = 38,
                .sorting_columns = {
                  sorting_column{
                    .column_idx = 1,
                    .descending = false,
                    .nulls_first = true,
                  },
                  sorting_column{
                    .column_idx = 2,
                    .descending = true,
                    .nulls_first = false,
                  },
                },
                .file_offset = 4123,
             }
          ),
          .key_value_metadata = {{"foo", "bar"}, {"baz", "qux"}, {"nice", ""}},
          .created_by = "The best Message Broker in the West",
        });
    default:
        throw std::runtime_error(
          fmt::format("unsupported test case: {}", test_case));
    }
}
// NOLINTEND(*magic-number*)

ss::future<> run_main(std::string output_file, size_t test_case) {
    auto handle = co_await ss::open_file_dma(
      output_file, ss::open_flags::rw | ss::open_flags::create);
    auto stream = co_await ss::make_file_output_stream(handle);
    auto buf = serialize_testcase(test_case);
    co_await write_iobuf_to_output_stream(std::move(buf), stream);
    co_await stream.flush();
    co_await stream.close();
}
} // namespace

namespace bpo = boost::program_options;

int main(int argc, char** argv) {
    ss::app_template::seastar_options seastar_config;
    // Use a small footprint to generate this single file.
    seastar_config.smp_opts.smp.set_value(1);
    seastar_config.smp_opts.memory_allocator = ss::memory_allocator::standard;
    seastar_config.log_opts.default_log_level.set_value(ss::log_level::warn);
    ss::app_template app(std::move(seastar_config));
    app.add_options()(
      "output", bpo::value<std::string>(), "e.g. --output /tmp/foo/bar");
    app.add_options()("test-case", bpo::value<size_t>(), "e.g. --test-case 99");
    app.run(argc, argv, [&app]() -> ss::future<> {
        const auto& config = app.configuration();
        return run_main(
          config["output"].as<std::string>(), config["test-case"].as<size_t>());
    });
}
