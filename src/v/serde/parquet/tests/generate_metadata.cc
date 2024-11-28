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
          .crc = crc::crc32(0xBEEF),
          .type = index_page_header{},
        });
    case 1:
        return encode(page_header{
          .uncompressed_page_size = 2,
          .compressed_page_size = 1,
          .crc = crc::crc32(0xDEAD),
          .type = data_page_header{
            .num_values = 22,
            .num_nulls = 0,
            .num_rows = 1,
            .data_encoding = encoding::plain,
            .definition_levels_byte_length = 21,
            .repetition_levels_byte_length = 1,
            .is_compressed = false,
            .stats = statistics{
              .null_count = 42,
              .max = std::make_optional<statistics::bound>(
                iobuf::from("\xDE\xAD\xBE\xEF"),
                false
              ),
              .min = std::make_optional<statistics::bound>(
                iobuf::from("\xDE\xAD\xBE\xE0"),
                true
              ),
            },
          },
        });
    case 2:
        return encode(page_header{
          .uncompressed_page_size = 99999,
          .compressed_page_size = 0,
          .crc = crc::crc32(0xEEEE),
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
            .path = {"root"},
            .children = list(
              schema_element{
                .type = i32_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"foo"},
                .field_id = 1,
                .logical_type = time_type{
                  .is_adjusted_to_utc = true,
                  .unit = time_unit::millis,
                },
              },
              schema_element{
                .type = byte_array_type{.fixed_length = 16},
                .repetition_type = field_repetition_type::required,
                .path = {"bar"},
                .field_id = 2,
                .logical_type = uuid_type{},
              },
              schema_element{
                .type = bool_type{},
                .repetition_type = field_repetition_type::repeated,
                .path = {"baz"},
                .field_id = 3,
              }
            ),
          }),
          .num_rows = std::numeric_limits<int64_t>::max(),
          .row_groups = list(
             row_group{
                .columns = list(
                  column_chunk{
                    .meta_data = {
                      .type = bool_type{},
                      .encodings = {encoding::plain, encoding::rle},
                      .path_in_schema = {"foo", "baz"},
                      .codec = compression_codec::uncompressed,
                      .num_values = 888,
                      .total_uncompressed_size = 9999,
                      .total_compressed_size = 9,
                      .key_value_metadata = {{"qux", "thid"}},
                      .data_page_offset = 2,
                      .index_page_offset = 5,
                      .dictionary_page_offset = 9,
                      .stats = statistics{
                        .null_count = 9,
                        .max = std::make_optional<statistics::bound>(
                           iobuf::from("\xFF"),
                           true
                        ),
                        .min = std::make_optional<statistics::bound>(
                          iobuf::from(std::string_view{"\x00", 1}),
                          false
                        ),
                      },
                    },
                  }
                ),
                .total_byte_size = 321,
                .num_rows = 1,
                .file_offset = 1234,
                .total_compressed_size = 231,
                .ordinal = 1,
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
                .total_compressed_size = 8888,
                .ordinal = 2,
             }
          ),
          .key_value_metadata = {{"foo", "bar"}, {"baz", "qux"}, {"nice", ""}},
          .created_by = "The best Message Broker in the West",
          .column_orders = {column_order::type_defined, column_order::type_defined, column_order::type_defined},
        });
    case 4:
        // logical type serialization test for all iceberg types
        return encode(file_metadata{
          .version = 2,
          .schema = flatten(schema_element{
            .path = {"root"},
            .children = list(
              schema_element{
                .type = bool_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_bool"},
              },
              schema_element{
                .type = i32_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_i32"},
                .logical_type = int_type{.bit_width = 32, .is_signed = true},
              },
              schema_element{
                .type = i64_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_i64"},
                .logical_type = int_type{.bit_width = 64, .is_signed = true},
              },
              schema_element{
                .type = f32_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_f32"},
              },
              schema_element{
                .type = f64_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_f64"},
              },
              schema_element{
                .type = byte_array_type{.fixed_length = 16},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_decimal"},
                .logical_type = decimal_type{.scale = 8, .precision = 38},
              },
              schema_element{
                .type = i32_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_date"},
                .logical_type = date_type{},
              },
              schema_element{
                .type = i64_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_time"},
                .logical_type
                = time_type{.is_adjusted_to_utc = false, .unit = time_unit::micros},
              },
              schema_element{
                .type = i64_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_timestamp"},
                .logical_type
                = timestamp_type{.is_adjusted_to_utc = false, .unit = time_unit::micros},
              },
              schema_element{
                .type = i64_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_timestamptz"},
                .logical_type
                = timestamp_type{.is_adjusted_to_utc = true, .unit = time_unit::micros},
              },
              schema_element{
                .type = byte_array_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_string"},
                .logical_type = string_type{},
              },
              schema_element{
                .type = byte_array_type{.fixed_length = 16},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_uuid"},
                .logical_type = uuid_type{},
              },
              schema_element{
                .type = byte_array_type{.fixed_length = 103},
                .repetition_type = field_repetition_type::required,
                .path = {"iceberg_fixed"},
              },
              schema_element{
                .type = byte_array_type{},
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_binary"},
              },
              schema_element{
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_string_int_map"},
                .children = list(schema_element{
                  .repetition_type = field_repetition_type::repeated,
                  .path = {"map"},
                  .children = list(
                    schema_element{
                      .type = byte_array_type{},
                      .repetition_type = field_repetition_type::required,
                      .path = {"key"},
                      .logical_type = string_type{},
                    },
                    schema_element{
                      .type = i32_type{},
                      .repetition_type = field_repetition_type::optional,
                      .path = {"value"},
                    }),
                }),
                .logical_type = map_type{},
              },
              schema_element{
                .repetition_type = field_repetition_type::optional,
                .path = {"iceberg_int_list"},
                .children = list(schema_element{
                  .repetition_type = field_repetition_type::repeated,
                  .path = {"list"},
                  .children = list(schema_element{
                    .type = i32_type{},
                    .repetition_type = field_repetition_type::optional,
                    .path = {"element"},
                  }),
                }),
                .logical_type = list_type{},
              }),
          }),
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
