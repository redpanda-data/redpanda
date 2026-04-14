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

#include "metadata.h"

#include "serde/thrift/compact.h"
#include "utils/vint.h"

#include <seastar/util/variant_utils.hh>

#include <climits>
#include <utility>
#include <variant>

namespace serde::parquet {

namespace {

iobuf encode_kv(std::string_view key, std::optional<std::string_view> val) {
    constexpr auto key_field_id = thrift::field_id(1);
    constexpr auto value_field_id = thrift::field_id(2);
    thrift::struct_encoder kv_encoder;
    kv_encoder.write_field(
      key_field_id, thrift::field_type::binary, thrift::encode_string(key));
    if (val) {
        kv_encoder.write_field(
          value_field_id,
          thrift::field_type::binary,
          thrift::encode_string(*val));
    }
    return std::move(kv_encoder).write_stop();
}

iobuf encode(time_unit t) {
    thrift::struct_encoder encoder;
    encoder.write_field(
      thrift::field_id(static_cast<int16_t>(t)),
      thrift::field_type::structure,
      thrift::struct_encoder::empty_struct);
    return std::move(encoder).write_stop();
}

iobuf encode(const statistics& stats) {
    constexpr auto null_count_field_id = thrift::field_id(3);
    constexpr auto max_value_field_id = thrift::field_id(5);
    constexpr auto min_value_field_id = thrift::field_id(6);
    constexpr auto is_max_value_exact_field_id = thrift::field_id(7);
    constexpr auto is_min_value_exact_field_id = thrift::field_id(8);
    thrift::struct_encoder encoder;
    if (stats.null_count) {
        encoder.write_field(
          null_count_field_id,
          thrift::field_type::i64,
          vint::to_bytes(*stats.null_count));
    }
    if (stats.max) {
        encoder.write_field(
          max_value_field_id,
          thrift::field_type::binary,
          thrift::encode_binary(stats.max->value.copy()));
    }
    if (stats.min) {
        encoder.write_field(
          min_value_field_id,
          thrift::field_type::binary,
          thrift::encode_binary(stats.min->value.copy()));
    }
    if (stats.max) {
        encoder.write_field(
          is_max_value_exact_field_id,
          stats.max->is_exact ? thrift::field_type::boolean_true
                              : thrift::field_type::boolean_false,
          bytes());
    }
    if (stats.min) {
        encoder.write_field(
          is_min_value_exact_field_id,
          stats.min->is_exact ? thrift::field_type::boolean_true
                              : thrift::field_type::boolean_false,
          bytes());
    }
    return std::move(encoder).write_stop();
}

/**
 * DEPRECATED: Common types used by frameworks(e.g. hive, pig) using
 * parquet. ConvertedType is superseded by LogicalType.  This enum should
 * not be extended.
 *
 * See LogicalTypes.md for conversion between ConvertedType and LogicalType.
 */
enum converted_type : uint8_t {
    utf8 = 0,
    map = 1,
    map_key_value = 2,
    list = 3,
    enumeration = 4,
    decimal = 5,
    date = 6,
    time_millis = 7,
    time_micros = 8,
    timestamp_millis = 9,
    timestamp_micros = 10,
    uint_8 = 11,
    uint_16 = 12,
    uint_32 = 13,
    uint_64 = 14,
    int_8 = 15,
    int_16 = 16,
    int_32 = 17,
    int_64 = 18,
    json = 19,
    bson = 20,
    interval = 21,
};

iobuf encode(const flattened_schema& schema, bool is_root) {
    constexpr auto type_field_id = thrift::field_id(1);
    constexpr auto type_length_field_id = thrift::field_id(2);
    constexpr auto repetition_type_field_id = thrift::field_id(3);
    constexpr auto name_field_id = thrift::field_id(4);
    constexpr auto num_children_field_id = thrift::field_id(5);
    constexpr auto converted_type_field_id = thrift::field_id(6);
    constexpr auto scale_field_id = thrift::field_id(7);
    constexpr auto precision_field_id = thrift::field_id(8);
    constexpr auto field_id_field_id = thrift::field_id(9); // whoa, meta!
    constexpr auto logical_type_field_id = thrift::field_id(10);

    enum physical_type : int8_t {
        boolean = 0,
        int32 = 1,
        int64 = 2,
        float32 = 4,
        float64 = 5,
        byte_array = 6,
        fixed_len_byte_array = 7,
    };
    thrift::struct_encoder encoder;
    ss::visit(
      schema.type,
      [](const std::monostate&) {},
      [&](const bool_type&) {
          encoder.write_field(
            type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(physical_type::boolean));
      },
      [&](const i32_type&) {
          encoder.write_field(
            type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(physical_type::int32));
      },
      [&](const i64_type&) {
          encoder.write_field(
            type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(physical_type::int64));
      },
      [&](const f32_type&) {
          encoder.write_field(
            type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(physical_type::float32));
      },
      [&](const f64_type&) {
          encoder.write_field(
            type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(physical_type::float64));
      },
      [&](const byte_array_type& t) {
          if (t.fixed_length.has_value()) {
              encoder.write_field(
                type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(physical_type::fixed_len_byte_array));
              encoder.write_field(
                type_length_field_id,
                thrift::field_type::i32,
                vint::to_bytes(t.fixed_length.value()));
          } else {
              encoder.write_field(
                type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(physical_type::byte_array));
          }
      });
    if (!is_root) {
        encoder.write_field(
          repetition_type_field_id,
          thrift::field_type::i32,
          vint::to_bytes(static_cast<int32_t>(schema.repetition_type)));
    }
    encoder.write_field(
      name_field_id,
      thrift::field_type::binary,
      thrift::encode_string(schema.name));
    encoder.write_field(
      num_children_field_id,
      thrift::field_type::i32,
      vint::to_bytes(schema.num_children));

    enum logical_type : int16_t {
        string = 1,
        map = 2,
        list = 3,
        enumeration = 4,
        decimal = 5,
        date = 6,
        time = 7,
        timestamp = 8,
        // nine is reserved for interval
        integer = 10,
        null = 11,
        json = 12,
        bson = 13,
        uuid = 14,
        float16 = 15,
    };

    thrift::struct_encoder logical_type_encoder;
    ss::visit(
      schema.logical_type,
      [](const std::monostate&) {},
      [&](const string_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::utf8));
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::string),
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const uuid_type&) {
          // No converted type
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::uuid),
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const map_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::map));
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::map),
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const list_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::list));
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::list),
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const enum_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::enumeration));
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::enumeration),
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const date_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::date));
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::date),
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const f16_type&) {
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::float16),
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const null_type&) {
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::null),
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const decimal_type& t) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::decimal));
          encoder.write_field(
            scale_field_id, thrift::field_type::i32, vint::to_bytes(t.scale));
          encoder.write_field(
            precision_field_id,
            thrift::field_type::i32,
            vint::to_bytes(t.precision));

          constexpr auto logical_scale_field_id = thrift::field_id(1);
          constexpr auto logical_precision_field_id = thrift::field_id(2);
          thrift::struct_encoder decimal;
          decimal.write_field(
            logical_scale_field_id,
            thrift::field_type::i32,
            vint::to_bytes(t.scale));
          decimal.write_field(
            logical_precision_field_id,
            thrift::field_type::i32,
            vint::to_bytes(t.precision));
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::decimal),
            thrift::field_type::structure,
            std::move(decimal).write_stop());
      },
      [&](const timestamp_type& t) {
          if (t.unit == time_unit::millis) {
              encoder.write_field(
                converted_type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(converted_type::timestamp_millis));
          } else if (t.unit == time_unit::micros) {
              encoder.write_field(
                converted_type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(converted_type::timestamp_micros));
          }
          constexpr auto utc_field_id = thrift::field_id(1);
          constexpr auto unit_field_id = thrift::field_id(2);
          thrift::struct_encoder time_struct;
          time_struct.write_field(
            utc_field_id,
            t.is_adjusted_to_utc ? thrift::field_type::boolean_true
                                 : thrift::field_type::boolean_false,
            bytes());
          time_struct.write_field(
            unit_field_id, thrift::field_type::structure, encode(t.unit));
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::timestamp),
            thrift::field_type::structure,
            std::move(time_struct).write_stop());
      },
      [&](const time_type& t) {
          if (t.unit == time_unit::millis) {
              encoder.write_field(
                converted_type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(converted_type::time_millis));
          } else if (t.unit == time_unit::micros) {
              encoder.write_field(
                converted_type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(converted_type::time_micros));
          }
          constexpr auto utc_field_id = thrift::field_id(1);
          constexpr auto unit_field_id = thrift::field_id(2);
          thrift::struct_encoder time_struct;
          time_struct.write_field(
            utc_field_id,
            t.is_adjusted_to_utc ? thrift::field_type::boolean_true
                                 : thrift::field_type::boolean_false,
            bytes());
          time_struct.write_field(
            unit_field_id, thrift::field_type::structure, encode(t.unit));
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::time),
            thrift::field_type::structure,
            std::move(time_struct).write_stop());
      },
      [&](const int_type& t) {
          switch (t.bit_width) {
          case sizeof(int8_t) * CHAR_WIDTH:
              encoder.write_field(
                converted_type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(
                  t.is_signed ? converted_type::int_8
                              : converted_type::uint_8));
              break;
          case sizeof(int16_t) * CHAR_WIDTH:
              encoder.write_field(
                converted_type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(
                  t.is_signed ? converted_type::int_16
                              : converted_type::uint_16));
              break;
          case sizeof(int32_t) * CHAR_WIDTH:
              encoder.write_field(
                converted_type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(
                  t.is_signed ? converted_type::int_32
                              : converted_type::uint_32));
              break;
          case sizeof(int64_t) * CHAR_WIDTH:
              encoder.write_field(
                converted_type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(
                  t.is_signed ? converted_type::int_64
                              : converted_type::uint_64));
              break;
          default:
              break;
          }
          constexpr auto bit_width_field_id = thrift::field_id(1);
          constexpr auto is_signed_field_id = thrift::field_id(2);
          thrift::struct_encoder int_struct;
          iobuf byte;
          auto bit_width = static_cast<uint8_t>(t.bit_width);
          byte.append(&bit_width, 1);
          int_struct.write_field(
            bit_width_field_id, thrift::field_type::i8, std::move(byte));
          int_struct.write_field(
            is_signed_field_id,
            t.is_signed ? thrift::field_type::boolean_true
                        : thrift::field_type::boolean_false,
            bytes());
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::integer),
            thrift::field_type::structure,
            std::move(int_struct).write_stop());
      },
      [&](const json_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::json));
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::json),
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const bson_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::bson));
          logical_type_encoder.write_field(
            thrift::field_id(logical_type::bson),
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      });
    if (schema.field_id.has_value()) {
        encoder.write_field(
          field_id_field_id,
          thrift::field_type::i32,
          vint::to_bytes(schema.field_id.value()));
    }
    if (!std::holds_alternative<std::monostate>(schema.logical_type)) {
        encoder.write_field(
          logical_type_field_id,
          thrift::field_type::structure,
          std::move(logical_type_encoder).write_stop());
    }
    return std::move(encoder).write_stop();
}

iobuf encode(const column_meta_data& metadata) {
    constexpr auto type_field_id = thrift::field_id(1);
    constexpr auto encodings_field_id = thrift::field_id(2);
    constexpr auto path_in_schema_field_id = thrift::field_id(3);
    constexpr auto codec_field_id = thrift::field_id(4);
    constexpr auto num_values_field_id = thrift::field_id(5);
    constexpr auto total_uncompressed_size_field_id = thrift::field_id(6);
    constexpr auto total_compressed_size_field_id = thrift::field_id(7);
    constexpr auto key_value_metadata_field_id = thrift::field_id(8);
    constexpr auto data_page_offset_field_id = thrift::field_id(9);
    constexpr auto index_page_offset_field_id = thrift::field_id(10);
    constexpr auto dictionary_page_offset_field_id = thrift::field_id(11);
    constexpr auto stats_field_id = thrift::field_id(12);
    thrift::struct_encoder encoder;
    enum physical_type : int8_t {
        boolean = 0,
        int32 = 1,
        int64 = 2,
        float32 = 4,
        float64 = 5,
        byte_array = 6,
        fixed_len_byte_array = 7,
    };
    ss::visit(
      metadata.type,
      [](const std::monostate&) {},
      [&](const bool_type&) {
          encoder.write_field(
            type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(physical_type::boolean));
      },
      [&](const i32_type&) {
          encoder.write_field(
            type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(physical_type::int32));
      },
      [&](const i64_type&) {
          encoder.write_field(
            type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(physical_type::int64));
      },
      [&](const f32_type&) {
          encoder.write_field(
            type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(physical_type::float32));
      },
      [&](const f64_type&) {
          encoder.write_field(
            type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(physical_type::float64));
      },
      [&](const byte_array_type& t) {
          if (t.fixed_length.has_value()) {
              encoder.write_field(
                type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(physical_type::fixed_len_byte_array));
          } else {
              encoder.write_field(
                type_field_id,
                thrift::field_type::i32,
                vint::to_bytes(physical_type::byte_array));
          }
      });

    thrift::list_encoder encodings_encoder(
      metadata.encodings.size(), thrift::field_type::i32);
    for (const auto& encoding : metadata.encodings) {
        encodings_encoder.write_element(
          vint::to_bytes(static_cast<int32_t>(encoding)));
    }
    encoder.write_field(
      encodings_field_id,
      thrift::field_type::list,
      std::move(encodings_encoder).finish());

    thrift::list_encoder path_in_schema_encoder(
      metadata.path_in_schema.size(), thrift::field_type::binary);
    for (const auto& segment : metadata.path_in_schema) {
        path_in_schema_encoder.write_element(thrift::encode_string(segment));
    }
    encoder.write_field(
      path_in_schema_field_id,
      thrift::field_type::list,
      std::move(path_in_schema_encoder).finish());
    encoder.write_field(
      codec_field_id,
      thrift::field_type::i32,
      vint::to_bytes(static_cast<int32_t>(metadata.codec)));
    encoder.write_field(
      num_values_field_id,
      thrift::field_type::i64,
      vint::to_bytes(metadata.num_values));
    encoder.write_field(
      total_uncompressed_size_field_id,
      thrift::field_type::i64,
      vint::to_bytes(metadata.total_uncompressed_size));
    encoder.write_field(
      total_compressed_size_field_id,
      thrift::field_type::i64,
      vint::to_bytes(metadata.total_compressed_size));
    thrift::list_encoder kv_metadata_encoder(
      metadata.key_value_metadata.size(), thrift::field_type::structure);
    for (const auto& kv_pair : metadata.key_value_metadata) {
        kv_metadata_encoder.write_element(
          encode_kv(kv_pair.first, kv_pair.second));
    }
    encoder.write_field(
      key_value_metadata_field_id,
      thrift::field_type::list,
      std::move(kv_metadata_encoder).finish());
    encoder.write_field(
      data_page_offset_field_id,
      thrift::field_type::i64,
      vint::to_bytes(metadata.data_page_offset));
    if (metadata.index_page_offset) {
        encoder.write_field(
          index_page_offset_field_id,
          thrift::field_type::i64,
          vint::to_bytes(*metadata.index_page_offset));
    }
    if (metadata.dictionary_page_offset) {
        encoder.write_field(
          dictionary_page_offset_field_id,
          thrift::field_type::i64,
          vint::to_bytes(*metadata.dictionary_page_offset));
    }
    if (metadata.stats) {
        encoder.write_field(
          stats_field_id,
          thrift::field_type::structure,
          encode(*metadata.stats));
    }
    return std::move(encoder).write_stop();
}

iobuf encode(const column_chunk& chunk) {
    constexpr auto file_path_field_id = thrift::field_id(1);
    constexpr auto file_offset_field_id = thrift::field_id(2);
    constexpr auto meta_data_field_id = thrift::field_id(3);
    thrift::struct_encoder encoder;
    if (chunk.file_path) {
        encoder.write_field(
          file_path_field_id,
          thrift::field_type::binary,
          thrift::encode_string(*chunk.file_path));
    }
    // Deprecated, but required. Always write 0
    encoder.write_field(
      file_offset_field_id, thrift::field_type::i64, vint::to_bytes(0));
    encoder.write_field(
      meta_data_field_id,
      thrift::field_type::structure,
      encode(chunk.meta_data));

    return std::move(encoder).write_stop();
}

iobuf encode(const sorting_column& column) {
    constexpr auto column_idx_field_id = thrift::field_id(1);
    constexpr auto descending_field_id = thrift::field_id(2);
    constexpr auto nulls_first_field_id = thrift::field_id(3);
    thrift::struct_encoder encoder;
    encoder.write_field(
      column_idx_field_id,
      thrift::field_type::i32,
      vint::to_bytes(column.column_idx));
    encoder.write_field(
      descending_field_id,
      column.descending ? thrift::field_type::boolean_true
                        : thrift::field_type::boolean_false,
      bytes());
    encoder.write_field(
      nulls_first_field_id,
      column.nulls_first ? thrift::field_type::boolean_true
                         : thrift::field_type::boolean_false,
      bytes());
    return std::move(encoder).write_stop();
}

iobuf encode(const row_group& group) {
    constexpr auto columns_field_id = thrift::field_id(1);
    constexpr auto total_field_id = thrift::field_id(2);
    constexpr auto num_rows_field_id = thrift::field_id(3);
    constexpr auto sorting_columns_field_id = thrift::field_id(4);
    constexpr auto file_offset_field_id = thrift::field_id(5);
    constexpr auto total_compressed_size_field_id = thrift::field_id(6);
    constexpr auto ordinal_field_id = thrift::field_id(7);

    thrift::struct_encoder encoder;

    thrift::list_encoder columns_encoder(
      group.columns.size(), thrift::field_type::structure);
    for (const auto& column : group.columns) {
        columns_encoder.write_element(encode(column));
    }
    encoder.write_field(
      columns_field_id,
      thrift::field_type::list,
      std::move(columns_encoder).finish());
    encoder.write_field(
      total_field_id,
      thrift::field_type::i64,
      vint::to_bytes(group.total_byte_size));
    encoder.write_field(
      num_rows_field_id,
      thrift::field_type::i64,
      vint::to_bytes(group.num_rows));
    if (!group.sorting_columns.empty()) {
        thrift::list_encoder sorting_columns_encoder(
          group.sorting_columns.size(), thrift::field_type::structure);
        for (const auto& sorting_column : group.sorting_columns) {
            sorting_columns_encoder.write_element(encode(sorting_column));
        }
        encoder.write_field(
          sorting_columns_field_id,
          thrift::field_type::list,
          std::move(sorting_columns_encoder).finish());
    }
    encoder.write_field(
      file_offset_field_id,
      thrift::field_type::i64,
      vint::to_bytes(group.file_offset));
    encoder.write_field(
      total_compressed_size_field_id,
      thrift::field_type::i64,
      vint::to_bytes(group.total_compressed_size));
    encoder.write_field(
      ordinal_field_id, thrift::field_type::i16, vint::to_bytes(group.ordinal));
    return std::move(encoder).write_stop();
}

iobuf encode(column_order col_order) {
    thrift::struct_encoder encoder;
    encoder.write_field(
      thrift::field_id(static_cast<int16_t>(col_order)),
      thrift::field_type::structure,
      thrift::struct_encoder::empty_struct);
    return std::move(encoder).write_stop();
}

} // namespace

iobuf encode(const file_metadata& metadata) {
    constexpr auto version_field_id = thrift::field_id(1);
    constexpr auto schema_field_id = thrift::field_id(2);
    constexpr auto num_rows_field_id = thrift::field_id(3);
    constexpr auto row_groups_field_id = thrift::field_id(4);
    constexpr auto key_value_metadata_field_id = thrift::field_id(5);
    constexpr auto created_by_field_id = thrift::field_id(6);
    constexpr auto column_orders_field_id = thrift::field_id(7);

    thrift::struct_encoder encoder;
    encoder.write_field(
      version_field_id,
      thrift::field_type::i32,
      vint::to_bytes(metadata.version));
    thrift::list_encoder schema_encoder(
      metadata.schema.size(), thrift::field_type::structure);
    bool is_root_schema = true;
    for (const auto& schema_element : metadata.schema) {
        schema_encoder.write_element(encode(schema_element, is_root_schema));
        is_root_schema = false;
    }
    encoder.write_field(
      schema_field_id,
      thrift::field_type::list,
      std::move(schema_encoder).finish());
    encoder.write_field(
      num_rows_field_id,
      thrift::field_type::i64,
      vint::to_bytes(metadata.num_rows));

    thrift::list_encoder row_groups_encoder(
      metadata.row_groups.size(), thrift::field_type::structure);
    for (const auto& row_group : metadata.row_groups) {
        row_groups_encoder.write_element(encode(row_group));
    }
    encoder.write_field(
      row_groups_field_id,
      thrift::field_type::list,
      std::move(row_groups_encoder).finish());

    thrift::list_encoder kv_metadata_encoder(
      metadata.key_value_metadata.size(), thrift::field_type::structure);
    for (const auto& kv_pair : metadata.key_value_metadata) {
        kv_metadata_encoder.write_element(
          encode_kv(kv_pair.first, kv_pair.second));
    }
    encoder.write_field(
      key_value_metadata_field_id,
      thrift::field_type::list,
      std::move(kv_metadata_encoder).finish());
    encoder.write_field(
      created_by_field_id,
      thrift::field_type::binary,
      thrift::encode_string(metadata.created_by));

    if (!metadata.column_orders.empty()) {
        thrift::list_encoder column_orders_encoder(
          metadata.column_orders.size(), thrift::field_type::structure);
        for (const auto& order : metadata.column_orders) {
            column_orders_encoder.write_element(encode(order));
        }
        encoder.write_field(
          column_orders_field_id,
          thrift::field_type::list,
          std::move(column_orders_encoder).finish());
    }

    return std::move(encoder).write_stop();
}

namespace {

iobuf encode(const data_page_header& header) {
    constexpr auto num_values_field_id = thrift::field_id(1);
    constexpr auto num_nulls_field_id = thrift::field_id(2);
    constexpr auto num_rows_field_id = thrift::field_id(3);
    constexpr auto encoding_field_id = thrift::field_id(4);
    constexpr auto definition_levels_byte_length_field_id = thrift::field_id(5);
    constexpr auto repetition_levels_byte_length_field_id = thrift::field_id(6);
    constexpr auto is_compressed_field_id = thrift::field_id(7);
    constexpr auto stats_field_id = thrift::field_id(8);
    thrift::struct_encoder encoder;
    encoder.write_field(
      num_values_field_id,
      thrift::field_type::i32,
      vint::to_bytes(header.num_values));
    encoder.write_field(
      num_nulls_field_id,
      thrift::field_type::i32,
      vint::to_bytes(header.num_nulls));
    encoder.write_field(
      num_rows_field_id,
      thrift::field_type::i32,
      vint::to_bytes(header.num_rows));
    encoder.write_field(
      encoding_field_id,
      thrift::field_type::i32,
      vint::to_bytes(static_cast<int32_t>(header.data_encoding)));
    encoder.write_field(
      definition_levels_byte_length_field_id,
      thrift::field_type::i32,
      vint::to_bytes(header.definition_levels_byte_length));
    encoder.write_field(
      repetition_levels_byte_length_field_id,
      thrift::field_type::i32,
      vint::to_bytes(header.repetition_levels_byte_length));
    encoder.write_field(
      is_compressed_field_id,
      header.is_compressed ? thrift::field_type::boolean_true
                           : thrift::field_type::boolean_false,
      bytes());
    if (header.stats) {
        encoder.write_field(
          stats_field_id, thrift::field_type::structure, encode(*header.stats));
    }
    return std::move(encoder).write_stop();
}

iobuf encode(const dictionary_page_header& header) {
    constexpr auto num_values_field_id = thrift::field_id(1);
    constexpr auto encoding_field_id = thrift::field_id(2);
    constexpr auto is_sorted_field_id = thrift::field_id(3);
    thrift::struct_encoder encoder;
    encoder.write_field(
      num_values_field_id,
      thrift::field_type::i32,
      vint::to_bytes(header.num_values));
    encoder.write_field(
      encoding_field_id,
      thrift::field_type::i32,
      vint::to_bytes(static_cast<int32_t>(header.data_encoding)));
    encoder.write_field(
      is_sorted_field_id,
      header.is_sorted ? thrift::field_type::boolean_true
                       : thrift::field_type::boolean_false,
      bytes());
    return std::move(encoder).write_stop();
}

iobuf encode(const data_page_header_v1& header) {
    constexpr auto num_values_field_id = thrift::field_id(1);
    constexpr auto encoding_field_id = thrift::field_id(2);
    constexpr auto definition_level_encoding_field_id = thrift::field_id(3);
    constexpr auto repetition_level_encoding_field_id = thrift::field_id(4);
    constexpr auto stats_field_id = thrift::field_id(5);
    thrift::struct_encoder encoder;
    encoder.write_field(
      num_values_field_id,
      thrift::field_type::i32,
      vint::to_bytes(header.num_values));
    encoder.write_field(
      encoding_field_id,
      thrift::field_type::i32,
      vint::to_bytes(static_cast<int32_t>(header.data_encoding)));
    encoder.write_field(
      definition_level_encoding_field_id,
      thrift::field_type::i32,
      vint::to_bytes(static_cast<int32_t>(header.definition_level_encoding)));
    encoder.write_field(
      repetition_level_encoding_field_id,
      thrift::field_type::i32,
      vint::to_bytes(static_cast<int32_t>(header.repetition_level_encoding)));
    if (header.stats) {
        encoder.write_field(
          stats_field_id, thrift::field_type::structure, encode(*header.stats));
    }
    return std::move(encoder).write_stop();
}

} // namespace

iobuf encode(const page_header& header) {
    constexpr auto type_field_id = thrift::field_id(1);
    constexpr auto uncompressed_page_size_field_id = thrift::field_id(2);
    constexpr auto compressed_page_size_field_id = thrift::field_id(3);
    constexpr auto crc_field_id = thrift::field_id(4);
    constexpr auto data_page_header_v1_field_id = thrift::field_id(5);
    constexpr auto index_page_header_field_id = thrift::field_id(6);
    constexpr auto dictionary_page_header_field_id = thrift::field_id(7);
    constexpr auto data_page_header_v2_field_id = thrift::field_id(8);
    thrift::struct_encoder encoder;
    enum page_type : int32_t {
        data_page = 0,
        index_page = 1,
        dictionary_page = 2,
        data_page_v2 = 3,
    };
    page_type enum_value = ss::visit(
      header.type,
      [](const index_page_header&) { return page_type::index_page; },
      [](const data_page_header&) { return page_type::data_page_v2; },
      [](const dictionary_page_header&) { return page_type::dictionary_page; },
      [](const data_page_header_v1&) { return page_type::data_page; });
    encoder.write_field(
      type_field_id,
      thrift::field_type::i32,
      vint::to_bytes(static_cast<int32_t>(enum_value)));
    encoder.write_field(
      uncompressed_page_size_field_id,
      thrift::field_type::i32,
      vint::to_bytes(header.uncompressed_page_size));
    encoder.write_field(
      compressed_page_size_field_id,
      thrift::field_type::i32,
      vint::to_bytes(header.compressed_page_size));
    encoder.write_field(
      crc_field_id,
      thrift::field_type::i32,
      // sign extend the crc so that it's zig-zag encoded correctly.
      vint::to_bytes(static_cast<int32_t>(header.crc.value())));
    ss::visit(
      header.type,
      [&](const index_page_header&) {
          encoder.write_field(
            index_page_header_field_id,
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const data_page_header& h) {
          encoder.write_field(
            data_page_header_v2_field_id,
            thrift::field_type::structure,
            encode(h));
      },
      [&](const dictionary_page_header& h) {
          encoder.write_field(
            dictionary_page_header_field_id,
            thrift::field_type::structure,
            encode(h));
      },
      [&](const data_page_header_v1& h) {
          encoder.write_field(
            data_page_header_v1_field_id,
            thrift::field_type::structure,
            encode(h));
      });
    return std::move(encoder).write_stop();
}

namespace {

physical_type decode_physical_type(int32_t type_val) {
    enum physical_type_enum : int8_t {
        boolean = 0,
        int32 = 1,
        int64 = 2,
        float32 = 4,
        float64 = 5,
        byte_array = 6,
        fixed_len_byte_array = 7,
    };
    switch (type_val) {
    case physical_type_enum::boolean:
        return bool_type();
    case physical_type_enum::int32:
        return i32_type();
    case physical_type_enum::int64:
        return i64_type();
    case physical_type_enum::float32:
        return f32_type();
    case physical_type_enum::float64:
        return f64_type();
    case physical_type_enum::byte_array:
        return byte_array_type();
    case physical_type_enum::fixed_len_byte_array:
        return byte_array_type();
    default:
        return std::monostate();
    }
}

time_unit decode_time_unit(iobuf_parser_base& parser) {
    thrift::struct_decoder dec(parser);
    time_unit result = time_unit::millis;
    while (auto hdr = dec.read_field_header()) {
        switch (hdr->id()) {
        case 1:
            result = time_unit::millis;
            // skip empty struct
            {
                thrift::struct_decoder inner(parser);
                while (auto ihdr = inner.read_field_header()) {
                    inner.skip_field(ihdr->type);
                }
            }
            break;
        case 2:
            result = time_unit::micros;
            {
                thrift::struct_decoder inner(parser);
                while (auto ihdr = inner.read_field_header()) {
                    inner.skip_field(ihdr->type);
                }
            }
            break;
        case 3:
            result = time_unit::nanos;
            {
                thrift::struct_decoder inner(parser);
                while (auto ihdr = inner.read_field_header()) {
                    inner.skip_field(ihdr->type);
                }
            }
            break;
        default:
            dec.skip_field(hdr->type);
            break;
        }
    }
    return result;
}

logical_type decode_logical_type(iobuf_parser_base& parser) {
    enum logical_type_id : int16_t {
        string = 1,
        map = 2,
        list = 3,
        enumeration = 4,
        decimal = 5,
        date = 6,
        time = 7,
        timestamp = 8,
        integer = 10,
        null = 11,
        json = 12,
        bson = 13,
        uuid = 14,
        float16 = 15,
    };

    logical_type result;
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        switch (hdr->id()) {
        case logical_type_id::string:
            result = string_type();
            dec.skip_field(hdr->type);
            break;
        case logical_type_id::map:
            result = map_type();
            dec.skip_field(hdr->type);
            break;
        case logical_type_id::list:
            result = list_type();
            dec.skip_field(hdr->type);
            break;
        case logical_type_id::enumeration:
            result = enum_type();
            dec.skip_field(hdr->type);
            break;
        case logical_type_id::decimal: {
            thrift::struct_decoder inner(parser);
            decimal_type dt;
            while (auto ihdr = inner.read_field_header()) {
                switch (ihdr->id()) {
                case 1:
                    dt.scale = thrift::decode_i32(parser);
                    break;
                case 2:
                    dt.precision = thrift::decode_i32(parser);
                    break;
                default:
                    inner.skip_field(ihdr->type);
                    break;
                }
            }
            result = dt;
            break;
        }
        case logical_type_id::date:
            result = date_type();
            dec.skip_field(hdr->type);
            break;
        case logical_type_id::time: {
            thrift::struct_decoder inner(parser);
            time_type tt;
            while (auto ihdr = inner.read_field_header()) {
                switch (ihdr->id()) {
                case 1:
                    tt.is_adjusted_to_utc = ihdr->type
                                            == thrift::field_type::boolean_true;
                    break;
                case 2:
                    tt.unit = decode_time_unit(parser);
                    break;
                default:
                    inner.skip_field(ihdr->type);
                    break;
                }
            }
            result = tt;
            break;
        }
        case logical_type_id::timestamp: {
            thrift::struct_decoder inner(parser);
            timestamp_type ts;
            while (auto ihdr = inner.read_field_header()) {
                switch (ihdr->id()) {
                case 1:
                    ts.is_adjusted_to_utc = ihdr->type
                                            == thrift::field_type::boolean_true;
                    break;
                case 2:
                    ts.unit = decode_time_unit(parser);
                    break;
                default:
                    inner.skip_field(ihdr->type);
                    break;
                }
            }
            result = ts;
            break;
        }
        case logical_type_id::integer: {
            thrift::struct_decoder inner(parser);
            int_type it;
            while (auto ihdr = inner.read_field_header()) {
                switch (ihdr->id()) {
                case 1:
                    it.bit_width = parser.consume_type<int8_t>();
                    break;
                case 2:
                    it.is_signed = ihdr->type
                                   == thrift::field_type::boolean_true;
                    break;
                default:
                    inner.skip_field(ihdr->type);
                    break;
                }
            }
            result = it;
            break;
        }
        case logical_type_id::null:
            result = null_type();
            dec.skip_field(hdr->type);
            break;
        case logical_type_id::json:
            result = json_type();
            dec.skip_field(hdr->type);
            break;
        case logical_type_id::bson:
            result = bson_type();
            dec.skip_field(hdr->type);
            break;
        case logical_type_id::uuid:
            result = uuid_type();
            dec.skip_field(hdr->type);
            break;
        case logical_type_id::float16:
            result = f16_type();
            dec.skip_field(hdr->type);
            break;
        default:
            dec.skip_field(hdr->type);
            break;
        }
    }
    return result;
}

flattened_schema
decode_flattened_schema(iobuf_parser_base& parser, bool /*is_root*/) {
    constexpr auto type_field_id = thrift::field_id(1);
    constexpr auto type_length_field_id = thrift::field_id(2);
    constexpr auto repetition_type_field_id = thrift::field_id(3);
    constexpr auto name_field_id = thrift::field_id(4);
    constexpr auto num_children_field_id = thrift::field_id(5);
    constexpr auto scale_field_id = thrift::field_id(7);
    constexpr auto precision_field_id = thrift::field_id(8);
    constexpr auto field_id_field_id = thrift::field_id(9);
    constexpr auto logical_type_field_id = thrift::field_id(10);

    flattened_schema result;
    result.repetition_type = field_repetition_type::required;
    std::optional<int32_t> type_length;

    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        if (hdr->id == type_field_id) {
            auto ptype = thrift::decode_i32(parser);
            result.type = decode_physical_type(ptype);
        } else if (hdr->id == type_length_field_id) {
            type_length = thrift::decode_i32(parser);
        } else if (hdr->id == repetition_type_field_id) {
            result.repetition_type = static_cast<field_repetition_type>(
              thrift::decode_i32(parser));
        } else if (hdr->id == name_field_id) {
            result.name = thrift::decode_string(parser);
        } else if (hdr->id == num_children_field_id) {
            result.num_children = thrift::decode_i32(parser);
        } else if (hdr->id == scale_field_id) {
            // Legacy scale field (for decimal) — handled by logical type
            thrift::decode_i32(parser);
        } else if (hdr->id == precision_field_id) {
            // Legacy precision field — handled by logical type
            thrift::decode_i32(parser);
        } else if (hdr->id == field_id_field_id) {
            result.field_id = thrift::decode_i32(parser);
        } else if (hdr->id == logical_type_field_id) {
            result.logical_type = decode_logical_type(parser);
        } else {
            dec.skip_field(hdr->type);
        }
    }

    if (type_length.has_value()) {
        if (auto* ba = std::get_if<byte_array_type>(&result.type)) {
            ba->fixed_length = type_length;
        }
    }

    return result;
}

statistics decode_statistics(iobuf_parser_base& parser) {
    constexpr auto null_count_field_id = thrift::field_id(3);
    constexpr auto max_value_field_id = thrift::field_id(5);
    constexpr auto min_value_field_id = thrift::field_id(6);
    constexpr auto is_max_value_exact_field_id = thrift::field_id(7);
    constexpr auto is_min_value_exact_field_id = thrift::field_id(8);

    statistics result;
    result.null_count = std::nullopt;
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        if (hdr->id == null_count_field_id) {
            result.null_count = thrift::decode_i64(parser);
        } else if (hdr->id == max_value_field_id) {
            result.max = statistics::bound{
              .value = thrift::decode_binary(parser)};
        } else if (hdr->id == min_value_field_id) {
            result.min = statistics::bound{
              .value = thrift::decode_binary(parser)};
        } else if (hdr->id == is_max_value_exact_field_id) {
            if (result.max) {
                result.max->is_exact = hdr->type
                                       == thrift::field_type::boolean_true;
            }
        } else if (hdr->id == is_min_value_exact_field_id) {
            if (result.min) {
                result.min->is_exact = hdr->type
                                       == thrift::field_type::boolean_true;
            }
        } else {
            dec.skip_field(hdr->type);
        }
    }
    return result;
}

column_meta_data decode_column_meta_data(iobuf_parser_base& parser) {
    constexpr auto type_field_id = thrift::field_id(1);
    constexpr auto encodings_field_id = thrift::field_id(2);
    constexpr auto path_in_schema_field_id = thrift::field_id(3);
    constexpr auto codec_field_id = thrift::field_id(4);
    constexpr auto num_values_field_id = thrift::field_id(5);
    constexpr auto total_uncompressed_size_field_id = thrift::field_id(6);
    constexpr auto total_compressed_size_field_id = thrift::field_id(7);
    constexpr auto key_value_metadata_field_id = thrift::field_id(8);
    constexpr auto data_page_offset_field_id = thrift::field_id(9);
    constexpr auto index_page_offset_field_id = thrift::field_id(10);
    constexpr auto dictionary_page_offset_field_id = thrift::field_id(11);
    constexpr auto stats_field_id = thrift::field_id(12);

    column_meta_data result;
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        if (hdr->id == type_field_id) {
            result.type = decode_physical_type(thrift::decode_i32(parser));
        } else if (hdr->id == encodings_field_id) {
            thrift::list_decoder list(parser);
            for (size_t i = 0; i < list.size(); ++i) {
                result.encodings.push_back(
                  static_cast<encoding>(thrift::decode_i32(parser)));
            }
        } else if (hdr->id == path_in_schema_field_id) {
            thrift::list_decoder list(parser);
            for (size_t i = 0; i < list.size(); ++i) {
                result.path_in_schema.push_back(thrift::decode_string(parser));
            }
        } else if (hdr->id == codec_field_id) {
            result.codec = static_cast<compression_codec>(
              thrift::decode_i32(parser));
        } else if (hdr->id == num_values_field_id) {
            result.num_values = thrift::decode_i64(parser);
        } else if (hdr->id == total_uncompressed_size_field_id) {
            result.total_uncompressed_size = thrift::decode_i64(parser);
        } else if (hdr->id == total_compressed_size_field_id) {
            result.total_compressed_size = thrift::decode_i64(parser);
        } else if (hdr->id == key_value_metadata_field_id) {
            thrift::list_decoder list(parser);
            for (size_t i = 0; i < list.size(); ++i) {
                thrift::struct_decoder kv_dec(parser);
                ss::sstring key;
                ss::sstring value;
                while (auto kv_hdr = kv_dec.read_field_header()) {
                    if (kv_hdr->id() == 1) {
                        key = thrift::decode_string(parser);
                    } else if (kv_hdr->id() == 2) {
                        value = thrift::decode_string(parser);
                    } else {
                        kv_dec.skip_field(kv_hdr->type);
                    }
                }
                result.key_value_metadata.emplace_back(
                  std::move(key), std::move(value));
            }
        } else if (hdr->id == data_page_offset_field_id) {
            result.data_page_offset = thrift::decode_i64(parser);
        } else if (hdr->id == index_page_offset_field_id) {
            result.index_page_offset = thrift::decode_i64(parser);
        } else if (hdr->id == dictionary_page_offset_field_id) {
            result.dictionary_page_offset = thrift::decode_i64(parser);
        } else if (hdr->id == stats_field_id) {
            result.stats = decode_statistics(parser);
        } else {
            dec.skip_field(hdr->type);
        }
    }
    return result;
}

column_chunk decode_column_chunk(iobuf_parser_base& parser) {
    constexpr auto file_path_field_id = thrift::field_id(1);
    // field 2 is deprecated file_offset, skip it
    constexpr auto meta_data_field_id = thrift::field_id(3);

    column_chunk result;
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        if (hdr->id == file_path_field_id) {
            result.file_path = thrift::decode_string(parser);
        } else if (hdr->id == meta_data_field_id) {
            result.meta_data = decode_column_meta_data(parser);
        } else {
            dec.skip_field(hdr->type);
        }
    }
    return result;
}

sorting_column decode_sorting_column(iobuf_parser_base& parser) {
    sorting_column result;
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        switch (hdr->id()) {
        case 1:
            result.column_idx = thrift::decode_i32(parser);
            break;
        case 2:
            result.descending = hdr->type == thrift::field_type::boolean_true;
            break;
        case 3:
            result.nulls_first = hdr->type == thrift::field_type::boolean_true;
            break;
        default:
            dec.skip_field(hdr->type);
            break;
        }
    }
    return result;
}

row_group decode_row_group(iobuf_parser_base& parser) {
    constexpr auto columns_field_id = thrift::field_id(1);
    constexpr auto total_field_id = thrift::field_id(2);
    constexpr auto num_rows_field_id = thrift::field_id(3);
    constexpr auto sorting_columns_field_id = thrift::field_id(4);
    constexpr auto file_offset_field_id = thrift::field_id(5);
    constexpr auto total_compressed_size_field_id = thrift::field_id(6);
    constexpr auto ordinal_field_id = thrift::field_id(7);

    row_group result{};
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        if (hdr->id == columns_field_id) {
            thrift::list_decoder list(parser);
            for (size_t i = 0; i < list.size(); ++i) {
                result.columns.push_back(decode_column_chunk(parser));
            }
        } else if (hdr->id == total_field_id) {
            result.total_byte_size = thrift::decode_i64(parser);
        } else if (hdr->id == num_rows_field_id) {
            result.num_rows = thrift::decode_i64(parser);
        } else if (hdr->id == sorting_columns_field_id) {
            thrift::list_decoder list(parser);
            for (size_t i = 0; i < list.size(); ++i) {
                result.sorting_columns.push_back(decode_sorting_column(parser));
            }
        } else if (hdr->id == file_offset_field_id) {
            result.file_offset = thrift::decode_i64(parser);
        } else if (hdr->id == total_compressed_size_field_id) {
            result.total_compressed_size = thrift::decode_i64(parser);
        } else if (hdr->id == ordinal_field_id) {
            result.ordinal = thrift::decode_i16(parser);
        } else {
            dec.skip_field(hdr->type);
        }
    }
    return result;
}

column_order decode_column_order(iobuf_parser_base& parser) {
    column_order result = column_order::type_defined;
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        result = static_cast<column_order>(hdr->id());
        dec.skip_field(hdr->type);
    }
    return result;
}

data_page_header_v1 decode_data_page_header_v1(iobuf_parser_base& parser) {
    data_page_header_v1 result{};
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        switch (hdr->id()) {
        case 1:
            result.num_values = thrift::decode_i32(parser);
            break;
        case 2:
            result.data_encoding = static_cast<encoding>(
              thrift::decode_i32(parser));
            break;
        case 3:
            result.definition_level_encoding = static_cast<encoding>(
              thrift::decode_i32(parser));
            break;
        case 4:
            result.repetition_level_encoding = static_cast<encoding>(
              thrift::decode_i32(parser));
            break;
        case 5:
            result.stats = decode_statistics(parser);
            break;
        default:
            dec.skip_field(hdr->type);
            break;
        }
    }
    return result;
}

data_page_header decode_data_page_header(iobuf_parser_base& parser) {
    data_page_header result{};
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        switch (hdr->id()) {
        case 1:
            result.num_values = thrift::decode_i32(parser);
            break;
        case 2:
            result.num_nulls = thrift::decode_i32(parser);
            break;
        case 3:
            result.num_rows = thrift::decode_i32(parser);
            break;
        case 4:
            result.data_encoding = static_cast<encoding>(
              thrift::decode_i32(parser));
            break;
        case 5:
            result.definition_levels_byte_length = thrift::decode_i32(parser);
            break;
        case 6:
            result.repetition_levels_byte_length = thrift::decode_i32(parser);
            break;
        case 7:
            result.is_compressed = hdr->type
                                   == thrift::field_type::boolean_true;
            break;
        case 8:
            result.stats = decode_statistics(parser);
            break;
        default:
            dec.skip_field(hdr->type);
            break;
        }
    }
    return result;
}

dictionary_page_header
decode_dictionary_page_header(iobuf_parser_base& parser) {
    dictionary_page_header result{};
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        switch (hdr->id()) {
        case 1:
            result.num_values = thrift::decode_i32(parser);
            break;
        case 2:
            result.data_encoding = static_cast<encoding>(
              thrift::decode_i32(parser));
            break;
        case 3:
            result.is_sorted = hdr->type == thrift::field_type::boolean_true;
            break;
        default:
            dec.skip_field(hdr->type);
            break;
        }
    }
    return result;
}

} // namespace

page_header decode(iobuf_parser_base& parser, page_header_tag) {
    constexpr auto type_field_id = thrift::field_id(1);
    constexpr auto uncompressed_page_size_field_id = thrift::field_id(2);
    constexpr auto compressed_page_size_field_id = thrift::field_id(3);
    constexpr auto crc_field_id = thrift::field_id(4);
    constexpr auto data_page_header_v1_field_id = thrift::field_id(5);
    constexpr auto index_page_header_field_id = thrift::field_id(6);
    constexpr auto dictionary_page_header_field_id = thrift::field_id(7);
    constexpr auto data_page_header_v2_field_id = thrift::field_id(8);

    enum page_type_enum : int32_t {
        data_page = 0,
        index_page = 1,
        dictionary_page = 2,
        data_page_v2 = 3,
    };

    page_header result{};
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        if (hdr->id == type_field_id) {
            // Consumed but not directly used — the actual type variant is
            // set from the type-specific header field below.
            thrift::decode_i32(parser);
        } else if (hdr->id == uncompressed_page_size_field_id) {
            result.uncompressed_page_size = thrift::decode_i32(parser);
        } else if (hdr->id == compressed_page_size_field_id) {
            result.compressed_page_size = thrift::decode_i32(parser);
        } else if (hdr->id == crc_field_id) {
            result.crc = crc::crc32(
              static_cast<uint32_t>(thrift::decode_i32(parser)));
        } else if (hdr->id == data_page_header_v1_field_id) {
            result.type = decode_data_page_header_v1(parser);
        } else if (hdr->id == index_page_header_field_id) {
            result.type = index_page_header{};
            // Skip the (empty) index page header struct
            dec.skip_field(hdr->type);
        } else if (hdr->id == dictionary_page_header_field_id) {
            result.type = decode_dictionary_page_header(parser);
        } else if (hdr->id == data_page_header_v2_field_id) {
            result.type = decode_data_page_header(parser);
        } else {
            dec.skip_field(hdr->type);
        }
    }
    return result;
}

file_metadata decode(iobuf data, file_metadata_tag) {
    iobuf_parser parser(std::move(data));

    constexpr auto version_field_id = thrift::field_id(1);
    constexpr auto schema_field_id = thrift::field_id(2);
    constexpr auto num_rows_field_id = thrift::field_id(3);
    constexpr auto row_groups_field_id = thrift::field_id(4);
    constexpr auto key_value_metadata_field_id = thrift::field_id(5);
    constexpr auto created_by_field_id = thrift::field_id(6);
    constexpr auto column_orders_field_id = thrift::field_id(7);

    file_metadata result{};
    thrift::struct_decoder dec(parser);
    while (auto hdr = dec.read_field_header()) {
        if (hdr->id == version_field_id) {
            result.version = thrift::decode_i32(parser);
        } else if (hdr->id == schema_field_id) {
            thrift::list_decoder list(parser);
            bool is_root = true;
            for (size_t i = 0; i < list.size(); ++i) {
                result.schema.push_back(
                  decode_flattened_schema(parser, is_root));
                is_root = false;
            }
        } else if (hdr->id == num_rows_field_id) {
            result.num_rows = thrift::decode_i64(parser);
        } else if (hdr->id == row_groups_field_id) {
            thrift::list_decoder list(parser);
            for (size_t i = 0; i < list.size(); ++i) {
                result.row_groups.push_back(decode_row_group(parser));
            }
        } else if (hdr->id == key_value_metadata_field_id) {
            thrift::list_decoder list(parser);
            for (size_t i = 0; i < list.size(); ++i) {
                thrift::struct_decoder kv_dec(parser);
                ss::sstring key;
                ss::sstring value;
                while (auto kv_hdr = kv_dec.read_field_header()) {
                    if (kv_hdr->id() == 1) {
                        key = thrift::decode_string(parser);
                    } else if (kv_hdr->id() == 2) {
                        value = thrift::decode_string(parser);
                    } else {
                        kv_dec.skip_field(kv_hdr->type);
                    }
                }
                result.key_value_metadata.emplace_back(
                  std::move(key), std::move(value));
            }
        } else if (hdr->id == created_by_field_id) {
            result.created_by = thrift::decode_string(parser);
        } else if (hdr->id == column_orders_field_id) {
            thrift::list_decoder list(parser);
            for (size_t i = 0; i < list.size(); ++i) {
                result.column_orders.push_back(decode_column_order(parser));
            }
        } else {
            dec.skip_field(hdr->type);
        }
    }
    return result;
}

footer_location
parse_footer_location(const iobuf& tail_bytes, int64_t file_size) {
    constexpr size_t magic_size = 4;
    constexpr size_t suffix_size = magic_size + sizeof(uint32_t);
    if (tail_bytes.size_bytes() < suffix_size) {
        throw std::runtime_error("tail bytes too small for parquet footer");
    }
    if (file_size < static_cast<int64_t>(magic_size + suffix_size)) {
        throw std::runtime_error("file too small to be a valid parquet file");
    }
    iobuf_const_parser parser(tail_bytes);
    auto footer_len = ss::le_to_cpu(parser.consume_type<uint32_t>());
    auto magic = parser.read_string_unsafe(magic_size);
    if (magic != "PAR1") {
        throw std::runtime_error("invalid parquet file: missing trailing PAR1");
    }
    if (
      static_cast<int64_t>(footer_len)
      > file_size - static_cast<int64_t>(magic_size + suffix_size)) {
        throw std::runtime_error(
          "invalid parquet file: footer length exceeds file size");
    }
    return footer_location{
      .offset = file_size - static_cast<int64_t>(suffix_size) - footer_len,
      .length = static_cast<int64_t>(footer_len),
    };
}

} // namespace serde::parquet
