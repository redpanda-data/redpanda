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
    constexpr int16_t key_field_id = 1;
    constexpr int16_t value_field_id = 2;
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
      static_cast<int16_t>(t),
      thrift::field_type::structure,
      thrift::struct_encoder::empty_struct);
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
    constexpr int16_t type_field_id = 1;
    constexpr int16_t type_length_field_id = 2;
    constexpr int16_t repetition_type_field_id = 3;
    constexpr int16_t name_field_id = 4;
    constexpr int16_t num_children_field_id = 5;
    constexpr int16_t converted_type_field_id = 6;
    constexpr int16_t scale_field_id = 7;
    constexpr int16_t precision_field_id = 8;
    constexpr int16_t field_id_field_id = 9; // whoa, meta!
    constexpr int16_t logical_type_field_id = 10;

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

    enum logical_type : int8_t {
        string = 1,
        map = 2,
        list = 3,
        enumeration = 4,
        decimal = 5,
        date = 6,
        time = 7,
        timestamp = 9,
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
            logical_type::string,
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const uuid_type&) {
          // No converted type
          logical_type_encoder.write_field(
            logical_type::uuid,
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const map_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::map));
          logical_type_encoder.write_field(
            logical_type::map,
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const list_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::list));
          logical_type_encoder.write_field(
            logical_type::list,
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const enum_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::enumeration));
          logical_type_encoder.write_field(
            logical_type::enumeration,
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const date_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::date));
          logical_type_encoder.write_field(
            logical_type::date,
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const f16_type&) {
          logical_type_encoder.write_field(
            logical_type::float16,
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const null_type&) {
          logical_type_encoder.write_field(
            logical_type::null,
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

          constexpr int16_t logical_scale_field_id = 1;
          constexpr int16_t logical_precision_field_id = 2;
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
            logical_type::decimal,
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
          constexpr int16_t utc_field_id = 1;
          constexpr int16_t unit_field_id = 2;
          thrift::struct_encoder time_struct;
          time_struct.write_field(
            utc_field_id,
            t.is_adjusted_to_utc ? thrift::field_type::boolean_true
                                 : thrift::field_type::boolean_false,
            bytes());
          time_struct.write_field(
            unit_field_id, thrift::field_type::structure, encode(t.unit));
          logical_type_encoder.write_field(
            logical_type::timestamp,
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
          constexpr int16_t utc_field_id = 1;
          constexpr int16_t unit_field_id = 2;
          thrift::struct_encoder time_struct;
          time_struct.write_field(
            utc_field_id,
            t.is_adjusted_to_utc ? thrift::field_type::boolean_true
                                 : thrift::field_type::boolean_false,
            bytes());
          time_struct.write_field(
            unit_field_id, thrift::field_type::structure, encode(t.unit));
          logical_type_encoder.write_field(
            logical_type::time,
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
          constexpr int16_t bit_width_field_id = 1;
          constexpr int16_t is_signed_field_id = 2;
          thrift::struct_encoder int_struct;
          int_struct.write_field(
            bit_width_field_id,
            thrift::field_type::i8,
            vint::to_bytes(t.bit_width));
          int_struct.write_field(
            is_signed_field_id,
            t.is_signed ? thrift::field_type::boolean_true
                        : thrift::field_type::boolean_false,
            bytes());
          logical_type_encoder.write_field(
            logical_type::integer,
            thrift::field_type::structure,
            std::move(int_struct).write_stop());
      },
      [&](const json_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::json));
          logical_type_encoder.write_field(
            logical_type::json,
            thrift::field_type::structure,
            thrift::struct_encoder::empty_struct);
      },
      [&](const bson_type&) {
          encoder.write_field(
            converted_type_field_id,
            thrift::field_type::i32,
            vint::to_bytes(converted_type::bson));
          logical_type_encoder.write_field(
            logical_type::bson,
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
    constexpr int16_t type_field_id = 1;
    constexpr int16_t encodings_field_id = 2;
    constexpr int16_t path_in_schema_field_id = 3;
    constexpr int16_t codec_field_id = 4;
    constexpr int16_t num_values_field_id = 5;
    constexpr int16_t total_uncompressed_size_field_id = 6;
    constexpr int16_t total_compressed_size_field_id = 7;
    constexpr int16_t key_value_metadata_field_id = 8;
    constexpr int16_t data_page_offset_field_id = 9;
    constexpr int16_t index_page_offset_field_id = 10;
    constexpr int16_t dictionary_page_offset_field_id = 11;
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
      metadata.encodings.size(), thrift::field_type::binary);
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
    encoder.write_field(
      index_page_offset_field_id,
      thrift::field_type::i64,
      vint::to_bytes(metadata.index_page_offset));
    encoder.write_field(
      dictionary_page_offset_field_id,
      thrift::field_type::i64,
      vint::to_bytes(metadata.dictionary_page_offset));
    return std::move(encoder).write_stop();
}

iobuf encode(const column_chunk& chunk) {
    constexpr int16_t file_path_field_id = 1;
    constexpr int16_t file_offset_field_id = 2;
    constexpr int16_t meta_data_field_id = 3;
    constexpr int16_t offset_index_offset_field_id = 4;
    constexpr int16_t offset_index_length_field_id = 5;
    constexpr int16_t column_index_offset_field_id = 6;
    constexpr int16_t column_index_length_field_id = 7;
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
    encoder.write_field(
      offset_index_offset_field_id,
      thrift::field_type::i64,
      vint::to_bytes(chunk.offset_index_offset));
    encoder.write_field(
      offset_index_length_field_id,
      thrift::field_type::i32,
      vint::to_bytes(chunk.offset_index_length));
    encoder.write_field(
      column_index_offset_field_id,
      thrift::field_type::i64,
      vint::to_bytes(chunk.column_index_offset));
    encoder.write_field(
      column_index_length_field_id,
      thrift::field_type::i32,
      vint::to_bytes(chunk.column_index_length));

    return std::move(encoder).write_stop();
}

iobuf encode(const sorting_column& column) {
    constexpr int16_t column_idx_field_id = 1;
    constexpr int16_t descending_field_id = 2;
    constexpr int16_t nulls_first_field_id = 3;
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
    constexpr int16_t columns_field_id = 1;
    constexpr int16_t total_field_id = 2;
    constexpr int16_t num_rows_field_id = 3;
    constexpr int16_t sorting_columns_field_id = 4;
    constexpr int16_t file_offset_field_id = 5;

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
    return std::move(encoder).write_stop();
}

} // namespace

iobuf encode(const file_metadata& metadata) {
    constexpr int16_t version_field_id = 1;
    constexpr int16_t schema_field_id = 2;
    constexpr int16_t num_rows_field_id = 3;
    constexpr int16_t row_groups_field_id = 4;
    constexpr int16_t key_value_metadata_field_id = 5;
    constexpr int16_t created_by_field_id = 6;

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

    return std::move(encoder).write_stop();
}

namespace {

iobuf encode(const data_page_header& header) {
    constexpr int16_t num_values_field_id = 1;
    constexpr int16_t num_nulls_field_id = 2;
    constexpr int16_t num_rows_field_id = 3;
    constexpr int16_t encoding_field_id = 4;
    constexpr int16_t definition_levels_byte_length_field_id = 5;
    constexpr int16_t repetition_levels_byte_length_field_id = 6;
    constexpr int16_t is_compressed_field_id = 7;
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
    return std::move(encoder).write_stop();
}

iobuf encode(const dictionary_page_header& header) {
    constexpr int16_t num_values_field_id = 1;
    constexpr int16_t encoding_field_id = 2;
    constexpr int16_t is_sorted_field_id = 3;
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

} // namespace

iobuf encode(const page_header& header) {
    constexpr int16_t type_field_id = 1;
    constexpr int16_t uncompressed_page_size_field_id = 2;
    constexpr int16_t compressed_page_size_field_id = 3;
    constexpr int16_t crc_field_id = 4;
    // constexpr int16_t data_page_header_field_id = 5;
    constexpr int16_t index_page_header_field_id = 6;
    constexpr int16_t dictionary_page_header_field_id = 7;
    constexpr int16_t data_page_header_v2_field_id = 8;
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
      [](const dictionary_page_header&) { return page_type::dictionary_page; });
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
      crc_field_id, thrift::field_type::i32, vint::to_bytes(header.crc));
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
      });
    return std::move(encoder).write_stop();
}

} // namespace serde::parquet
