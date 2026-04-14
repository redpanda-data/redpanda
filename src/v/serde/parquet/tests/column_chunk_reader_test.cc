/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iostream.h"
#include "serde/parquet/column_chunk_reader.h"
#include "serde/parquet/encoding.h"
#include "serde/parquet/metadata.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/writer.h"

#include <gtest/gtest.h>

namespace serde::parquet {

namespace {

schema_element make_schema(
  ss::sstring col_name,
  field_repetition_type rep,
  physical_type ptype,
  logical_type ltype = {}) {
    chunked_vector<schema_element> children;
    children.push_back(
      schema_element{
        .type = ptype,
        .repetition_type = rep,
        .path = {std::move(col_name)},
        .logical_type = ltype,
      });
    return {
      .repetition_type = field_repetition_type::required,
      .path = {"root"},
      .children = std::move(children),
    };
}

struct written_file {
    iobuf data;
    file_metadata metadata;
};

written_file write_simple_file(
  schema_element schema,
  chunked_vector<group_value> rows,
  bool compress = false) {
    iobuf file;
    writer w(
      {.schema = std::move(schema), .compress = compress},
      make_iobuf_ref_output_stream(file));
    w.init().get();
    for (auto& row : rows) {
        w.write_row(std::move(row)).get();
    }
    w.close().get();

    // Parse footer to get metadata
    auto file_size = file.size_bytes();
    // Footer layout: [metadata] [4-byte footer len LE] [PAR1]
    auto footer_region = file.share(file_size - 8, 8);
    iobuf_parser footer_parser(std::move(footer_region));
    auto footer_len = ss::le_to_cpu(footer_parser.consume_type<int32_t>());

    auto metadata_bytes = file.share(file_size - 8 - footer_len, footer_len);
    auto metadata = decode(std::move(metadata_bytes), file_metadata_tag{});

    return {std::move(file), std::move(metadata)};
}

iobuf extract_column_bytes(iobuf& file, const column_meta_data& col_meta) {
    return file.share(
      col_meta.data_page_offset, col_meta.total_compressed_size);
}

} // namespace

// NOLINTBEGIN(*magic-number*)

TEST(ColumnChunkReader, RequiredInt32) {
    chunked_vector<group_value> rows;
    for (int i = 0; i < 10; ++i) {
        group_value row;
        row.push_back(group_member{int32_value{i * 10}});
        rows.push_back(std::move(row));
    }
    auto [file, metadata] = write_simple_file(
      make_schema("val", field_repetition_type::required, i32_type()),
      std::move(rows));

    // Reconstruct schema for the decoder
    auto schema = make_schema(
      "val", field_repetition_type::required, i32_type());
    index_schema(schema);
    const auto& col_meta = metadata.row_groups[0].columns[0].meta_data;
    auto col_bytes = extract_column_bytes(file, col_meta);
    auto result = decode_column_chunk(
                    std::move(col_bytes), col_meta, schema.children[0])
                    .get();

    EXPECT_EQ(result.values.length, 10);
    auto* i32d = std::get_if<column_array::i32_data>(&result.values.data);
    ASSERT_NE(i32d, nullptr);
    ASSERT_EQ(i32d->values.size(), 10);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(i32d->values[i], i * 10);
    }
}

TEST(ColumnChunkReader, OptionalInt64) {
    chunked_vector<group_value> rows;
    for (int i = 0; i < 5; ++i) {
        group_value row;
        if (i % 2 == 0) {
            row.push_back(group_member{int64_value{i * 100}});
        } else {
            row.push_back(group_member{null_value{}});
        }
        rows.push_back(std::move(row));
    }
    auto [file, metadata] = write_simple_file(
      make_schema("val", field_repetition_type::optional, i64_type()),
      std::move(rows));

    auto schema = make_schema(
      "val", field_repetition_type::optional, i64_type());
    index_schema(schema);
    const auto& col_meta = metadata.row_groups[0].columns[0].meta_data;
    auto col_bytes = extract_column_bytes(file, col_meta);
    auto result = decode_column_chunk(
                    std::move(col_bytes), col_meta, schema.children[0])
                    .get();

    EXPECT_EQ(result.values.length, 5);
    // Null positions tracked via def levels, not a bitmap
    ASSERT_EQ(result.def_levels.size(), 5);
    EXPECT_EQ(result.def_levels[0], def_level(1)); // non-null
    EXPECT_EQ(result.def_levels[1], def_level(0)); // null
    EXPECT_EQ(result.def_levels[2], def_level(1)); // non-null
    EXPECT_EQ(result.def_levels[3], def_level(0)); // null
    EXPECT_EQ(result.def_levels[4], def_level(1)); // non-null

    auto* i64d = std::get_if<column_array::i64_data>(&result.values.data);
    ASSERT_NE(i64d, nullptr);
    ASSERT_EQ(i64d->values.size(), 3);
    EXPECT_EQ(i64d->values[0], 0);
    EXPECT_EQ(i64d->values[1], 200);
    EXPECT_EQ(i64d->values[2], 400);
}

TEST(ColumnChunkReader, RequiredInt32Compressed) {
    chunked_vector<group_value> rows;
    for (int i = 0; i < 20; ++i) {
        group_value row;
        row.push_back(group_member{int32_value{i}});
        rows.push_back(std::move(row));
    }
    auto [file, metadata] = write_simple_file(
      make_schema("val", field_repetition_type::required, i32_type()),
      std::move(rows),
      /*compress=*/true);

    auto schema = make_schema(
      "val", field_repetition_type::required, i32_type());
    index_schema(schema);
    const auto& col_meta = metadata.row_groups[0].columns[0].meta_data;
    EXPECT_EQ(col_meta.codec, compression_codec::zstd);
    auto col_bytes = extract_column_bytes(file, col_meta);
    auto result = decode_column_chunk(
                    std::move(col_bytes), col_meta, schema.children[0])
                    .get();

    EXPECT_EQ(result.values.length, 20);
    auto* i32d = std::get_if<column_array::i32_data>(&result.values.data);
    ASSERT_NE(i32d, nullptr);
    ASSERT_EQ(i32d->values.size(), 20);
    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(i32d->values[i], i);
    }
}

TEST(ColumnChunkReader, RequiredByteArray) {
    chunked_vector<group_value> rows;
    for (int i = 0; i < 5; ++i) {
        group_value row;
        row.push_back(
          group_member{
            byte_array_value{iobuf::from(fmt::format("str_{}", i))}});
        rows.push_back(std::move(row));
    }
    auto [file, metadata] = write_simple_file(
      make_schema(
        "val",
        field_repetition_type::required,
        byte_array_type{},
        string_type{}),
      std::move(rows));

    auto schema = make_schema(
      "val", field_repetition_type::required, byte_array_type{}, string_type{});
    index_schema(schema);
    const auto& col_meta = metadata.row_groups[0].columns[0].meta_data;
    auto col_bytes = extract_column_bytes(file, col_meta);
    auto result = decode_column_chunk(
                    std::move(col_bytes), col_meta, schema.children[0])
                    .get();

    EXPECT_EQ(result.values.length, 5);
    auto* bad = std::get_if<column_array::byte_array_data>(&result.values.data);
    ASSERT_NE(bad, nullptr);
    ASSERT_EQ(bad->offsets.size(), 6);
    EXPECT_EQ(bad->offsets[0], 0);
}

// Hand-craft a column chunk with a dictionary page + dictionary-encoded
// V2 data page. The dictionary holds PLAIN-encoded i32 values; the data
// page stores RLE-encoded indices.
TEST(ColumnChunkReader, DictionaryEncodedInt32V2) {
    constexpr int32_t num_values = 8;
    // Dictionary: [100, 200, 300]
    plain_encoder<int32_value> dict_enc;
    dict_enc.add_value(int32_value{100});
    dict_enc.add_value(int32_value{200});
    dict_enc.add_value(int32_value{300});
    iobuf dict_data = dict_enc.get_encoded_buf();
    auto dict_data_size = static_cast<int32_t>(dict_data.size_bytes());

    page_header dict_hdr{
      .uncompressed_page_size = dict_data_size,
      .compressed_page_size = dict_data_size,
      .type = dictionary_page_header{
        .num_values = 3,
        .data_encoding = encoding::plain,
      },
    };
    iobuf dict_page = encode(dict_hdr);
    dict_page.append(std::move(dict_data));

    // Data page: indices [0,1,2,0,1,2,0,1] encoded as RLE/bitpack.
    // bit_width=2 is enough for max index 2.
    chunked_vector<rep_level> index_levels;
    for (auto idx : {0, 1, 2, 0, 1, 2, 0, 1}) {
        index_levels.push_back(rep_level(idx));
    }
    // encode_levels produces the same RLE/bitpack wire format.
    iobuf index_rle = encode_levels(rep_level(2), index_levels);
    // Prepend the bit_width byte.
    iobuf data_section;
    uint8_t bit_width = 2;
    data_section.append(&bit_width, 1);
    data_section.append(std::move(index_rle));
    auto data_section_size = static_cast<int32_t>(data_section.size_bytes());

    page_header data_hdr{
      .uncompressed_page_size = data_section_size,
      .compressed_page_size = data_section_size,
      .type = data_page_header{
        .num_values = num_values,
        .num_nulls = 0,
        .num_rows = num_values,
        .data_encoding = encoding::rle_dictionary,
        .definition_levels_byte_length = 0,
        .repetition_levels_byte_length = 0,
        .is_compressed = false,
      },
    };
    iobuf data_page = encode(data_hdr);
    data_page.append(std::move(data_section));

    iobuf chunk;
    chunk.append(std::move(dict_page));
    chunk.append(std::move(data_page));

    column_meta_data col_meta{
      .codec = compression_codec::uncompressed,
      .num_values = num_values,
      .total_uncompressed_size = static_cast<int64_t>(chunk.size_bytes()),
      .total_compressed_size = static_cast<int64_t>(chunk.size_bytes()),
      .data_page_offset = 0,
    };
    auto schema = make_schema(
      "val", field_repetition_type::required, i32_type());
    index_schema(schema);

    auto result = decode_column_chunk(
                    std::move(chunk), col_meta, schema.children[0])
                    .get();

    EXPECT_EQ(result.values.length, num_values);
    auto* i32d = std::get_if<column_array::i32_data>(&result.values.data);
    ASSERT_NE(i32d, nullptr);
    ASSERT_EQ(i32d->values.size(), num_values);
    // Expected: dict[0]=100, dict[1]=200, dict[2]=300, repeat...
    EXPECT_EQ(i32d->values[0], 100);
    EXPECT_EQ(i32d->values[1], 200);
    EXPECT_EQ(i32d->values[2], 300);
    EXPECT_EQ(i32d->values[3], 100);
    EXPECT_EQ(i32d->values[4], 200);
    EXPECT_EQ(i32d->values[5], 300);
    EXPECT_EQ(i32d->values[6], 100);
    EXPECT_EQ(i32d->values[7], 200);
}

// Hand-craft a V1 data page with uncompressed body containing
// 4-byte-prefixed def levels + PLAIN data.
TEST(ColumnChunkReader, DataPageV1OptionalInt32) {
    constexpr int32_t num_values = 5;
    // def levels: [1, 0, 1, 0, 1] -> 3 non-null, 2 null
    chunked_vector<def_level> def_levels;
    def_levels.push_back(def_level(1));
    def_levels.push_back(def_level(0));
    def_levels.push_back(def_level(1));
    def_levels.push_back(def_level(0));
    def_levels.push_back(def_level(1));
    iobuf encoded_defs = encode_levels(def_level(1), def_levels);
    auto def_len = static_cast<int32_t>(encoded_defs.size_bytes());

    // PLAIN-encoded data: 3 non-null i32 values [10, 30, 50]
    plain_encoder<int32_value> data_enc;
    data_enc.add_value(int32_value{10});
    data_enc.add_value(int32_value{30});
    data_enc.add_value(int32_value{50});
    iobuf data_buf = data_enc.get_encoded_buf();

    // Build the V1 page body: [4-byte def_len LE] [def levels] [data]
    iobuf body;
    auto def_len_le = ss::cpu_to_le(def_len);
    body.append(reinterpret_cast<const char*>(&def_len_le), sizeof(def_len_le));
    body.append(std::move(encoded_defs));
    body.append(std::move(data_buf));
    auto body_size = static_cast<int32_t>(body.size_bytes());

    page_header v1_hdr{
      .uncompressed_page_size = body_size,
      .compressed_page_size = body_size,
      .type = data_page_header_v1{
        .num_values = num_values,
        .data_encoding = encoding::plain,
        .definition_level_encoding = encoding::rle,
        .repetition_level_encoding = encoding::rle,
      },
    };
    iobuf chunk = encode(v1_hdr);
    chunk.append(std::move(body));

    column_meta_data col_meta{
      .codec = compression_codec::uncompressed,
      .num_values = num_values,
      .total_uncompressed_size = static_cast<int64_t>(chunk.size_bytes()),
      .total_compressed_size = static_cast<int64_t>(chunk.size_bytes()),
      .data_page_offset = 0,
    };
    auto schema = make_schema(
      "val", field_repetition_type::optional, i32_type());
    index_schema(schema);

    auto result = decode_column_chunk(
                    std::move(chunk), col_meta, schema.children[0])
                    .get();

    EXPECT_EQ(result.values.length, num_values);
    ASSERT_EQ(result.def_levels.size(), num_values);
    EXPECT_EQ(result.def_levels[0], def_level(1));
    EXPECT_EQ(result.def_levels[1], def_level(0));
    EXPECT_EQ(result.def_levels[2], def_level(1));
    EXPECT_EQ(result.def_levels[3], def_level(0));
    EXPECT_EQ(result.def_levels[4], def_level(1));

    auto* i32d = std::get_if<column_array::i32_data>(&result.values.data);
    ASSERT_NE(i32d, nullptr);
    ASSERT_EQ(i32d->values.size(), 3);
    EXPECT_EQ(i32d->values[0], 10);
    EXPECT_EQ(i32d->values[1], 30);
    EXPECT_EQ(i32d->values[2], 50);
}

// V1 page with dictionary encoding.
TEST(ColumnChunkReader, DictionaryEncodedV1) {
    constexpr int32_t num_values = 4;
    // Dictionary: [42, 99]
    plain_encoder<int32_value> dict_enc;
    dict_enc.add_value(int32_value{42});
    dict_enc.add_value(int32_value{99});
    iobuf dict_data = dict_enc.get_encoded_buf();
    auto dict_data_size = static_cast<int32_t>(dict_data.size_bytes());

    page_header dict_hdr{
      .uncompressed_page_size = dict_data_size,
      .compressed_page_size = dict_data_size,
      .type = dictionary_page_header{
        .num_values = 2,
        .data_encoding = encoding::plain,
      },
    };
    iobuf dict_page = encode(dict_hdr);
    dict_page.append(std::move(dict_data));

    // Indices [0, 1, 1, 0] as RLE
    chunked_vector<rep_level> index_levels;
    index_levels.push_back(rep_level(0));
    index_levels.push_back(rep_level(1));
    index_levels.push_back(rep_level(1));
    index_levels.push_back(rep_level(0));
    iobuf index_rle = encode_levels(rep_level(1), index_levels);

    // V1 body for required column: no rep/def levels, just data.
    // Data section: [bit_width byte] [RLE indices]
    iobuf body;
    uint8_t bit_width = 1;
    body.append(&bit_width, 1);
    body.append(std::move(index_rle));
    auto body_size = static_cast<int32_t>(body.size_bytes());

    page_header v1_hdr{
      .uncompressed_page_size = body_size,
      .compressed_page_size = body_size,
      .type = data_page_header_v1{
        .num_values = num_values,
        .data_encoding = encoding::rle_dictionary,
        .definition_level_encoding = encoding::rle,
        .repetition_level_encoding = encoding::rle,
      },
    };
    iobuf data_page = encode(v1_hdr);
    data_page.append(std::move(body));

    iobuf chunk;
    chunk.append(std::move(dict_page));
    chunk.append(std::move(data_page));

    column_meta_data col_meta{
      .codec = compression_codec::uncompressed,
      .num_values = num_values,
      .total_uncompressed_size = static_cast<int64_t>(chunk.size_bytes()),
      .total_compressed_size = static_cast<int64_t>(chunk.size_bytes()),
      .data_page_offset = 0,
    };
    auto schema = make_schema(
      "val", field_repetition_type::required, i32_type());
    index_schema(schema);

    auto result = decode_column_chunk(
                    std::move(chunk), col_meta, schema.children[0])
                    .get();

    EXPECT_EQ(result.values.length, num_values);
    auto* i32d = std::get_if<column_array::i32_data>(&result.values.data);
    ASSERT_NE(i32d, nullptr);
    ASSERT_EQ(i32d->values.size(), num_values);
    EXPECT_EQ(i32d->values[0], 42);
    EXPECT_EQ(i32d->values[1], 99);
    EXPECT_EQ(i32d->values[2], 99);
    EXPECT_EQ(i32d->values[3], 42);
}

// NOLINTEND(*magic-number*)

} // namespace serde::parquet
