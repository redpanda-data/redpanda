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

#include "serde/parquet/flattened_schema.h"
#include "serde/parquet/metadata.h"
#include "serde/parquet/schema.h"

#include <seastar/core/byteorder.hh>

#include <gtest/gtest.h>

using namespace serde::parquet;

namespace {

template<typename... Args>
chunked_vector<Args...> list(auto&&... items) {
    chunked_vector<Args...> v;
    (v.push_back(std::forward<decltype(items)>(items)), ...);
    return v;
}

} // namespace

TEST(MetadataRoundTrip, PageHeaderDataV2) {
    page_header original{
      .uncompressed_page_size = 4096,
      .compressed_page_size = 2048,
      .crc = crc::crc32(0xDEAD),
      .type = data_page_header{
        .num_values = 100,
        .num_nulls = 5,
        .num_rows = 95,
        .data_encoding = encoding::plain,
        .definition_levels_byte_length = 12,
        .repetition_levels_byte_length = 8,
        .is_compressed = true,
        .stats = statistics{
          .null_count = 5,
          .max = std::make_optional<statistics::bound>(
            iobuf::from("\xFF\xFE"), true),
          .min = std::make_optional<statistics::bound>(
            iobuf::from("\x00\x01"), false),
        },
      },
    };
    auto encoded = encode(original);
    iobuf_parser parser(std::move(encoded));
    auto decoded = decode(parser, page_header_tag{});

    EXPECT_EQ(decoded.uncompressed_page_size, original.uncompressed_page_size);
    EXPECT_EQ(decoded.compressed_page_size, original.compressed_page_size);
    EXPECT_EQ(decoded.crc.value(), original.crc.value());

    auto* dph = std::get_if<data_page_header>(&decoded.type);
    ASSERT_NE(dph, nullptr);
    auto* oph = std::get_if<data_page_header>(&original.type);
    EXPECT_EQ(dph->num_values, oph->num_values);
    EXPECT_EQ(dph->num_nulls, oph->num_nulls);
    EXPECT_EQ(dph->num_rows, oph->num_rows);
    EXPECT_EQ(dph->data_encoding, oph->data_encoding);
    EXPECT_EQ(
      dph->definition_levels_byte_length, oph->definition_levels_byte_length);
    EXPECT_EQ(
      dph->repetition_levels_byte_length, oph->repetition_levels_byte_length);
    EXPECT_EQ(dph->is_compressed, oph->is_compressed);
    ASSERT_TRUE(dph->stats.has_value());
    EXPECT_EQ(dph->stats->null_count, 5);
    ASSERT_TRUE(dph->stats->max.has_value());
    EXPECT_EQ(dph->stats->max->value, oph->stats->max->value);
    EXPECT_EQ(dph->stats->max->is_exact, true);
    ASSERT_TRUE(dph->stats->min.has_value());
    EXPECT_EQ(dph->stats->min->value, oph->stats->min->value);
    EXPECT_EQ(dph->stats->min->is_exact, false);
    EXPECT_EQ(parser.bytes_left(), 0);
}

TEST(MetadataRoundTrip, PageHeaderDataV1) {
    page_header original{
      .uncompressed_page_size = 4096,
      .compressed_page_size = 2048,
      .crc = crc::crc32(0xBEEF),
      .type = data_page_header_v1{
        .num_values = 100,
        .data_encoding = encoding::rle_dictionary,
        .definition_level_encoding = encoding::rle,
        .repetition_level_encoding = encoding::rle,
      },
    };
    auto encoded = encode(original);
    iobuf_parser parser(std::move(encoded));
    auto decoded = decode(parser, page_header_tag{});

    EXPECT_EQ(decoded.uncompressed_page_size, 4096);
    EXPECT_EQ(decoded.compressed_page_size, 2048);
    auto* v1 = std::get_if<data_page_header_v1>(&decoded.type);
    ASSERT_NE(v1, nullptr);
    EXPECT_EQ(v1->num_values, 100);
    EXPECT_EQ(v1->data_encoding, encoding::rle_dictionary);
    EXPECT_EQ(v1->definition_level_encoding, encoding::rle);
    EXPECT_EQ(v1->repetition_level_encoding, encoding::rle);
}

TEST(MetadataRoundTrip, PageHeaderDictionary) {
    page_header original{
      .uncompressed_page_size = 99999,
      .compressed_page_size = 0,
      .crc = crc::crc32(0xEEEE),
      .type = dictionary_page_header{
        .num_values = 44,
        .data_encoding = encoding::rle,
        .is_sorted = true,
      },
    };
    auto encoded = encode(original);
    iobuf_parser parser(std::move(encoded));
    auto decoded = decode(parser, page_header_tag{});

    EXPECT_EQ(decoded.uncompressed_page_size, 99999);
    EXPECT_EQ(decoded.crc.value(), 0xEEEE);
    auto* dp = std::get_if<dictionary_page_header>(&decoded.type);
    ASSERT_NE(dp, nullptr);
    EXPECT_EQ(dp->num_values, 44);
    EXPECT_EQ(dp->data_encoding, encoding::rle);
    EXPECT_EQ(dp->is_sorted, true);
    EXPECT_EQ(parser.bytes_left(), 0);
}

TEST(MetadataRoundTrip, FileMetadataFull) {
    file_metadata original{
      .version = 2,
      .schema = flatten(schema_element{
        .path = {"root"},
        .children = list<schema_element>(
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
          }),
      }),
      .num_rows = 999,
      .row_groups = list<row_group>(row_group{
        .columns = list<column_chunk>(column_chunk{
          .meta_data = {
            .type = bool_type{},
            .encodings = {encoding::plain, encoding::rle},
            .path_in_schema = {"foo", "baz"},
            .codec = compression_codec::zstd,
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
                iobuf::from("\xFF"), true),
              .min = std::make_optional<statistics::bound>(
                iobuf::from(std::string_view{"\x00", 1}), false),
            },
          },
        }),
        .total_byte_size = 321,
        .num_rows = 1,
        .sorting_columns = list<sorting_column>(
          sorting_column{.column_idx = 0, .descending = true, .nulls_first = false}),
        .file_offset = 1234,
        .total_compressed_size = 231,
        .ordinal = 0,
      }),
      .key_value_metadata = {{"key1", "val1"}, {"key2", "val2"}},
      .created_by = "redpanda test",
      .column_orders = {column_order::type_defined},
    };

    auto encoded = encode(original);
    auto decoded = decode(std::move(encoded), file_metadata_tag{});

    EXPECT_EQ(decoded.version, 2);
    EXPECT_EQ(decoded.num_rows, 999);
    EXPECT_EQ(decoded.created_by, "redpanda test");

    // Schema
    ASSERT_EQ(decoded.schema.size(), original.schema.size());
    for (size_t i = 0; i < decoded.schema.size(); ++i) {
        EXPECT_EQ(decoded.schema[i].name, original.schema[i].name);
        EXPECT_EQ(decoded.schema[i].type, original.schema[i].type);
        EXPECT_EQ(
          decoded.schema[i].repetition_type,
          original.schema[i].repetition_type);
        EXPECT_EQ(decoded.schema[i].field_id, original.schema[i].field_id);
        EXPECT_EQ(
          decoded.schema[i].logical_type, original.schema[i].logical_type);
        EXPECT_EQ(
          decoded.schema[i].num_children, original.schema[i].num_children);
    }

    // Row groups
    ASSERT_EQ(decoded.row_groups.size(), 1);
    const auto& rg = decoded.row_groups[0];
    EXPECT_EQ(rg.total_byte_size, 321);
    EXPECT_EQ(rg.num_rows, 1);
    EXPECT_EQ(rg.file_offset, 1234);
    EXPECT_EQ(rg.total_compressed_size, 231);
    EXPECT_EQ(rg.ordinal, 0);

    // Sorting columns
    ASSERT_EQ(rg.sorting_columns.size(), 1);
    EXPECT_EQ(rg.sorting_columns[0].column_idx, 0);
    EXPECT_EQ(rg.sorting_columns[0].descending, true);
    EXPECT_EQ(rg.sorting_columns[0].nulls_first, false);

    // Column chunk
    ASSERT_EQ(rg.columns.size(), 1);
    const auto& cc = rg.columns[0];
    EXPECT_EQ(cc.meta_data.codec, compression_codec::zstd);
    EXPECT_EQ(cc.meta_data.num_values, 888);
    EXPECT_EQ(cc.meta_data.data_page_offset, 2);
    EXPECT_EQ(cc.meta_data.index_page_offset, 5);
    EXPECT_EQ(cc.meta_data.dictionary_page_offset, 9);
    ASSERT_EQ(cc.meta_data.encodings.size(), 2);
    EXPECT_EQ(cc.meta_data.encodings[0], encoding::plain);
    EXPECT_EQ(cc.meta_data.encodings[1], encoding::rle);
    ASSERT_EQ(cc.meta_data.path_in_schema.size(), 2);
    EXPECT_EQ(cc.meta_data.path_in_schema[0], "foo");
    EXPECT_EQ(cc.meta_data.path_in_schema[1], "baz");
    ASSERT_EQ(cc.meta_data.key_value_metadata.size(), 1);
    EXPECT_EQ(cc.meta_data.key_value_metadata[0].first, "qux");
    EXPECT_EQ(cc.meta_data.key_value_metadata[0].second, "thid");

    // Key-value metadata
    ASSERT_EQ(decoded.key_value_metadata.size(), 2);
    EXPECT_EQ(decoded.key_value_metadata[0].first, "key1");
    EXPECT_EQ(decoded.key_value_metadata[0].second, "val1");

    // Column orders
    ASSERT_EQ(decoded.column_orders.size(), 1);
    EXPECT_EQ(decoded.column_orders[0], column_order::type_defined);
}

TEST(MetadataRoundTrip, FileMetadataMinimal) {
    file_metadata original{
      .version = 2,
      .schema = flatten(
        schema_element{
          .path = {"root"},
          .children = list<schema_element>(schema_element{
            .type = i64_type{},
            .repetition_type = field_repetition_type::required,
            .path = {"col"},
          }),
        }),
      .num_rows = 0,
      .created_by = "test",
    };

    auto encoded = encode(original);
    auto decoded = decode(std::move(encoded), file_metadata_tag{});

    EXPECT_EQ(decoded.version, 2);
    EXPECT_EQ(decoded.num_rows, 0);
    EXPECT_EQ(decoded.schema.size(), 2);
    EXPECT_TRUE(decoded.row_groups.empty());
}

TEST(MetadataRoundTrip, SchemaWithAllLogicalTypes) {
    file_metadata original{
      .version = 2,
      .schema = flatten(schema_element{
        .path = {"root"},
        .children = list<schema_element>(
          schema_element{
            .type = byte_array_type{},
            .repetition_type = field_repetition_type::required,
            .path = {"str"},
            .logical_type = string_type{},
          },
          schema_element{
            .type = i32_type{},
            .repetition_type = field_repetition_type::required,
            .path = {"date"},
            .logical_type = date_type{},
          },
          schema_element{
            .type = i64_type{},
            .repetition_type = field_repetition_type::required,
            .path = {"ts"},
            .logical_type = timestamp_type{
              .is_adjusted_to_utc = false,
              .unit = time_unit::nanos,
            },
          },
          schema_element{
            .type = byte_array_type{},
            .repetition_type = field_repetition_type::required,
            .path = {"dec"},
            .logical_type = decimal_type{.scale = 3, .precision = 18},
          },
          schema_element{
            .type = i32_type{},
            .repetition_type = field_repetition_type::required,
            .path = {"u16"},
            .logical_type = int_type{.bit_width = 16, .is_signed = false},
          }),
      }),
      .num_rows = 0,
      .created_by = "test",
    };

    auto encoded = encode(original);
    auto decoded = decode(std::move(encoded), file_metadata_tag{});

    ASSERT_EQ(decoded.schema.size(), original.schema.size());
    for (size_t i = 0; i < decoded.schema.size(); ++i) {
        EXPECT_EQ(
          decoded.schema[i].logical_type, original.schema[i].logical_type)
          << "mismatch at schema index " << i;
    }
}

// Build a minimal parquet file in memory:
//   [PAR1] [encoded footer] [footer_len LE uint32] [PAR1]
// Then verify parse_footer_location returns the correct offset/length
// and that decoding the footer at that location succeeds.
TEST(MetadataRoundTrip, ParseFooterLocation) {
    file_metadata original{
      .version = 2,
      .schema = flatten(
        schema_element{
          .path = {"root"},
          .children = list<schema_element>(schema_element{
            .type = i64_type{},
            .repetition_type = field_repetition_type::required,
            .path = {"col"},
          }),
        }),
      .num_rows = 42,
      .created_by = "test",
    };

    auto encoded_footer = encode(original);
    auto footer_len = static_cast<uint32_t>(encoded_footer.size_bytes());

    // Assemble file: PAR1 + footer + footer_len(LE) + PAR1
    iobuf file_data;
    file_data.append("PAR1", 4);
    file_data.append(encoded_footer.share(0, encoded_footer.size_bytes()));
    auto le_len = ss::cpu_to_le(footer_len);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    file_data.append(reinterpret_cast<const char*>(&le_len), sizeof(le_len));
    file_data.append("PAR1", 4);

    auto file_size = static_cast<int64_t>(file_data.size_bytes());

    // Extract last 8 bytes
    auto tail = file_data.share(file_data.size_bytes() - 8, 8);
    auto loc = parse_footer_location(tail, file_size);

    EXPECT_EQ(loc.offset, 4);
    EXPECT_EQ(loc.length, static_cast<int64_t>(footer_len));

    // Decode the footer at the reported location
    auto footer_bytes = file_data.share(loc.offset, loc.length);
    auto decoded = decode(std::move(footer_bytes), file_metadata_tag{});
    EXPECT_EQ(decoded.version, 2);
    EXPECT_EQ(decoded.num_rows, 42);
    EXPECT_EQ(decoded.created_by, "test");
}

TEST(MetadataRoundTrip, ParseFooterLocationBadMagic) {
    iobuf tail;
    uint32_t len = 10;
    auto le_len = ss::cpu_to_le(len);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    tail.append(reinterpret_cast<const char*>(&le_len), sizeof(le_len));
    tail.append("XXXX", 4);
    EXPECT_THROW(parse_footer_location(tail, 100), std::runtime_error);
}

TEST(MetadataRoundTrip, ParseFooterLocationTooSmall) {
    iobuf tail;
    tail.append("PAR1", 4);
    EXPECT_THROW(parse_footer_location(tail, 100), std::runtime_error);
}
