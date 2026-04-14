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

#include "serde/parquet/column_chunk_reader.h"

#include "compression/compression.h"
#include "compression/snappy_standard_compressor.h"
#include "serde/parquet/encoding.h"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/variant_utils.hh>

namespace serde::parquet {

namespace {

ss::future<iobuf> decompress_data(
  iobuf data, compression_codec codec, int32_t uncompressed_size) {
    switch (codec) {
    case compression_codec::uncompressed:
        co_return std::move(data);
    case compression_codec::zstd:
        co_return co_await compression::stream_compressor::uncompress(
          std::move(data), compression::type::zstd);
    case compression_codec::snappy:
        co_return compression::snappy_standard_compressor::uncompress(data);
    default:
        throw std::runtime_error(
          fmt::format(
            "unsupported compression codec: {}", static_cast<int>(codec)));
    }
}

void append_to_column_array(
  column_array& arr, iobuf_parser_base& data_parser, int32_t num_non_null) {
    ss::visit(
      arr.data,
      [&](column_array::boolean_data& d) {
          plain_decoder<boolean_value> dec(data_parser);
          uint8_t current_byte = 0;
          auto start_bit = d.num_values % CHAR_BIT;
          if (start_bit != 0) {
              // Continue partial byte — not expected across pages in V2,
              // but handle defensively.
              current_byte = 0;
          }
          for (int32_t i = 0; i < num_non_null; ++i) {
              auto bit_pos = (d.num_values + i) % CHAR_BIT;
              current_byte |= static_cast<uint8_t>(dec.read_value().val)
                              << bit_pos;
              if (bit_pos == CHAR_BIT - 1) {
                  d.packed_bits.append(&current_byte, 1);
                  current_byte = 0;
              }
          }
          if ((d.num_values + num_non_null) % CHAR_BIT != 0) {
              d.packed_bits.append(&current_byte, 1);
          }
          d.num_values += num_non_null;
      },
      [&](column_array::i32_data& d) {
          plain_decoder<int32_value> dec(data_parser);
          for (int32_t i = 0; i < num_non_null; ++i) {
              d.values.push_back(dec.read_value().val);
          }
      },
      [&](column_array::i64_data& d) {
          plain_decoder<int64_value> dec(data_parser);
          for (int32_t i = 0; i < num_non_null; ++i) {
              d.values.push_back(dec.read_value().val);
          }
      },
      [&](column_array::f32_data& d) {
          plain_decoder<float32_value> dec(data_parser);
          for (int32_t i = 0; i < num_non_null; ++i) {
              d.values.push_back(dec.read_value().val);
          }
      },
      [&](column_array::f64_data& d) {
          plain_decoder<float64_value> dec(data_parser);
          for (int32_t i = 0; i < num_non_null; ++i) {
              d.values.push_back(dec.read_value().val);
          }
      },
      [&](column_array::byte_array_data& d) {
          plain_decoder<byte_array_value> dec(data_parser);
          for (int32_t i = 0; i < num_non_null; ++i) {
              auto v = dec.read_value();
              auto len = static_cast<int64_t>(v.val.size_bytes());
              d.data.append(std::move(v.val));
              d.offsets.push_back(d.offsets.back() + len);
          }
      },
      [&](column_array::fixed_byte_array_data& d) {
          plain_decoder<fixed_byte_array_value> dec(
            data_parser, d.fixed_length);
          for (int32_t i = 0; i < num_non_null; ++i) {
              d.data.append(std::move(dec.read_value().val));
          }
      });
}

chunked_vector<value> decode_dictionary_plain(
  iobuf_parser_base& parser,
  int32_t num_values,
  const schema_element& schema_elem) {
    chunked_vector<value> dict;
    dict.reserve(num_values);
    ss::visit(
      schema_elem.type,
      [&](const std::monostate&) {
          plain_decoder<boolean_value> dec(parser);
          for (int32_t i = 0; i < num_values; ++i) {
              dict.push_back(dec.read_value());
          }
      },
      [&](const bool_type&) {
          plain_decoder<boolean_value> dec(parser);
          for (int32_t i = 0; i < num_values; ++i) {
              dict.push_back(dec.read_value());
          }
      },
      [&](const i32_type&) {
          plain_decoder<int32_value> dec(parser);
          for (int32_t i = 0; i < num_values; ++i) {
              dict.push_back(dec.read_value());
          }
      },
      [&](const i64_type&) {
          plain_decoder<int64_value> dec(parser);
          for (int32_t i = 0; i < num_values; ++i) {
              dict.push_back(dec.read_value());
          }
      },
      [&](const f32_type&) {
          plain_decoder<float32_value> dec(parser);
          for (int32_t i = 0; i < num_values; ++i) {
              dict.push_back(dec.read_value());
          }
      },
      [&](const f64_type&) {
          plain_decoder<float64_value> dec(parser);
          for (int32_t i = 0; i < num_values; ++i) {
              dict.push_back(dec.read_value());
          }
      },
      [&](const byte_array_type& t) {
          if (t.fixed_length.has_value()) {
              plain_decoder<fixed_byte_array_value> dec(
                parser, *t.fixed_length);
              for (int32_t i = 0; i < num_values; ++i) {
                  dict.push_back(dec.read_value());
              }
          } else {
              plain_decoder<byte_array_value> dec(parser);
              for (int32_t i = 0; i < num_values; ++i) {
                  dict.push_back(dec.read_value());
              }
          }
      });
    return dict;
}

void append_dictionary_values(
  column_array& arr,
  const chunked_vector<value>& dictionary,
  const chunked_vector<int32_t>& indices) {
    ss::visit(
      arr.data,
      [&](column_array::boolean_data& d) {
          uint8_t current_byte = 0;
          for (auto idx : indices) {
              const auto& v = std::get<boolean_value>(dictionary[idx]);
              auto bit_pos = d.num_values % CHAR_BIT;
              current_byte |= static_cast<uint8_t>(v.val) << bit_pos;
              if (bit_pos == CHAR_BIT - 1) {
                  d.packed_bits.append(&current_byte, 1);
                  current_byte = 0;
              }
              ++d.num_values;
          }
          if (d.num_values % CHAR_BIT != 0) {
              d.packed_bits.append(&current_byte, 1);
          }
      },
      [&](column_array::i32_data& d) {
          for (auto idx : indices) {
              d.values.push_back(std::get<int32_value>(dictionary[idx]).val);
          }
      },
      [&](column_array::i64_data& d) {
          for (auto idx : indices) {
              d.values.push_back(std::get<int64_value>(dictionary[idx]).val);
          }
      },
      [&](column_array::f32_data& d) {
          for (auto idx : indices) {
              d.values.push_back(std::get<float32_value>(dictionary[idx]).val);
          }
      },
      [&](column_array::f64_data& d) {
          for (auto idx : indices) {
              d.values.push_back(std::get<float64_value>(dictionary[idx]).val);
          }
      },
      [&](column_array::byte_array_data& d) {
          for (auto idx : indices) {
              const auto& v = std::get<byte_array_value>(dictionary[idx]);
              auto len = static_cast<int64_t>(v.val.size_bytes());
              d.data.append(v.val.copy());
              d.offsets.push_back(d.offsets.back() + len);
          }
      },
      [&](column_array::fixed_byte_array_data& d) {
          for (auto idx : indices) {
              const auto& v = std::get<fixed_byte_array_value>(dictionary[idx]);
              d.data.append(v.val.copy());
          }
      });
}

/// Decode values from a data section, dispatching on PLAIN vs dictionary.
void decode_data_values(
  column_array& arr,
  iobuf encoded_data,
  encoding data_encoding,
  int32_t num_non_null,
  const chunked_vector<value>& dictionary) {
    iobuf_parser data_parser(std::move(encoded_data));
    if (
      data_encoding == encoding::rle_dictionary
      || data_encoding == encoding::plain_dictionary) {
        auto bit_width = static_cast<int32_t>(
          data_parser.consume_type<uint8_t>());
        auto remaining = static_cast<int32_t>(data_parser.bytes_left());
        auto indices = decode_rle_bp_int32(
          data_parser, num_non_null, remaining, bit_width);
        append_dictionary_values(arr, dictionary, indices);
    } else {
        append_to_column_array(arr, data_parser, num_non_null);
    }
}

column_array make_empty_column_array(const schema_element& schema_elem) {
    column_array arr;
    arr.ptype = schema_elem.type;
    arr.ltype = schema_elem.logical_type;
    ss::visit(
      schema_elem.type,
      [&](const std::monostate&) {
          arr.data.emplace<column_array::boolean_data>();
      },
      [&](const bool_type&) { arr.data.emplace<column_array::boolean_data>(); },
      [&](const i32_type&) { arr.data.emplace<column_array::i32_data>(); },
      [&](const i64_type&) { arr.data.emplace<column_array::i64_data>(); },
      [&](const f32_type&) { arr.data.emplace<column_array::f32_data>(); },
      [&](const f64_type&) { arr.data.emplace<column_array::f64_data>(); },
      [&](const byte_array_type& t) {
          if (t.fixed_length.has_value()) {
              arr.data.emplace<column_array::fixed_byte_array_data>(
                column_array::fixed_byte_array_data{
                  .fixed_length = *t.fixed_length});
          } else {
              auto& d = arr.data.emplace<column_array::byte_array_data>();
              d.offsets.push_back(0);
          }
      });
    return arr;
}

} // namespace

ss::future<column_chunk_data> decode_column_chunk(
  iobuf data, const column_meta_data& meta, const schema_element& schema_elem) {
    iobuf_parser parser(std::move(data));

    auto arr = make_empty_column_array(schema_elem);
    chunked_vector<def_level> all_def_levels;
    chunked_vector<rep_level> all_rep_levels;
    chunked_vector<value> dictionary;

    while (parser.bytes_left() > 0) {
        auto header = decode(parser, page_header_tag{});

        if (auto* dph = std::get_if<dictionary_page_header>(&header.type)) {
            iobuf page_bytes = parser.copy(header.compressed_page_size);
            if (meta.codec != compression_codec::uncompressed) {
                page_bytes = co_await decompress_data(
                  std::move(page_bytes),
                  meta.codec,
                  header.uncompressed_page_size);
            }
            iobuf_parser dict_parser(std::move(page_bytes));
            dictionary = decode_dictionary_plain(
              dict_parser, dph->num_values, schema_elem);
            continue;
        }

        if (auto* v2 = std::get_if<data_page_header>(&header.type)) {
            auto num_non_null = v2->num_values - v2->num_nulls;

            iobuf rep_level_bytes;
            if (v2->repetition_levels_byte_length > 0) {
                rep_level_bytes = parser.copy(
                  v2->repetition_levels_byte_length);
            }
            iobuf def_level_bytes;
            if (v2->definition_levels_byte_length > 0) {
                def_level_bytes = parser.copy(
                  v2->definition_levels_byte_length);
            }

            auto data_size = header.compressed_page_size
                             - v2->repetition_levels_byte_length
                             - v2->definition_levels_byte_length;
            iobuf encoded_data = parser.copy(data_size);

            if (v2->is_compressed) {
                auto uncompressed_data_size
                  = header.uncompressed_page_size
                    - v2->repetition_levels_byte_length
                    - v2->definition_levels_byte_length;
                encoded_data = co_await decompress_data(
                  std::move(encoded_data), meta.codec, uncompressed_data_size);
            }

            if (v2->repetition_levels_byte_length > 0) {
                iobuf_parser rep_parser(std::move(rep_level_bytes));
                auto levels = decode_levels(
                  rep_parser,
                  v2->num_values,
                  v2->repetition_levels_byte_length,
                  schema_elem.max_repetition_level);
                for (auto& l : levels) {
                    all_rep_levels.push_back(l);
                }
            } else {
                for (int32_t i = 0; i < v2->num_values; ++i) {
                    all_rep_levels.push_back(rep_level(0));
                }
            }

            if (v2->definition_levels_byte_length > 0) {
                iobuf_parser def_parser(std::move(def_level_bytes));
                auto levels = decode_levels(
                  def_parser,
                  v2->num_values,
                  v2->definition_levels_byte_length,
                  schema_elem.max_definition_level);
                for (auto& l : levels) {
                    all_def_levels.push_back(l);
                }
            } else {
                for (int32_t i = 0; i < v2->num_values; ++i) {
                    all_def_levels.push_back(schema_elem.max_definition_level);
                }
            }

            decode_data_values(
              arr,
              std::move(encoded_data),
              v2->data_encoding,
              num_non_null,
              dictionary);
            arr.length += v2->num_values;
            co_await ss::coroutine::maybe_yield();
            continue;
        }

        if (auto* v1 = std::get_if<data_page_header_v1>(&header.type)) {
            iobuf page_bytes = parser.copy(header.compressed_page_size);
            if (meta.codec != compression_codec::uncompressed) {
                page_bytes = co_await decompress_data(
                  std::move(page_bytes),
                  meta.codec,
                  header.uncompressed_page_size);
            }
            iobuf_parser body(std::move(page_bytes));

            // V1: levels are prefixed with a 4-byte LE length.
            if (schema_elem.max_repetition_level > rep_level(0)) {
                auto rep_len = ss::le_to_cpu(body.consume_type<int32_t>());
                iobuf rep_bytes = body.copy(rep_len);
                iobuf_parser rep_parser(std::move(rep_bytes));
                auto levels = decode_levels(
                  rep_parser,
                  v1->num_values,
                  rep_len,
                  schema_elem.max_repetition_level);
                for (auto& l : levels) {
                    all_rep_levels.push_back(l);
                }
            } else {
                for (int32_t i = 0; i < v1->num_values; ++i) {
                    all_rep_levels.push_back(rep_level(0));
                }
            }

            int32_t num_nulls = 0;
            if (schema_elem.max_definition_level > def_level(0)) {
                auto def_len = ss::le_to_cpu(body.consume_type<int32_t>());
                iobuf def_bytes = body.copy(def_len);
                iobuf_parser def_parser(std::move(def_bytes));
                auto levels = decode_levels(
                  def_parser,
                  v1->num_values,
                  def_len,
                  schema_elem.max_definition_level);
                for (auto& l : levels) {
                    if (l < schema_elem.max_definition_level) {
                        ++num_nulls;
                    }
                    all_def_levels.push_back(l);
                }
            } else {
                for (int32_t i = 0; i < v1->num_values; ++i) {
                    all_def_levels.push_back(schema_elem.max_definition_level);
                }
            }

            auto num_non_null = v1->num_values - num_nulls;
            iobuf remaining_data = body.copy(body.bytes_left());

            decode_data_values(
              arr,
              std::move(remaining_data),
              v1->data_encoding,
              num_non_null,
              dictionary);
            arr.length += v1->num_values;
            co_await ss::coroutine::maybe_yield();
            continue;
        }

        // Skip unknown page types (e.g. index pages).
        parser.skip(header.compressed_page_size);
    }

    co_return column_chunk_data{
      .values = std::move(arr),
      .def_levels = std::move(all_def_levels),
      .rep_levels = std::move(all_rep_levels),
    };
}

} // namespace serde::parquet
