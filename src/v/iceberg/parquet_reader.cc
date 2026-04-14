/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/parquet_reader.h"

#include "container/chunked_hash_map.h"
#include "iceberg/datatypes.h"
#include "serde/parquet/column_chunk_reader.h"
#include "serde/parquet/flattened_schema.h"
#include "serde/parquet/metadata.h"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/variant_utils.hh>

#include <cstdint>
#include <stdexcept>

namespace iceberg {

namespace {

namespace sp = serde::parquet;

struct file_leaf_info {
    int32_t leaf_index;
    const sp::schema_element* element;
};

struct read_from_file {
    int32_t file_leaf_index;
    const sp::schema_element* file_leaf;
    sp::physical_type table_ptype;
};

struct fill_null {
    sp::physical_type ptype;
    sp::logical_type ltype;
};

using column_action = std::variant<read_from_file, fill_null>;

/// Map an Iceberg primitive_type to the corresponding parquet physical and
/// logical types. Mirrors the mapping in schema_parquet.cc.
std::pair<sp::physical_type, sp::logical_type>
iceberg_to_parquet_types(const primitive_type& pt) {
    return std::visit(
      [](const auto& t) -> std::pair<sp::physical_type, sp::logical_type> {
          using T = std::decay_t<decltype(t)>;
          if constexpr (std::is_same_v<T, boolean_type>) {
              return {sp::bool_type{}, {}};
          } else if constexpr (std::is_same_v<T, int_type>) {
              return {
                sp::i32_type{},
                sp::int_type{.bit_width = 32, .is_signed = true}};
          } else if constexpr (std::is_same_v<T, long_type>) {
              return {
                sp::i64_type{},
                sp::int_type{.bit_width = 64, .is_signed = true}};
          } else if constexpr (std::is_same_v<T, float_type>) {
              return {sp::f32_type{}, {}};
          } else if constexpr (std::is_same_v<T, double_type>) {
              return {sp::f64_type{}, {}};
          } else if constexpr (std::is_same_v<T, decimal_type>) {
              return {
                sp::byte_array_type{.fixed_length = 16},
                sp::decimal_type{
                  .scale = static_cast<int32_t>(t.scale),
                  .precision = static_cast<int32_t>(t.precision)}};
          } else if constexpr (std::is_same_v<T, date_type>) {
              return {sp::i32_type{}, sp::date_type{}};
          } else if constexpr (std::is_same_v<T, time_type>) {
              return {
                sp::i64_type{},
                sp::time_type{
                  .is_adjusted_to_utc = false, .unit = sp::time_unit::micros}};
          } else if constexpr (std::is_same_v<T, timestamp_type>) {
              return {
                sp::i64_type{},
                sp::timestamp_type{
                  .is_adjusted_to_utc = false, .unit = sp::time_unit::micros}};
          } else if constexpr (std::is_same_v<T, timestamptz_type>) {
              return {
                sp::i64_type{},
                sp::timestamp_type{
                  .is_adjusted_to_utc = true, .unit = sp::time_unit::micros}};
          } else if constexpr (std::is_same_v<T, string_type>) {
              return {sp::byte_array_type{}, sp::string_type{}};
          } else if constexpr (std::is_same_v<T, uuid_type>) {
              return {sp::byte_array_type{.fixed_length = 16}, sp::uuid_type{}};
          } else if constexpr (std::is_same_v<T, fixed_type>) {
              return {sp::byte_array_type{.fixed_length = t.length}, {}};
          } else if constexpr (std::is_same_v<T, binary_type>) {
              return {sp::byte_array_type{}, {}};
          } else {
              // variant_type or unknown — not supported as a missing column
              throw std::runtime_error(
                "unsupported iceberg type for null fill");
          }
      },
      pt);
}

sp::column_array make_empty_column_array_for_type(
  const sp::physical_type& ptype, const sp::logical_type& ltype) {
    sp::column_array arr;
    arr.ptype = ptype;
    arr.ltype = ltype;
    ss::visit(
      ptype,
      [&](const std::monostate&) {
          arr.data.emplace<sp::column_array::boolean_data>();
      },
      [&](const sp::bool_type&) {
          arr.data.emplace<sp::column_array::boolean_data>();
      },
      [&](const sp::i32_type&) {
          arr.data.emplace<sp::column_array::i32_data>();
      },
      [&](const sp::i64_type&) {
          arr.data.emplace<sp::column_array::i64_data>();
      },
      [&](const sp::f32_type&) {
          arr.data.emplace<sp::column_array::f32_data>();
      },
      [&](const sp::f64_type&) {
          arr.data.emplace<sp::column_array::f64_data>();
      },
      [&](const sp::byte_array_type& t) {
          if (t.fixed_length.has_value()) {
              arr.data.emplace<sp::column_array::fixed_byte_array_data>(
                sp::column_array::fixed_byte_array_data{
                  .fixed_length = *t.fixed_length});
          } else {
              auto& d = arr.data.emplace<sp::column_array::byte_array_data>();
              d.offsets.push_back(0);
          }
      });
    return arr;
}

sp::column_chunk_data
make_null_column(const fill_null& spec, int64_t num_rows) {
    auto arr = make_empty_column_array_for_type(spec.ptype, spec.ltype);
    arr.length = num_rows;

    chunked_vector<sp::def_level> def_levels;
    chunked_vector<sp::rep_level> rep_levels;
    def_levels.reserve(num_rows);
    rep_levels.reserve(num_rows);
    for (int64_t i = 0; i < num_rows; ++i) {
        def_levels.push_back(sp::def_level(0));
        rep_levels.push_back(sp::rep_level(0));
    }

    return sp::column_chunk_data{
      .values = std::move(arr),
      .def_levels = std::move(def_levels),
      .rep_levels = std::move(rep_levels),
    };
}

/// Filter a chunked_vector, keeping only elements where keep[i] is true.
template<typename T>
chunked_vector<T>
filter_vec(const chunked_vector<T>& src, const chunked_vector<bool>& keep) {
    chunked_vector<T> out;
    for (size_t i = 0; i < src.size(); ++i) {
        if (keep[i]) {
            out.push_back(src[i]);
        }
    }
    return out;
}

/// Filter values from a column that stores only non-null entries.
/// `keep` is indexed by row; def_levels maps rows to value indices.
/// Returns a new container of kept non-null values.
template<typename T>
chunked_vector<T> filter_values(
  const chunked_vector<T>& values,
  const chunked_vector<sp::def_level>& def_levels,
  sp::def_level max_def,
  const chunked_vector<bool>& keep) {
    chunked_vector<T> out;
    size_t val_idx = 0;
    for (size_t row = 0; row < def_levels.size(); ++row) {
        bool has_value = def_levels[row] == max_def;
        if (keep[row] && has_value) {
            out.push_back(values[val_idx]);
        }
        if (has_value) {
            ++val_idx;
        }
    }
    return out;
}

/// Filter boolean packed bits, keeping only bits at non-null, kept positions.
sp::column_array::boolean_data filter_boolean_data(
  const sp::column_array::boolean_data& src,
  const chunked_vector<sp::def_level>& def_levels,
  sp::def_level max_def,
  const chunked_vector<bool>& keep) {
    sp::column_array::boolean_data out;

    // Linearize source bits for random access.
    auto src_bytes = iobuf_to_bytes(src.packed_bits);
    auto get_bit = [&](size_t idx) -> bool {
        return (src_bytes[idx / 8] >> (idx % 8)) & 1;
    };

    // Build filtered packed bits.
    chunked_vector<uint8_t> tmp;
    uint8_t cur_byte = 0;
    int bit_pos = 0;
    int64_t kept = 0;

    size_t val_idx = 0;
    for (size_t row = 0; row < def_levels.size(); ++row) {
        bool has_value = def_levels[row] == max_def;
        if (keep[row] && has_value) {
            if (get_bit(val_idx)) {
                cur_byte |= static_cast<uint8_t>(1 << bit_pos);
            }
            ++bit_pos;
            ++kept;
            if (bit_pos == 8) {
                tmp.push_back(cur_byte);
                cur_byte = 0;
                bit_pos = 0;
            }
        }
        if (has_value) {
            ++val_idx;
        }
    }
    if (bit_pos > 0) {
        tmp.push_back(cur_byte);
    }

    iobuf result;
    for (auto b : tmp) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        result.append(reinterpret_cast<const char*>(&b), 1);
    }
    out.packed_bits = std::move(result);
    out.num_values = kept;
    return out;
}

/// Filter byte_array_data, keeping only values at non-null, kept positions.
sp::column_array::byte_array_data filter_byte_array_data(
  sp::column_array::byte_array_data& src,
  const chunked_vector<sp::def_level>& def_levels,
  sp::def_level max_def,
  const chunked_vector<bool>& keep) {
    sp::column_array::byte_array_data out;
    out.offsets.push_back(0);
    int64_t current_offset = 0;

    size_t val_idx = 0;
    for (size_t row = 0; row < def_levels.size(); ++row) {
        bool has_value = def_levels[row] == max_def;
        if (keep[row] && has_value) {
            auto start = src.offsets[val_idx];
            auto len = src.offsets[val_idx + 1] - start;
            if (len > 0) {
                out.data.append(src.data.share(start, len));
            }
            current_offset += len;
            out.offsets.push_back(current_offset);
        }
        if (has_value) {
            ++val_idx;
        }
    }
    return out;
}

/// Filter fixed_byte_array_data, keeping only values at non-null, kept
/// positions.
sp::column_array::fixed_byte_array_data filter_fixed_byte_array_data(
  sp::column_array::fixed_byte_array_data& src,
  const chunked_vector<sp::def_level>& def_levels,
  sp::def_level max_def,
  const chunked_vector<bool>& keep) {
    sp::column_array::fixed_byte_array_data out;
    out.fixed_length = src.fixed_length;

    size_t val_idx = 0;
    for (size_t row = 0; row < def_levels.size(); ++row) {
        bool has_value = def_levels[row] == max_def;
        if (keep[row] && has_value) {
            int64_t start = static_cast<int64_t>(val_idx) * src.fixed_length;
            out.data.append(src.data.share(start, src.fixed_length));
        }
        if (has_value) {
            ++val_idx;
        }
    }
    return out;
}

/// Filter a column_array by a row-level keep mask, respecting def_levels
/// to correctly map row indices to value indices.
sp::column_array filter_column(
  sp::column_array& arr,
  const chunked_vector<sp::def_level>& def_levels,
  sp::def_level max_def,
  const chunked_vector<bool>& keep,
  int64_t kept_count) {
    sp::column_array out;
    out.ptype = arr.ptype;
    out.ltype = arr.ltype;
    out.length = kept_count;

    ss::visit(
      arr.data,
      [&](sp::column_array::boolean_data& d) {
          out.data = filter_boolean_data(d, def_levels, max_def, keep);
      },
      [&](sp::column_array::i32_data& d) {
          out.data = sp::column_array::i32_data{
            filter_values(d.values, def_levels, max_def, keep)};
      },
      [&](sp::column_array::i64_data& d) {
          out.data = sp::column_array::i64_data{
            filter_values(d.values, def_levels, max_def, keep)};
      },
      [&](sp::column_array::f32_data& d) {
          out.data = sp::column_array::f32_data{
            filter_values(d.values, def_levels, max_def, keep)};
      },
      [&](sp::column_array::f64_data& d) {
          out.data = sp::column_array::f64_data{
            filter_values(d.values, def_levels, max_def, keep)};
      },
      [&](sp::column_array::byte_array_data& d) {
          out.data = filter_byte_array_data(d, def_levels, max_def, keep);
      },
      [&](sp::column_array::fixed_byte_array_data& d) {
          out.data = filter_fixed_byte_array_data(d, def_levels, max_def, keep);
      });
    return out;
}

/// Collect the max_definition_level for each leaf column in schema order.
chunked_vector<sp::def_level>
leaf_max_def_levels(const sp::schema_element& schema) {
    chunked_vector<sp::def_level> result;
    schema.for_each([&](const sp::schema_element& elem) {
        if (elem.is_leaf()) {
            result.push_back(elem.max_definition_level);
        }
    });
    return result;
}

/// Remove rows from a batch at positions listed in deleted_positions.
/// row_offset is the file-global index of the first row in this batch.
void apply_position_deletes(
  sp::columnar_batch& batch,
  const chunked_hash_set<int64_t>& deleted_positions,
  int64_t row_offset,
  const chunked_vector<sp::def_level>& max_def_levels) {
    auto num_rows = batch.num_rows;
    chunked_vector<bool> keep;
    keep.reserve(num_rows);
    int64_t kept_count = 0;
    for (int64_t i = 0; i < num_rows; ++i) {
        bool should_keep = !deleted_positions.contains(row_offset + i);
        keep.push_back(should_keep);
        if (should_keep) {
            ++kept_count;
        }
    }

    if (kept_count == num_rows) {
        return;
    }

    for (size_t col = 0; col < batch.columns.size(); ++col) {
        batch.columns[col] = filter_column(
          batch.columns[col],
          batch.levels[col].def_levels,
          max_def_levels[col],
          keep,
          kept_count);
        batch.levels[col].def_levels = filter_vec(
          batch.levels[col].def_levels, keep);
        batch.levels[col].rep_levels = filter_vec(
          batch.levels[col].rep_levels, keep);
    }
    batch.num_rows = kept_count;
}

/// Promote a decoded column from file type to table type.
/// Only int32->int64 and float32->float64 are valid Iceberg promotions.
void promote_column(
  sp::column_array& arr, const sp::physical_type& table_type) {
    if (arr.ptype == table_type) {
        return;
    }
    if (
      std::holds_alternative<sp::i32_type>(arr.ptype)
      && std::holds_alternative<sp::i64_type>(table_type)) {
        auto* src = std::get_if<sp::column_array::i32_data>(&arr.data);
        if (!src) {
            return;
        }
        sp::column_array::i64_data promoted;
        promoted.values.reserve(src->values.size());
        for (const auto& v : src->values) {
            promoted.values.push_back(static_cast<int64_t>(v));
        }
        arr.data = std::move(promoted);
        arr.ptype = sp::i64_type{};
        return;
    }
    if (
      std::holds_alternative<sp::f32_type>(arr.ptype)
      && std::holds_alternative<sp::f64_type>(table_type)) {
        auto* src = std::get_if<sp::column_array::f32_data>(&arr.data);
        if (!src) {
            return;
        }
        sp::column_array::f64_data promoted;
        promoted.values.reserve(src->values.size());
        for (const auto& v : src->values) {
            promoted.values.push_back(static_cast<double>(v));
        }
        arr.data = std::move(promoted);
        arr.ptype = sp::f64_type{};
        return;
    }
    throw std::runtime_error(
      "incompatible type: file and table schemas have different "
      "physical types for the same field ID, and the types are not "
      "promotable");
}

/// Build a map from field_id to leaf info by walking the file schema
/// depth-first.
chunked_hash_map<int32_t, file_leaf_info>
build_file_leaf_index(const sp::schema_element& file_schema) {
    chunked_hash_map<int32_t, file_leaf_info> index;
    int32_t leaf_idx = 0;
    file_schema.for_each([&](const sp::schema_element& elem) {
        if (!elem.is_leaf()) {
            return;
        }
        if (!elem.field_id.has_value()) {
            throw std::runtime_error(
              fmt::format(
                "file schema leaf '{}' has no field_id", elem.name()));
        }
        auto [_, inserted] = index.emplace(
          *elem.field_id, file_leaf_info{leaf_idx, &elem});
        if (!inserted) {
            throw std::runtime_error(
              fmt::format(
                "duplicate field_id {} in file schema", *elem.field_id));
        }
        ++leaf_idx;
    });
    return index;
}

/// Walk the table schema depth-first, matching leaves by field ID against the
/// file schema index. Populates `actions` with per-leaf read or null-fill
/// instructions, and returns a schema_element tree mirroring the table schema
/// structure.
struct table_walker {
    const chunked_hash_map<int32_t, file_leaf_info>& file_index;
    chunked_vector<column_action>& actions;

    sp::schema_element walk_field(const nested_field& field) {
        auto rep = field.required ? sp::field_repetition_type::required
                                  : sp::field_repetition_type::optional;
        return std::visit(
          [&](const auto& t) { return walk_type(field, t, rep); }, field.type);
    }

    sp::schema_element walk_type(
      const nested_field& field,
      const primitive_type& pt,
      sp::field_repetition_type rep) {
        int32_t field_id = field.id;
        auto [table_ptype, table_ltype] = iceberg_to_parquet_types(pt);
        auto it = file_index.find(field_id);
        if (it != file_index.end()) {
            const auto& info = it->second;
            actions.push_back(
              read_from_file{
                .file_leaf_index = info.leaf_index,
                .file_leaf = info.element,
                .table_ptype = table_ptype,
              });
            return sp::schema_element{
              .type = table_ptype,
              .repetition_type = rep,
              .path = {field.name},
              .field_id = field_id,
              .logical_type = table_ltype,
            };
        }
        auto [ptype, ltype] = iceberg_to_parquet_types(pt);
        actions.push_back(
          fill_null{
            .ptype = ptype,
            .ltype = ltype,
          });
        return sp::schema_element{
          .type = ptype,
          .repetition_type = sp::field_repetition_type::optional,
          .path = {field.name},
          .field_id = field_id,
          .logical_type = ltype,
        };
    }

    sp::schema_element walk_type(
      const nested_field& field,
      const struct_type& st,
      sp::field_repetition_type rep) {
        sp::schema_element group;
        group.repetition_type = rep;
        group.path.emplace_back(field.name);
        group.field_id = static_cast<int32_t>(field.id);
        for (const auto& child : st.fields) {
            group.children.push_back(walk_field(*child));
        }
        return group;
    }

    sp::schema_element walk_type(
      const nested_field& field,
      const list_type& lt,
      sp::field_repetition_type rep) {
        sp::schema_element list_node;
        list_node.logical_type = sp::list_type{};
        list_node.repetition_type = rep;
        list_node.path.emplace_back(field.name);
        list_node.field_id = static_cast<int32_t>(field.id);

        sp::schema_element list_wrapper;
        list_wrapper.repetition_type = sp::field_repetition_type::repeated;
        list_wrapper.path.emplace_back("list");

        list_wrapper.children.push_back(walk_field(*lt.element_field));
        list_node.children.push_back(std::move(list_wrapper));
        return list_node;
    }

    sp::schema_element walk_type(
      const nested_field& field,
      const map_type& mt,
      sp::field_repetition_type rep) {
        sp::schema_element map_node;
        map_node.repetition_type = rep;
        map_node.path.emplace_back(field.name);
        map_node.field_id = static_cast<int32_t>(field.id);

        sp::schema_element kv_wrapper;
        kv_wrapper.repetition_type = sp::field_repetition_type::repeated;
        kv_wrapper.path.emplace_back("key_value");

        kv_wrapper.children.push_back(walk_field(*mt.key_field));
        kv_wrapper.children.push_back(walk_field(*mt.value_field));
        map_node.children.push_back(std::move(kv_wrapper));
        return map_node;
    }
};

} // namespace

ss::future<parquet_reader_result> read_parquet(
  const struct_type& read_schema,
  sp::file_io& io,
  chunked_vector<delete_file_entry> delete_files) {
    // Step 1: Read and parse footer.
    auto file_size = io.size();
    auto tail = co_await io.read(file_size - 8, 8);
    auto loc = sp::parse_footer_location(tail, file_size);
    auto footer_bytes = co_await io.read(loc.offset, loc.length);
    auto metadata = sp::decode(
      std::move(footer_bytes), sp::file_metadata_tag{});
    auto file_schema = sp::unflatten(metadata.schema);
    sp::index_schema(file_schema);

    // Step 2: Build field-ID column mapping.
    auto file_index = build_file_leaf_index(file_schema);

    chunked_vector<column_action> actions;
    table_walker walker{file_index, actions};

    // Build the result schema from the table schema structure.
    sp::schema_element result_schema;
    result_schema.repetition_type = sp::field_repetition_type::required;
    result_schema.path.emplace_back("root");
    for (const auto& field : read_schema.fields) {
        result_schema.children.push_back(walker.walk_field(*field));
    }
    sp::index_schema(result_schema);

    // Collect position deletes into a lookup set.
    chunked_hash_set<int64_t> deleted_positions;
    for (auto& entry : delete_files) {
        if (auto* pds = std::get_if<position_delete_set>(&entry)) {
            for (auto pos : pds->positions) {
                deleted_positions.insert(pos);
            }
        }
    }

    // Precompute per-leaf max definition levels for position delete filtering.
    auto max_def_levels = leaf_max_def_levels(result_schema);

    // Step 3: Read column chunks per row group.
    chunked_vector<sp::columnar_batch> row_group_batches;
    int64_t row_offset = 0;
    for (const auto& rg : metadata.row_groups) {
        sp::columnar_batch batch;
        batch.num_rows = rg.num_rows;

        for (const auto& action : actions) {
            sp::column_chunk_data col_data;
            if (const auto* r = std::get_if<read_from_file>(&action)) {
                const auto& cc = rg.columns[r->file_leaf_index];
                auto offset = cc.meta_data.dictionary_page_offset.has_value()
                                ? std::min(
                                    *cc.meta_data.dictionary_page_offset,
                                    cc.meta_data.data_page_offset)
                                : cc.meta_data.data_page_offset;
                auto end = cc.meta_data.data_page_offset
                           + cc.meta_data.total_compressed_size;
                auto length = end - offset;

                auto col_bytes = co_await io.read(offset, length);
                col_data = co_await sp::decode_column_chunk(
                  std::move(col_bytes), cc.meta_data, *r->file_leaf);
                promote_column(col_data.values, r->table_ptype);
            } else {
                const auto& spec = std::get<fill_null>(action);
                col_data = make_null_column(spec, rg.num_rows);
            }

            batch.columns.push_back(std::move(col_data.values));
            batch.levels.push_back(
              sp::columnar_batch::level_data{
                .def_levels = std::move(col_data.def_levels),
                .rep_levels = std::move(col_data.rep_levels),
              });
            co_await ss::coroutine::maybe_yield();
        }

        if (!deleted_positions.empty()) {
            apply_position_deletes(
              batch, deleted_positions, row_offset, max_def_levels);
        }
        row_offset += rg.num_rows;

        row_group_batches.push_back(std::move(batch));
    }

    co_return parquet_reader_result{
      .schema = std::move(result_schema),
      .row_groups = std::move(row_group_batches),
    };
}

} // namespace iceberg
