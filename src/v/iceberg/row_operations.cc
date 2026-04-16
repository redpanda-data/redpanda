/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/row_operations.h"

#include "bytes/hash.h"
#include "container/chunked_hash_map.h"
#include "iceberg/equality_delete_file.h"
#include "iceberg/logger.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_list.h"
#include "iceberg/position_delete_file.h"
#include "serde/parquet/encoding.h"
#include "serde/parquet/reader.h"
#include "serde/parquet/value.h"

#include <cmath>

namespace iceberg {

namespace {

namespace sp = serde::parquet;

const schema& current_schema(const table_metadata& table) {
    auto* s = table.get_schema(table.current_schema_id);
    vassert(s, "table has no schema for current_schema_id");
    return *s;
}

struct key_field_info {
    nested_field::id_t field_id;
    // Path of field names from root to the key field.
    chunked_vector<ss::sstring> path;
    // Top-level field index in the schema (for stats access).
    size_t top_level_idx;
};

bool find_field_path(
  const struct_type& st,
  nested_field::id_t target,
  chunked_vector<ss::sstring>& path) {
    for (const auto& f : st.fields) {
        if (!f) {
            continue;
        }
        path.push_back(f->name);
        if (f->id == target) {
            return true;
        }
        if (auto* inner = std::get_if<struct_type>(&f->type)) {
            if (find_field_path(*inner, target, path)) {
                return true;
            }
        }
        path.pop_back();
    }
    return false;
}

chunked_vector<key_field_info> resolve_key_fields(
  const schema& sch, const chunked_vector<nested_field::id_t>& key_field_ids) {
    chunked_vector<key_field_info> result;
    result.reserve(key_field_ids.size());
    for (auto id : key_field_ids) {
        chunked_vector<ss::sstring> path;
        bool found = find_field_path(sch.schema_struct, id, path);
        vassert(found, "key field id {} not found in schema", id());
        // Top-level index: find which top-level field the path starts in.
        size_t top_idx = 0;
        for (size_t i = 0; i < sch.schema_struct.fields.size(); ++i) {
            if (sch.schema_struct.fields[i]->name == path[0]) {
                top_idx = i;
                break;
            }
        }
        result.push_back(
          key_field_info{
            .field_id = id,
            .path = std::move(path),
            .top_level_idx = top_idx,
          });
    }
    return result;
}

size_t hash_value(const sp::value& v) {
    return std::visit(
      [](const auto& x) -> size_t {
          using T = std::decay_t<decltype(x)>;
          if constexpr (std::is_same_v<T, sp::null_value>) {
              return 0;
          } else if constexpr (std::is_same_v<T, sp::boolean_value>) {
              return std::hash<bool>{}(x.val);
          } else if constexpr (std::is_same_v<T, sp::int32_value>) {
              return std::hash<int32_t>{}(x.val);
          } else if constexpr (std::is_same_v<T, sp::int64_value>) {
              return std::hash<int64_t>{}(x.val);
          } else if constexpr (std::is_same_v<T, sp::float32_value>) {
              return std::hash<float>{}(x.val);
          } else if constexpr (std::is_same_v<T, sp::float64_value>) {
              return std::hash<double>{}(x.val);
          } else if constexpr (
            std::is_same_v<T, sp::byte_array_value>
            || std::is_same_v<T, sp::fixed_byte_array_value>) {
              return std::hash<iobuf>{}(x.val);
          } else {
              return 0;
          }
      },
      v);
}

struct hashable_key {
    sp::group_value val;

    bool operator==(const hashable_key& other) const {
        return val == other.val;
    }

    template<typename H>
    friend H AbslHashValue(H h, const hashable_key& k) {
        for (const auto& member : k.val) {
            h = H::combine(std::move(h), hash_value(member.field));
        }
        return h;
    }
};

std::optional<sp::value>
decode_stats_bound(const iobuf& encoded, const field_type& type) {
    if (encoded.size_bytes() == 0) {
        return std::nullopt;
    }
    auto* prim = std::get_if<primitive_type>(&type);
    if (!prim) {
        return std::nullopt;
    }
    return std::visit(
      [&](const auto& t) -> std::optional<sp::value> {
          using T = std::decay_t<decltype(t)>;
          if constexpr (
            std::is_same_v<T, int_type> || std::is_same_v<T, date_type>) {
              return sp::decode_stats_int32(encoded);
          } else if constexpr (
            std::is_same_v<T, long_type> || std::is_same_v<T, time_type>
            || std::is_same_v<T, timestamp_type>
            || std::is_same_v<T, timestamptz_type>) {
              return sp::decode_stats_int64(encoded);
          } else if constexpr (std::is_same_v<T, float_type>) {
              return sp::decode_stats_float32(encoded);
          } else if constexpr (std::is_same_v<T, double_type>) {
              return sp::decode_stats_float64(encoded);
          } else if constexpr (
            std::is_same_v<T, string_type> || std::is_same_v<T, binary_type>
            || std::is_same_v<T, fixed_type>) {
              return sp::decode_stats_byte_array(encoded);
          } else if constexpr (std::is_same_v<T, boolean_type>) {
              return sp::decode_stats_boolean(encoded);
          } else {
              return std::nullopt;
          }
      },
      *prim);
}

bool is_nan_value(const sp::value& v) {
    if (auto* f = std::get_if<sp::float32_value>(&v)) {
        return std::isnan(f->val);
    }
    if (auto* d = std::get_if<sp::float64_value>(&v)) {
        return std::isnan(d->val);
    }
    return false;
}

const iobuf* get_bytes(const sp::value& v) {
    if (auto* ba = std::get_if<sp::byte_array_value>(&v)) {
        return &ba->val;
    }
    if (auto* fba = std::get_if<sp::fixed_byte_array_value>(&v)) {
        return &fba->val;
    }
    return nullptr;
}

std::optional<int> compare_values(const sp::value& a, const sp::value& b) {
    if (is_nan_value(a) || is_nan_value(b)) {
        return std::nullopt;
    }
    const iobuf* a_bytes = get_bytes(a);
    const iobuf* b_bytes = get_bytes(b);
    if (a_bytes && b_bytes) {
        auto cmp = *a_bytes <=> *b_bytes;
        return cmp < 0 ? -1 : (cmp > 0 ? 1 : 0);
    }
    if (a.index() != b.index()) {
        return std::nullopt;
    }
    return std::visit(
      [&](const auto& av) -> std::optional<int> {
          using T = std::decay_t<decltype(av)>;
          auto* bv = std::get_if<T>(&b);
          if (!bv) {
              return std::nullopt;
          }
          if constexpr (std::is_same_v<T, sp::int32_value>) {
              return av.val < bv->val ? -1 : (av.val > bv->val ? 1 : 0);
          } else if constexpr (std::is_same_v<T, sp::int64_value>) {
              return av.val < bv->val ? -1 : (av.val > bv->val ? 1 : 0);
          } else if constexpr (std::is_same_v<T, sp::float32_value>) {
              return av.val < bv->val ? -1 : (av.val > bv->val ? 1 : 0);
          } else if constexpr (std::is_same_v<T, sp::float64_value>) {
              return av.val < bv->val ? -1 : (av.val > bv->val ? 1 : 0);
          } else if constexpr (std::is_same_v<T, sp::boolean_value>) {
              return static_cast<int>(av.val) - static_cast<int>(bv->val);
          } else {
              return std::nullopt;
          }
      },
      a);
}

struct key_field_bounds {
    nested_field::id_t field_id;
    size_t schema_idx;
    std::optional<sp::value> min_val;
    std::optional<sp::value> max_val;
};

chunked_vector<key_field_bounds> compute_key_bounds(
  const chunked_vector<key_field_info>& key_fields,
  const chunked_vector<sp::group_value>& new_keys) {
    chunked_vector<key_field_bounds> bounds;
    for (size_t ki = 0; ki < key_fields.size(); ++ki) {
        key_field_bounds fb{
          .field_id = key_fields[ki].field_id,
          .schema_idx = key_fields[ki].top_level_idx};
        std::optional<size_t> min_idx;
        std::optional<size_t> max_idx;
        for (size_t i = 0; i < new_keys.size(); ++i) {
            const auto& val = new_keys[i][ki].field;
            if (
              std::holds_alternative<sp::null_value>(val)
              || is_nan_value(val)) {
                continue;
            }
            if (
              !min_idx
              || compare_values(val, new_keys[*min_idx][ki].field).value_or(0)
                   < 0) {
                min_idx = i;
            }
            if (
              !max_idx
              || compare_values(val, new_keys[*max_idx][ki].field).value_or(0)
                   > 0) {
                max_idx = i;
            }
        }
        if (min_idx) {
            fb.min_val = sp::copy(new_keys[*min_idx][ki].field);
        }
        if (max_idx) {
            fb.max_val = sp::copy(new_keys[*max_idx][ki].field);
        }
        bounds.push_back(std::move(fb));
    }
    return bounds;
}

bool may_contain_keys(
  const data_file& df,
  const schema& sch,
  const chunked_vector<key_field_bounds>& key_bounds) {
    if (!df.lower_bounds.has_value() || !df.upper_bounds.has_value()) {
        return true;
    }
    for (const auto& kb : key_bounds) {
        if (!kb.min_val || !kb.max_val) {
            continue;
        }
        auto lb_it = df.lower_bounds->find(kb.field_id);
        auto ub_it = df.upper_bounds->find(kb.field_id);
        if (
          lb_it == df.lower_bounds->end() || ub_it == df.upper_bounds->end()) {
            continue;
        }
        if (
          kb.schema_idx >= sch.schema_struct.fields.size()
          || !sch.schema_struct.fields[kb.schema_idx]) {
            continue;
        }
        const auto& ftype = sch.schema_struct.fields[kb.schema_idx]->type;
        auto file_lower = decode_stats_bound(lb_it->second, ftype);
        auto file_upper = decode_stats_bound(ub_it->second, ftype);
        if (!file_lower || !file_upper) {
            continue;
        }
        auto cmp_upper = compare_values(*file_upper, *kb.min_val);
        if (cmp_upper && *cmp_upper < 0) {
            return false;
        }
        auto cmp_lower = compare_values(*file_lower, *kb.max_val);
        if (cmp_lower && *cmp_lower > 0) {
            return false;
        }
    }
    return true;
}

sp::reader_options
make_key_projection(const chunked_vector<key_field_info>& key_fields) {
    sp::reader_options opts;
    for (const auto& kf : key_fields) {
        chunked_vector<ss::sstring> path;
        for (const auto& p : kf.path) {
            path.push_back(p);
        }
        opts.column_projection.push_back(std::move(path));
    }
    return opts;
}

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
              throw std::runtime_error(
                "unsupported iceberg type for parquet mapping");
          }
      },
      pt);
}

} // namespace

ss::future<chunked_vector<sp::group_value>> extract_keys(
  const table_metadata& table,
  const chunked_vector<file_to_append>& files,
  const chunked_vector<nested_field::id_t>& key_field_ids,
  read_file_fn read_file) {
    const auto& sch = current_schema(table);
    auto key_fields = resolve_key_fields(sch, key_field_ids);

    chunked_vector<sp::group_value> keys;
    for (const auto& f : files) {
        auto file_data = co_await read_file(f.file.file_path);
        auto records = co_await sp::read_file_as_records(
          std::move(file_data), make_key_projection(key_fields));
        for (auto& record : records) {
            keys.push_back(std::move(record));
        }
    }
    co_return keys;
}

ss::future<chunked_vector<position_delete_entry>> find_matching_positions(
  const table_metadata& table,
  manifest_io& mio,
  const chunked_vector<sp::group_value>& keys,
  const chunked_vector<nested_field::id_t>& key_field_ids,
  const chunked_hash_set<ss::sstring>& exclude_file_paths,
  read_file_fn read_file) {
    if (keys.empty()) {
        co_return chunked_vector<position_delete_entry>{};
    }

    const auto& sch = current_schema(table);
    auto key_fields = resolve_key_fields(sch, key_field_ids);

    chunked_hash_set<hashable_key> key_set;
    for (const auto& k : keys) {
        sp::group_value key_copy;
        for (const auto& m : k) {
            key_copy.push_back(sp::group_member{sp::copy(m.field)});
        }
        key_set.insert(hashable_key{std::move(key_copy)});
    }

    auto key_bounds = compute_key_bounds(key_fields, keys);

    if (!table.current_snapshot_id.has_value()) {
        co_return chunked_vector<position_delete_entry>{};
    }

    const snapshot* snap = nullptr;
    if (table.snapshots.has_value()) {
        for (const auto& s : *table.snapshots) {
            if (s.id == *table.current_snapshot_id) {
                snap = &s;
                break;
            }
        }
    }
    if (!snap) {
        co_return chunked_vector<position_delete_entry>{};
    }

    auto mlist_res = co_await mio.download_manifest_list(
      snap->manifest_list_path);
    if (mlist_res.has_error()) {
        vlog(
          iceberg::log.warn,
          "failed to download manifest list for upsert planning");
        co_return chunked_vector<position_delete_entry>{};
    }

    struct candidate_file {
        uri file_path;
        partition_key partition;
    };
    chunked_vector<candidate_file> candidates;

    for (const auto& mf : mlist_res.value().files) {
        if (mf.content != manifest_file_content::data) {
            continue;
        }
        auto manifest_res = co_await mio.download_manifest(mf.manifest_path);
        if (manifest_res.has_error()) {
            vlog(
              iceberg::log.warn,
              "failed to download manifest, skipping for upsert planning");
            continue;
        }
        for (const auto& entry : manifest_res.value().entries) {
            if (entry.status == manifest_entry_status::deleted) {
                continue;
            }
            if (entry.data_file.content_type != data_file_content_type::data) {
                continue;
            }
            if (exclude_file_paths.contains(entry.data_file.file_path())) {
                continue;
            }
            if (may_contain_keys(entry.data_file, sch, key_bounds)) {
                candidates.push_back(
                  candidate_file{
                    .file_path = entry.data_file.file_path,
                    .partition = entry.data_file.partition.copy(),
                  });
            }
        }
    }

    chunked_vector<position_delete_entry> all_deletes;
    for (const auto& candidate : candidates) {
        auto file_data = co_await read_file(candidate.file_path);
        auto records = co_await sp::read_file_as_records(
          std::move(file_data), make_key_projection(key_fields));
        for (int64_t row_idx = 0;
             row_idx < static_cast<int64_t>(records.size());
             ++row_idx) {
            if (key_set.contains(hashable_key{std::move(records[row_idx])})) {
                all_deletes.push_back(
                  position_delete_entry{
                    .file_path = candidate.file_path,
                    .pos = row_idx,
                    .partition = candidate.partition.copy(),
                  });
            }
        }
    }

    co_return all_deletes;
}

ss::future<chunked_vector<pending_delete>> make_position_deletes(
  const table_metadata& table,
  chunked_vector<position_delete_entry> positions,
  bool compress) {
    chunked_vector<pending_delete> result;
    if (positions.empty()) {
        co_return result;
    }

    // Sort by partition, then file_path, then pos. Entries within the same
    // data file share the same partition, so grouping by partition is
    // equivalent to grouping contiguous runs after a stable sort on
    // partition hash.
    std::sort(
      positions.begin(),
      positions.end(),
      [](const position_delete_entry& a, const position_delete_entry& b) {
          auto ah = std::hash<partition_key>{}(a.partition);
          auto bh = std::hash<partition_key>{}(b.partition);
          if (ah != bh) {
              return ah < bh;
          }
          if (a.file_path() != b.file_path()) {
              return a.file_path() < b.file_path();
          }
          return a.pos < b.pos;
      });

    // Write one position delete file per unique partition.
    size_t group_start = 0;
    while (group_start < positions.size()) {
        size_t group_end = group_start + 1;
        while (group_end < positions.size()
               && positions[group_end].partition
                    == positions[group_start].partition) {
            ++group_end;
        }

        auto group_partition = positions[group_start].partition.copy();
        chunked_vector<position_delete_entry> group;
        for (size_t i = group_start; i < group_end; ++i) {
            group.push_back(std::move(positions[i]));
        }

        auto del_result = co_await write_position_delete_file(
          std::move(group), compress);
        del_result.manifest_entry.partition = std::move(group_partition);

        result.push_back(
          pending_delete{
            .file = file_to_delete{
              .file = std::move(del_result.manifest_entry),
              .schema_id = table.current_schema_id,
              .partition_spec_id = table.default_spec_id,
            },
            .data = std::move(del_result.file_data),
          });

        group_start = group_end;
    }

    co_return result;
}

ss::future<chunked_vector<file_to_delete>> make_equality_deletes(
  const table_metadata& table,
  const chunked_vector<sp::group_value>& keys,
  const chunked_vector<nested_field::id_t>& key_field_ids,
  bool compress) {
    chunked_vector<file_to_delete> result;
    if (keys.empty()) {
        co_return result;
    }

    const auto& sch = current_schema(table);
    auto key_fields = resolve_key_fields(sch, key_field_ids);

    chunked_vector<sp::schema_element> children;
    for (size_t i = 0; i < key_fields.size(); ++i) {
        // Walk the schema tree to find the actual nested_field.
        const nested_field* nf = nullptr;
        const struct_type* cur = &sch.schema_struct;
        for (const auto& name : key_fields[i].path) {
            for (const auto& f : cur->fields) {
                if (f && f->name == name) {
                    nf = f.get();
                    if (auto* inner = std::get_if<struct_type>(&f->type)) {
                        cur = inner;
                    }
                    break;
                }
            }
        }
        vassert(nf, "key field not found in schema");
        auto* prim = std::get_if<primitive_type>(&nf->type);
        vassert(prim, "key field {} must be primitive type", nf->name);

        auto [ptype, ltype] = iceberg_to_parquet_types(*prim);

        sp::schema_element elem{
          .type = std::move(ptype),
          .repetition_type = sp::field_repetition_type::required,
          .path = {nf->name},
          .field_id = nf->id(),
        };
        if (!std::holds_alternative<std::monostate>(ltype)) {
            elem.logical_type = std::move(ltype);
        }
        children.push_back(std::move(elem));
    }

    sp::schema_element parquet_schema{
      .type = std::monostate{},
      .repetition_type = sp::field_repetition_type::required,
      .path = {ss::sstring("equality_delete")},
      .children = std::move(children),
    };

    chunked_vector<nested_field::id_t> eq_ids;
    for (auto id : key_field_ids) {
        eq_ids.push_back(id);
    }

    chunked_vector<sp::group_value> rows;
    for (const auto& k : keys) {
        sp::group_value row;
        for (const auto& m : k) {
            row.push_back(sp::group_member{sp::copy(m.field)});
        }
        rows.push_back(std::move(row));
    }

    equality_delete_options opts{
      .parquet_schema = std::move(parquet_schema),
      .equality_field_ids = std::move(eq_ids),
      .compress = compress,
    };

    auto del_result = co_await write_equality_delete_file(
      std::move(opts), std::move(rows));

    result.push_back(
      file_to_delete{
        .file = std::move(del_result.manifest_entry),
        .schema_id = table.current_schema_id,
        .partition_spec_id = table.default_spec_id,
      });

    co_return result;
}

} // namespace iceberg
