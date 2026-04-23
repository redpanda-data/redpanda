/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/tests/iceberg_table_reader.h"

#include "serde/parquet/assembler.h"
#include "serde/parquet/file_io.h"
#include "serde/parquet/reader.h"

#include <algorithm>
#include <stdexcept>

namespace datalake::tests {

namespace sp = serde::parquet;

ss::future<chunked_hash_map<ss::sstring, iceberg::position_delete_set>>
read_position_deletes(
  iceberg::manifest_io& io, const iceberg::data_file& del_file) {
    chunked_hash_map<ss::sstring, iceberg::position_delete_set> result;

    auto bytes = co_await io.download_object_bytes(del_file.file_path);
    if (bytes.has_error()) {
        co_return result;
    }
    auto records = co_await sp::read_file_as_records(std::move(bytes.value()));

    // Position delete schema: (file_path: string, pos: int64)
    for (const auto& row : records) {
        if (row.size() < 2) {
            continue;
        }
        auto* path_val = std::get_if<sp::byte_array_value>(&row[0].field);
        auto* pos_val = std::get_if<sp::int64_value>(&row[1].field);
        if (!path_val || !pos_val) {
            continue;
        }
        auto path_str = path_val->val.linearize_to_string();
        result[path_str].positions.push_back(pos_val->val);
    }

    // Sort and deduplicate positions per file.
    for (auto& [_, pds] : result) {
        std::sort(pds.positions.begin(), pds.positions.end());
        chunked_vector<int64_t> deduped;
        for (const auto& p : pds.positions) {
            if (deduped.empty() || deduped.back() != p) {
                deduped.push_back(p);
            }
        }
        pds.positions = std::move(deduped);
    }

    co_return result;
}

ss::future<iceberg::equality_delete_set> read_equality_deletes(
  iceberg::manifest_io& io, const iceberg::data_file& del_file) {
    iceberg::equality_delete_set eds;

    // Populate field_ids from the manifest entry's equality_ids.
    if (del_file.equality_ids.has_value()) {
        for (auto id : *del_file.equality_ids) {
            eds.field_ids.push_back(static_cast<int32_t>(id()));
        }
    }

    auto bytes = co_await io.download_object_bytes(del_file.file_path);
    if (bytes.has_error()) {
        co_return eds;
    }
    auto records = co_await sp::read_file_as_records(std::move(bytes.value()));

    // Flatten each record to leaf values and extract only the key
    // fields (everything except the trailing _redpanda_offset which
    // is not in equality_ids).
    for (auto& row : records) {
        sp::group_value leaves;
        std::function<void(const sp::group_value&)> collect =
          [&](const sp::group_value& gv) {
              for (const auto& m : gv) {
                  if (auto* nested = std::get_if<sp::group_value>(&m.field)) {
                      collect(*nested);
                  } else {
                      leaves.push_back(sp::group_member{sp::copy(m.field)});
                  }
              }
          };
        collect(row);

        // The delete file has key columns followed by
        // _redpanda_offset. The number of key columns equals
        // equality_ids.size(). Take only those.
        sp::group_value key;
        size_t n_key_cols = eds.field_ids.size();
        for (size_t i = 0; i < std::min(n_key_cols, leaves.size()); ++i) {
            key.push_back(std::move(leaves[i]));
        }
        eds.keys.insert(std::move(key));
    }

    co_return eds;
}

ss::future<chunked_vector<sp::group_value>> read_iceberg_table(
  iceberg::manifest_io& io, const iceberg::table_metadata& table) {
    if (!table.current_snapshot_id.has_value()) {
        co_return chunked_vector<sp::group_value>{};
    }

    const auto& table_schema = *table.get_schema(table.current_schema_id);
    auto snap = table.get_snapshots_by_id().at(*table.current_snapshot_id);

    auto mlist_res = co_await io.download_manifest_list(
      snap.manifest_list_path);
    if (mlist_res.has_error()) {
        throw std::runtime_error("Failed to download manifest list");
    }

    // Collect all files with their sequence numbers.
    chunked_vector<sequenced_file> data_files;
    chunked_vector<sequenced_file> eq_deletes;
    chunked_vector<sequenced_file> pos_deletes;

    for (const auto& mf : mlist_res.value().files) {
        auto m_res = co_await io.download_manifest(mf.manifest_path);
        if (m_res.has_error()) {
            throw std::runtime_error("Failed to download manifest");
        }
        for (auto& e : m_res.value().entries) {
            int64_t seq = e.sequence_number.has_value()
                            ? e.sequence_number.value()()
                            : 0;
            switch (e.data_file.content_type) {
            case iceberg::data_file_content_type::data:
                data_files.push_back({std::move(e.data_file), seq});
                break;
            case iceberg::data_file_content_type::equality_deletes:
                eq_deletes.push_back({std::move(e.data_file), seq});
                break;
            case iceberg::data_file_content_type::position_deletes:
                pos_deletes.push_back({std::move(e.data_file), seq});
                break;
            }
        }
    }

    // Pre-read all position delete files.
    chunked_hash_map<ss::sstring, iceberg::position_delete_set> all_pos_deletes;
    struct pos_del_with_seq {
        chunked_hash_map<ss::sstring, iceberg::position_delete_set> by_path;
        int64_t seq_num;
    };
    chunked_vector<pos_del_with_seq> pos_del_entries;
    for (const auto& pd : pos_deletes) {
        auto by_path = co_await read_position_deletes(io, pd.file);
        pos_del_entries.push_back({std::move(by_path), pd.seq_num});
    }

    // Pre-read all equality delete files.
    struct eq_del_with_seq {
        iceberg::equality_delete_set eds;
        int64_t seq_num;
    };
    chunked_vector<eq_del_with_seq> eq_del_entries;
    for (const auto& ed : eq_deletes) {
        auto eds = co_await read_equality_deletes(io, ed.file);
        eq_del_entries.push_back({std::move(eds), ed.seq_num});
    }

    // Read each data file with applicable deletes.
    chunked_vector<sp::group_value> all_records;

    for (const auto& df : data_files) {
        auto bytes = co_await io.download_object_bytes(df.file.file_path);
        if (bytes.has_error()) {
            continue;
        }

        chunked_vector<iceberg::delete_file_entry> delete_entries;

        // Position deletes: apply when del.seq >= data.seq
        for (const auto& pde : pos_del_entries) {
            if (pde.seq_num < df.seq_num) {
                continue;
            }
            auto it = pde.by_path.find(df.file.file_path());
            if (it != pde.by_path.end()) {
                iceberg::position_delete_set pds;
                pds.positions = it->second.positions.copy();
                delete_entries.push_back(std::move(pds));
            }
        }

        // Equality deletes: apply when del.seq > data.seq (strictly)
        for (const auto& ede : eq_del_entries) {
            if (ede.seq_num <= df.seq_num) {
                continue;
            }
            // Copy the equality delete set.
            iceberg::equality_delete_set eds_copy;
            eds_copy.field_ids = ede.eds.field_ids.copy();
            for (const auto& k : ede.eds.keys) {
                sp::group_value kc;
                for (const auto& m : k) {
                    kc.push_back(sp::group_member{sp::copy(m.field)});
                }
                eds_copy.keys.insert(std::move(kc));
            }
            delete_entries.push_back(std::move(eds_copy));
        }

        sp::iobuf_file_io file_io(std::move(bytes.value()));
        auto result = co_await iceberg::read_parquet(
          table_schema.schema_struct, file_io, std::move(delete_entries));

        for (const auto& batch : result.row_groups) {
            auto assembled = sp::assemble_records(result.schema, batch);
            for (auto& r : assembled) {
                all_records.push_back(std::move(r));
            }
        }
    }

    co_return all_records;
}

} // namespace datalake::tests
