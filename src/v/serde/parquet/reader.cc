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

#include "serde/parquet/reader.h"

#include "serde/parquet/assembler.h"
#include "serde/parquet/column_chunk_reader.h"
#include "serde/parquet/flattened_schema.h"
#include "serde/parquet/metadata.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <stdexcept>

namespace serde::parquet {

namespace {

/// Resolve which leaf columns to read based on projection paths.
/// Returns a set of schema positions for projected leaf columns.
/// If projection is empty, all leaves are included.
chunked_vector<bool> resolve_projection(
  const schema_element& schema,
  const chunked_vector<chunked_vector<ss::sstring>>& projection) {
    chunked_vector<bool> included;
    if (projection.empty()) {
        schema.for_each([&](const schema_element& elem) {
            if (elem.is_leaf()) {
                included.push_back(true);
            }
        });
        return included;
    }
    // Build inclusion set from projection paths
    schema.for_each([&](const schema_element& elem) {
        if (!elem.is_leaf()) {
            return;
        }
        bool match = false;
        for (const auto& proj_path : projection) {
            if (elem.path.size() < proj_path.size()) {
                continue;
            }
            bool path_match = true;
            for (size_t i = 0; i < proj_path.size(); ++i) {
                // Compare from the end of the path (leaf name matching)
                auto elem_idx = elem.path.size() - proj_path.size() + i;
                if (elem.path[elem_idx] != proj_path[i]) {
                    path_match = false;
                    break;
                }
            }
            if (path_match) {
                match = true;
                break;
            }
        }
        included.push_back(match);
    });
    return included;
}

/// Build a projected schema tree containing only the included leaves
/// and their ancestor groups. `included` is a bool per leaf in
/// depth-first order (same as resolve_projection output).
schema_element project_schema(
  const schema_element& schema, const chunked_vector<bool>& included) {
    // Helper: recursively prune. Returns nullopt if no descendants are
    // included. leaf_idx tracks position in the included vector.
    struct pruner {
        const chunked_vector<bool>& included;
        size_t leaf_idx = 0;

        std::optional<schema_element> prune(const schema_element& node) {
            if (node.is_leaf()) {
                bool keep = leaf_idx < included.size() && included[leaf_idx];
                ++leaf_idx;
                if (keep) {
                    return schema_element{
                      .position = node.position,
                      .type = node.type,
                      .repetition_type = node.repetition_type,
                      .path = node.path.copy(),
                      .field_id = node.field_id,
                      .logical_type = node.logical_type,
                      .max_definition_level = node.max_definition_level,
                      .max_repetition_level = node.max_repetition_level,
                    };
                }
                return std::nullopt;
            }
            chunked_vector<schema_element> kept_children;
            for (const auto& child : node.children) {
                auto projected = prune(child);
                if (projected) {
                    kept_children.push_back(std::move(*projected));
                }
            }
            if (kept_children.empty()) {
                return std::nullopt;
            }
            return schema_element{
              .position = node.position,
              .type = node.type,
              .repetition_type = node.repetition_type,
              .path = node.path.copy(),
              .children = std::move(kept_children),
              .field_id = node.field_id,
              .logical_type = node.logical_type,
              .max_definition_level = node.max_definition_level,
              .max_repetition_level = node.max_repetition_level,
            };
        }
    };

    pruner p{included};
    auto result = p.prune(schema);
    if (result) {
        return std::move(*result);
    }
    return schema_element{};
}

} // namespace

ss::future<file_reader_result> read_file(iobuf file_data, reader_options opts) {
    constexpr size_t tail_size = 8;
    auto tail = file_data.share(file_data.size_bytes() - tail_size, tail_size);
    auto loc = parse_footer_location(tail, file_data.size_bytes());
    auto footer_bytes = file_data.share(loc.offset, loc.length);
    auto metadata = decode(std::move(footer_bytes), file_metadata_tag{});
    auto schema = unflatten(metadata.schema);
    index_schema(schema);

    auto projection = resolve_projection(schema, opts.column_projection);

    chunked_vector<columnar_batch> row_group_batches;
    for (const auto& rg : metadata.row_groups) {
        columnar_batch batch;
        batch.num_rows = rg.num_rows;

        int32_t leaf_idx = 0;
        for (const auto& cc : rg.columns) {
            if (
              leaf_idx < static_cast<int32_t>(projection.size())
              && !projection[leaf_idx]) {
                ++leaf_idx;
                continue;
            }

            // Find the schema element for this column.
            // path_in_schema omits the root name, so we compare from
            // index 1 of the schema element's path.
            const schema_element* col_schema = nullptr;
            schema.for_each([&](const schema_element& elem) {
                if (!elem.is_leaf()) {
                    return;
                }
                if (
                  elem.path.size() != cc.meta_data.path_in_schema.size() + 1) {
                    return;
                }
                bool match = true;
                for (size_t i = 0; i < cc.meta_data.path_in_schema.size();
                     ++i) {
                    if (elem.path[i + 1] != cc.meta_data.path_in_schema[i]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    col_schema = &elem;
                }
            });
            if (!col_schema) {
                throw std::runtime_error(
                  fmt::format(
                    "schema element not found for column path: {}",
                    fmt::join(cc.meta_data.path_in_schema, ".")));
            }

            auto col_start
              = cc.meta_data.dictionary_page_offset.value_or(
                cc.meta_data.data_page_offset);
            auto col_bytes = file_data.share(
              col_start, cc.meta_data.total_compressed_size);

            auto col_data = co_await decode_column_chunk(
              std::move(col_bytes), cc.meta_data, *col_schema);

            batch.columns.push_back(std::move(col_data.values));
            batch.levels.push_back(
              columnar_batch::level_data{
                .def_levels = std::move(col_data.def_levels),
                .rep_levels = std::move(col_data.rep_levels),
              });
            ++leaf_idx;
        }

        row_group_batches.push_back(std::move(batch));
        co_await ss::coroutine::maybe_yield();
    }

    // Return a projected schema that matches the batch columns, so
    // callers (including assemble_records) see a coherent pair.
    auto result_schema = opts.column_projection.empty()
                           ? std::move(schema)
                           : project_schema(schema, projection);

    co_return file_reader_result{
      .metadata = std::move(metadata),
      .schema = std::move(result_schema),
      .row_groups = std::move(row_group_batches),
    };
}

ss::future<chunked_vector<group_value>>
read_file_as_records(iobuf file_data, reader_options opts) {
    auto result = co_await read_file(std::move(file_data), std::move(opts));

    chunked_vector<group_value> all_records;
    for (const auto& batch : result.row_groups) {
        auto records = assemble_records(result.schema, batch);
        for (auto& rec : records) {
            all_records.push_back(std::move(rec));
        }
        co_await ss::coroutine::maybe_yield();
    }
    co_return all_records;
}

} // namespace serde::parquet
