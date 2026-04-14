/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/conversion/stats_parquet.h"

#include "serde/parquet/flattened_schema.h"

namespace iceberg::conversion {

iceberg_column_stats
extract_iceberg_stats(const serde::parquet::file_metadata& metadata) {
    // Build a mapping from leaf column index to field_id. Leaf elements
    // in the flattened schema (num_children == 0) correspond 1:1 with
    // column chunks in each row group, in depth-first pre-order.
    struct leaf_info {
        nested_field::id_t field_id;
    };
    chunked_hash_map<size_t, leaf_info> leaf_field_ids;
    size_t leaf_idx = 0;
    for (const auto& elem : metadata.schema) {
        if (elem.num_children == 0) {
            if (elem.field_id.has_value()) {
                leaf_field_ids.emplace(
                  leaf_idx,
                  leaf_info{nested_field::id_t{elem.field_id.value()}});
            }
            ++leaf_idx;
        }
    }

    if (leaf_field_ids.empty()) {
        return {};
    }

    chunked_hash_map<nested_field::id_t, int64_t> column_sizes;
    chunked_hash_map<nested_field::id_t, int64_t> value_counts;
    chunked_hash_map<nested_field::id_t, int64_t> null_value_counts;
    chunked_hash_map<nested_field::id_t, iobuf> lower_bounds;
    chunked_hash_map<nested_field::id_t, iobuf> upper_bounds;
    bool any_bounds = false;

    for (const auto& rg : metadata.row_groups) {
        for (size_t col_idx = 0; col_idx < rg.columns.size(); ++col_idx) {
            auto it = leaf_field_ids.find(col_idx);
            if (it == leaf_field_ids.end()) {
                continue;
            }
            const auto& field_id = it->second.field_id;
            const auto& col_meta = rg.columns[col_idx].meta_data;

            column_sizes[field_id] += col_meta.total_compressed_size;
            value_counts[field_id] += col_meta.num_values;

            if (col_meta.stats.has_value()) {
                const auto& stats = col_meta.stats.value();
                if (stats.null_count.has_value()) {
                    null_value_counts[field_id] += stats.null_count.value();
                }
                if (stats.min.has_value()) {
                    any_bounds = true;
                    auto lb_it = lower_bounds.find(field_id);
                    if (lb_it == lower_bounds.end()) {
                        lower_bounds.emplace(
                          field_id, stats.min.value().value.copy());
                    } else if (stats.min.value().value < lb_it->second) {
                        lb_it->second = stats.min.value().value.copy();
                    }
                }
                if (stats.max.has_value()) {
                    any_bounds = true;
                    auto ub_it = upper_bounds.find(field_id);
                    if (ub_it == upper_bounds.end()) {
                        upper_bounds.emplace(
                          field_id, stats.max.value().value.copy());
                    } else if (stats.max.value().value > ub_it->second) {
                        ub_it->second = stats.max.value().value.copy();
                    }
                }
            }
        }
    }

    iceberg_column_stats result;
    result.column_sizes = std::move(column_sizes);
    result.value_counts = std::move(value_counts);
    result.null_value_counts = std::move(null_value_counts);
    if (any_bounds) {
        result.lower_bounds = std::move(lower_bounds);
        result.upper_bounds = std::move(upper_bounds);
    }
    return result;
}

} // namespace iceberg::conversion
