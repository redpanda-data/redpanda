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
#include "serde/parquet/column_chunk_reader.h"
#include "serde/parquet/flattened_schema.h"
#include "serde/parquet/metadata.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <stdexcept>

namespace iceberg {

namespace {

namespace sp = serde::parquet;

struct file_leaf_info {
    int32_t leaf_index;
    const sp::schema_element* element;
};

struct leaf_mapping {
    int32_t file_leaf_index;
    const sp::schema_element* file_leaf;
};

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
/// file schema index. Populates `mappings` and returns a schema_element tree
/// mirroring the table schema structure with physical types from the file.
struct table_walker {
    const chunked_hash_map<int32_t, file_leaf_info>& file_index;
    chunked_vector<leaf_mapping>& mappings;

    sp::schema_element walk_field(const nested_field& field) {
        auto rep = field.required ? sp::field_repetition_type::required
                                  : sp::field_repetition_type::optional;
        return std::visit(
          [&](const auto& t) { return walk_type(field, t, rep); }, field.type);
    }

    sp::schema_element walk_type(
      const nested_field& field,
      const primitive_type&,
      sp::field_repetition_type rep) {
        int32_t field_id = field.id;
        auto it = file_index.find(field_id);
        if (it == file_index.end()) {
            // Column not present in file - skip
            return {};
        }
        const auto& info = it->second;
        mappings.push_back(
          leaf_mapping{
            .file_leaf_index = info.leaf_index,
            .file_leaf = info.element,
          });
        return sp::schema_element{
          .type = info.element->type,
          .repetition_type = rep,
          .path = {field.name},
          .field_id = field_id,
          .logical_type = info.element->logical_type,
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
            auto child_elem = walk_field(*child);
            if (child_elem.path.empty()) {
                // Field not found in file
                continue;
            }
            group.children.push_back(std::move(child_elem));
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

        auto element = walk_field(*lt.element_field);
        if (!element.path.empty()) {
            list_wrapper.children.push_back(std::move(element));
            list_node.children.push_back(std::move(list_wrapper));
        }
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

        auto key = walk_field(*mt.key_field);
        auto val = walk_field(*mt.value_field);
        if (!key.path.empty()) {
            kv_wrapper.children.push_back(std::move(key));
        }
        if (!val.path.empty()) {
            kv_wrapper.children.push_back(std::move(val));
        }
        if (!kv_wrapper.children.empty()) {
            map_node.children.push_back(std::move(kv_wrapper));
        }
        return map_node;
    }
};

} // namespace

ss::future<parquet_reader_result>
read_parquet(const struct_type& read_schema, sp::file_io& io) {
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

    chunked_vector<leaf_mapping> mappings;
    table_walker walker{file_index, mappings};

    // Build the result schema from the table schema structure.
    sp::schema_element result_schema;
    result_schema.repetition_type = sp::field_repetition_type::required;
    result_schema.path.emplace_back("root");
    for (const auto& field : read_schema.fields) {
        auto elem = walker.walk_field(*field);
        if (elem.path.empty()) {
            continue;
        }
        result_schema.children.push_back(std::move(elem));
    }
    sp::index_schema(result_schema);

    // Step 3: Read column chunks per row group.
    chunked_vector<sp::columnar_batch> row_group_batches;
    for (const auto& rg : metadata.row_groups) {
        sp::columnar_batch batch;
        batch.num_rows = rg.num_rows;

        for (const auto& m : mappings) {
            const auto& cc = rg.columns[m.file_leaf_index];
            auto offset = cc.meta_data.dictionary_page_offset.has_value()
                            ? std::min(
                                *cc.meta_data.dictionary_page_offset,
                                cc.meta_data.data_page_offset)
                            : cc.meta_data.data_page_offset;
            auto end = cc.meta_data.data_page_offset
                       + cc.meta_data.total_compressed_size;
            auto length = end - offset;

            auto col_bytes = co_await io.read(offset, length);
            auto col_data = co_await sp::decode_column_chunk(
              std::move(col_bytes), cc.meta_data, *m.file_leaf);

            batch.columns.push_back(std::move(col_data.values));
            batch.levels.push_back(
              sp::columnar_batch::level_data{
                .def_levels = std::move(col_data.def_levels),
                .rep_levels = std::move(col_data.rep_levels),
              });
            co_await ss::coroutine::maybe_yield();
        }

        row_group_batches.push_back(std::move(batch));
    }

    co_return parquet_reader_result{
      .schema = std::move(result_schema),
      .row_groups = std::move(row_group_batches),
    };
}

} // namespace iceberg
