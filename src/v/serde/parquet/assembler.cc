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

#include "serde/parquet/assembler.h"

#include <seastar/util/variant_utils.hh>

#include <stdexcept>

namespace serde::parquet {

namespace {

/// Per-column cursor state during assembly.
struct column_cursor {
    const column_array* data;
    const chunked_vector<def_level>* def_levels;
    const chunked_vector<rep_level>* rep_levels;
    size_t level_index = 0;
    size_t value_index = 0;

    def_level current_def() const { return (*def_levels)[level_index]; }
    rep_level current_rep() const { return (*rep_levels)[level_index]; }
    bool exhausted() const { return level_index >= def_levels->size(); }
    void advance_level() { ++level_index; }
    void advance_value() { ++value_index; }
};

/// Read the next non-null value from a column cursor.
value read_leaf_value(column_cursor& cursor) {
    auto idx = cursor.value_index;
    cursor.advance_value();
    return ss::visit(
      cursor.data->data,
      [&](const column_array::boolean_data& d) -> value {
          auto byte_idx = idx / CHAR_BIT;
          auto bit_idx = idx % CHAR_BIT;
          for (const auto& frag : d.packed_bits) {
              auto frag_size = static_cast<size_t>(frag.size());
              if (byte_idx < frag_size) {
                  auto byte = static_cast<uint8_t>(frag.get()[byte_idx]);
                  return boolean_value{
                    static_cast<bool>((byte >> bit_idx) & 1)};
              }
              byte_idx -= frag_size;
          }
          throw std::runtime_error("boolean value index out of range");
      },
      [&](const column_array::i32_data& d) -> value {
          return int32_value{d.values[idx]};
      },
      [&](const column_array::i64_data& d) -> value {
          return int64_value{d.values[idx]};
      },
      [&](const column_array::f32_data& d) -> value {
          return float32_value{d.values[idx]};
      },
      [&](const column_array::f64_data& d) -> value {
          return float64_value{d.values[idx]};
      },
      [&](const column_array::byte_array_data& d) -> value {
          auto start = d.offsets[idx];
          auto end = d.offsets[idx + 1];
          auto len = static_cast<size_t>(end - start);
          // Copy the slice from the concatenated data iobuf.
          iobuf result;
          size_t remaining = len;
          size_t skip = static_cast<size_t>(start);
          for (const auto& frag : d.data) {
              if (skip >= frag.size()) {
                  skip -= frag.size();
                  continue;
              }
              auto copy_len = std::min(frag.size() - skip, remaining);
              result.append(frag.get() + skip, copy_len);
              remaining -= copy_len;
              skip = 0;
              if (remaining == 0) {
                  break;
              }
          }
          return byte_array_value{std::move(result)};
      },
      [&](const column_array::fixed_byte_array_data& d) -> value {
          auto offset = static_cast<size_t>(idx) * d.fixed_length;
          iobuf result;
          size_t remaining = d.fixed_length;
          size_t skip = offset;
          for (const auto& frag : d.data) {
              if (skip >= frag.size()) {
                  skip -= frag.size();
                  continue;
              }
              auto copy_len = std::min(frag.size() - skip, remaining);
              result.append(frag.get() + skip, copy_len);
              remaining -= copy_len;
              skip = 0;
              if (remaining == 0) {
                  break;
              }
          }
          return fixed_byte_array_value{std::move(result)};
      });
}

/// Map from schema element position to column cursor index.
struct cursor_map {
    /// cursor_index[schema_position] = index into cursors vector, or -1.
    chunked_vector<int32_t> cursor_index;
    chunked_vector<column_cursor> cursors;

    column_cursor& get(int32_t schema_position) {
        return cursors[cursor_index[schema_position]];
    }
};

/// Find the first leaf descendant of a schema node, used to peek at
/// the current def/rep levels for non-leaf nodes.
const schema_element& first_leaf(const schema_element& node) {
    if (node.is_leaf()) {
        return node;
    }
    return first_leaf(node.children[0]);
}

value assemble_value(const schema_element& node, cursor_map& cursors);

value assemble_leaf(const schema_element& leaf, cursor_map& cursors) {
    auto& cursor = cursors.get(leaf.position);
    auto dl = cursor.current_def();
    cursor.advance_level();

    if (dl < leaf.max_definition_level) {
        return null_value();
    }
    return read_leaf_value(cursor);
}

value assemble_group(const schema_element& node, cursor_map& cursors) {
    group_value fields;
    for (const auto& child : node.children) {
        fields.push_back(group_member{assemble_value(child, cursors)});
    }
    return value(std::move(fields));
}

value assemble_repeated(const schema_element& node, cursor_map& cursors) {
    const auto& leaf = first_leaf(node);
    auto& leaf_cursor = cursors.get(leaf.position);

    // Check if the entire repeated field is null (empty list).
    auto dl = leaf_cursor.current_def();
    if (dl < node.max_definition_level) {
        // Null at or above this node's level — consume levels for all
        // descendant leaves and return null.
        for (auto& child : node.children) {
            child.for_each([&](const schema_element& elem) {
                if (elem.is_leaf()) {
                    cursors.get(elem.position).advance_level();
                }
            });
        }
        return null_value();
    }

    repeated_value elements;
    // First element: assemble using the node's optional semantics.
    // For repeated group nodes, the shredder processes them as optional
    // group values within the repeated wrapper.
    if (node.is_leaf()) {
        elements.push_back(repeated_element{assemble_leaf(node, cursors)});
    } else {
        elements.push_back(repeated_element{assemble_group(node, cursors)});
    }

    // Subsequent elements: continue while rep_level >= this node's max_rep.
    while (!leaf_cursor.exhausted()
           && leaf_cursor.current_rep() >= node.max_repetition_level) {
        if (node.is_leaf()) {
            elements.push_back(repeated_element{assemble_leaf(node, cursors)});
        } else {
            elements.push_back(repeated_element{assemble_group(node, cursors)});
        }
    }
    return value(std::move(elements));
}

value assemble_value(const schema_element& node, cursor_map& cursors) {
    if (node.repetition_type == field_repetition_type::repeated) {
        return assemble_repeated(node, cursors);
    }

    if (node.is_leaf()) {
        return assemble_leaf(node, cursors);
    }

    // For optional group nodes, check if the value is null by peeking
    // at the first descendant leaf's def level.
    if (node.repetition_type == field_repetition_type::optional) {
        const auto& leaf = first_leaf(node);
        auto& leaf_cursor = cursors.get(leaf.position);
        auto dl = leaf_cursor.current_def();
        if (dl < node.max_definition_level) {
            // Null at this group level — consume levels for all leaves.
            for (const auto& child : node.children) {
                child.for_each([&](const schema_element& elem) {
                    if (elem.is_leaf()) {
                        cursors.get(elem.position).advance_level();
                    }
                });
            }
            return null_value();
        }
    }

    return assemble_group(node, cursors);
}

} // namespace

chunked_vector<group_value>
assemble_records(const schema_element& schema, const columnar_batch& batch) {
    // Build cursor map from schema positions to column indices.
    cursor_map cmap;
    int32_t max_position = 0;
    schema.for_each([&](const schema_element& elem) {
        max_position = std::max(max_position, elem.position);
    });
    for (int32_t i = 0; i <= max_position; ++i) {
        cmap.cursor_index.push_back(-1);
    }

    int32_t col_idx = 0;
    schema.for_each([&](const schema_element& elem) {
        if (!elem.is_leaf()) {
            return;
        }
        cmap.cursor_index[elem.position] = col_idx;
        cmap.cursors.push_back(
          column_cursor{
            .data = &batch.columns[col_idx],
            .def_levels = &batch.levels[col_idx].def_levels,
            .rep_levels = &batch.levels[col_idx].rep_levels,
          });
        ++col_idx;
    });

    chunked_vector<group_value> records;
    for (int64_t row = 0; row < batch.num_rows; ++row) {
        // Each row assembles a group_value from the root's children.
        group_value fields;
        for (const auto& child : schema.children) {
            fields.push_back(group_member{assemble_value(child, cmap)});
        }
        records.push_back(std::move(fields));
    }
    return records;
}

} // namespace serde::parquet
