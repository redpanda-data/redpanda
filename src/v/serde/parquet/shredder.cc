/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/parquet/shredder.h"

#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/variant_utils.hh>

#include <boost/range/irange.hpp>

#include <ranges>
#include <stdexcept>

using boost::irange;
using std::ranges::reverse_view;

namespace serde::parquet {

namespace {

// A struct to track all the level related information we need during the tree
// shredding traversal.
struct traversal_levels {
    rep_level repetition_level = rep_level(0);
    def_level definition_level = def_level(0);
};

class record_shredder {
public:
    record_shredder(
      const schema_element& root,
      group_value record,
      absl::FunctionRef<ss::future<>(shredded_value)> cb)
      : _callback(cb) {
        if (root.repetition_type != field_repetition_type::required) {
            throw std::runtime_error("schema root nodes must be required");
        }
        _stack.emplace_back(
          &root, traversal_levels{}, value(std::move(record)));
    }

    ss::future<> shred() {
        while (!_stack.empty()) {
            auto [element, levels, value] = std::move(_stack.back());
            _stack.pop_back();
            if (element->is_leaf()) {
                co_await process_leaf_value(element, levels, std::move(value));
            } else {
                co_await process_group_node(element, levels, std::move(value));
            }
        }
    }

private:
    ss::future<> process_leaf_value(
      const schema_element* element, traversal_levels levels, value val) {
        return ss::visit(
          std::move(val),
          [this, element, levels](repeated_value& list) {
              return process_repeated_leaf_value(
                element, std::move(list), levels);
          },
          [element](group_value&) {
              return ss::make_exception_future(std::runtime_error(fmt::format(
                "unexpected struct value for leaf schema element {}",
                element->name())));
          },
          [this, element, levels](null_value& v) {
              // If this is the level in the tree that is turning NULL that is
              // invalid, this is a required node, however parent nodes could be
              // propagating a null value, which is valid.
              if (
                element->repetition_type == field_repetition_type::required
                && element->max_definition_level == levels.definition_level) {
                  return ss::make_exception_future(std::runtime_error(
                    "detected null value for required leaf node"));
              }
              return _callback({
                .schema_element_position = element->position,
                .val = v,
                .rep_level = levels.repetition_level,
                .def_level = levels.definition_level,
              });
          },
          [this, element, levels](auto& v) {
              traversal_levels leaf_levels = levels;
              if (element->repetition_type != field_repetition_type::required) {
                  ++leaf_levels.definition_level;
              }
              return _callback({
                .schema_element_position = element->position,
                .val = value(std::move(v)),
                .rep_level = leaf_levels.repetition_level,
                .def_level = leaf_levels.definition_level,
              });
          });
    }

    ss::future<> process_repeated_leaf_value(
      const schema_element* element,
      repeated_value list,
      traversal_levels levels) {
        // Empty lists are treated as `null`
        if (list.empty()) {
            co_return co_await _callback({
              .schema_element_position = element->position,
              .val = null_value(),
              .rep_level = levels.repetition_level,
              .def_level = levels.definition_level,
            });
        }
        traversal_levels child_levels = levels;
        auto it = list.begin();
        co_await process_leaf_value(
          element, child_levels, std::move(it->element));
        // Repeated elements use this node's repetition level, as to signal
        // at which level in the tree the repetition is happening at.
        child_levels.repetition_level = element->max_repetition_level;
        for (++it; it != list.end(); ++it) {
            co_await process_leaf_value(
              element, child_levels, std::move(it->element));
        }
    }

    ss::future<> process_group_node(
      const schema_element* element, traversal_levels levels, value val) {
        switch (element->repetition_type) {
        case field_repetition_type::required:
            return process_required_group_node(element, levels, std::move(val));
        case field_repetition_type::optional:
            return process_optional_group_node(element, levels, std::move(val));
        case field_repetition_type::repeated:
            return process_repeated_group_node(element, levels, std::move(val));
        }
    }

    ss::future<> process_required_group_node(
      const schema_element* element, traversal_levels levels, value val) {
        return ss::visit(
          std::move(val),
          [this, element, levels](group_value& groups) -> ss::future<> {
              return process_required_group_value(
                element, levels, std::move(groups));
          },
          [this, element, levels](null_value&) -> ss::future<> {
              // If this is the level in the tree that is turning NULL that is
              // invalid, this is a required node, however parent nodes could be
              // propagating a null value, which is valid.
              if (element->max_definition_level == levels.definition_level) {
                  return ss::make_exception_future(std::runtime_error(
                    "detected null value for required group node"));
              }
              return process_optional_null_group(element, levels);
          },
          [element](repeated_value&) -> ss::future<> {
              return ss::make_exception_future(std::runtime_error(fmt::format(
                "unexpected list value for non-repeated schema element {}",
                element->name())));
          },
          [element](auto& v) -> ss::future<> {
              return ss::make_exception_future(std::runtime_error(fmt::format(
                "unexpected leaf value for required schema element {}: {}",
                element->name(),
                value(std::move(v)))));
          });
    }

    ss::future<> process_repeated_group_node(
      const schema_element* element, traversal_levels levels, value val) {
        return ss::visit(
          std::move(val),
          [this, element, levels](null_value&) {
              return process_optional_null_group(element, levels);
          },
          [this, element, levels](repeated_value& list) {
              return process_repeated_value(element, levels, std::move(list));
          },
          [element](group_value&) {
              return ss::make_exception_future(std::runtime_error(fmt::format(
                "unexpected struct value for repeated schema element {}",
                element->name())));
          },
          [element](auto& v) {
              return ss::make_exception_future(std::runtime_error(fmt::format(
                "unexpected leaf value for repeated schema element {}: {}",
                element->name(),
                value(std::move(v)))));
          });
    }

    ss::future<> process_optional_group_node(
      const schema_element* element, traversal_levels levels, value val) {
        return ss::visit(
          std::move(val),
          [this, element, levels](group_value& group) -> ss::future<> {
              return process_optional_group_value(
                element, levels, std::move(group));
          },
          [this, element, levels](null_value&) -> ss::future<> {
              return process_optional_null_group(element, levels);
          },
          [element](repeated_value&) -> ss::future<> {
              return ss::make_exception_future(std::runtime_error(fmt::format(
                "unexpected list value for non-repeated schema element {}",
                element->name())));
          },
          [element](auto& v) -> ss::future<> {
              return ss::make_exception_future(std::runtime_error(fmt::format(
                "unexpected leaf value for optional schema element {}: {}",
                element->name(),
                value(std::move(v)))));
          });
    }

    ss::future<> process_repeated_value(
      const schema_element* element,
      traversal_levels levels,
      repeated_value list) {
        // Empty lists are equivalent to a `null` value.
        if (list.empty()) {
            co_return co_await process_optional_null_group(element, levels);
        }
        traversal_levels child_levels = levels;
        // Since these elements are repeated, we need to mark that they are
        // repeated at this level within the tree.
        child_levels.repetition_level = element->max_repetition_level;
        for (size_t i = list.size() - 1; i > 0; --i) {
            co_await process_optional_group_node(
              element, child_levels, std::move(list[i].element));
        }
        // However the first node uses the parent repetition_level
        // as to mark the start of a new list.
        child_levels.repetition_level = levels.repetition_level;
        co_await process_optional_group_node(
          element, child_levels, std::move(list.front().element));
    }

    ss::future<> process_optional_group_value(
      const schema_element* element,
      traversal_levels levels,
      group_value groups) {
        // Increment the definition level as this node in the hierarchy is
        // defined.
        ++levels.definition_level;
        return process_required_group_value(element, levels, std::move(groups));
    }

    ss::future<> process_optional_null_group(
      const schema_element* element, traversal_levels levels) {
        // If the value is `null`, we use the parent definition_level so that
        // assembly can determine where the `null` started.
        for (size_t i : reverse_view(irange(element->children.size()))) {
            const schema_element* child = &element->children[i];
            _stack.emplace_back(child, levels, value(null_value()));
            co_await ss::coroutine::maybe_yield();
        }
    }

    ss::future<> process_required_group_value(
      const schema_element* element,
      traversal_levels levels,
      group_value group) {
        if (group.size() != element->children.size()) {
            co_return co_await ss::make_exception_future(
              std::runtime_error(fmt::format(
                "schema/struct mismatch, schema had {} children, struct had {} "
                "fields. At column {}",
                element->children.size(),
                group.size(),
                element->position)));
        }
        // Levels don't change for require elements because they always have
        // to be there so no additional bits need to be tracked (they'd be
        // wasteful).
        for (size_t i : reverse_view(irange(group.size()))) {
            group_member& member = group[i];
            const schema_element* child = &element->children[i];
            _stack.emplace_back(child, levels, std::move(member.field));
            co_await ss::coroutine::maybe_yield();
        }
    }

    struct entry {
        const schema_element* element;
        traversal_levels levels;
        value val;
    };
    chunked_vector<entry> _stack;
    absl::FunctionRef<ss::future<>(shredded_value)> _callback;
};

} // namespace

ss::future<> shred_record(
  // NOLINTNEXTLINE(*reference*)
  const schema_element& root,
  group_value record,
  absl::FunctionRef<ss::future<>(shredded_value)> callback) {
    record_shredder shredder(root, std::move(record), callback);
    co_return co_await shredder.shred();
}

} // namespace serde::parquet
