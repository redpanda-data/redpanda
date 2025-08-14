/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "container/chunked_vector.h"
#include "iceberg/datatypes.h"

#include <ranges>

namespace iceberg {

namespace detail {

// Collects children of the given type in reverse order. The reversed order can
// be useful, e.g. if the collection is being used as a stack.
template<typename T>
struct reverse_field_collecting_visitor_impl {
public:
    explicit reverse_field_collecting_visitor_impl(
      chunked_vector<T>& collection)
      : collection_(collection) {}
    chunked_vector<T>& collection_;

    void operator()(const iceberg::primitive_type&) {
        // No-op, no additional fields to collect.
    }
    void operator()(const iceberg::list_type& t) {
        collection_.push_back(t.element_field.get());
    }
    void operator()(const iceberg::struct_type& t) {
        for (auto& f : std::ranges::reverse_view(t.fields)) {
            collection_.push_back(f.get());
        }
    }
    void operator()(const iceberg::map_type& t) {
        collection_.push_back(t.value_field.get());
        collection_.push_back(t.key_field.get());
    }
};
} // namespace detail

using reverse_field_collecting_visitor
  = detail::reverse_field_collecting_visitor_impl<nested_field*>;

using reverse_const_field_collecting_visitor
  = detail::reverse_field_collecting_visitor_impl<const nested_field*>;

} // namespace iceberg
