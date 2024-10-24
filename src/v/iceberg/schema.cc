// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/schema.h"

namespace iceberg {

namespace {
struct nested_field_collecting_visitor {
public:
    explicit nested_field_collecting_visitor(
      chunked_vector<const nested_field*>& to_visit)
      : to_visit_(to_visit) {}
    chunked_vector<const nested_field*>& to_visit_;

    void operator()(const primitive_type&) {
        // No-op, no additional fields to collect.
    }
    void operator()(const list_type& t) {
        to_visit_.emplace_back(t.element_field.get());
    }
    void operator()(const struct_type& t) {
        for (const auto& field : t.fields) {
            to_visit_.emplace_back(field.get());
        }
    }
    void operator()(const map_type& t) {
        to_visit_.emplace_back(t.key_field.get());
        to_visit_.emplace_back(t.value_field.get());
    }
};
} // namespace

schema::ids_types_map_t
schema::ids_to_types(chunked_hash_set<nested_field::id_t> target_ids) const {
    chunked_vector<const nested_field*> to_visit;
    for (const auto& field : schema_struct.fields) {
        to_visit.emplace_back(field.get());
    }
    bool has_filter = !target_ids.empty();
    schema::ids_types_map_t ret;
    while (!to_visit.empty()) {
        if (has_filter && target_ids.empty()) {
            // We've filtered everything.
            break;
        }
        auto* field = to_visit.back();
        to_visit.pop_back();
        if (!field) {
            continue;
        }
        const auto& type = field->type;
        if (has_filter) {
            const auto iter = target_ids.find(field->id);
            if (iter != target_ids.end()) {
                target_ids.erase(iter);
                ret.emplace(field->id, &type);
            }
        } else {
            ret.emplace(field->id, &type);
        }
        std::visit(nested_field_collecting_visitor{to_visit}, type);
    }
    return ret;
}

std::optional<nested_field::id_t> schema::highest_field_id() const {
    chunked_vector<const nested_field*> to_visit;
    for (const auto& field : schema_struct.fields) {
        to_visit.emplace_back(field.get());
    }
    std::optional<nested_field::id_t> highest;
    while (!to_visit.empty()) {
        auto* field = to_visit.back();
        to_visit.pop_back();
        if (!field) {
            continue;
        }
        const auto& type = field->type;
        highest.emplace(
          highest.has_value() ? std::max(highest.value(), field->id)
                              : field->id());
        std::visit(nested_field_collecting_visitor{to_visit}, type);
    }
    return highest;
}

} // namespace iceberg
