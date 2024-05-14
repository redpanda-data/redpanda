// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/compatibility.h"
#include "pandaproxy/schema_registry/types.h"

#include <absl/container/flat_hash_set.h>

namespace pps = pandaproxy::schema_registry;

enum class test_schema_type {
    AVRO = 1,
    PROTO2 = 2,
    PROTO3 = 3,
};

template<typename incompatibility>
struct compat_test_data {
    compat_test_data(
      pps::canonical_schema_definition reader,
      pps::canonical_schema_definition writer,
      test_schema_type schema_type,
      absl::flat_hash_set<incompatibility> exp)
      : reader(std::move(reader))
      , writer(std::move(writer)) {
        if constexpr (std::is_same_v<
                        incompatibility,
                        pps::proto_incompatibility>) {
            absl::erase_if(exp, [schema_type](const auto& e) {
                return schema_type == test_schema_type::PROTO3
                  && (e.type() == incompatibility::Type::required_field_removed || //
                      e.type() == incompatibility::Type::required_field_added);
            });
        }
        expected = [&exp]() {
            pps::raw_compatibility_result raw;
            absl::c_for_each(std::move(exp), [&raw](auto e) {
                raw.emplace<incompatibility>(std::move(e));
            });
            return std::move(raw)(pps::verbose::yes);
        }();
    }

    pps::canonical_schema_definition reader;
    pps::canonical_schema_definition writer;
    pps::compatibility_result expected;
};
