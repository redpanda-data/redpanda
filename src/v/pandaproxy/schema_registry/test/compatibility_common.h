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

template<typename incompatibility>
struct compat_test_data {
    compat_test_data(
      pps::canonical_schema_definition reader,
      pps::canonical_schema_definition writer,
      absl::flat_hash_set<incompatibility> exp)
      : reader(std::move(reader))
      , writer(std::move(writer))
      , expected([&exp]() {
          pps::raw_compatibility_result raw;
          absl::c_for_each(std::move(exp), [&raw](auto e) {
              raw.emplace<incompatibility>(std::move(e));
          });
          return std::move(raw)(pps::verbose::yes);
      }()) {}

    pps::canonical_schema_definition reader;
    pps::canonical_schema_definition writer;
    pps::compatibility_result expected;
};
