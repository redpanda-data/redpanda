// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/unresolved_partition_spec.h"

#include "base/vassert.h"
#include "iceberg/transform.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <type_traits>

namespace iceberg {

bool unresolved_partition_spec::is_valid_for_default_spec() const {
    static constexpr std::string_view redpanda_field = "redpanda";
    static const std::vector<std::string_view> allowed_fields = {
      "partition",
      "timestamp",
      "key",
    };
    for (const auto& field : fields) {
        if (field.source_name.size() != 2) {
            return false;
        }
        if (field.source_name[0] != redpanda_field) {
            return false;
        }
        if (
          std::find(
            allowed_fields.begin(), allowed_fields.end(), field.source_name[1])
          == allowed_fields.end()) {
            return false;
        }
    }
    return true;
}

namespace {
template<typename Transform>
constexpr auto transform_suffix = fixed_string{"_"} + Transform::key;

template<>
constexpr auto transform_suffix<identity_transform> = fixed_string{""};
} // namespace

void unresolved_partition_spec::field::autogenerate_name() {
    vassert(name == "", "partition already has a name");
    name = fmt::format(
      "{}{}",
      fmt::join(this->source_name, "."),
      std::visit(
        [](auto tr) -> std::string_view {
            return transform_suffix<decltype(tr)>;
        },
        transform));
}

std::ostream&
operator<<(std::ostream& o, const unresolved_partition_spec::field& f) {
    fmt::print(
      o,
      "{{source_name: {}, transform: {}, name: {}}}",
      f.source_name,
      f.transform,
      f.name);
    return o;
}

std::ostream& operator<<(std::ostream& o, const unresolved_partition_spec& ps) {
    fmt::print(o, "{{fields: {}}}", ps.fields);
    return o;
}

} // namespace iceberg
