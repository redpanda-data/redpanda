/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/protobuf.h"

#include <fmt/ostream.h>
#include <google/protobuf/descriptor.h>

namespace pandaproxy::schema_registry {

namespace {

namespace pb = google::protobuf;

}

struct protobuf_schema_definition::impl {
    pb::DescriptorPool _dp;
    const pb::FileDescriptor* fd{};
};

canonical_schema_definition::raw_string
protobuf_schema_definition::raw() const {
    return canonical_schema_definition::raw_string{_impl->fd->DebugString()};
}

bool operator==(
  const protobuf_schema_definition& lhs,
  const protobuf_schema_definition& rhs) {
    return lhs.raw() == rhs.raw();
}

std::ostream&
operator<<(std::ostream& os, const protobuf_schema_definition& def) {
    fmt::print(
      os, "type: {}, definition: {}", to_string_view(def.type()), def.raw()());
    return os;
}

} // namespace pandaproxy::schema_registry
