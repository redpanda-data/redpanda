/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "pandaproxy/schema_registry/fwd.h"
#include "pandaproxy/schema_registry/types.h"

namespace google::protobuf {
class Descriptor;
} // namespace google::protobuf

namespace pandaproxy::schema_registry {

ss::future<protobuf_schema_definition>
make_protobuf_schema_definition(schema_getter& store, canonical_schema schema);

ss::future<canonical_schema_definition>
validate_protobuf_schema(sharded_store& store, canonical_schema schema);

ss::future<canonical_schema>
make_canonical_protobuf_schema(sharded_store& store, unparsed_schema schema);

compatibility_result check_compatible(
  const protobuf_schema_definition& reader,
  const protobuf_schema_definition& writer,
  verbose is_verbose = verbose::no);

///\brief Returns a reference to the `Descriptor` at the offset specified by
///`fields`.
/// Note that the returned reference to is an object owned by
/// `protobuf_schema_definition` and therefore should only be used while that
/// object is alive.
::result<
  std::reference_wrapper<const google::protobuf::Descriptor>,
  kafka::error_code>
descriptor(const protobuf_schema_definition&, const std::vector<int>& fields);

} // namespace pandaproxy::schema_registry
