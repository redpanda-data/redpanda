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

#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "pandaproxy/schema_registry/types.h"

namespace pandaproxy::schema_registry {

ss::future<avro_schema_definition>
make_avro_schema_definition(sharded_store& store, canonical_schema schema);

result<canonical_schema_definition>
sanitize_avro_schema_definition(unparsed_schema_definition def);

compatibility_result check_compatible(
  const avro_schema_definition& reader,
  const avro_schema_definition& writer,
  verbose is_verbose = verbose::no);

} // namespace pandaproxy::schema_registry
