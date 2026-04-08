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
#pragma once

#include "base/seastarx.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/future.hh>

namespace pandaproxy::schema_registry {

class schema_getter {
public:
    virtual ss::future<stored_schema> get_subject_schema(
      context_subject sub,
      std::optional<schema_version> version,
      include_deleted inc_dec) = 0;
    virtual ss::future<schema_definition>
    get_schema_definition(context_schema_id id) = 0;
    virtual ss::future<std::optional<schema_definition>>
    maybe_get_schema_definition(context_schema_id id) = 0;
    virtual ~schema_getter() = default;
};

} // namespace pandaproxy::schema_registry
