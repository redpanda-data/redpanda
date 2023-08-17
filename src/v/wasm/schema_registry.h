/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "pandaproxy/schema_registry/api.h"
#include "pandaproxy/schema_registry/types.h"

namespace wasm {
/**
 * A wrapper around the schema registry implementation within Redpanda.
 *
 * Putting an interface behind schema registry within Wasm allows for testing
 * schema registry without needing to stand up a full implementation (which
 * requires the kafka API), and since schema registry can be turned off, we
 * don't have to handle an explicit nullptr being passed around and can instead
 * create a dummy implementation for this case.
 *
 */
class schema_registry {
public:
    static std::unique_ptr<schema_registry>
    make_default(pandaproxy::schema_registry::api*);

    schema_registry() = default;
    schema_registry(const schema_registry&) = delete;
    schema_registry& operator=(const schema_registry&) = delete;
    schema_registry(schema_registry&&) = default;
    schema_registry& operator=(schema_registry&&) = default;
    virtual ~schema_registry() = default;

    virtual bool is_enabled() const = 0;

    virtual ss::future<pandaproxy::schema_registry::canonical_schema_definition>
      get_schema_definition(pandaproxy::schema_registry::schema_id) const = 0;
    virtual ss::future<pandaproxy::schema_registry::subject_schema>
      get_subject_schema(
        pandaproxy::schema_registry::subject,
        std::optional<pandaproxy::schema_registry::schema_version>) const
      = 0;
    virtual ss::future<pandaproxy::schema_registry::schema_id>
      create_schema(pandaproxy::schema_registry::unparsed_schema) = 0;
};
} // namespace wasm
