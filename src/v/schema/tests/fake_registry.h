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

#include "schema/registry.h"

#include <seastar/core/future.hh>

namespace schema {

// This is a fake schema registry for testing. Schemas are maintained in local
// memory only, not replicated or persisted to stable storage.
class fake_registry : public schema::registry {
public:
    bool is_enabled() const override { return true; };

    ss::future<pandaproxy::schema_registry::canonical_schema_definition>
    get_schema_definition(
      pandaproxy::schema_registry::schema_id id) const override;

    ss::future<pandaproxy::schema_registry::subject_schema> get_subject_schema(
      pandaproxy::schema_registry::subject sub,
      std::optional<pandaproxy::schema_registry::schema_version> version)
      const override;

    ss::future<pandaproxy::schema_registry::schema_id> create_schema(
      pandaproxy::schema_registry::unparsed_schema unparsed) override;

    const std::vector<pandaproxy::schema_registry::subject_schema>& get_all();

private:
    std::vector<pandaproxy::schema_registry::subject_schema> _schemas;
};
} // namespace schema
