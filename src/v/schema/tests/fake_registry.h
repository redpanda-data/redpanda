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
#include "schema/registry.h"

#include <seastar/core/future.hh>

namespace schema {

namespace {
namespace ppsr = pandaproxy::schema_registry;
} // namespace

// this is a fake schema registry that works enough for the tests we need to do
// with wasm.
class fake_registry : public schema::registry {
public:
    bool is_enabled() const override { return true; };

    ss::future<ppsr::canonical_schema_definition>
    get_schema_definition(ppsr::schema_id id) const override;

    ss::future<ppsr::subject_schema> get_subject_schema(
      ppsr::subject sub,
      std::optional<ppsr::schema_version> version) const override;

    ss::future<ppsr::schema_id>
    create_schema(ppsr::unparsed_schema unparsed) override;

    const std::vector<ppsr::subject_schema>& get_all();

private:
    std::vector<ppsr::subject_schema> _schemas;
};
} // namespace schema
