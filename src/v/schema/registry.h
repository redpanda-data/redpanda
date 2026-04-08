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

#include "pandaproxy/schema_registry/fwd.h"
#include "pandaproxy/schema_registry/schema_getter.h"
#include "pandaproxy/schema_registry/types.h"

namespace schema {
/**
 * A wrapper around the schema registry implementation within Redpanda.
 *
 * Putting an interface in front of the schema registry allows for testing
 * schema registry without needing to stand up a full implementation (which
 * requires the kafka API), and since schema registry can be turned off, we
 * don't have to handle an explicit nullptr being passed around and can instead
 * create a dummy implementation for this case.
 *
 */
class registry {
public:
    static std::unique_ptr<registry>
    make_default(pandaproxy::schema_registry::api*);

    registry() = default;
    registry(const registry&) = delete;
    registry& operator=(const registry&) = delete;
    registry(registry&&) = default;
    registry& operator=(registry&&) = default;
    virtual ~registry() = default;

    virtual bool is_enabled() const = 0;

    virtual ss::future<pandaproxy::schema_registry::schema_getter*>
    getter() const = 0;

    // Returns a getter after syncing with the underlying store.
    virtual ss::future<pandaproxy::schema_registry::schema_getter*>
    synced_getter() const = 0;

    /// Synchronizes the underlying store if the time since the last sync
    /// exceeds the specified threshold.
    ///
    /// \param max_age maximum duration allowed since last sync before forcing
    ///                a new sync. If zero (default), always forces a sync.
    /// \return time point of the last sync (either current time if sync was
    ///         performed, or the previous sync time if no sync was needed).
    virtual ss::future<ss::lowres_clock::time_point>
    sync(ss::lowres_clock::duration max_age = {}) = 0;

    ss::future<std::optional<pandaproxy::schema_registry::valid_schema>>
    get_valid_schema(
      pandaproxy::schema_registry::context_schema_id schema_id) const;

    virtual ss::future<pandaproxy::schema_registry::schema_definition>
      get_schema_definition(
        pandaproxy::schema_registry::context_schema_id) const = 0;
    virtual ss::future<pandaproxy::schema_registry::stored_schema>
      get_subject_schema(
        pandaproxy::schema_registry::context_subject,
        std::optional<pandaproxy::schema_registry::schema_version>) const = 0;
    virtual ss::future<pandaproxy::schema_registry::context_schema_id>
      create_schema(pandaproxy::schema_registry::subject_schema) = 0;
};
} // namespace schema
