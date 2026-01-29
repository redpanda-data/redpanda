// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "pandaproxy/schema_registry/sharded_store.h"

namespace pandaproxy::schema_registry::test_utils {

// Base store fixture for tests that need a sharded_store with lifecycle
// management. Provides automatic start/stop and common helper methods.
class store_fixture {
public:
    store_fixture();
    ~store_fixture();

    store_fixture(const store_fixture&) = delete;
    store_fixture& operator=(const store_fixture&) = delete;
    store_fixture(store_fixture&&) = delete;
    store_fixture& operator=(store_fixture&&) = delete;

    schema_id insert(
      const context_subject& sub,
      const schema_definition& schema_def,
      schema_version version);

    schema_id insert(const subject_schema& sub_schema, schema_version version);

    sharded_store& store() { return _store; }
    const sharded_store& store() const { return _store; }

protected:
    std::unique_ptr<ss::smp_service_group> _smp_svc_group;
    sharded_store _store;
    schema_id _next_id{1};
};

} // namespace pandaproxy::schema_registry::test_utils
