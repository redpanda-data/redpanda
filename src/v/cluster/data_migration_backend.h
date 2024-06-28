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
#include "cluster/data_migration_table.h"
#include "container/chunked_hash_map.h"
#include "data_migration_types.h"
#include "fwd.h"

#include <seastar/core/abort_source.hh>

namespace cluster::data_migrations {

class backend {
public:
    backend(migrations_table& table, frontend& frontend, ss::abort_source&);

    ss::future<> start();
    ss::future<> stop();

private:
    struct reconciliation_state {};
    void handle_migration_update(id id);

    ss::future<> reconcile_data_migration(id id);

    chunked_hash_map<id, reconciliation_state> _states;
    ss::gate _gate;
    migrations_table& _table;
    frontend& _frontend;
    ss::abort_source& _as;
    migrations_table::notification_id _id;
};
} // namespace cluster::data_migrations
