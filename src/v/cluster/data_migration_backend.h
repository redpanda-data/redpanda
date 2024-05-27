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

namespace cluster {

class data_migration_backend {
public:
    data_migration_backend(
      data_migration_table& table,
      data_migration_frontend& frontend,
      ss::abort_source&);

    ss::future<> start();
    ss::future<> stop();

private:
    struct reconciliation_state {};
    void handle_migration_update(data_migration_id id);

    ss::future<> reconcile_data_migration(data_migration_id id);

    chunked_hash_map<data_migration_id, reconciliation_state> _states;
    ss::gate _gate;
    data_migration_table& _table;
    data_migration_frontend& _frontend;
    ss::abort_source& _as;
    data_migration_table::notification_id _id;
};
} // namespace cluster
