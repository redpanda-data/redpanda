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
#include "cluster/data_migration_backend.h"

#include "data_migration_types.h"
#include "fwd.h"
#include "logger.h"

#include <seastar/core/abort_source.hh>

namespace cluster {

data_migration_backend::data_migration_backend(
  data_migration_table& table,
  data_migration_frontend& frontend,
  ss::abort_source& as)
  : _table(table)
  , _frontend(frontend)
  , _as(as) {}

ss::future<> data_migration_backend::start() {
    _id = _table.register_notification(
      [this](data_migration_id id) { handle_migration_update(id); });
    co_return;
}

ss::future<> data_migration_backend::stop() {
    _table.unregister_notification(_id);
    co_return;
}

void data_migration_backend::handle_migration_update(data_migration_id id) {
    vlog(dm_log.debug, "received data migration {} notification", id);
}

} // namespace cluster
