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

namespace cluster::data_migrations {

backend::backend(
  migrations_table& table, frontend& frontend, ss::abort_source& as)
  : _table(table)
  , _frontend(frontend)
  , _as(as) {}

void backend::start() {
    _id = _table.register_notification(
      [this](id id) { handle_migration_update(id); });
}

ss::future<> backend::stop() {
    _table.unregister_notification(_id);
    co_return;
}

void backend::handle_migration_update(id id) {
    vlog(dm_log.debug, "received data migration {} notification", id);
}

ss::future<check_ntp_states_reply>
backend::check_ntp_states_locally(check_ntp_states_request&& req) {
    vlog(dm_log.debug, "processing node request {}", req);
    check_ntp_states_reply reply;

    vlog(dm_log.debug, "node request reply: {}", reply);
    return ssx::now(std::move(reply));
}

} // namespace cluster::data_migrations
