/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cluster/commands.h"
#include "cluster/fwd.h"

#include <seastar/core/sharded.hh>

namespace cluster {
class panda_link_backend {
public:
    explicit panda_link_backend(ss::sharded<panda_link_table>*);

    ss::future<std::error_code> apply_update(model::record_batch);
    bool is_batch_applicable(const model::record_batch&) const;

    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

private:
    static constexpr auto accepted_commands
      = make_commands_list<panda_link_upsert_cmd, panda_link_remove_cmd>();

    ss::sharded<panda_link_table>* _table;
};
} // namespace cluster
