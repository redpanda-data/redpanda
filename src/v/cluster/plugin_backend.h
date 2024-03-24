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

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "cluster/plugin_table.h"

#include <seastar/core/sharded.hh>

namespace cluster {
/**
 * The plugin backend is responsible for dispatching controller updates into the
 * `plugin` table.
 */
class plugin_backend {
public:
    explicit plugin_backend(ss::sharded<plugin_table>*);

    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

    ss::future<std::error_code> apply_update(model::record_batch);
    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type == model::record_batch_type::plugin_update;
    }

private:
    static constexpr auto accepted_commands
      = make_commands_list<transform_update_cmd, transform_remove_cmd>();

    ss::sharded<plugin_table>* _table;
};
} // namespace cluster
