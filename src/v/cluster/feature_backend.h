/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "cluster/feature_table.h"
#include "cluster/fwd.h"
#include "cluster/types.h"

namespace cluster {

/**
 * This class applies `feature_update_cmd` messages from the raft0 log
 * onto the `feature_table`.  It is very simple: all the intelligence
 * for managing features and deciding when to activate a feature lives
 * in `feature_manager`.
 */
class feature_backend {
public:
    feature_backend(ss::sharded<feature_table>& table)
      : _feature_table(table) {}

    ss::future<std::error_code> apply_update(model::record_batch);

    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type == model::record_batch_type::feature_update;
    }

private:
    static constexpr auto accepted_commands
      = make_commands_list<feature_update_cmd>();

    ss::sharded<feature_table>& _feature_table;
};
} // namespace cluster
