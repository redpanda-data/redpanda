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
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "features/fwd.h"
#include "storage/fwd.h"

#include <seastar/core/future.hh>

#include <system_error>

namespace cluster {

/**
 * This class applies `feature_update_cmd` messages from the raft0 log
 * onto the `feature_table`.  It is very simple: all the intelligence
 * for managing features and deciding when to activate a feature lives
 * in `feature_manager`.
 */
class feature_backend {
public:
    feature_backend(
      ss::sharded<features::feature_table>& table,
      ss::sharded<storage::api>& storage)
      : _feature_table(table)
      , _storage(storage) {}

    ss::future<std::error_code> apply_update(model::record_batch);
    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

    /// this functions deal with the snapshot stored in local kvstore (in
    /// contrast to fill/apply_snapshot which deal with the feature table data
    /// in the replicated controller snapshot).
    bool has_local_snapshot();
    ss::future<> save_local_snapshot();

    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type == model::record_batch_type::feature_update;
    }

private:
    ss::future<> apply_feature_update_command(feature_update_cmd);
    static constexpr auto accepted_commands = make_commands_list<
      feature_update_cmd,
      feature_update_license_update_cmd>();

    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<storage::api>& _storage;
};
} // namespace cluster
