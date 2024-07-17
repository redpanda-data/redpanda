// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "migrations/shard_placement_migrator.h"

#include "base/vlog.h"
#include "cluster/controller.h"
#include "cluster/feature_manager.h"
#include "cluster/shard_balancer.h"
#include "features/feature_table.h"
#include "features/logger.h"

namespace features::migrators {

ss::future<> shard_placement_migrator::do_migrate() {
    features::feature_table& ft = _controller.get_feature_table().local();
    cluster::feature_manager& fm = _controller.get_feature_manager().local();
    ss::abort_source& as = _as.value();
    const auto barrier_tag = cluster::feature_barrier_tag{
      "node_local_core_assignment"};

    if (ft.is_active(_feature)) {
        fm.exit_barrier(barrier_tag);
        co_return;
    }

    co_await ft.await_feature_preparing(_feature, as);
    if (!ft.is_preparing(_feature)) {
        co_return;
    }

    // persist shard_placement_table and wait for everybody to persist theirs.
    co_await _controller.get_shard_balancer().invoke_on(
      cluster::shard_balancer::shard_id,
      [](cluster::shard_balancer& sb) { return sb.enable_persistence(); });
    vlog(featureslog.info, "entering the barrier...");
    co_await fm.barrier(barrier_tag);
    vlog(featureslog.info, "exited the barrier, all nodes ready");

    // activate the feature
    while (!as.abort_requested() && ft.is_preparing(_feature)) {
        if (_controller.is_raft0_leader()) {
            try {
                auto ec = co_await fm.write_action(
                  cluster::feature_update_action{
                    .feature_name = ss::sstring(to_string_view(_feature)),
                    .action = cluster::feature_update_action::action_t::
                      complete_preparing,
                  });

                if (!ec) {
                    vlog(featureslog.info, "migration finished");
                    break;
                } else {
                    vlog(featureslog.warn, "activating feature failed: {}", ec);
                }
            } catch (...) {
                auto ex = std::current_exception();
                if (ssx::is_shutdown_exception(ex)) {
                    break;
                } else {
                    vlog(featureslog.warn, "activating feature failed: {}", ex);
                }
            }
        }

        co_await ss::sleep_abortable(5s, as);
    }
}

} // namespace features::migrators
