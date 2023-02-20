// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "feature_migrator.h"

#include "cluster/controller.h"
#include "cluster/feature_manager.h"
#include "features/feature_table.h"
#include "features/logger.h"
#include "ssx/future-util.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/sleep.hh>

#include <type_traits>

using namespace std::chrono_literals;

namespace features {

ss::future<> feature_migrator::stop() {
    if (_as) {
        ss::abort_source& as = *_as;
        vassert(as.abort_requested(), "Stopped without requesting abort");
    }
    return _gate.close();
}

void feature_migrator::start(ss::abort_source& as) {
    _as = as;
    features::feature_table& ft = _controller.get_feature_table().local();
    if (ft.is_active(get_feature())) {
        vlog(
          featureslog.trace,
          "Feature {} already active, no migration required.",
          get_feature());
        return;
    }

    ssx::spawn_with_gate(_gate, [this]() { return do_migrate(); });
}

ss::future<> feature_migrator::do_migrate() {
    features::feature_table& ft = _controller.get_feature_table().local();
    if (ft.is_active(get_feature())) {
        vlog(
          featureslog.trace,
          "Feature {} already active, no migration required.",
          get_feature());
        co_return;
    }

    co_await ft.await_feature_preparing(get_feature(), *_as);

    while (ft.is_preparing(get_feature())
           && !abort_source().abort_requested()) {
        // If I am the controller leader, do the migration
        if (!_controller.is_raft0_leader()) {
            vlog(
              featureslog.trace,
              "Feature {} migration: not leader, in state {}",
              get_feature(),
              ft.get_state(get_feature()).get_state());
            co_await ss::sleep_abortable(5s, abort_source());
            continue;
        }

        vlog(
          featureslog.trace, "Feature {} migration: executing", get_feature());
        bool success = false;
        try {
            co_await do_mutate();

            // Successfully mutated: set the feature to active state
            co_await _controller.get_feature_manager().local().write_action(
              cluster::feature_update_action{
                .feature_name = ss::sstring(to_string_view(get_feature())),
                .action
                = cluster::feature_update_action::action_t::complete_preparing,
              });
            success = true;
        } catch (...) {
            // This may happen if we e.g. lose leadership partway through
            vlog(
              featureslog.warn,
              "Feature {} migration failed: {}",
              get_feature(),
              std::current_exception());
        }

        if (success) {
            vlog(
              featureslog.info,
              "Successfully applied migration for feature {}",
              get_feature());
            break;
        } else {
            co_await ss::sleep_abortable(5s, abort_source());
        }
    }
}

} // namespace features