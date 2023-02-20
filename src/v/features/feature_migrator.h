// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/fwd.h"
#include "features/feature_table.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

namespace features {

/**
 * A feature migrator is responsible for waiting til a feature is in
 * 'preparing' state, then executing a certain action at least once
 * on the controller leader.
 *
 * This is not the general case of all migrations: some of them may require
 * running in parallel across all nodes (e.g. `group_metadata_migration`): this
 * class is for the relatively simple cases where we just want to update
 * some controller state on an upgrade.
 */
class feature_migrator {
public:
    feature_migrator(cluster::controller& c)
      : _controller(c) {}
    virtual ~feature_migrator() = default;

    virtual void start(ss::abort_source&);
    virtual ss::future<> stop();

protected:
    virtual features::feature get_feature() = 0;

    /**
     * If not overriding `start` and `do_migrate`, then implement
     * `do_mutate` to express the change that should be made to
     * the system during upgrade.
     */
    virtual ss::future<> do_mutate() { return ss::now(); }
    ss::future<> do_migrate();

    ss::abort_source& abort_source() { return _as->get(); }

    cluster::controller& _controller;
    ss::gate _gate;
    std::optional<std::reference_wrapper<ss::abort_source>> _as;
};

} // namespace features