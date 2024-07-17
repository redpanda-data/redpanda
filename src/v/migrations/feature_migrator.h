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
 * 'preparing' state, then executing certain actions to get it to 'active'
 * state.
 *
 * This base class provides support for both simple cases (when some action has
 * to be executed at least once on the controller leader), as well as more
 * general migrations requiring coordinating actions executed on several nodes.
 */
class feature_migrator {
public:
    feature_migrator(cluster::controller& c)
      : _controller(c) {}
    virtual ~feature_migrator() = default;

    void start(ss::abort_source&);
    ss::future<> stop();

protected:
    virtual features::feature get_feature() = 0;

    /**
     * Subclasses should override either `do_migrate` (for migrations that need
     * to be executed on all nodes) or `do_mutate` (for simpler migrations that
     * can be executed just on the controller leader).
     *
     * `do_mutate` should be idempotent as it may be executed multiple times if
     * there is a leader reelection while do_mutate is being executed.
     */
    virtual ss::future<> do_migrate();
    virtual ss::future<> do_mutate() { return ss::now(); }

    ss::abort_source& abort_source() { return _as->get(); }

    cluster::controller& _controller;
    ss::gate _gate;
    std::optional<std::reference_wrapper<ss::abort_source>> _as;
};

} // namespace features
