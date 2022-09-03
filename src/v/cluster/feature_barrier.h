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

#include "cluster/fwd.h"
#include "cluster/types.h"
#include "rpc/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

namespace cluster {

class feature_barrier_tag_state {
public:
    feature_barrier_tag_state() {}

    explicit feature_barrier_tag_state(bool e)
      : _exited(e) {}

    explicit feature_barrier_tag_state(std::pair<model::node_id, bool> p)
      : _exited(false) {
        _nodes_entered.insert(p);
    }

    void node_enter(model::node_id nid, bool entered) {
        _nodes_entered[nid] = entered;
    }

    bool is_node_entered(model::node_id nid) {
        auto i = _nodes_entered.find(nid);
        if (i == _nodes_entered.end()) {
            return false;
        } else {
            return i->second;
        }
    }

    void complete() {
        _exited = true;
        _exit_wait.signal();
    }

    /**
     * Wait for exited=true
     */
    ss::future<> wait_abortable(ss::abort_source& as);

    bool is_complete() { return _exited; }

private:
    // Which peers have told us they entered the barrier
    absl::flat_hash_map<model::node_id, bool> _nodes_entered;

    // Have we passed through or pre-emptively cancelled the barrier?
    bool _exited{false};
    ss::condition_variable _exit_wait;
};

/**
 * State for a one-shot tagged barrier.
 *
 * Sometimes, we want to wait not only for consensus on the state of
 * a feature, but for all nodes to have seen that consensus result (i.e.
 * for the feature_table on all nodes to reflect the new state).
 *
 * Consider a data migration in the `preparing` state, where nodes will stop
 * writing to the old data location once in `preparing`: to be sure that there
 * will be no more writes to the old data, we need to check that all peers
 * have seen the preparing state.
 *
 * Subsequently, before cleaning up some old data, we might want to barrier
 * on all nodes seeing the `active` state.
 */
class feature_barrier_state_base {
public:
    using rpc_fn_ret
      = ss::future<result<rpc::client_context<feature_barrier_response>>>;
    using rpc_fn = ss::noncopyable_function<rpc_fn_ret(
      model::node_id, model::node_id, feature_barrier_tag, bool)>;

    feature_barrier_state_base(
      model::node_id self,
      members_table& members,
      ss::abort_source& as,
      ss::gate& gate,
      rpc_fn fn)
      : _members(members)
      , _as(as)
      , _gate(gate)
      , _self(self)
      , _rpc_hook(std::move(fn)) {}

    virtual ~feature_barrier_state_base() = default;

    ss::future<> barrier(feature_barrier_tag tag);

    void exit_barrier(feature_barrier_tag tag) {
        _barrier_state.erase(tag);
        _barrier_state.emplace(tag, true);
    }

    struct update_barrier_result {
        bool entered{false};
        bool complete{false};
    };

    feature_barrier_response
    update_barrier(feature_barrier_tag tag, model::node_id peer, bool entered);

    /**
     * Test helper.
     */
    const feature_barrier_tag_state&
    testing_only_peek_state(const feature_barrier_tag& tag) {
        return _barrier_state[tag];
    }

protected:
    virtual ss::future<> retry_sleep() = 0;

    members_table& _members;
    ss::abort_source& _as;
    ss::gate& _gate;

    absl::node_hash_map<feature_barrier_tag, feature_barrier_tag_state>
      _barrier_state;

    model::node_id _self;

    rpc_fn _rpc_hook;

    ss::abort_source::subscription _abort_sub;
};

/**
 * Concrete class, with a clock.
 *
 * Instantiate this with a real clock for normal builds, with a manual_clock
 * for unit testing.
 */
template<typename Clock = ss::lowres_clock>
class feature_barrier_state final : public feature_barrier_state_base {
public:
    feature_barrier_state(
      model::node_id self,
      members_table& members,
      ss::abort_source& as,
      ss::gate& gate,
      rpc_fn fn)
      : feature_barrier_state_base(self, members, as, gate, std::move(fn)) {}

protected:
    static constexpr std::chrono::duration retry_period = 500ms;

    ss::future<> retry_sleep() override {
        using namespace std::chrono_literals;
        return ss::sleep_abortable<Clock>(retry_period, _as);
    };
};

} // namespace cluster
