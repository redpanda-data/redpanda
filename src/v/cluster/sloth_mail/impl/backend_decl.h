/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/members_table.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "types.h"

#include <chrono>
#include <cstddef>

using namespace std::chrono_literals;

namespace cluster::sloth_mail::impl {
template<mail_config Config>
class backend final {
    using kinds = typename Config::supported_kinds;
    using types = types<kinds>;

public:
    backend(
      Config config,
      model::node_id self,
      members_table& members,
      size_t max_buffered_bytes_per_node = 64_KiB,
      deadline_t::duration retry_in = 5min);
    backend(const backend&) = delete;
    backend(backend&&) = delete;
    backend& operator=(const backend&) = delete;
    backend& operator=(backend&&) = delete;
    ~backend() noexcept;

    void start();
    ss::future<> stop();

    template<mail_kind Kind>
    void dispatch(
      model::node_id destination,
      Kind::key_t&& key,
      Kind::value_t&& value,
      deadline_t deadline);

private:
    void force_generate_dispatch_for_all_kinds();

    struct destination_state {
        types::kv_map_t queue;
        // cumulative size of all entries in queue
        size_t cumulative_size_bytes = 0;
        // earliest deadline of all entries in queue
        deadline_t deadline = deadline_t::max();
        // Invariants to hold on scheduling points.
        // I. @deadline is `deadline_t::max()` iff queue is empty.
        // II. @in_flight_fiber_as is not triggered if not null.
        // III. A fiber shipping to the destination can be either
        // a) Defunct: having its abort_source distinct from @in_flight_fiber_as
        //    and triggered.
        // b) Healthy: having its abort_source == in_flight_fiber_as.
        // IV. Exactly one of the following is true:
        // 1) @queue is empty,
        //    @timer is disarmed,
        //    @in_flight_fiber_as == nullptr,
        //    no healthy shipping fibers
        // 2) @queue is non-empty,
        //    @timer is armed to deadline,
        //    @in_flight_fiber_as == nullptr,
        //    no healthy shipping fibers
        // 3) @timer is disarmed
        //    @in_flight_fiber_as != nullptr,
        //    one healthy shipping fiber
        // V. All shipping fibers have distinct abort_sources.
        ss::timer<deadline_t::clock> timer;
        ss::lw_shared_ptr<ss::abort_source> in_flight_fiber_as;

        destination_state(backend&, model::node_id);
        destination_state(const destination_state&) = delete;
        destination_state(destination_state&&) = default;
        destination_state& operator=(const destination_state&) = delete;
        destination_state& operator=(destination_state&& other) {
            // we should better implement move assignment for ss::timer
            return *new (this) destination_state(std::move(other));
        };
        ~destination_state();
    };
    using destination_map_t
      = chunked_hash_map<model::node_id, destination_state>;
    destination_map_t _destination_states;

    void ship_or_rearm_if_needed(destination_map_t::value_type& state);
    void ship_queued(model::node_id destination);
    void ship_queued(destination_map_t::value_type& dest_state);
    ss::future<> ship_batch(
      model::node_id destination,
      types::kv_map_t batch,
      ss::lw_shared_ptr<ss::abort_source> as);
    types::mail_request build_request(const types::kv_map_t& batch);

    notification_id_type _members_update_notification_id;
    ss::gate _gate;

    Config _config;
    model::node_id _self;
    members_table& _members;
    size_t _max_buffered_bytes_per_node;
    deadline_t::duration _retry_in;
};

} // namespace cluster::sloth_mail::impl
