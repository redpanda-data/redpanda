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

#include "cluster/members_table.h"
#include "cluster/node_status_rpc_service.h"
#include "cluster/node_status_table.h"
#include "config/property.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"
#include "seastarx.h"
#include "ssx/metrics.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include <absl/container/flat_hash_set.h>

namespace cluster {

/*
 * node_status_backend is the backend behind node status sub-system. Its purpose
 * is to collect metadata from which a peer's status can be inferred outside of
 * Raft. It is intended for lower-level use cases such as the RPC and consensus
 * layers. At higher levels of abstraction health_monitor should be used.
 *
 * node_status_backend runs on shard 0 of every node. Its operation is as
 * follows:
 * 1. Maintain a list of peers for this node. This is currently done via a
 * callback from the members_table.
 * 2. Send a periodic node_status RPC to all known peers
 * 3. Update the shard-local node_status_table with the metadata from the
 * responses
 */
class node_status_backend {
public:
    static constexpr ss::shard_id shard = 0;

    node_status_backend(
      model::node_id,
      ss::sharded<members_table>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<node_status_table>&,
      config::binding<std::chrono::milliseconds> /* period*/,
      config::binding<std::chrono::milliseconds> /* max_backoff*/,
      ss::sharded<ss::abort_source>&);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> drain_notifications_queue();
    ss::future<> handle_members_updated_notification(
      model::node_id, model::membership_state);

    void tick();

    ss::future<> collect_and_store_updates();
    ss::future<std::vector<node_status>> collect_updates_from_peers();

    result<node_status> process_reply(result<node_status_reply>);
    ss::future<node_status_reply> process_request(node_status_request);

    ss::future<result<node_status>>
      send_node_status_request(model::node_id, node_status_request);

    ss::future<ss::shard_id>
      maybe_create_client(model::node_id, net::unresolved_address);

    void setup_metrics(ssx::metrics::metric_groups&);

    struct statistics {
        int64_t rpcs_sent;
        int64_t rpcs_timed_out;
        int64_t rpcs_received;
    };

private:
    ss::shard_id connection_source_shard(model::node_id target) const {
        return target % ss::smp::count;
    }

    rpc::backoff_policy create_backoff_policy() const {
        static constexpr auto default_backoff_base = 1000ms;
        return rpc::make_exponential_backoff_policy<rpc::backoff_policy>(
          std::min(default_backoff_base, _max_reconnect_backoff()),
          _max_reconnect_backoff());
    }
    model::node_id _self;
    ss::sharded<members_table>& _members_table;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<node_status_table>& _node_status_table;

    config::binding<std::chrono::milliseconds> _period;
    config::binding<std::chrono::milliseconds> _max_reconnect_backoff;
    config::tls_config _rpc_tls_config;
    ss::sharded<rpc::connection_cache> _node_connection_cache;

    absl::flat_hash_set<model::node_id> _discovered_peers;
    ss::gate _gate;
    ss::timer<ss::lowres_clock> _timer;
    notification_id_type _members_table_notification_handle;

    statistics _stats{};
    ssx::metrics::metric_groups _metrics
      = ssx::metrics::metric_groups::make_internal();
    ssx::metrics::metric_groups _public_metrics
      = ssx::metrics::metric_groups::make_public();

    ss::sharded<ss::abort_source>& _as;
    struct member_notification {
        member_notification(model::node_id id, model::membership_state state)
          : id(id)
          , state(state) {}

        model::node_id id;
        model::membership_state state;
    };

    ss::chunked_fifo<member_notification> _pending_member_notifications;

    bool _draining = false;

    friend class node_status_rpc_handler;
};

} // namespace cluster
