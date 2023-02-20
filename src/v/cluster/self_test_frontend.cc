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

#include "cluster/self_test_frontend.h"

#include "cluster/logger.h"
#include "cluster/self_test/netcheck.h"
#include "cluster/self_test_backend.h"
#include "ssx/future-util.h"
#include "vlog.h"

namespace cluster {

namespace {

self_test_frontend::node_test_state enrich(get_status_response r) {
    return self_test_frontend::node_test_state{.response = std::move(r)};
}

self_test_frontend::node_test_state
enrich(result<rpc::client_context<get_status_response>> r) {
    if (!r.has_error()) {
        return enrich(r.value().data);
    }
    return self_test_frontend::node_test_state{};
}

} // namespace

self_test_status self_test_frontend::node_test_state::status() const {
    if (!response) {
        return self_test_status::unreachable;
    }
    return response->status;
}

self_test_frontend::global_test_state::global_test_state(
  std::vector<underlying_t::value_type>&& rs)
  : _participants(rs.begin(), rs.end()) {}

bool self_test_frontend::global_test_state::finished() const {
    return !_participants.empty() && active_participant_ids().empty();
}

std::vector<model::node_id>
self_test_frontend::global_test_state::active_participant_ids() const {
    std::vector<model::node_id> ids;
    ids.reserve(_participants.size());
    for (const auto& [id, participant] : _participants) {
        if (participant.status() == self_test_status::running) {
            ids.push_back(id);
        }
    }
    return ids;
}

/// Implementation of methods in self_test_frontend::local_invoke

ss::future<self_test_frontend::node_test_state>
self_test_frontend::local_invoke::start_test(start_test_request req) {
    co_return enrich(_self_be.local().start_test(req));
}

ss::future<self_test_frontend::node_test_state>
self_test_frontend::local_invoke::stop_test() {
    co_return enrich(co_await _self_be.local().stop_test());
}

ss::future<self_test_frontend::node_test_state>
self_test_frontend::local_invoke::get_status() {
    co_return enrich(_self_be.local().get_status());
}

/// Implementation of methods in self_test_frontend::remote_invoke

ss::future<self_test_frontend::node_test_state>
self_test_frontend::remote_invoke::start_test(start_test_request req) {
    auto v = co_await send_self_test_rpc(
      [req](self_test_rpc_client_protocol c) mutable {
          return c.start_test(
            std::move(req), rpc::client_opts(raft::clock_type::now() + 5s));
      });
    co_return enrich(v);
}

ss::future<self_test_frontend::node_test_state>
self_test_frontend::remote_invoke::stop_test() {
    auto v = co_await send_self_test_rpc([](self_test_rpc_client_protocol c) {
        /// Bigger timeout to wait for executing jobs to shutdown
        return c.stop_test(
          empty_request{}, rpc::client_opts(raft::clock_type::now() + 30s));
    });
    co_return enrich(v);
}

ss::future<self_test_frontend::node_test_state>
self_test_frontend::remote_invoke::get_status() {
    auto v = co_await send_self_test_rpc([](self_test_rpc_client_protocol c) {
        return c.get_status(
          empty_request{}, rpc::client_opts(raft::clock_type::now() + 5s));
    });
    co_return enrich(v);
}

/// Implementation of methods in self_test_frontend

self_test_frontend::self_test_frontend(
  model::node_id self,
  ss::sharded<cluster::members_table>& members,
  ss::sharded<cluster::self_test_backend>& self_be,
  ss::sharded<rpc::connection_cache>& connections)
  : _self(self)
  , _members(members)
  , _self_be(self_be)
  , _connections(connections) {}

ss::future<> self_test_frontend::start() { return ss::now(); }

ss::future<> self_test_frontend::stop() { co_await _gate.close(); }

ss::future<uuid_t> self_test_frontend::start_test(
  start_test_request req, std::vector<model::node_id> ids) {
    auto gate_holder = _gate.hold();
    if (ids.empty()) {
        throw self_test_exception("No node ids provided");
    }
    if (req.dtos.empty() && req.ntos.empty()) {
        throw self_test_exception("No tests specified to run");
    }
    /// Validate input
    const auto brokers = _members.local().node_ids();
    using nid_set_t = absl::flat_hash_set<model::node_id>;
    const nid_set_t ids_set{ids.begin(), ids.end()};
    const nid_set_t brokers_set{brokers.begin(), brokers.end()};
    if (std::any_of(
          ids_set.begin(),
          ids_set.end(),
          [&brokers_set](const model::node_id& id) {
              return !brokers_set.contains(id);
          })) {
        throw self_test_exception("Request to start self test contained "
                                  "node_ids that aren't part of the cluster");
    }

    const auto stopped_results = co_await stop_test();
    if (!stopped_results.finished()) {
        vlog(
          clusterlog.warn,
          "Could not stop already running test before calling start,  "
          "sending start RPCs out anyway.");
    }

    /// Invoke command to start test on all nodes, using the same test id
    const auto test_id = uuid_t::create();
    const auto network_plan = self_test::netcheck::network_test_plan(ids);
    co_await invoke_on_all_nodes(
      [test_id, ids_set, req, &network_plan](model::node_id nid, auto& handle) {
          /// Clear last results of nodes who don't participate in this run
          if (!ids_set.contains(nid)) {
              return handle->start_test(start_test_request{.id = test_id});
          }
          /// Alert each peer of which other peers it should run their
          /// network tests against
          auto new_ntos = req.ntos;
          if (!new_ntos.empty()) {
              auto found = network_plan.find(nid);
              if (found != network_plan.end()) {
                  for (auto& n : new_ntos) {
                      n.peers = found->second;
                      n.max_duration = n.duration * network_plan.size();
                  }
              } else {
                  /// Plan does not involve current 'node_id', therefore omit
                  /// sending any network test information to this node
                  new_ntos.clear();
              }
          }
          return handle->start_test(start_test_request{
            .id = test_id, .dtos = req.dtos, .ntos = new_ntos});
      });
    co_return test_id;
}

ss::future<self_test_frontend::global_test_state>
self_test_frontend::stop_test() {
    auto gate_holder = _gate.hold();
    int8_t retries = 3;
    auto test_state = co_await status();
    while (!test_state.finished() && retries-- > 0) {
        /// invoke_on_all will filter brokers that have shut down cleanly
        /// from its participants list, retries (if occur) will only occur
        /// on previously unreachable nodes
        test_state = co_await invoke_on_all_nodes(
          [](model::node_id, auto& handle) { return handle->stop_test(); });
        if (!test_state.finished()) {
            vlog(
              clusterlog.warn,
              "Failed to sucessfully stop all outstanding jobs on all "
              "remaining_participants: {} , trying again, retries: {}",
              test_state.active_participant_ids(),
              retries);
        }
        co_await ss::sleep(1s);
    }
    co_return test_state;
}

ss::future<self_test_frontend::global_test_state> self_test_frontend::status() {
    auto gate_holder = _gate.hold();
    co_return co_await invoke_on_all_nodes(
      [](model::node_id, auto& handle) { return handle->get_status(); });
}

template<typename Func>
ss::future<self_test_frontend::global_test_state>
self_test_frontend::invoke_on_all_nodes(Func f) {
    auto gate_holder = _gate.hold();
    auto brokers = _members.local().node_ids();
    auto node_state_vec = co_await ssx::parallel_transform(
      brokers, [this, f = std::move(f)](const auto& node_id) {
          std::unique_ptr<invoke_wrapper> handle;
          if (_self == node_id) {
              handle = std::make_unique<local_invoke>(node_id, _self_be);
          } else {
              handle = std::make_unique<remote_invoke>(
                node_id, _self, _connections);
          }
          return ss::do_with(
                   std::move(handle),
                   [node_id, f = std::move(f)](auto& handle) {
                       return f(node_id, handle);
                   })
            .then([node_id](node_test_state result) {
                return std::pair<const model::node_id, node_test_state>(
                  node_id, std::move(result));
            });
      });
    co_return global_test_state(std::move(node_state_vec));
}

} // namespace cluster
