// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "cluster/cluster_utils.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "random/generators.h"
#include "ssx/sformat.h"
#include "storage/api.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <chrono>

namespace cluster {

static inline ss::future<>
wait_for_next_join_retry(std::chrono::milliseconds tout, ss::abort_source& as) {
    using namespace std::chrono_literals; // NOLINT
    vlog(
      clusterlog.info,
      "Next cluster join attempt in {} milliseconds",
      tout.count());
    return ss::sleep_abortable(tout, as).handle_exception_type(
      [](const ss::sleep_aborted&) {
          vlog(clusterlog.debug, "Aborting join sequence");
      });
}

ss::future<join_node_reply>
members_manager::make_join_node_success_reply(model::node_id id) {
    // Provide the joining node with a controller snapshot, so
    // that it may load correct configuration + feature table
    // before applying the controller log.
    return _controller_stm.local().maybe_make_join_snapshot().then(
      [id](std::optional<iobuf> snapshot) {
          vlog(
            clusterlog.debug,
            "Responding to node {} join with {} byte snapshot",
            id,
            snapshot.has_value() ? snapshot.value().size_bytes() : 0);
          return join_node_reply(
            join_node_reply::status_code::success, id, std::move(snapshot));
      });
}

ss::future<result<join_node_reply>> members_manager::dispatch_join_to_remote(
  const config::seed_server& target, join_node_request&& req) {
    vlog(
      clusterlog.info,
      "Sending join request to {} timeout: {}",
      target.addr,
      _join_timeout / 1ms);
    return do_with_client_one_shot<controller_client_protocol>(
      target.addr,
      _rpc_tls_config,
      _join_timeout,
      rpc::transport_version::v2,
      [req = std::move(req), timeout = rpc::clock_type::now() + _join_timeout](
        controller_client_protocol c) mutable {
          return c.join_node(std::move(req), rpc::client_opts(timeout))
            .then(&rpc::get_ctx_data<join_node_reply>);
      });
}

void members_manager::join_raft0() {
    ssx::spawn_with_gate(_gate, [this] {
        vlog(clusterlog.debug, "Trying to join the cluster");
        return ss::repeat([this] {
                   return dispatch_join_to_seed_server(
                            std::cbegin(_seed_servers),
                            join_node_request{
                              features::feature_table::
                                get_latest_logical_version(),
                              features::feature_table::
                                get_earliest_logical_version(),
                              _storage.local().node_uuid()().to_vector(),
                              _self})
                     .then([this](result<join_node_reply> r) {
                         bool success = r && r.value().success;
                         // stop on success or closed gate
                         if (
                           success || _gate.is_closed()
                           || is_already_member()) {
                             return ss::make_ready_future<ss::stop_iteration>(
                               ss::stop_iteration::yes);
                         }

                         return wait_for_next_join_retry(
                                  std::chrono::duration_cast<
                                    std::chrono::milliseconds>(
                                    _join_retry_jitter.next_duration()),
                                  _as.local())
                           .then([] { return ss::stop_iteration::no; });
                     });
               })
          .then([this] {
              if (is_already_member()) {
                  return maybe_update_current_node_configuration();
              }
              return ss::now();
          });
    });
}

bool members_manager::try_register_node_id(
  const model::node_id& requested_node_id,
  const model::node_uuid& requested_node_uuid) {
    vassert(requested_node_id != model::unassigned_node_id, "invalid node ID");
    vlog(
      clusterlog.info,
      "Registering node ID {} as node UUID {}",
      requested_node_id,
      requested_node_uuid);
    const auto it = _id_by_uuid.find(requested_node_uuid);
    if (it == _id_by_uuid.end()) {
        if (_members_table.local().contains(requested_node_id)) {
            // The cluster was likely just upgraded from a version that didn't
            // have node UUIDs. If the node ID is already a part of the
            // member's table, accept the requested UUID.
            clusterlog.info(
              "registering node ID that is already a member of the cluster");
        }
        // This is a brand new node with node ID assignment support that's
        // requesting the given node ID.
        _id_by_uuid.emplace(requested_node_uuid, requested_node_id);
        return true;
    }
    const auto& node_id = it->second;
    return node_id == requested_node_id;
}

std::optional<model::node_id>
members_manager::get_or_assign_node_id(const model::node_uuid& node_uuid) {
    const auto it = _id_by_uuid.find(node_uuid);
    if (it == _id_by_uuid.end()) {
        while (_members_table.local().contains(_next_assigned_id)
               || _members_table.local()
                    .get_removed_node_metadata_ref(_next_assigned_id)
                    .has_value()) {
            if (_next_assigned_id == INT_MAX) {
                return std::nullopt;
            }
            ++_next_assigned_id;
        }
        if (_next_assigned_id == INT_MAX) {
            return std::nullopt;
        }
        _id_by_uuid.emplace(node_uuid, _next_assigned_id);
        vlog(
          clusterlog.info,
          "Assigned node UUID {} a node ID {}",
          node_uuid,
          _next_assigned_id);
        return _next_assigned_id++;
    }
    return it->second;
}

ss::future<result<join_node_reply>>
members_manager::dispatch_join_to_seed_server(
  seed_iterator it, const join_node_request& req) {
    using ret_t = result<join_node_reply>;
    auto f = ss::make_ready_future<ret_t>(errc::seed_servers_exhausted);
    if (it == std::cend(_seed_servers)) {
        return f;
    }
    // Current node is a seed server, just call the method
    if (it->addr == _self.rpc_address()) {
        vlog(clusterlog.debug, "Using current node as a seed server");
        f = handle_join_request(req);
    } else {
        // If seed is the other server then dispatch join requst to it.
        // Copy request because if this fails we will proceed to next
        // see server and reuse original request object
        f = dispatch_join_to_remote(*it, join_node_request(req));
    }

    return f.then_wrapped([it, this, req](ss::future<ret_t> fut) {
        try {
            auto r = fut.get();
            if (r.has_error()) {
                vlog(
                  clusterlog.warn,
                  "Error joining cluster using {} seed server - {}",
                  it->addr,
                  r.error().message());
            } else if (
              r.value().status() != join_node_reply::status_code::success) {
                if (r.value().retryable()) {
                    // This is normal: when many nodes try to join during
                    // cluster creation, some of them will get `busy` responses
                    // because the seed server is already in the middle of
                    // a raft0 config change.
                    vlog(
                      clusterlog.info,
                      "Can't joining cluster yet via {} seed server ({})",
                      it->addr,
                      r.value().status_msg());
                } else {
                    vlog(
                      clusterlog.warn,
                      "Error joining cluster using {} seed server ({})",
                      it->addr,
                      r.value().status_msg());
                }

            } else {
                return ss::make_ready_future<ret_t>(std::move(r));
            }
        } catch (...) {
            // just log an exception, we will retry joining cluster in next loop
            // iteration
            vlog(
              clusterlog.info,
              "Error joining cluster using {} seed server - {}",
              it->addr,
              std::current_exception());
        }

        // Dispatch to next server
        return dispatch_join_to_seed_server(std::next(it), req);
    });
}

template<typename Func>
auto members_manager::dispatch_rpc_to_leader(
  rpc::clock_type::duration connection_timeout, Func&& f) {
    using inner_t = std::invoke_result_t<Func, controller_client_protocol>;
    using fut_t = ss::futurize<result_wrap_t<inner_t>>;

    std::optional<model::node_id> leader_id = _raft0->get_leader_id();
    if (!leader_id) {
        return fut_t::convert(errc::no_leader_controller);
    }

    auto leader = _members_table.local().get_node_metadata_ref(
      leader_id.value());

    if (!leader) {
        return fut_t::convert(errc::no_leader_controller);
    }

    return with_client<controller_client_protocol, Func>(
      _self.id(),
      _connection_cache,
      *leader_id,
      leader->get().broker.rpc_address(),
      _rpc_tls_config,
      connection_timeout,
      std::forward<Func>(f));
}

ss::future<result<join_node_reply>> members_manager::replicate_new_node_uuid(
  const model::node_uuid& node_uuid,
  const std::optional<model::node_id>& node_id) {
    using ret_t = result<join_node_reply>;
    ss::sstring node_id_str = node_id ? ssx::sformat("node ID {}", *node_id)
                                      : "no node ID";
    vlog(
      clusterlog.debug,
      "Replicating registration of node UUID {} with {}",
      node_uuid,
      node_id_str);
    // Otherwise, replicate a request to register the UUID.
    auto errc = co_await replicate_and_wait(
      _controller_stm,
      _as,
      register_node_uuid_cmd(node_uuid, node_id),
      model::timeout_clock::now() + 30s);
    vlog(
      clusterlog.debug,
      "Registration replication completed for node UUID '{}': {}",
      node_uuid,
      errc);
    if (errc != errc::success) {
        co_return errc;
    }
    const auto assigned_node_id = get_node_id(node_uuid);
    if (node_id && assigned_node_id != *node_id) {
        vlog(
          clusterlog.warn,
          "Node registration for node UUID {} as {} completed but already "
          "assigned as {}",
          node_uuid,
          *node_id,
          assigned_node_id);
        co_return errc::invalid_request;
    }

    // On success, return the node ID.
    co_return ret_t(
      co_await make_join_node_success_reply(get_node_id(node_uuid)));
}

ss::future<result<join_node_reply>>
members_manager::handle_join_request(const join_node_request req) {
    using ret_t = result<join_node_reply>;
    using status_t = join_node_reply::status_code;

    bool req_has_node_uuid = !req.node_uuid.empty();
    if (!req_has_node_uuid) {
        vlog(
          clusterlog.warn,
          "Invalid join request for node ID {}, node UUID is required",
          req.node.id());
        co_return errc::invalid_request;
    }
    std::optional<model::node_id> req_node_id = std::nullopt;
    if (req.node.id() >= 0) {
        req_node_id = req.node.id();
    }
    if (
      req_has_node_uuid
      && req.node_uuid.size() != model::node_uuid::type::length) {
        vlog(
          clusterlog.warn,
          "Invalid join request, expected node UUID or empty; got {}-byte "
          "value",
          req.node_uuid.size());
        co_return errc::invalid_request;
    }
    model::node_uuid node_uuid;
    if (!req_node_id && !req_has_node_uuid) {
        vlog(clusterlog.warn, "Node ID assignment attempt had no node UUID");
        co_return errc::invalid_request;
    }

    ss::sstring node_uuid_str = "no node_uuid";
    if (req_has_node_uuid) {
        node_uuid = model::node_uuid(uuid_t(req.node_uuid));
        node_uuid_str = ssx::sformat("{}", node_uuid);
    }
    vlog(
      clusterlog.info,
      "Processing node '{} ({})' join request (version {}-{})",
      req.node.id(),
      node_uuid_str,
      req.earliest_logical_version,
      req.latest_logical_version);

    if (!_raft0->is_elected_leader()) {
        vlog(clusterlog.debug, "Not the leader; dispatching to leader node");
        // Current node is not the leader have to send an RPC to leader
        // controller
        co_return co_await dispatch_rpc_to_leader(
          _join_timeout,
          [req, tout = rpc::clock_type::now() + _join_timeout](
            controller_client_protocol c) mutable {
              return c.join_node(join_node_request(req), rpc::client_opts(tout))
                .then(&rpc::get_ctx_data<join_node_reply>);
          })
          .handle_exception([](const std::exception_ptr& e) {
              vlog(
                clusterlog.warn,
                "Error while dispatching join request to leader node - {}",
                e);
              return ss::make_ready_future<ret_t>(
                errc::join_request_dispatch_error);
          });
    }

    if (!_controller_stm.local().ready_to_snapshot()) {
        vlog(
          clusterlog.info,
          "Rejecting node '{} ({})' join request, cluster is not yet ready to "
          "add nodes",
          req.node.id(),
          node_uuid_str);

        co_return ret_t(
          join_node_reply{status_t::not_ready, model::unassigned_node_id});
    }

    if (likely(req_has_node_uuid)) {
        const auto it = _id_by_uuid.find(node_uuid);
        if (!req_node_id) {
            if (it == _id_by_uuid.end()) {
                // The UUID isn't yet in our table. Register it, but return,
                // expecting the node to come back with another join request
                // once its Raft subsystems are up.
                co_return co_await replicate_new_node_uuid(
                  node_uuid, req_node_id);
            }
            // The requested UUID already exists; this is a duplicate request
            // to assign a node ID. Just return the registered node ID.
            co_return ret_t(co_await make_join_node_success_reply(it->second));
        }
        // We've been passed a node ID. The caller expects to be added to the
        // Raft group by the end of this function.
        if (it == _id_by_uuid.end()) {
            // The node ID was manually provided and this is a new attempt to
            // register the UUID.
            auto r = co_await replicate_new_node_uuid(node_uuid, req_node_id);
            if (r.has_error() || !r.value().success) {
                co_return r;
            }
        } else {
            // Validate that the node ID matches the one in our table.
            if (*req_node_id != it->second) {
                co_return ret_t(
                  join_node_reply{
                    status_t::id_changed, model::unassigned_node_id});
            }
            // if node was removed from the cluster doesn't allow it to rejoin
            // with the same UUID
            if (_members_table.local()
                  .get_removed_node_metadata_ref(it->second)
                  .has_value()) {
                vlog(
                  clusterlog.warn,
                  "Preventing decommissioned node {} with UUID {} from joining "
                  "the cluster",
                  it->second,
                  it->first);
                co_return ret_t(
                  join_node_reply{
                    status_t::bad_rejoin, model::unassigned_node_id});
            }
        }

        // Proceed to adding the node ID to the controller Raft group.
        // Presumably the node that made this join request started its Raft
        // subsystem with the node ID and is waiting to join the group.
    }

    // if configuration contains the broker already just update its config
    // with data from join request

    if (_members_table.local().contains(req.node.id())) {
        vlog(
          clusterlog.info,
          "Broker {} is already member of a cluster, updating "
          "configuration",
          req.node.id());
        auto node_id = req.node.id();
        auto update_req = configuration_update_request(req.node, _self.id());
        co_return co_await handle_configuration_update_request(
          std::move(update_req))
          .then(
            [this, node_id](
              result<configuration_update_reply> r) -> ss::future<ret_t> {
                if (r) {
                    if (r.value().success) {
                        return make_join_node_success_reply(node_id).then(
                          [](join_node_reply r) { return ret_t(r); });
                    } else {
                        return ss::make_ready_future<ret_t>(join_node_reply{
                          status_t::error, model::unassigned_node_id});
                    }
                }
                return ss::make_ready_future<ret_t>(r.error());
            });
    }

    if (req.node.id() != _self.id()) {
        co_await update_broker_client(
          _self.id(),
          _connection_cache,
          req.node.id(),
          req.node.rpc_address(),
          _rpc_tls_config);
    }

    co_return co_await add_node(req.node).then(
      [this, node = req.node](std::error_code ec) {
          if (!ec) {
              vlog(
                clusterlog.info,
                "Added node {} to cluster, preparing response",
                node.id());

              return make_join_node_success_reply(node.id()).then(
                [](join_node_reply r) { return ret_t(r); });
          }
          vlog(
            clusterlog.warn,
            "Error adding node {} with id {} to cluster - {}",
            node,
            node.id(),
            ec.message());
          return ss::make_ready_future<ret_t>(ret_t(ec));
      });
}

ss::future<result<configuration_update_reply>>
members_manager::do_dispatch_configuration_update(
  model::node_id target_id,
  net::unresolved_address address,
  model::broker updated_cfg) {
    if (target_id == _self.id()) {
        return handle_configuration_update_request(
          configuration_update_request(std::move(updated_cfg), _self.id()));
    }
    vlog(
      clusterlog.trace,
      "dispatching configuration update request to {}",
      target_id);
    return with_client<controller_client_protocol>(
      _self.id(),
      _connection_cache,
      target_id,
      std::move(address),
      _rpc_tls_config,
      _join_timeout,
      [broker = std::move(updated_cfg),
       timeout = rpc::clock_type::now() + _join_timeout,
       target_id](controller_client_protocol c) mutable {
          return c
            .update_node_configuration(
              configuration_update_request(std::move(broker), target_id),
              rpc::client_opts(timeout))
            .then(&rpc::get_ctx_data<configuration_update_reply>);
      });
}

model::broker get_update_request_target(
  std::optional<model::node_id> current_leader,
  const members_table::cache_t& brokers) {
    if (current_leader) {
        auto it = brokers.find(*current_leader);

        if (it != brokers.end()) {
            return it->second.broker;
        }
    }

    return std::next(
             brokers.begin(),
             random_generators::get_int<size_t>(0, brokers.size() - 1))
      ->second.broker;
}

ss::future<>
members_manager::dispatch_configuration_update(model::broker broker) {
    // right after start current node has no information about the current
    // leader (it may never receive one as its addres might have been
    // changed), dispatch request to any cluster node, it will eventually
    // forward it to current leader
    bool update_success = false;
    while (!update_success) {
        const auto& brokers = _members_table.local().nodes();
        auto target = get_update_request_target(
          _raft0->get_leader_id(), brokers);
        auto r = co_await do_dispatch_configuration_update(
          target.id(), target.rpc_address(), broker);
        if (r.has_error() || r.value().success == false) {
            co_await ss::sleep_abortable(
              _join_retry_jitter.base_duration(), _as.local());
        } else {
            update_success = true;
        }
    }
}

ss::future<result<configuration_update_reply>>
members_manager::handle_configuration_update_request(
  configuration_update_request req) {
    if (req.target_node != _self.id()) {
        vlog(
          clusterlog.warn,
          "Current node id {} is different than requested target: {}. Ignoring "
          "configuration update.",
          _self,
          req.target_node);
        co_return configuration_update_reply{false};
    }
    vlog(
      clusterlog.trace, "Handling node {} configuration update", req.node.id());
    auto& all_brokers = _members_table.local().nodes();
    if (auto err = check_result_configuration(all_brokers, req.node); err) {
        vlog(
          clusterlog.warn,
          "Rejecting invalid configuration update. Reason: {}, new broker: {}, "
          "current brokers list: {}",
          err.value(),
          req.node,
          all_brokers);
        co_return errc::invalid_configuration_update;
    }

    try {
        co_await update_broker_client(
          _self.id(),
          _connection_cache,
          req.node.id(),
          req.node.rpc_address(),
          _rpc_tls_config);
    } catch (...) {
        vlog(
          clusterlog.warn,
          "Unable to handle configuration update due to broker update error: "
          "{}",
          std::current_exception());
        co_return configuration_update_reply{false};
    }

    // Current node is not the leader have to send an RPC to leader
    // controller
    std::optional<model::node_id> leader_id = _raft0->get_leader_id();
    if (!leader_id) {
        vlog(
          clusterlog.warn,
          "Unable to handle configuration update, no leader controller",
          req.node.id());
        co_return errc::no_leader_controller;
    }
    // curent node is a leader
    if (leader_id == _self.id()) {
        // Just update raft0 configuration
        std::error_code ec = co_await update_node(std::move(req.node));
        if (ec) {
            vlog(
              clusterlog.warn,
              "Unable to handle configuration update - {}",
              ec.message());
            co_return ec;
        }
        co_return configuration_update_reply{true};
    }

    auto leader = _members_table.local().get_node_metadata_ref(*leader_id);
    if (!leader) {
        co_return errc::no_leader_controller;
    }

    try {
        co_return co_await with_client<controller_client_protocol>(
          _self.id(),
          _connection_cache,
          *leader_id,
          leader->get().broker.rpc_address(),
          _rpc_tls_config,
          _join_timeout,
          [tout = ss::lowres_clock::now() + _join_timeout,
           node = req.node,
           target = *leader_id](controller_client_protocol c) mutable {
              return c
                .update_node_configuration(
                  configuration_update_request(std::move(node), target),
                  rpc::client_opts(tout))
                .then(&rpc::get_ctx_data<configuration_update_reply>);
          });
    } catch (...) {
        vlog(
          clusterlog.warn,
          "Error while dispatching configuration update request - {}",
          std::current_exception());
        co_return errc::join_request_dispatch_error;
    }
}

std::ostream&
operator<<(std::ostream& o, const members_manager::node_update& u) {
    fmt::print(
      o,
      "{{node_id: {}, type: {}, offset: {}, update_raft0: {}, "
      "decom_upd_revision: {}}}",
      u.id,
      u.type,
      u.offset,
      u.need_raft0_update,
      u.decommission_update_revision);
    return o;
}

ss::future<>
members_manager::initialize_broker_connection(const model::broker& broker) {
    auto broker_id = broker.id();
    vlog(
      clusterlog.trace,
      "initializing connection to broker {} at {}",
      broker_id,
      broker.rpc_address());
    co_await with_client<controller_client_protocol>(
      _self.id(),
      _connection_cache,
      broker_id,
      broker.rpc_address(),
      _rpc_tls_config,
      2s,
      [self = _self.id(), this](controller_client_protocol c) {
          hello_request req{
            .peer = self,
            .start_time = _application_start_time,
          };
          return c.hello(std::move(req), rpc::client_opts(2s))
            .then(&rpc::get_ctx_data<hello_reply>);
      })
      .then([broker_id](result<hello_reply> r) {
          if (r) {
              if (r.value().error != errc::success) {
                  vlog(
                    clusterlog.info,
                    "Hello response from {} contained error {}",
                    broker_id,
                    r.value().error);
              }
              return;
          }

          /*
           * In a rolling upgrade scenario the peer may not have the hello
           * rpc endpoint available. hello is an optimization, so ignore.
           */
          if (r.error() == rpc::errc::method_not_found) {
              vlog(
                clusterlog.debug,
                "Ignoring failed hello request to {}: {}",
                broker_id,
                r.error().message());
              return;
          }

          vlog(
            clusterlog.info,
            "Node {} did not respond to Hello message ({})",
            broker_id,
            r.error().message());
      });
}

ss::future<std::error_code> members_manager::add_node(model::broker broker) {
    return replicate_and_wait(
      _controller_stm,
      _as,
      add_node_cmd(0, std::move(broker)),
      _join_timeout + model::timeout_clock::now());
}

ss::future<std::error_code> members_manager::update_node(model::broker broker) {
    return replicate_and_wait(
      _controller_stm,
      _as,
      update_node_cfg_cmd(0, std::move(broker)),
      _join_timeout + model::timeout_clock::now());
}

ss::future<>
members_manager::persist_members_in_kvstore(model::offset update_offset) {
    static const auto cluster_members_key = bytes::from_string(
      "cluster_members");
    auto current_members_snapshot = read_members_from_kvstore();
    if (current_members_snapshot.update_offset >= update_offset) {
        vlog(
          clusterlog.trace,
          "skipping persisting members, update offset {}, current snapshot "
          "offset: {}",
          update_offset,
          current_members_snapshot.update_offset);
        return ss::now();
    }
    std::vector<model::broker> brokers;
    brokers.reserve(_members_table.local().node_count());
    for (auto& [_, node_metadata] : _members_table.local().nodes()) {
        brokers.push_back(node_metadata.broker);
    }
    for (auto id : _removed_nodes_still_in_raft0) {
        // we persist broker info for removed nodes that are still part of the
        // controller group because after restart we still need to open
        // connections to these nodes.
        auto node_md = _members_table.local().get_removed_node_metadata_ref(id);
        vassert(node_md, "metadata for removed node {} must be present", id);
        brokers.push_back(node_md.value().get().broker);
    }
    return _storage.local().kvs().put(
      storage::kvstore::key_space::controller,
      cluster_members_key,
      serde::to_iobuf(
        members_snapshot{
          .members = std::move(brokers), .update_offset = update_offset}));
}

members_manager::members_snapshot members_manager::read_members_from_kvstore() {
    static const auto cluster_members_key = bytes::from_string(
      "cluster_members");
    auto buffer = _storage.local().kvs().get(
      storage::kvstore::key_space::controller, cluster_members_key);
    if (buffer) {
        return serde::from_iobuf<members_snapshot>(std::move(*buffer));
    }
    return {};
}

} // namespace cluster
