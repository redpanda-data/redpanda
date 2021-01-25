// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/members_manager.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_allocator.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

#include <chrono>
#include <system_error>
namespace cluster {

members_manager::members_manager(
  consensus_ptr raft0,
  ss::sharded<members_table>& members_table,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_allocator>& allocator,
  ss::sharded<storage::api>& storage,
  ss::sharded<ss::abort_source>& as)
  : _seed_servers(config::shard_local_cfg().seed_servers())
  , _self(make_self_broker(config::shard_local_cfg()))
  , _join_retry_jitter(config::shard_local_cfg().join_retry_timeout_ms())
  , _join_timeout(std::chrono::seconds(2))
  , _raft0(raft0)
  , _members_table(members_table)
  , _connection_cache(connections)
  , _allocator(allocator)
  , _storage(storage)
  , _as(as)
  , _rpc_tls_config(config::shard_local_cfg().rpc_server_tls()) {}

ss::future<> members_manager::start() {
    vlog(clusterlog.info, "starting cluster::members_manager...");
    // validate node id change
    return validate_configuration_invariants().then([this] {
        // join raft0
        if (!is_already_member()) {
            join_raft0();
        }

        return start_config_changes_watcher();
    });
}

ss::future<> members_manager::start_config_changes_watcher() {
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this]() {
              return _raft0
                ->wait_for_config_change(
                  _last_seen_configuration_offset, _as.local())
                .then([this](raft::offset_configuration oc) {
                    return handle_raft0_cfg_update(std::move(oc.cfg))
                      .then([this, offset = oc.offset] {
                          _last_seen_configuration_offset = offset;
                      });
                })
                .handle_exception_type(
                  [](const ss::abort_requested_exception&) {});
          });
    }).handle_exception_type([](const ss::gate_closed_exception&) {});

    // handle initial configuration
    return handle_raft0_cfg_update(_raft0->config()).then([this] {
        if (is_already_member()) {
            return maybe_update_current_node_configuration();
        }
        return ss::now();
    });
} // namespace cluster

ss::future<> members_manager::maybe_update_current_node_configuration() {
    auto active_configuration = _members_table.local().get_broker(_self.id());
    vassert(
      active_configuration.has_value(),
      "Current broker is expected to be present in members configuration");

    // configuration is up to date, do nothing
    if (*active_configuration.value() == _self) {
        return ss::now();
    }

    return dispatch_configuration_update(_self)
      .then([] {
          vlog(clusterlog.info, "Node configuration updated successfully");
      })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(clusterlog.error, "Unable to update node configuration - {}", e);
      });
}

cluster::patch<broker_ptr>
calculate_brokers_diff(members_table& m, const raft::group_configuration& cfg) {
    std::vector<broker_ptr> new_list;
    cfg.for_each_broker([&new_list](const model::broker& br) {
        new_list.push_back(ss::make_lw_shared<model::broker>(br));
    });
    std::vector<broker_ptr> old_list = m.all_brokers();

    return calculate_changed_brokers(std::move(new_list), std::move(old_list));
}

ss::future<>
members_manager::handle_raft0_cfg_update(raft::group_configuration cfg) {
    // distribute to all cluster::members_table
    return _allocator
      .invoke_on(
        partition_allocator::shard,
        [cfg](partition_allocator& allocator) {
            cfg.for_each_broker([&allocator](const model::broker& n) {
                if (!allocator.contains_node(n.id())) {
                    allocator.register_node(std::make_unique<allocation_node>(
                      allocation_node(n.id(), n.properties().cores, {})));
                }
            });
        })
      .then([this, cfg = std::move(cfg)]() mutable {
          auto diff = calculate_brokers_diff(_members_table.local(), cfg);
          return _members_table
            .invoke_on_all([cfg = std::move(cfg)](members_table& m) mutable {
                m.update_brokers(calculate_brokers_diff(m, cfg));
            })
            .then([this, diff = std::move(diff)]() mutable {
                // update internode connections
                return update_connections(std::move(diff));
            });
      });
}

ss::future<std::error_code>
members_manager::apply_update(model::record_batch b) {
    auto cfg = reflection::from_iobuf<raft::group_configuration>(
      b.copy_records().begin()->release_value());
    return handle_raft0_cfg_update(std::move(cfg)).then([] {
        return std::error_code(errc::success);
    });
}

ss::future<> members_manager::stop() {
    vlog(clusterlog.info, "stopping cluster::members_manager...");
    return _gate.close();
}

ss::future<> members_manager::update_connections(patch<broker_ptr> diff) {
    return ss::do_with(std::move(diff), [this](patch<broker_ptr>& diff) {
        return ss::do_for_each(
                 diff.deletions,
                 [this](broker_ptr removed) {
                     return remove_broker_client(
                       _self.id(), _connection_cache, removed->id());
                 })
          .then([this, &diff] {
              return ss::do_for_each(diff.additions, [this](broker_ptr b) {
                  if (b->id() == _self.id()) {
                      // Do not create client to local broker
                      return ss::make_ready_future<>();
                  }
                  return update_broker_client(
                    _self.id(),
                    _connection_cache,
                    b->id(),
                    b->rpc_address(),
                    _rpc_tls_config);
              });
          });
    });
}

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

ss::future<result<join_reply>> members_manager::dispatch_join_to_remote(
  const config::seed_server& target, model::broker joining_node) {
    vlog(clusterlog.info, "Sending join request to {}", target.addr);

    return do_with_client_one_shot<controller_client_protocol>(
      target.addr,
      _rpc_tls_config,
      [joining_node = std::move(joining_node),
       tout = rpc::clock_type::now()
              + _join_timeout](controller_client_protocol c) mutable {
          return c
            .join(join_request(std::move(joining_node)), rpc::client_opts(tout))
            .then(&rpc::get_ctx_data<join_reply>);
      });
}

void members_manager::join_raft0() {
    (void)ss::with_gate(_gate, [this] {
        vlog(clusterlog.debug, "Trying to join the cluster");
        return ss::repeat([this] {
            return dispatch_join_to_seed_server(std::cbegin(_seed_servers))
              .then([this](result<join_reply> r) {
                  bool success = r && r.value().success;
                  // stop on success or closed gate
                  if (success || _gate.is_closed() || is_already_member()) {
                      return ss::make_ready_future<ss::stop_iteration>(
                        ss::stop_iteration::yes);
                  }

                  return wait_for_next_join_retry(
                           _join_retry_jitter.next_duration(), _as.local())
                    .then([] { return ss::stop_iteration::no; });
              });
        });
    });
}

ss::future<result<join_reply>>
members_manager::dispatch_join_to_seed_server(seed_iterator it) {
    using ret_t = result<join_reply>;
    auto f = ss::make_ready_future<ret_t>(errc::seed_servers_exhausted);
    if (it == std::cend(_seed_servers)) {
        return f;
    }
    // Current node is a seed server, just call the method
    if (it->addr == _self.rpc_address()) {
        vlog(clusterlog.debug, "Using current node as a seed server");
        f = handle_join_request(_self);
    } else {
        // If seed is the other server then dispatch join requst to it
        f = dispatch_join_to_remote(*it, _self);
    }

    return f.then_wrapped([it, this](ss::future<ret_t> fut) {
        if (!fut.failed()) {
            if (auto r = fut.get0(); r.has_value()) {
                return ss::make_ready_future<ret_t>(std::move(r));
            }
        }
        vlog(
          clusterlog.info,
          "Error joining cluster using {} seed server - {}",
          it->addr,
          fut.get_exception());

        // Dispatch to next server
        return dispatch_join_to_seed_server(std::next(it));
    });
}

template<typename Func>
auto members_manager::dispatch_rpc_to_leader(Func&& f) {
    using inner_t = std::invoke_result_t<Func, controller_client_protocol>;
    using fut_t = ss::futurize<result_wrap_t<inner_t>>;

    std::optional<model::node_id> leader_id = _raft0->get_leader_id();
    if (!leader_id) {
        return fut_t::convert(errc::no_leader_controller);
    }

    auto leader = _raft0->config().find_broker(*leader_id);

    if (!leader) {
        return fut_t::convert(errc::no_leader_controller);
    }

    return with_client<controller_client_protocol, Func>(
      _self.id(),
      _connection_cache,
      *leader_id,
      leader->rpc_address(),
      _rpc_tls_config,
      std::forward<Func>(f));
}

ss::future<result<join_reply>>
members_manager::handle_join_request(model::broker broker) {
    using ret_t = result<join_reply>;
    vlog(clusterlog.info, "Processing node '{}' join request", broker.id());
    // curent node is a leader
    if (_raft0->is_leader()) {
        // Just update raft0 configuration
        // we do not use revisions in raft0 configuration, it is always revision
        // 0 which is perfectly fine. this will work like revision less raft
        // protocol.
        return _raft0
          ->add_group_members({std::move(broker)}, model::revision_id(0))
          .then([](std::error_code ec) {
              if (!ec) {
                  return ret_t(join_reply{true});
              }

              return ret_t(ec);
          });
    }
    // Current node is not the leader have to send an RPC to leader
    // controller
    return dispatch_rpc_to_leader([broker = std::move(broker),
                                   tout = rpc::clock_type::now()
                                          + _join_timeout](
                                    controller_client_protocol c) mutable {
               return c
                 .join(join_request(std::move(broker)), rpc::client_opts(tout))
                 .then(&rpc::get_ctx_data<join_reply>);
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

ss::future<> members_manager::validate_configuration_invariants() {
    static const bytes invariants_key("configuration_invariants");
    auto invariants_buf = _storage.local().kvs().get(
      storage::kvstore::key_space::controller, invariants_key);

    if (!invariants_buf) {
        // store configuration invariants
        return _storage.local().kvs().put(
          storage::kvstore::key_space::controller,
          invariants_key,
          reflection::to_iobuf(
            configuration_invariants(_self.id(), ss::smp::count)));
    }
    auto current = configuration_invariants(_self.id(), ss::smp::count);
    auto invariants = reflection::from_iobuf<configuration_invariants>(
      std::move(*invariants_buf));
    // node id changed

    if (invariants.node_id != current.node_id) {
        vlog(
          clusterlog.error,
          "Detected node id change from {} to {}. Node id change is not "
          "supported",
          invariants.node_id,
          current.node_id);
        return ss::make_exception_future(
          configuration_invariants_changed(invariants, current));
    }
    if (invariants.core_count > current.core_count) {
        vlog(
          clusterlog.error,
          "Detected change in number of cores dedicated to run redpanda."
          "Decreasing redpanda core count is not allowed. Expected core "
          "count "
          "{}, currently have {} cores.",
          invariants.core_count,
          ss::smp::count);
        return ss::make_exception_future(
          configuration_invariants_changed(invariants, current));
    }
    return ss::now();
}

ss::future<result<configuration_update_reply>>
members_manager::do_dispatch_configuration_update(
  const model::broker& target, model::broker updated_cfg) {
    if (target.id() == _self.id()) {
        return handle_configuration_update_request(
          configuration_update_request(std::move(updated_cfg), _self.id()));
    }

    return with_client<controller_client_protocol>(
      _self.id(),
      _connection_cache,
      target.id(),
      target.rpc_address(),
      _rpc_tls_config,
      [broker = std::move(updated_cfg),
       tout = rpc::clock_type::now() + _join_timeout,
       target_id = target.id()](controller_client_protocol c) mutable {
          return c
            .update_node_configuration(
              configuration_update_request(std::move(broker), target_id),
              rpc::client_opts(tout))
            .then(&rpc::get_ctx_data<configuration_update_reply>);
      });
}

ss::future<>
members_manager::dispatch_configuration_update(model::broker broker) {
    // right after start current node has no information about the current
    // leader (it may never receive one as its addres might have been changed),
    // dispatch request to any cluster node, it will eventually forward it to
    // current leader

    return ss::do_with(
      _members_table.local().all_brokers(),
      [this, broker](std::vector<broker_ptr>& all_brokers) mutable {
          return ss::repeat([this, &all_brokers, broker]() mutable {
              // shuffle brokers
              std::shuffle(
                all_brokers.begin(),
                all_brokers.end(),
                random_generators::internal::gen);
              // pick one
              auto it = std::find_if(
                std::cbegin(all_brokers),
                std::cend(all_brokers),
                [this](const broker_ptr& ptr) {
                    return ptr->id() != _self.id();
                });

              // if current node is the only node in the cluster, handle locally
              auto& target = it != std::cend(all_brokers) ? *it->get() : _self;

              return do_dispatch_configuration_update(target, broker)
                .then([](result<configuration_update_reply> r) {
                    if (r.has_error()) {
                        return ss::make_ready_future<ss::stop_iteration>(
                          ss::stop_iteration::no);
                    }
                    return ss::make_ready_future<ss::stop_iteration>(
                      ss::stop_iteration::yes);
                });
          });
      });
}

ss::future<result<configuration_update_reply>>
members_manager::handle_configuration_update_request(
  configuration_update_request req) {
    using ret_t = result<configuration_update_reply>;
    if (req.target_node != _self.id()) {
        vlog(
          clusterlog.warn,
          "Current node id {} is different than requested target: {}. Ignoring "
          "configuration update.",
          _self,
          req.target_node);
        return ss::make_ready_future<ret_t>(configuration_update_reply{false});
    }
    vlog(
      clusterlog.info, "Handling node {} configuration update", req.node.id());
    auto node_ptr = ss::make_lw_shared(std::move(req.node));
    patch<broker_ptr> broker_update_patch{
      .additions = {node_ptr}, .deletions = {}};
    auto f = update_connections(std::move(broker_update_patch));
    // Current node is not the leader have to send an RPC to leader
    // controller
    std::optional<model::node_id> leader_id = _raft0->get_leader_id();
    if (!leader_id) {
        return ss::make_ready_future<ret_t>(errc::no_leader_controller);
    }
    // curent node is a leader
    if (leader_id == _self.id()) {
        // Just update raft0 configuration
        return _raft0->update_group_member(*node_ptr).then(
          [node_ptr](std::error_code ec) {
              if (ec) {
                  return ss::make_ready_future<ret_t>(ec);
              }
              return ss::make_ready_future<ret_t>(
                configuration_update_reply{true});
          });
    }

    auto leader = _members_table.local().get_broker(*leader_id);
    if (!leader) {
        return ss::make_ready_future<ret_t>(errc::no_leader_controller);
    }

    auto tout = ss::lowres_clock::now() + _join_timeout;
    return with_client<controller_client_protocol>(
             _self.id(),
             _connection_cache,
             *leader_id,
             (*leader)->rpc_address(),
             _rpc_tls_config,
             [tout, node = *node_ptr, target = *leader_id](
               controller_client_protocol c) mutable {
                 return c
                   .update_node_configuration(
                     configuration_update_request(std::move(node), target),
                     rpc::client_opts(tout))
                   .then(&rpc::get_ctx_data<configuration_update_reply>);
             })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(
            clusterlog.warn,
            "Error while dispatching configuration update request - {}",
            e);
          return ss::make_ready_future<ret_t>(
            errc::join_request_dispatch_error);
      });
} // namespace cluster

} // namespace cluster
