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
#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/api.h"

#include <seastar/core/coroutine.hh>
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
  ss::sharded<drain_manager>& drain_manager,
  ss::sharded<ss::abort_source>& as)
  : _seed_servers(config::node().seed_servers())
  , _self(make_self_broker(config::node()))
  , _join_retry_jitter(config::shard_local_cfg().join_retry_timeout_ms())
  , _join_timeout(std::chrono::seconds(2))
  , _raft0(raft0)
  , _members_table(members_table)
  , _connection_cache(connections)
  , _allocator(allocator)
  , _storage(storage)
  , _drain_manager(drain_manager)
  , _as(as)
  , _rpc_tls_config(config::node().rpc_server_tls())
  , _update_queue(max_updates_queue_size) {
    auto sub = _as.local().subscribe([this]() noexcept {
        _update_queue.abort(
          std::make_exception_ptr(ss::abort_requested_exception{}));
    });
    if (!sub) {
        _update_queue.abort(
          std::make_exception_ptr(ss::abort_requested_exception{}));
    } else {
        _queue_abort_subscription = std::move(*sub);
    }
}

ss::future<> members_manager::start() {
    vlog(clusterlog.info, "starting cluster::members_manager...");
    // join raft0
    for (auto& b : _raft0->config().brokers()) {
        if (b.id() == _self.id()) {
            continue;
        }
        co_await update_broker_client(
          _self.id(),
          _connection_cache,
          b.id(),
          b.rpc_address(),
          _rpc_tls_config);
    }
}

/**
 * Sends a join RPC if we aren't already a member, else sends a node
 * configuration update if our local state differs from that stored
 * in the members table.
 *
 * This is separate to start() so that calling it can be delayed until
 * after our internal RPC listener is up: as soon as we send a join message,
 * the controller leader will expect us to be listening for its raft messages,
 * and if we're not ready it'll back off and make joining take several seconds
 * longer than it should.
 * (ref https://github.com/vectorizedio/redpanda/issues/3030)
 */
ss::future<> members_manager::join_cluster() {
    if (is_already_member()) {
        ssx::spawn_with_gate(
          _gate, [this] { return maybe_update_current_node_configuration(); });
    } else {
        join_raft0();
    }

    return ss::now();
}

bool members_manager::is_already_member() const {
    return _raft0->config().contains_broker(_self.id());
}

ss::future<> members_manager::maybe_update_current_node_configuration() {
    auto active_configuration = _raft0->config().find_broker(_self.id());
    vassert(
      active_configuration.has_value(),
      "Current broker is expected to be present in members configuration");

    // configuration is up to date, do nothing
    if (active_configuration.value() == _self) {
        return ss::now();
    }
    vlog(
      clusterlog.debug,
      "Redpanda broker configuration changed from {} to {}",
      active_configuration.value(),
      _self);
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

ss::future<> members_manager::handle_raft0_cfg_update(
  raft::group_configuration cfg, model::offset update_offset) {
    // distribute to all cluster::members_table
    vlog(
      clusterlog.debug,
      "updating cluster configuration with {}",
      cfg.brokers());
    return _allocator
      .invoke_on(
        partition_allocator::shard,
        [cfg](partition_allocator& allocator) {
            allocator.update_allocation_nodes(cfg.brokers());
        })
      .then([this, cfg = std::move(cfg), update_offset]() mutable {
          auto diff = calculate_brokers_diff(_members_table.local(), cfg);
          auto added_brokers = diff.additions;
          return _members_table
            .invoke_on_all(
              [cfg = std::move(cfg), update_offset](members_table& m) mutable {
                  m.update_brokers(update_offset, cfg.brokers());
              })
            .then([this, diff = std::move(diff)]() mutable {
                // update internode connections
                return update_connections(std::move(diff));
            })
            .then([this,
                   added_nodes = std::move(added_brokers),
                   update_offset]() mutable {
                return ss::do_with(
                  std::move(added_nodes),
                  [this, update_offset](std::vector<broker_ptr>& added_nodes) {
                      return ss::do_for_each(
                        added_nodes,
                        [this, update_offset](const broker_ptr& broker) {
                            return _update_queue.push_eventually(node_update{
                              .id = broker->id(),
                              .type = node_update_type::added,
                              .offset = update_offset,
                            });
                        });
                  });
            });
      });
}

ss::future<std::error_code>
members_manager::apply_update(model::record_batch b) {
    if (b.header().type == model::record_batch_type::raft_configuration) {
        co_return co_await apply_raft_configuration_batch(std::move(b));
    }

    auto update_offset = b.base_offset();
    // handle node managements command
    auto cmd = co_await cluster::deserialize(std::move(b), accepted_commands);

    co_return co_await ss::visit(
      cmd,
      [this, update_offset](decommission_node_cmd cmd) mutable {
          auto id = cmd.key;
          return dispatch_updates_to_cores(update_offset, cmd)
            .then([this, id](std::error_code error) {
                auto f = ss::now();
                if (!error) {
                    _allocator.local().decommission_node(id);
                    f = _update_queue.push_eventually(node_update{
                      .id = id, .type = node_update_type::decommissioned});
                }
                return f.then([error] { return error; });
            });
      },
      [this, update_offset](recommission_node_cmd cmd) mutable {
          auto id = cmd.key;
          return dispatch_updates_to_cores(update_offset, cmd)
            .then([this, id](std::error_code error) {
                auto f = ss::now();
                if (!error) {
                    _allocator.local().recommission_node(id);
                    f = _update_queue.push_eventually(node_update{
                      .id = id, .type = node_update_type::recommissioned});
                }
                return f.then([error] { return error; });
            });
      },
      [this](finish_reallocations_cmd cmd) mutable {
          // we do not have to dispatch this command to members table since this
          // command is only used by a backend to signal successfully finished
          // node reallocations
          return _update_queue
            .push_eventually(node_update{
              .id = cmd.key, .type = node_update_type::reallocation_finished})
            .then([] { return make_error_code(errc::success); });
      });
}
ss::future<std::error_code>
members_manager::apply_raft_configuration_batch(model::record_batch b) {
    vassert(
      b.record_count() == 1,
      "raft configuration batches are expected to have exactly one record. "
      "Current batch contains {} records",
      b.record_count());

    // members manager already seen this configuration, skip
    if (b.base_offset() < _last_seen_configuration_offset) {
        co_return make_error_code(errc::success);
    }

    auto cfg = reflection::from_iobuf<raft::group_configuration>(
      b.copy_records().front().release_value());

    co_await handle_raft0_cfg_update(std::move(cfg), b.base_offset());

    co_return make_error_code(errc::success);
}

ss::future<std::vector<members_manager::node_update>>
members_manager::get_node_updates() {
    if (_update_queue.empty()) {
        return _update_queue.pop_eventually().then(
          [](node_update update) { return std::vector<node_update>{update}; });
    }

    std::vector<node_update> ret;
    ret.reserve(_update_queue.size());
    while (!_update_queue.empty()) {
        ret.push_back(_update_queue.pop());
    }

    return ss::make_ready_future<std::vector<node_update>>(std::move(ret));
}

template<typename Cmd>
ss::future<std::error_code> members_manager::dispatch_updates_to_cores(
  model::offset update_offset, Cmd cmd) {
    return _members_table
      .map([cmd, update_offset](members_table& mt) {
          return mt.apply(update_offset, cmd);
      })
      .then([](std::vector<std::error_code> results) {
          auto sentinel = results.front();
          auto state_consistent = std::all_of(
            results.begin(), results.end(), [sentinel](std::error_code res) {
                return sentinel == res;
            });

          vassert(
            state_consistent,
            "State inconsistency across shards detected, "
            "expected result: {}, have: {}",
            sentinel,
            results);

          return sentinel;
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

ss::future<result<join_node_reply>> members_manager::dispatch_join_to_remote(
  const config::seed_server& target, join_node_request&& req) {
    vlog(clusterlog.info, "Sending join request to {}", target.addr);
    return do_with_client_one_shot<controller_client_protocol>(
      target.addr,
      _rpc_tls_config,
      _join_timeout,
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
                            std::move(join_node_request{
                              feature_table::get_latest_logical_version(),
                              _self}))
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
                                  _join_retry_jitter.next_duration(),
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

ss::future<result<join_node_reply>>
members_manager::dispatch_join_to_seed_server(
  seed_iterator it, join_node_request const& req) {
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
            auto r = fut.get0();
            if (r && r.value().success) {
                return ss::make_ready_future<ret_t>(r);
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
      connection_timeout,
      std::forward<Func>(f));
}

ss::future<result<join_node_reply>>
members_manager::handle_join_request(join_node_request const req) {
    using ret_t = result<join_node_reply>;
    vlog(
      clusterlog.info,
      "Processing node '{}' join request (version {})",
      req.node.id(),
      req.logical_version);

    // curent node is a leader
    if (_raft0->is_leader()) {
        // if configuration contains the broker already just update its config
        // with data from join request

        if (_raft0->config().contains_broker(req.node.id())) {
            vlog(
              clusterlog.info,
              "Broker {} is already member of a cluster, updating "
              "configuration",
              req.node.id());
            auto node_id = req.node.id();
            auto update_req = configuration_update_request(
              req.node, _self.id());
            co_return co_await handle_configuration_update_request(
              std::move(update_req))
              .then([node_id](result<configuration_update_reply> r) {
                  if (r) {
                      auto success = r.value().success;
                      return ret_t(join_node_reply{
                        .success = success,
                        .id = success ? node_id : model::node_id{-1}});
                  }
                  return ret_t(r.error());
              });
        }

        if (_raft0->config().contains_address(req.node.rpc_address())) {
            vlog(
              clusterlog.info,
              "Broker {} address ({}) conflicts with the address of another "
              "node",
              req.node.id(),
              req.node.rpc_address());
            co_return ret_t(join_node_reply{.success = false});
        }
        if (req.node.id() != _self.id()) {
            co_await update_broker_client(
              _self.id(),
              _connection_cache,
              req.node.id(),
              req.node.rpc_address(),
              _rpc_tls_config);
        }
        // Just update raft0 configuration
        // we do not use revisions in raft0 configuration, it is always revision
        // 0 which is perfectly fine. this will work like revision less raft
        // protocol.
        co_return co_await _raft0
          ->add_group_members({req.node}, model::revision_id(0))
          .then([broker = req.node](std::error_code ec) {
              if (!ec) {
                  return ret_t(join_node_reply{true, broker.id()});
              }
              vlog(
                clusterlog.warn,
                "Error adding node {} to cluster - {}",
                broker,
                ec.message());
              return ret_t(ec);
          });
    }
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

/**
 * Validate that:
 * - node_id never changes
 * - core count only increases, never decreases.
 *
 * Core count decreases are forbidden because our partition placement
 * code does not know how to re-assign partitions away from non-existent
 * cores if some cores are removed.  This may be improved in future, at
 * which time we may remove this restriction on core count decreases.
 *
 * These checks are applied early during startup based on a locally
 * stored record from previous startup, to prevent a misconfigured node
 * from startup up far enough to disrupt the rest of the cluster.
 * @return
 */
ss::future<> members_manager::validate_configuration_invariants() {
    static const bytes invariants_key("configuration_invariants");
    auto invariants_buf = _storage.local().kvs().get(
      storage::kvstore::key_space::controller, invariants_key);

    auto current = configuration_invariants(_self.id(), ss::smp::count);

    if (!invariants_buf) {
        // store configuration invariants
        return _storage.local().kvs().put(
          storage::kvstore::key_space::controller,
          invariants_key,
          reflection::to_iobuf(std::move(current)));
    }
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
    } else if (invariants.core_count != current.core_count) {
        // Update the persistent invariants to reflect increased core
        // count -- this tracks the high water mark of core count, to
        // reject subsequent decreases.
        return _storage.local().kvs().put(
          storage::kvstore::key_space::controller,
          invariants_key,
          reflection::to_iobuf(std::move(current)));
    }
    return ss::now();
}

ss::future<result<configuration_update_reply>>
members_manager::do_dispatch_configuration_update(
  model::broker target, model::broker updated_cfg) {
    if (target.id() == _self.id()) {
        return handle_configuration_update_request(
          configuration_update_request(std::move(updated_cfg), _self.id()));
    }
    vlog(
      clusterlog.trace,
      "dispatching configuration update request to {}",
      target);
    return with_client<controller_client_protocol>(
      _self.id(),
      _connection_cache,
      target.id(),
      target.rpc_address(),
      _rpc_tls_config,
      _join_timeout,
      [broker = std::move(updated_cfg),
       timeout = rpc::clock_type::now() + _join_timeout,
       target_id = target.id()](controller_client_protocol c) mutable {
          return c
            .update_node_configuration(
              configuration_update_request(std::move(broker), target_id),
              rpc::client_opts(timeout))
            .then(&rpc::get_ctx_data<configuration_update_reply>);
      });
}

model::broker get_update_request_target(
  std::optional<model::node_id> current_leader,
  const std::vector<model::broker>& brokers) {
    if (current_leader) {
        auto it = std::find_if(
          brokers.cbegin(),
          brokers.cend(),
          [current_leader](const model::broker& b) {
              return b.id() == current_leader;
          });

        if (it != brokers.cend()) {
            return *it;
        }
    }
    return brokers[random_generators::get_int(brokers.size() - 1)];
}

ss::future<>
members_manager::dispatch_configuration_update(model::broker broker) {
    // right after start current node has no information about the current
    // leader (it may never receive one as its addres might have been
    // changed), dispatch request to any cluster node, it will eventually
    // forward it to current leader
    bool update_success = false;
    while (!update_success) {
        auto brokers = _raft0->config().brokers();
        auto target = get_update_request_target(
          _raft0->get_leader_id(), brokers);
        auto r = co_await do_dispatch_configuration_update(target, broker);
        if (r.has_error() || r.value().success == false) {
            co_await ss::sleep_abortable(
              _join_retry_jitter.base_duration(), _as.local());
        } else {
            update_success = true;
        }
    }
}

bool is_result_configuration_valid(
  const std::vector<broker_ptr>& current_brokers,
  const model::broker& to_update) {
    /**
     * validate if any two of the brokers would listen on the same addresses
     * after applying configuration update
     */
    for (auto& current : current_brokers) {
        if (current->id() == to_update.id()) {
            /**
             * do no allow to decrease node core count
             */
            if (current->properties().cores > to_update.properties().cores) {
                return false;
            }
            continue;
        }
        // error, nodes would point to the same addresses
        if (current->rpc_address() == to_update.rpc_address()) {
            return false;
        }
        for (auto& current_ep : current->kafka_advertised_listeners()) {
            auto any_is_the_same = std::any_of(
              to_update.kafka_advertised_listeners().begin(),
              to_update.kafka_advertised_listeners().end(),
              [&current_ep](const model::broker_endpoint& ep) {
                  return current_ep == ep;
              });
            // error, kafka endpoint would point to the same addresses
            if (any_is_the_same) {
                return false;
            }
        }
    }
    return true;
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
      clusterlog.trace, "Handling node {} configuration update", req.node.id());
    auto all_brokers = _members_table.local().all_brokers();
    if (!is_result_configuration_valid(all_brokers, req.node)) {
        vlog(
          clusterlog.warn,
          "Rejecting invalid configuration update: {}, current brokers list: "
          "{}",
          req.node,
          all_brokers);
        return ss::make_ready_future<ret_t>(errc::invalid_configuration_update);
    }
    auto node_ptr = ss::make_lw_shared(std::move(req.node));
    patch<broker_ptr> broker_update_patch{
      .additions = {node_ptr}, .deletions = {}};
    auto f = update_connections(std::move(broker_update_patch));
    // Current node is not the leader have to send an RPC to leader
    // controller
    std::optional<model::node_id> leader_id = _raft0->get_leader_id();
    if (!leader_id) {
        vlog(
          clusterlog.warn,
          "Unable to handle configuration update, no leader controller",
          req.node.id());
        return ss::make_ready_future<ret_t>(errc::no_leader_controller);
    }
    // curent node is a leader
    if (leader_id == _self.id()) {
        // Just update raft0 configuration
        return _raft0->update_group_member(*node_ptr).then(
          [node_ptr](std::error_code ec) {
              if (ec) {
                  vlog(
                    clusterlog.warn,
                    "Unable to handle configuration update - {}",
                    ec.message());
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

    return with_client<controller_client_protocol>(
             _self.id(),
             _connection_cache,
             *leader_id,
             (*leader)->rpc_address(),
             _rpc_tls_config,
             _join_timeout,
             [tout = ss::lowres_clock::now() + _join_timeout,
              node = *node_ptr,
              target = *leader_id](controller_client_protocol c) mutable {
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
}
std::ostream&
operator<<(std::ostream& o, const members_manager::node_update_type& tp) {
    switch (tp) {
    case members_manager::node_update_type::added:
        return o << "added";
    case members_manager::node_update_type::decommissioned:
        return o << "decommissioned";
    case members_manager::node_update_type::recommissioned:
        return o << "recommissioned";
    case members_manager::node_update_type::reallocation_finished:
        return o << "reallocation_finished";
    }
    return o << "unknown";
}

std::ostream&
operator<<(std::ostream& o, const members_manager::node_update& u) {
    fmt::print(o, "{{node_id: {}, type: {}}}", u.id, u.type);
    return o;
}

} // namespace cluster
