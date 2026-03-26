// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/members_manager.h"

#include "base/seastarx.h"
#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_service.h"
#include "cluster/controller_snapshot.h"
#include "cluster/controller_stm.h"
#include "cluster/drain_manager.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
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

#include <fmt/ranges.h>

#include <chrono>
#include <exception>
#include <system_error>
namespace cluster {

members_manager::members_manager(
  consensus_ptr raft0,
  ss::sharded<controller_stm>& controller_stm,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<members_table>& members_table,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_allocator>& allocator,
  ss::sharded<storage::api>& storage,
  ss::sharded<drain_manager>& drain_manager,
  ss::sharded<partition_balancer_state>& pb_state,
  ss::sharded<ss::abort_source>& as,
  std::chrono::milliseconds application_start_time)
  : _seed_servers(config::node().seed_servers())
  , _self(make_self_broker(config::node()))
  , _join_retry_jitter(config::shard_local_cfg().join_retry_timeout_ms())
  , _join_timeout(std::chrono::seconds(2))
  , _raft0(raft0)
  , _controller_stm(controller_stm)
  , _feature_table(feature_table)
  , _members_table(members_table)
  , _connection_cache(connections)
  , _allocator(allocator)
  , _storage(storage)
  , _drain_manager(drain_manager)
  , _pb_state(pb_state)
  , _as(as)
  , _rpc_tls_config(config::node().rpc_server_tls())
  , _update_queue(max_updates_queue_size)
  , _next_assigned_id(model::node_id(1))
  , _application_start_time(application_start_time) {
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

ss::future<> members_manager::start(std::vector<model::broker> brokers) {
    vlog(
      clusterlog.info,
      "starting  members manager with founding brokers: {}",
      brokers);
    // no initial brokers, cluster already exists, read members from kv-store
    if (brokers.empty()) {
        if (unlikely(_raft0->config().is_with_brokers())) {
            brokers = _raft0->config().brokers();
            _last_connection_update_offset
              = _raft0->get_latest_configuration_offset();
        } else {
            auto snapshot = read_members_from_kvstore();
            brokers = std::move(snapshot.members);
            _last_connection_update_offset = snapshot.update_offset;
        }
    }

    /*
     * Initialize connections to cluster members. Since raft0 is a cluster-wide
     * raft group this will create a connection to all known brokers. Once a
     * connection is established a 'hello' request is sent to the node to allow
     * it to react to the newly started node. See cluster::service::hello for
     * more information about how this signal is used. A short timeout is used
     * for the 'hello' request as this is a best effort optimization.
     */
    for (auto& b : brokers) {
        if (b.id() == _self.id()) {
            continue;
        }
        ssx::spawn_with_gate(_gate, [this, &b]() -> ss::future<> {
            return initialize_broker_connection(b);
        });
    }
    co_return;
}

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
    return _members_table.local().contains(_self.id());
}

ss::future<> members_manager::maybe_update_current_node_configuration() {
    auto current_properties = _members_table.local().get_node_metadata_ref(
      _self.id());
    vassert(
      current_properties.has_value(),
      "Current broker is expected to be present in members configuration");

    // configuration is up to date, do nothing
    if (current_properties->get().broker == _self) {
        return ss::now();
    }
    vlog(
      clusterlog.debug,
      "Redpanda broker configuration changed from {} to {}",
      current_properties.value().get().broker,
      _self);
    return dispatch_configuration_update(_self)
      .then([] {
          vlog(clusterlog.info, "Node configuration updated successfully");
      })
      .handle_exception_type([](const ss::sleep_aborted&) {})
      .handle_exception([](const std::exception_ptr& e) {
          vlog(clusterlog.error, "Unable to update node configuration - {}", e);
      });
}

members_manager::changed_nodes members_manager::calculate_changed_nodes(
  const raft::group_configuration& cfg) const {
    changed_nodes ret;
    for (auto& cfg_broker : cfg.brokers()) {
        // current members table doesn't contain configuration broker, it was
        // added
        auto node = _members_table.local().get_node_metadata_ref(
          cfg_broker.id());

        if (!node) {
            ret.added.push_back(cfg_broker);
        } else if (node->get().broker != cfg_broker) {
            ret.updated.push_back(cfg_broker);
        }
    }
    for (auto [id, broker] : _members_table.local().nodes()) {
        if (!cfg.contains_broker(id)) {
            ret.removed.push_back(id);
        }
    }

    return ret;
}

ss::future<> members_manager::handle_raft0_cfg_update(
  raft::group_configuration cfg, model::offset update_offset) {
    absl::flat_hash_set<model::node_id> fully_removed_nodes;

    // skip if configuration does not contain brokers
    if (unlikely(
          cfg.is_with_brokers()
          && update_offset < _first_node_operation_command_offset)) {
        vlog(
          clusterlog.info,
          "processing raft-0 configuration at offset: {} with brokers: {}",
          update_offset,
          cfg.brokers());

        auto diff = calculate_changed_nodes(cfg);
        for (auto& broker : diff.added) {
            vlog(
              clusterlog.debug,
              "node addition from raft-0 configuration: {}",
              broker);
            co_await do_apply_add_node(
              add_node_cmd(0, std::move(broker)), update_offset);
        }
        for (auto& broker : diff.updated) {
            vlog(
              clusterlog.debug,
              "node update from raft-0 configuration: {}",
              broker);
            co_await do_apply_update_node(
              update_node_cfg_cmd(0, std::move(broker)), update_offset);
        }
        for (auto& id : diff.removed) {
            vlog(
              clusterlog.debug,
              "node deletion from raft-0 configuration: {}",
              id);
            co_await do_apply_remove_node(
              remove_node_cmd(id, 0), update_offset);
        }

        // The cluster hasn't yet switched to using node management commands so
        // all nodes that were just removed in do_apply_remove_node are
        // considered fully removed. We can immediately close connections to
        // them.
        std::swap(_removed_nodes_still_in_raft0, fully_removed_nodes);
    } else {
        for (const auto& id : _removed_nodes_still_in_raft0) {
            if (!cfg.contains(raft::vnode(id, model::revision_id(0)))) {
                fully_removed_nodes.insert(id);
            }
        }

        for (auto id : fully_removed_nodes) {
            _removed_nodes_still_in_raft0.erase(id);
        }
    }

    for (auto id : fully_removed_nodes) {
        _in_progress_updates.erase(id);
    }

    if (update_offset >= _last_connection_update_offset) {
        for (auto id : fully_removed_nodes) {
            if (id != _self.id()) {
                co_await remove_broker_client(
                  _self.id(), _connection_cache, id);
            }
        }

        _last_connection_update_offset = update_offset;
    }
}

bool members_manager::is_batch_applicable(const model::record_batch& b) const {
    return b.header().type == model::record_batch_type::node_management_cmd
           || b.header().type == model::record_batch_type::raft_configuration;
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
          vlog(
            clusterlog.info,
            "applying decommission_node_cmd, offset: {}, node id: {}",
            update_offset,
            id);

          return dispatch_updates_to_cores(update_offset, cmd)
            .then([this, id, update_offset](std::error_code error) {
                auto f = ss::now();
                if (!error) {
                    _allocator.local().decommission_node(id);
                    auto update = node_update{
                      .id = id,
                      .type = node_update_type::decommissioned,
                      .offset = update_offset,
                    };
                    _in_progress_updates[id] = update;
                    f = _update_queue.push_eventually(std::move(update));
                }
                return f.then([error] { return error; });
            });
      },
      [this, update_offset](recommission_node_cmd cmd) mutable {
          auto id = cmd.key;
          vlog(
            clusterlog.info,
            "applying recommission_node_cmd, offset: {}, node id: {}",
            update_offset,
            id);

          // TODO: remove this part after we introduce simplified raft
          // configuration handling as this will be commands driven
          auto raft0_cfg = _raft0->config();
          if (raft0_cfg.get_state() == raft::configuration_state::joint) {
              auto it = std::find_if(
                raft0_cfg.old_config()->learners.begin(),
                raft0_cfg.old_config()->learners.end(),
                [id](const raft::vnode& vn) { return vn.id() == id; });
              /**
               * If a node is a demoted voter and about to be removed, do not
               * allow for recommissioning.
               */
              if (it != raft0_cfg.old_config()->learners.end()) {
                  return ss::make_ready_future<std::error_code>(
                    errc::invalid_node_operation);
              }
          }

          auto update_it = _in_progress_updates.find(id);
          if (update_it == _in_progress_updates.end()) {
              return ss::make_ready_future<std::error_code>(
                errc::invalid_node_operation);
          }
          if (update_it->second.type != node_update_type::decommissioned) {
              return ss::make_ready_future<std::error_code>(
                errc::invalid_node_operation);
          }
          auto corresponding_decom_rev = model::revision_id{
            update_it->second.offset};

          return dispatch_updates_to_cores(update_offset, cmd)
            .then([this, id, update_offset, corresponding_decom_rev](
                    std::error_code error) {
                auto f = ss::now();
                if (!error) {
                    _allocator.local().recommission_node(id);
                    auto update = node_update{
                      .id = id,
                      .type = node_update_type::recommissioned,
                      .offset = update_offset,
                      .decommission_update_revision = corresponding_decom_rev};
                    _in_progress_updates[id] = update;
                    f = _update_queue.push_eventually(std::move(update));
                }
                return f.then([error] { return error; });
            });
      },
      [this, update_offset](finish_reallocations_cmd cmd) mutable {
          // we do not have to dispatch this command to members table since this
          // command is only used by a backend to signal successfully finished
          // node reallocations

          model::node_id id = cmd.key;
          vlog(
            clusterlog.info,
            "applying finish_reallocations_cmd, offset: {}, node id: {}",
            update_offset,
            id);

          if (auto it = _in_progress_updates.find(id);
              it != _in_progress_updates.end()) {
              auto update_type = it->second.type;
              // We could have started decommissioning the node while we
              // were finishing reallocations for node addition or
              // recommissioning so we need to verify the update type before
              // deleting. Unfortunately, there is no way to verify that this
              // command really comes from processing the it->second update and
              // not from some earlier one, but it will be stopped anyway so we
              // can safely delete it.

              if (
                update_type == node_update_type::added
                || update_type == node_update_type::recommissioned) {
                  _in_progress_updates.erase(it);
              }
          }

          _pb_state.local().remove_node_to_rebalance(id);

          return _update_queue
            .push_eventually(
              node_update{
                .id = id,
                .type = node_update_type::reallocation_finished,
                .offset = update_offset})
            .then([] { return make_error_code(errc::success); });
      },
      [this, update_offset](maintenance_mode_cmd cmd) {
          vlog(
            clusterlog.info,
            "applying maintenance_mode_cmd, offset: {}, node id: {}, enabled: "
            "{}",
            update_offset,
            cmd.key,
            cmd.value);

          return dispatch_updates_to_cores(update_offset, cmd)
            .then([this, cmd](std::error_code error) {
                auto f = ss::now();
                if (!error && cmd.key == _self.id()) {
                    if (cmd.value) {
                        f = _drain_manager.local().drain();
                    } else {
                        f = _drain_manager.local().restore();
                    }
                }
                return f.then([error] { return error; });
            });
      },
      [this, update_offset](add_node_cmd cmd) {
          vlog(
            clusterlog.info,
            "applying node add command - broker: {}, offset: {}",
            cmd.value,
            update_offset);
          _first_node_operation_command_offset = std::min(
            update_offset, _first_node_operation_command_offset);
          return do_apply_add_node(std::move(cmd), update_offset);
      },
      [this, update_offset](update_node_cfg_cmd cmd) {
          vlog(
            clusterlog.info,
            "applying node update command - broker: {}, offset: {}",
            cmd.value,
            update_offset);
          _first_node_operation_command_offset = std::min(
            update_offset, _first_node_operation_command_offset);
          return do_apply_update_node(std::move(cmd), update_offset);
      },
      [this, update_offset](remove_node_cmd cmd) {
          vlog(
            clusterlog.info,
            "applying node delete command - node: {}, offset: {}",
            cmd.key,
            update_offset);
          _first_node_operation_command_offset = std::min(
            update_offset, _first_node_operation_command_offset);
          return do_apply_remove_node(cmd, update_offset);
      },
      [this](register_node_uuid_cmd cmd) {
          const auto& node_uuid = cmd.key;
          const auto& requested_node_id = cmd.value;
          const auto node_id_str = requested_node_id == std::nullopt
                                     ? "no node ID"
                                     : fmt::to_string(*requested_node_id);
          vlog(
            clusterlog.info,
            "Applying registration of node UUID {} with {}",
            node_uuid,
            node_id_str);
          if (requested_node_id) {
              if (likely(try_register_node_id(*requested_node_id, node_uuid))) {
                  return ss::make_ready_future<std::error_code>(errc::success);
              }
              vlog(
                clusterlog.warn,
                "Couldn't register node UUID {}, node ID {} already taken",
                node_uuid,
                requested_node_id);
              return ss::make_ready_future<std::error_code>(
                errc::join_request_dispatch_error);
          }
          auto node_id = get_or_assign_node_id(node_uuid);
          if (node_id == std::nullopt) {
              vlog(clusterlog.error, "No more node IDs to assign");
              return ss::make_ready_future<std::error_code>(
                errc::invalid_node_operation);
          }
          vlog(
            clusterlog.info, "Node UUID {} has node ID {}", node_uuid, node_id);
          return ss::make_ready_future<std::error_code>(errc::success);
      });
}

ss::future<std::error_code> members_manager::do_apply_add_node(
  add_node_cmd cmd, model::offset update_offset) {
    // update members table
    auto ec = co_await dispatch_updates_to_cores(update_offset, cmd);
    if (ec) {
        co_return ec;
    }
    co_await persist_members_in_kvstore(update_offset);
    // update partition allocator
    co_await _allocator.invoke_on(
      partition_allocator::shard, [cmd](partition_allocator& allocator) {
          allocator.upsert_allocation_node(cmd.value);
      });

    // update internode connections
    auto target_id = cmd.value.id();
    if (
      update_offset >= _last_connection_update_offset
      && target_id != _self.id()) {
        co_await update_broker_client(
          _self.id(),
          _connection_cache,
          target_id,
          cmd.value.rpc_address(),
          _rpc_tls_config);

        _last_connection_update_offset = update_offset;
    }

    auto update = node_update{
      .id = cmd.value.id(),
      .type = node_update_type::added,
      .offset = update_offset,
      .need_raft0_update = update_offset
                           >= _first_node_operation_command_offset,
    };
    _in_progress_updates[update.id] = update;

    _pb_state.local().add_node_to_rebalance(update.id);

    co_await _update_queue.push_eventually(std::move(update));

    co_return errc::success;
}

ss::future<std::error_code> members_manager::do_apply_update_node(
  update_node_cfg_cmd cmd, model::offset update_offset) {
    vlog(
      clusterlog.info,
      "processing node update command - broker: {}, offset: {}",
      cmd.value,
      update_offset);
    // update members table
    auto ec = co_await dispatch_updates_to_cores(update_offset, cmd);
    if (ec) {
        co_return ec;
    }
    co_await persist_members_in_kvstore(update_offset);
    // update partition allocator
    co_await _allocator.invoke_on(
      partition_allocator::shard, [cmd](partition_allocator& allocator) {
          allocator.upsert_allocation_node(cmd.value);
      });

    // update internode connections
    auto target_id = cmd.value.id();
    if (
      update_offset >= _last_connection_update_offset
      && target_id != _self.id()) {
        co_await update_broker_client(
          _self.id(),
          _connection_cache,
          target_id,
          cmd.value.rpc_address(),
          _rpc_tls_config);

        _last_connection_update_offset = update_offset;
    }
    co_return errc::success;
}
ss::future<std::error_code> members_manager::do_apply_remove_node(
  remove_node_cmd cmd, model::offset update_offset) {
    // update members table
    auto ec = co_await dispatch_updates_to_cores(update_offset, cmd);
    if (ec) {
        co_return ec;
    }

    _removed_nodes_still_in_raft0.insert(cmd.key);

    co_await persist_members_in_kvstore(update_offset);

    // update partition allocator
    co_await _allocator.invoke_on(
      partition_allocator::shard, [cmd](partition_allocator& allocator) {
          allocator.remove_allocation_node(cmd.key);
      });

    auto update = node_update{
      .id = cmd.key,
      .type = node_update_type::removed,
      .offset = update_offset,
      .need_raft0_update = update_offset
                           >= _first_node_operation_command_offset,
    };
    _in_progress_updates[update.id] = update;
    co_await _update_queue.push_eventually(std::move(update));

    co_return errc::success;
}
ss::future<std::error_code>
members_manager::apply_raft_configuration_batch(model::record_batch b) {
    vassert(
      b.record_count() == 1,
      "raft configuration batches are expected to have exactly one record. "
      "Current batch contains {} records",
      b.record_count());
    iobuf_parser parser(b.copy_records().front().release_value());
    auto cfg = raft::details::deserialize_configuration(parser);

    co_await handle_raft0_cfg_update(std::move(cfg), b.base_offset());

    co_return make_error_code(errc::success);
}

ss::future<>
members_manager::fill_snapshot(controller_snapshot& controller_snap) const {
    auto& snap = controller_snap.members;
    snap.node_ids_by_uuid = _id_by_uuid;
    snap.next_assigned_id = _next_assigned_id;

    _members_table.local().fill_snapshot(controller_snap);

    snap.removed_nodes_still_in_raft0 = _removed_nodes_still_in_raft0;

    for (const auto& [id, update] : _in_progress_updates) {
        snap.in_progress_updates.emplace(
          id,
          controller_snapshot_parts::members_t::update_t{
            .type = update.type,
            .offset = update.offset,
            .decommission_update_revision
            = update.decommission_update_revision});
    }

    snap.first_node_operation_command_offset
      = _first_node_operation_command_offset;

    co_return;
}

ss::future<> members_manager::apply_snapshot(
  model::offset snap_offset, const controller_snapshot& controller_snap) {
    const auto& snap = controller_snap.members;

    // 1. update uuid map

    _id_by_uuid = snap.node_ids_by_uuid;
    _next_assigned_id = snap.next_assigned_id;

    // 2. calculate brokers diff to update inter-node connections

    changed_nodes diff;
    for (const auto& [id, new_node] : snap.nodes) {
        auto old_node = _members_table.local().get_node_metadata_ref(id);
        if (!old_node) {
            diff.added.push_back(new_node.broker);
        } else if (old_node->get().broker != new_node.broker) {
            diff.updated.push_back(new_node.broker);
        }
    }
    for (const auto& [id, old_node] : _members_table.local().nodes()) {
        if (!snap.nodes.contains(id)) {
            if (!snap.removed_nodes_still_in_raft0.contains(id)) {
                diff.removed.push_back(id);
            } else {
                auto new_node = snap.removed_nodes.find(id);
                vassert(
                  new_node != snap.removed_nodes.end(),
                  "info about removed node {} must be present in the snapshot",
                  id);
                if (new_node->second.broker != old_node.broker) {
                    diff.updated.push_back(new_node->second.broker);
                }
            }
        }
    }
    for (auto id : _removed_nodes_still_in_raft0) {
        if (!snap.removed_nodes_still_in_raft0.contains(id)) {
            diff.removed.push_back(id);
        }
    }

    // 3. calculate self maintenance state diff

    std::optional<model::maintenance_state> old_self_maintenance_state;
    std::optional<model::maintenance_state> new_self_maintenance_state;
    if (auto it = _members_table.local().nodes().find(_self.id());
        it != _members_table.local().nodes().end()) {
        old_self_maintenance_state = it->second.state.get_maintenance_state();
    }
    if (auto it = snap.nodes.find(_self.id()); it != snap.nodes.end()) {
        new_self_maintenance_state = it->second.state.get_maintenance_state();
    }

    // 4. update members table

    _first_node_operation_command_offset
      = snap.first_node_operation_command_offset;

    co_await _members_table.invoke_on_all(
      [snap_offset, &controller_snap](members_table& mt) {
          return mt.apply_snapshot(snap_offset, controller_snap);
      });

    _removed_nodes_still_in_raft0 = snap.removed_nodes_still_in_raft0;

    co_await persist_members_in_kvstore(snap_offset);

    // partition allocator will be updated by topic_updates_dispatcher

    // 5. reconcile _in_progress_updates and generate corresponding
    // members_backend updates

    std::vector<node_update> updates;

    auto interrupt_previous_update = [&](model::node_id id) {
        updates.push_back(
          node_update{
            .id = id,
            .type = node_update_type::interrupted,
            .offset = snap_offset,
          });
    };

    for (const auto& [id, update] : snap.in_progress_updates) {
        bool need_raft0_update
          = (update.type == node_update_type::added
             || update.type == node_update_type::removed)
            && update.offset >= snap.first_node_operation_command_offset;

        auto new_update = node_update{
          .id = id,
          .type = update.type,
          .offset = update.offset,
          .need_raft0_update = need_raft0_update,
          .decommission_update_revision = update.decommission_update_revision};

        auto old_it = _in_progress_updates.find(id);
        if (old_it == _in_progress_updates.end()) {
            _in_progress_updates[id] = new_update;
            updates.push_back(new_update);
        } else {
            auto& old_update = old_it->second;
            if (old_update.offset != new_update.offset) {
                interrupt_previous_update(id);
                old_update = new_update;
                updates.push_back(new_update);
            }
        }

        if (update.type == node_update_type::added) {
            _pb_state.local().add_node_to_rebalance(id);
        }
    }

    for (auto old_it = _in_progress_updates.begin();
         old_it != _in_progress_updates.end();) {
        auto it_copy = old_it++;
        model::node_id node_id = it_copy->first;
        if (!snap.in_progress_updates.contains(node_id)) {
            interrupt_previous_update(node_id);
            _pb_state.local().remove_node_to_rebalance(node_id);
            _in_progress_updates.erase(it_copy);
        }
    }

    for (auto& update : updates) {
        co_await _update_queue.push_eventually(std::move(update));
    }

    // 6. update drain_mamager

    if (old_self_maintenance_state != new_self_maintenance_state) {
        bool should_drain = new_self_maintenance_state
                            == model::maintenance_state::active;
        bool should_restore = old_self_maintenance_state
                              == model::maintenance_state::active;
        if (should_drain) {
            co_return co_await _drain_manager.local().drain();
        } else if (should_restore) {
            co_return co_await _drain_manager.local().restore();
        }
    }

    // 7. update connections

    if (snap_offset >= _last_connection_update_offset) {
        for (auto& broker : diff.added) {
            if (broker.id() != _self.id()) {
                co_await update_broker_client(
                  _self.id(),
                  _connection_cache,
                  broker.id(),
                  broker.rpc_address(),
                  _rpc_tls_config);
            }
        }
        for (auto& broker : diff.updated) {
            if (broker.id() != _self.id()) {
                co_await update_broker_client(
                  _self.id(),
                  _connection_cache,
                  broker.id(),
                  broker.rpc_address(),
                  _rpc_tls_config);
            }
        }
        for (model::node_id id : diff.removed) {
            if (id != _self.id()) {
                co_await remove_broker_client(
                  _self.id(), _connection_cache, id);
            }
        }

        _last_connection_update_offset = snap_offset;
    }
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

model::node_id members_manager::get_node_id(const model::node_uuid& node_uuid) {
    const auto it = _id_by_uuid.find(node_uuid);
    vassert(
      it != _id_by_uuid.end(),
      "Node registration must be completed before calling");
    return it->second;
}

ss::future<> members_manager::set_initial_state(
  std::vector<model::broker> initial_brokers,
  uuid_map_t id_by_uuid,
  model::offset update_offset) {
    vassert(_id_by_uuid.empty(), "will not overwrite existing data");

    vlog(
      clusterlog.info,
      "initializing cluster state with initial brokers {}, and node UUID map: "
      "{} at offset: {}",
      initial_brokers,
      id_by_uuid,
      update_offset);
    // Start the node ID assignment counter just past the highest node ID. This
    // helps ensure removed seed servers are accounted for when auto-assigning
    // node IDs, since seed servers don't call get_or_assign_node_id().
    for (const auto& [uuid, id] : id_by_uuid) {
        if (id == INT_MAX) {
            _next_assigned_id = id;
            break;
        }
        _next_assigned_id = std::max(_next_assigned_id, id + 1);
    }
    _id_by_uuid = std::move(id_by_uuid);

    co_await _members_table.invoke_on_all(
      [initial_brokers](members_table& table) {
          table.set_initial_brokers(initial_brokers);
      });

    co_await persist_members_in_kvstore(update_offset);
    // update partition allocator
    co_await _allocator.invoke_on(
      partition_allocator::shard,
      [&brokers = initial_brokers](partition_allocator& allocator) {
          for (auto& b : brokers) {
              allocator.upsert_allocation_node(b);
          }
      });

    // update internode connections
    if (_last_connection_update_offset < update_offset) {
        for (auto& b : initial_brokers) {
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

        _last_connection_update_offset = update_offset;
    }
    for (auto& b : initial_brokers) {
        auto update = node_update{
          .id = b.id(),
          .type = node_update_type::added,
          .offset = model::offset{0},
          .need_raft0_update = false,
        };
        _in_progress_updates[b.id()] = update;
        co_await _update_queue.push_eventually(std::move(update));
    }
}

template<typename Cmd>
ss::future<std::error_code> members_manager::dispatch_updates_to_cores(
  model::offset update_offset, Cmd cmd) {
    auto results = co_await _members_table.map(
      [cmd = std::move(cmd), update_offset](members_table& mt) {
          return mt.apply(update_offset, cmd);
      });

    auto error = results.front();
    auto state_consistent = std::all_of(
      results.begin(), results.end(), [error](std::error_code res) {
          return error == res;
      });

    vassert(
      state_consistent,
      "State inconsistency across shards detected, "
      "expected result: {}, have: {}",
      error,
      results);
    if (error) {
        vlog(
          clusterlog.warn,
          "error applying command with type {} at offset {} - {}",
          Cmd::type,
          update_offset,
          error.message());
    }

    co_return error;
}

ss::future<> members_manager::stop() {
    vlog(clusterlog.info, "stopping cluster::members_manager...");
    return _gate.close();
}

} // namespace cluster
