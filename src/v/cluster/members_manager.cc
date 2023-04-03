// Copyright 2020 Redpanda Data, Inc.
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
#include "cluster/controller_service.h"
#include "cluster/controller_snapshot.h"
#include "cluster/controller_stm.h"
#include "cluster/drain_manager.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/types.h"
#include "random/generators.h"
#include "redpanda/application.h"
#include "reflection/adl.h"
#include "seastarx.h"
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
  ss::sharded<ss::abort_source>& as)
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
  , _as(as)
  , _rpc_tls_config(config::node().rpc_server_tls())
  , _update_queue(max_updates_queue_size)
  , _next_assigned_id(model::node_id(1)) {
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
            clusterlog.trace,
            "applying decommission_node_cmd, offset: {}, node id: {}",
            id,
            update_offset);

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
            clusterlog.trace,
            "applying recommission_node_cmd, offset: {}, node id: {}",
            id,
            update_offset);

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
            clusterlog.trace,
            "applying finish_reallocations_cmd, offset: {}, node id: {}",
            id,
            update_offset);

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
          return _update_queue
            .push_eventually(node_update{
              .id = id,
              .type = node_update_type::reallocation_finished,
              .offset = update_offset})
            .then([] { return make_error_code(errc::success); });
      },
      [this, update_offset](maintenance_mode_cmd cmd) {
          vlog(
            clusterlog.trace,
            "applying maintenance_mode_cmd, offset: {}, node id: {}, enabled: "
            "{}",
            cmd.key,
            update_offset,
            cmd.value);

          return dispatch_updates_to_cores(update_offset, cmd)
            .then([this, cmd](std::error_code error) {
                auto f = ss::now();
                if (!error && cmd.key == _self.id()) {
                    f = _drain_manager.invoke_on_all(
                      [enabled = cmd.value](cluster::drain_manager& dm) {
                          if (enabled) {
                              return dm.drain();
                          } else {
                              return dm.restore();
                          }
                      });
                }
                return f.then([error] { return error; });
            });
      },
      [this, update_offset](add_node_cmd cmd) {
          vlog(
            clusterlog.info,
            "processing node add command - broker: {}, offset: {}",
            cmd.value,
            update_offset);
          _first_node_operation_command_offset = std::min(
            update_offset, _first_node_operation_command_offset);
          return do_apply_add_node(std::move(cmd), update_offset);
      },
      [this, update_offset](update_node_cfg_cmd cmd) {
          vlog(
            clusterlog.info,
            "processing node update command - broker: {}, offset: {}",
            cmd.value,
            update_offset);
          _first_node_operation_command_offset = std::min(
            update_offset, _first_node_operation_command_offset);
          return do_apply_update_node(std::move(cmd), update_offset);
      },
      [this, update_offset](remove_node_cmd cmd) {
          vlog(
            clusterlog.info,
            "processing node delete command - node: {}, offset: {}",
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

    auto cfg = reflection::from_iobuf<raft::group_configuration>(
      b.copy_records().front().release_value());

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
        updates.push_back(node_update{
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
    }

    for (auto old_it = _in_progress_updates.begin();
         old_it != _in_progress_updates.end();) {
        auto it_copy = old_it++;
        if (!snap.in_progress_updates.contains(it_copy->first)) {
            interrupt_previous_update(it_copy->first);
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
        if (should_drain || should_restore) {
            co_await _drain_manager.invoke_on_all(
              [should_drain](cluster::drain_manager& dm) {
                  if (should_drain) {
                      return dm.drain();
                  } else {
                      return dm.restore();
                  }
              });
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
  std::vector<model::broker> initial_brokers, uuid_map_t id_by_uuid) {
    vassert(_id_by_uuid.empty(), "will not overwrite existing data");
    if (!id_by_uuid.empty()) {
        vlog(clusterlog.info, "Initial node UUID map: {}", id_by_uuid);
    }
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
      [initial_brokers = std::move(initial_brokers)](members_table& table) {
          table.set_initial_brokers(initial_brokers);
      });

    co_await persist_members_in_kvstore(model::offset(0));
    // update partition allocator
    co_await _allocator.invoke_on(
      partition_allocator::shard,
      [&brokers = initial_brokers](partition_allocator& allocator) {
          for (auto& b : brokers) {
              allocator.upsert_allocation_node(b);
          }
      });

    // update internode connections

    if (_last_connection_update_offset < model::offset{0}) {
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

        _last_connection_update_offset = model::offset{0};
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
                            std::move(join_node_request{
                              features::feature_table::
                                get_latest_logical_version(),
                              features::feature_table::
                                get_earliest_logical_version(),
                              _storage.local().node_uuid()().to_vector(),
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
            if (r.has_error() || !r.value().success) {
                vlog(
                  clusterlog.warn,
                  "Error joining cluster using {} seed server - {}",
                  it->addr,
                  r.has_error() ? r.error().message() : "not allowed to join");
            } else {
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
      _feature_table,
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
    co_return ret_t(join_node_reply{true, get_node_id(node_uuid)});
}

static bool contains_address(
  const net::unresolved_address& address,
  const members_table::cache_t& brokers) {
    return std::find_if(
             brokers.begin(),
             brokers.end(),
             [&address](const auto& p) {
                 return p.second.broker.rpc_address() == address;
             })
           != brokers.end();
}

ss::future<result<join_node_reply>>
members_manager::handle_join_request(join_node_request const req) {
    using ret_t = result<join_node_reply>;

    bool node_id_assignment_supported = _feature_table.local().is_active(
      features::feature::node_id_assignment);
    bool req_has_node_uuid = !req.node_uuid.empty();
    if (node_id_assignment_supported && !req_has_node_uuid) {
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
    if (!node_id_assignment_supported && !req_node_id) {
        vlog(
          clusterlog.warn,
          "Got request to assign node ID, but feature not active",
          req.node.id());
        co_return errc::invalid_request;
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

    if (likely(node_id_assignment_supported && req_has_node_uuid)) {
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
            co_return ret_t(join_node_reply{true, it->second});
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
                  join_node_reply{false, model::unassigned_node_id});
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
                  join_node_reply{false, model::unassigned_node_id});
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
          .then([node_id](result<configuration_update_reply> r) {
              if (r) {
                  auto success = r.value().success;
                  return ret_t(join_node_reply{
                    success, success ? node_id : model::unassigned_node_id});
              }
              return ret_t(r.error());
          });
    }

    // Older versions of Redpanda don't support having multiple servers pointed
    // at the same address.
    if (
      !node_id_assignment_supported
      && contains_address(
        req.node.rpc_address(), _members_table.local().nodes())) {
        vlog(
          clusterlog.info,
          "Broker {} address ({}) conflicts with the address of another "
          "node",
          req.node.id(),
          req.node.rpc_address());
        co_return ret_t(join_node_reply{false, model::unassigned_node_id});
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
      [node = req.node](std::error_code ec) {
          if (!ec) {
              vlog(clusterlog.info, "Added node {} to cluster", node.id());
              return ret_t(join_node_reply{true, node.id()});
          }
          vlog(
            clusterlog.warn,
            "Error adding node {} with id {} to cluster - {}",
            node,
            node.id(),
            ec.message());
          return ret_t(ec);
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
          target.id(), std::move(target.rpc_address()), broker);
        if (r.has_error() || r.value().success == false) {
            co_await ss::sleep_abortable(
              _join_retry_jitter.base_duration(), _as.local());
        } else {
            update_success = true;
        }
    }
}

/**
 * @brief Check that the configuration is valid, if not return a string with the
 * error cause.
 *
 * @param current_brokers current broker vector
 * @param to_update broker being added
 * @return std::optional<ss::sstring> - present if there is an error, nullopt
 * otherwise
 */
std::optional<ss::sstring> check_result_configuration(
  const members_table::cache_t& current_brokers,
  const model::broker& to_update) {
    for (const auto& [id, current] : current_brokers) {
        if (id == to_update.id()) {
            /**
             * do no allow to decrease node core count
             */
            if (
              current.broker.properties().cores
              > to_update.properties().cores) {
                return "core count must not decrease on any broker";
            }
            continue;
        }

        /**
         * validate if any two of the brokers would listen on the same addresses
         * after applying configuration update
         */
        if (current.broker.rpc_address() == to_update.rpc_address()) {
            // error, nodes would listen on the same rpc addresses
            return fmt::format(
              "duplicate rpc endpoint {} with existing node {}",
              to_update.rpc_address(),
              id);
        }
        for (auto& current_ep : current.broker.kafka_advertised_listeners()) {
            auto any_is_the_same = std::any_of(
              to_update.kafka_advertised_listeners().begin(),
              to_update.kafka_advertised_listeners().end(),
              [&current_ep](const model::broker_endpoint& ep) {
                  return current_ep == ep;
              });
            // error, kafka endpoint would point to the same addresses
            if (any_is_the_same) {
                return fmt::format(
                  "duplicate kafka advertised endpoint {} with existing node "
                  "{}",
                  current_ep,
                  id);
                ;
            }
        }
    }
    return {};
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
    auto& all_brokers = _members_table.local().nodes();
    if (auto err = check_result_configuration(all_brokers, req.node); err) {
        vlog(
          clusterlog.warn,
          "Rejecting invalid configuration update. Reason: {}, new broker: {}, "
          "current brokers list: {}",
          err.value(),
          req.node,
          all_brokers);
        return ss::make_ready_future<ret_t>(errc::invalid_configuration_update);
    }

    auto f = update_broker_client(
      _self.id(),
      _connection_cache,
      req.node.id(),
      req.node.rpc_address(),
      _rpc_tls_config);
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
        return update_node(std::move(req.node)).then([](std::error_code ec) {
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

    auto leader = _members_table.local().get_node_metadata_ref(*leader_id);
    if (!leader) {
        return ss::make_ready_future<ret_t>(errc::no_leader_controller);
    }

    return with_client<controller_client_protocol>(
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
      [self = _self.id()](controller_client_protocol c) {
          hello_request req{
            .peer = self,
            .start_time = redpanda_start_time,
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
    if (!command_based_membership_active()) {
        return _raft0->add_group_member(
          std::move(broker), model::revision_id(0));
    }
    return replicate_and_wait(
      _controller_stm,
      _feature_table,
      _as,
      add_node_cmd(0, std::move(broker)),
      _join_timeout + model::timeout_clock::now());
}

ss::future<std::error_code> members_manager::update_node(model::broker broker) {
    if (!command_based_membership_active()) {
        return _raft0->update_group_member(std::move(broker));
    }

    return replicate_and_wait(
      _controller_stm,
      _feature_table,
      _as,
      update_node_cfg_cmd(0, std::move(broker)),
      _join_timeout + model::timeout_clock::now());
}

ss::future<>
members_manager::persist_members_in_kvstore(model::offset update_offset) {
    static const bytes cluster_members_key("cluster_members");
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
      serde::to_iobuf(members_snapshot{
        .members = std::move(brokers), .update_offset = update_offset}));
}

members_manager::members_snapshot members_manager::read_members_from_kvstore() {
    static const bytes cluster_members_key("cluster_members");
    auto buffer = _storage.local().kvs().get(
      storage::kvstore::key_space::controller, cluster_members_key);
    if (buffer) {
        return serde::from_iobuf<members_snapshot>(std::move(*buffer));
    }
    return {};
}

} // namespace cluster
