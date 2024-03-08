/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cloud_metadata/cluster_recovery_backend.h"

#include "base/seastarx.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_file.h"
#include "cloud_storage/types.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/cloud_metadata/offsets_recovery_rpc_types.h"
#include "cluster/cloud_metadata/producer_id_recovery_manager.h"
#include "cluster/cluster_recovery_reconciler.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/feature_manager.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/security_frontend.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "raft/group_manager.h"
#include "serde/async.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

#include <exception>

namespace cluster::cloud_metadata {

cluster_recovery_backend::cluster_recovery_backend(
  cluster::cluster_recovery_manager& mgr,
  raft::group_manager& raft_mgr,
  cloud_storage::remote& remote,
  cloud_storage::cache& cache,
  cluster::members_table& members_table,
  features::feature_table& features,
  security::credential_store& creds,
  cluster::topic_table& topics,
  cluster::controller_api& api,
  cluster::feature_manager& feature_manager,
  cluster::config_frontend& config_frontend,
  cluster::security_frontend& security_frontend,
  cluster::topics_frontend& topics_frontend,
  ss::shared_ptr<producer_id_recovery_manager> producer_id_recovery,
  ss::shared_ptr<offsets_recovery_requestor> offsets_recovery,
  ss::sharded<cluster_recovery_table>& recovery_table,
  consensus_ptr raft0)
  : _recovery_manager(mgr)
  , _raft_group_manager(raft_mgr)
  , _remote(remote)
  , _cache(cache)
  , _members_table(members_table)
  , _features(features)
  , _creds(creds)
  , _topics(topics)
  , _controller_api(api)
  , _feature_manager(feature_manager)
  , _config_frontend(config_frontend)
  , _security_frontend(security_frontend)
  , _topics_frontend(topics_frontend)
  , _producer_id_recovery(std::move(producer_id_recovery))
  , _offsets_recovery(std::move(offsets_recovery))
  , _recovery_table(recovery_table)
  , _raft0(std::move(raft0)) {
    vassert(_producer_id_recovery, "expected initialized producer_id_recovery");
    vassert(_offsets_recovery, "expected initialized offsets_recovery");
}

void cluster_recovery_backend::start() {
    _leader_cb_id = _raft_group_manager.register_leadership_notification(
      [this](
        raft::group_id group,
        model::term_id,
        std::optional<model::node_id> leader_id) {
          if (group != _raft0->group()) {
              return;
          }
          if (_as.abort_requested() || _gate.is_closed()) {
              return;
          }
          // If there's an on-going recovery instance, abort it. Even if this
          // node has been re-elected leader, the recovery needs to re-sync
          // with the contents of the controller log in case in the new term.
          if (_term_as.has_value()) {
              _term_as.value().get().request_abort();
          }
          if (_raft0->self().id() != leader_id) {
              return;
          }
          _leader_cond.signal();
      });
    ssx::spawn_with_gate(_gate, [this] { return recover_until_abort(); });
}

ss::future<> cluster_recovery_backend::stop_and_wait() {
    vlog(clusterlog.info, "Stopping cluster recovery backend");
    _raft_group_manager.unregister_leadership_notification(_leader_cb_id);
    _leader_cond.broken();
    if (_term_as.has_value()) {
        _term_as.value().get().request_abort();
    }
    _as.request_abort();
    co_await _recovery_table.invoke_on_all(
      &cluster_recovery_table::stop_waiters);
    vlog(clusterlog.info, "Closing cluster recovery backend gate...");
    co_await _gate.close();
}

ss::future<bool> cluster_recovery_backend::sync_in_term(
  ss::abort_source& term_as, model::term_id synced_term) {
    const auto cur_term_opt = co_await _recovery_manager.sync_leader(term_as);
    co_return _recovery_table.local().is_recovery_active()
      && synced_term == cur_term_opt;
}

ss::future<cluster::errc>
cluster_recovery_backend::apply_controller_actions_in_term(
  ss::abort_source& term_as,
  model::term_id term,
  cloud_metadata::controller_snapshot_reconciler::controller_actions actions) {
    for (const auto next_stage : actions.stages) {
        if (!co_await sync_in_term(term_as, term)) {
            co_return cluster::errc::not_leader_controller;
        }
        auto errc = co_await do_action(term_as, next_stage, actions);
        if (errc != cluster::errc::success) {
            co_return co_await _recovery_manager.replicate_update(
              term,
              recovery_stage::failed,
              ssx::sformat(
                "Failed to apply action for {}: {}", next_stage, errc));
        }
        errc = co_await _recovery_manager.replicate_update(term, next_stage);
        if (errc != cluster::errc::success) {
            co_return errc;
        }
    }
    co_return cluster::errc::success;
}

ss::future<cluster::errc> cluster_recovery_backend::do_action(
  ss::abort_source& term_as,
  recovery_stage next_stage,
  controller_snapshot_reconciler::controller_actions& actions) {
    retry_chain_node parent_retry(term_as, 3600s, 1s);
    switch (next_stage) {
    case recovery_stage::initialized:
    case recovery_stage::starting:
    case recovery_stage::recovered_offsets_topic:
    case recovery_stage::recovered_tx_coordinator:
    case recovery_stage::failed:
    case recovery_stage::complete:
        vlog(clusterlog.error, "Invalid action");
        co_return cluster::errc::invalid_request;

    case recovery_stage::recovered_license: {
        auto license = actions.license.value();
        auto err = co_await _feature_manager.update_license(std::move(license));
        if (err != make_error_code(errc::success)) {
            co_return cluster::errc::replication_error;
        }
        break;
    }
    case recovery_stage::recovered_cluster_config: {
        retry_chain_node config_retry(&parent_retry);
        auto patch_res = co_await _config_frontend.patch(
          std::move(actions.config), config_retry.get_deadline());
        if (patch_res.errc) {
            co_return cluster::errc::replication_error;
        }
        break;
    }
    case recovery_stage::recovered_users: {
        retry_chain_node users_retry(&parent_retry);
        // TODO: batch this up.
        std::vector<cluster::user_credential> users;
        for (size_t i = 0; i < actions.users.size(); i++) {
            users.emplace_back(std::move(actions.users[i]));
        }
        for (auto& uc : users) {
            auto err = co_await _security_frontend.create_user(
              std::move(uc.user),
              std::move(uc.cred),
              users_retry.get_deadline());
            if (err != make_error_code(errc::success)) {
                co_return cluster::errc::replication_error;
            }
        }
        break;
    }
    case recovery_stage::recovered_acls: {
        retry_chain_node acls_retry(&parent_retry);
        // TODO: batch this up.
        std::vector<security::acl_binding> acls;
        for (size_t i = 0; i < actions.acls.size(); i++) {
            acls.emplace_back(std::move(actions.acls[i]));
        }
        auto errs = co_await _security_frontend.create_acls(
          std::move(acls), acls_retry.get_timeout());
        for (const auto err : errs) {
            if (err != make_error_code(errc::success)) {
                co_return cluster::errc::replication_error;
            }
        }
        break;
    }
    case recovery_stage::recovered_remote_topic_data: {
        retry_chain_node topics_retry(&parent_retry);
        // TODO: batch this up.
        topic_configuration_vector topics;
        for (size_t i = 0; i < actions.remote_topics.size(); i++) {
            auto& topic_cfg = actions.remote_topics[i];
            if (topic_cfg.is_internal()) {
                vlog(
                  clusterlog.debug,
                  "Skipping topic recovery for internal topic {}",
                  topic_cfg.tp_ns);
                continue;
            }
            topics.emplace_back(std::move(topic_cfg));
            vlog(
              clusterlog.debug,
              "Creating recovery topic {}: {}",
              topics.back().tp_ns,
              topics.back());
        }
        auto results = co_await _topics_frontend.autocreate_topics(
          std::move(topics), topics_retry.get_timeout());
        for (const auto& res : results) {
            if (res.ec != make_error_code(errc::success)) {
                co_return res.ec;
            }
        }
        break;
    }
    case recovery_stage::recovered_topic_data: {
        retry_chain_node topics_retry(&parent_retry);
        // TODO: batch this up.
        topic_configuration_vector topics;
        for (size_t i = 0; i < actions.local_topics.size(); i++) {
            topics.emplace_back(std::move(actions.local_topics[i]));
            vlog(clusterlog.debug, "Creating topic {}", topics.back().tp_ns);
        }
        auto results = co_await _topics_frontend.autocreate_topics(
          std::move(topics), topics_retry.get_timeout());
        for (const auto& res : results) {
            if (res.ec != make_error_code(errc::success)) {
                co_return res.ec;
            }
        }
        break;
    }
    case recovery_stage::recovered_controller_snapshot:
        // Wait for leadership of all partitions.
        auto synced_term = _raft0->term();
        absl::btree_set<model::topic_namespace> topics_to_wait;
        for (auto& t : _topics.all_topics()) {
            topics_to_wait.emplace(std::move(t));
        }
        while (true) {
            if (_raft0->term() != synced_term) {
                co_return cluster::errc::not_leader;
            }
            if (topics_to_wait.empty()) {
                break;
            }
            retry_chain_node leadership_retry(60s, 1s, &parent_retry);
            auto permit = leadership_retry.retry();
            if (!permit.is_allowed) {
                co_return cluster::errc::timeout;
            }
            std::vector<model::topic_namespace> done;
            done.reserve(topics_to_wait.size());
            co_await ss::max_concurrent_for_each(
              topics_to_wait,
              10,
              [this, &done, &permit](model::topic_namespace tp_ns) {
                  return _controller_api
                    .wait_for_topic(
                      tp_ns, ss::lowres_clock::now() + permit.delay)
                    .then([&done, tp_ns](std::error_code ec) {
                        if (!ec) {
                            done.emplace_back(std::move(tp_ns));
                        } else {
                            vlog(
                              clusterlog.debug,
                              "Failed to wait for {}: {}",
                              tp_ns,
                              ec);
                            // Fall through to retry the wait.
                        }
                    });
              });
            for (const auto& tp_ns : done) {
                topics_to_wait.erase(tp_ns);
            }
        }
    };
    co_return cluster::errc::success;
}

ss::future<std::optional<cluster::controller_snapshot>>
cluster_recovery_backend::find_controller_snapshot_in_bucket(
  ss::abort_source& term_as, cloud_storage_clients::bucket_name bucket) {
    auto fib = retry_chain_node{term_as, 30s, 1s};
    if (!_recovery_table.local().is_recovery_active()) {
        co_return std::nullopt;
    }
    auto recovery_state
      = _recovery_table.local().current_recovery().value().get();
    const auto& cluster_manifest = recovery_state.manifest;

    // Download the controller snapshot.
    const auto& controller_path_str = cluster_manifest.controller_snapshot_path;
    if (cluster_manifest.controller_snapshot_path.empty()) {
        co_return std::nullopt;
    }
    vlog(
      clusterlog.info,
      "Using controller snapshot at remote path {} in bucket {}",
      controller_path_str,
      bucket);
    auto remote_controller_snapshot = cloud_storage::remote_file(
      _remote,
      _cache,
      bucket,
      cloud_storage::remote_segment_path{controller_path_str},
      fib,
      "controller_snapshot");

    try {
        auto f = co_await remote_controller_snapshot.hydrate_readable_file();
        ss::file_input_stream_options options;
        auto input = ss::make_file_input_stream(f, options);
        storage::snapshot_reader reader(
          std::move(f),
          std::move(input),
          remote_controller_snapshot.local_path());

        // Parse the snapshot, and make sure to close the snapshot reader
        // before destructing it, even on failure.
        std::exception_ptr eptr;
        cluster::controller_snapshot snapshot;
        try {
            auto snap_metadata_buf = co_await reader.read_metadata();
            auto snap_metadata_parser = iobuf_parser(
              std::move(snap_metadata_buf));
            auto snap_metadata = reflection::adl<raft::snapshot_metadata>{}
                                   .from(snap_metadata_parser);
            const size_t snap_size = co_await reader.get_snapshot_size();
            auto snap_buf_parser = iobuf_parser{
              co_await read_iobuf_exactly(reader.input(), snap_size)};
            snapshot = co_await serde::read_async<cluster::controller_snapshot>(
              snap_buf_parser);
        } catch (...) {
            eptr = std::current_exception();
        }
        co_await reader.close();
        if (eptr) {
            std::rethrow_exception(eptr);
        }
        co_return snapshot;
    } catch (...) {
        vlog(
          clusterlog.warn,
          "Error processing controller snapshot: {}",
          std::current_exception());
        co_return std::nullopt;
    }
}

ss::future<> cluster_recovery_backend::recover_until_abort() {
    co_await _features.await_feature(
      features::feature::cloud_metadata_cluster_recovery, _as);
    while (!_as.abort_requested()) {
        auto& recovery_table = _recovery_table.local();
        co_await recovery_table.wait_for_active_recovery();
        if (recovery_table.is_recovery_active()) {
            if (!_raft0->is_leader()) {
                try {
                    co_await _leader_cond.wait();
                } catch (...) {
                }
                if (_as.abort_requested()) {
                    co_return;
                }
            }
            if (recovery_table.is_recovery_active()) {
                try {
                    co_await recover_until_term_change();
                } catch (...) {
                    auto eptr = std::current_exception();
                    if (ssx::is_shutdown_exception(eptr)) {
                        vlog(
                          clusterlog.debug,
                          "Shutdown error caught while recovering: {}",
                          eptr);
                    } else {
                        vlog(
                          clusterlog.error,
                          "Unexpected error caught while recovering: {}",
                          eptr);
                    }
                }
            }
        }
    }
}

ss::future<> cluster_recovery_backend::recover_until_term_change() {
    if (!_raft0->is_leader()) {
        co_return;
    }
    ss::abort_source term_as;
    _term_as = term_as;
    auto reset_term_as = ss::defer([this] { _term_as.reset(); });
    auto synced_term = _raft0->term();
    if (!co_await sync_in_term(term_as, synced_term)) {
        co_return;
    }
    auto recovery_state
      = _recovery_table.local().current_recovery().value().get();
    if (may_require_controller_recovery(recovery_state.stage)) {
        auto controller_snap = co_await find_controller_snapshot_in_bucket(
          term_as, recovery_state.bucket);
        if (!controller_snap.has_value()) {
            vlog(
              clusterlog.error,
              "Failed to download controller snapshot from bucket: {}",
              recovery_state.bucket);
            co_await _recovery_manager.replicate_update(
              synced_term,
              recovery_stage::failed,
              ssx::sformat(
                "Failed to download controller snapshot {} in bucket {}",
                recovery_state.manifest.controller_snapshot_path,
                recovery_state.bucket));
            co_return;
        }
        vlog(
          clusterlog.info,
          "Downloaded controller snapshot. Proceeding with reconciliation...");

        if (recovery_state.wait_for_nodes) {
            const auto& nodes = controller_snap.value().members.nodes;
            vlog(
              clusterlog.info,
              "Original cluster had {} nodes. Waiting for cluster "
              "membership...",
              nodes.size());
            retry_chain_node membership_retry(term_as, 600s, 10s);
            while (_members_table.node_count() < nodes.size()) {
                if (term_as.abort_requested()) {
                    co_return;
                }
                auto permit = membership_retry.retry();
                if (!permit.is_allowed) {
                    co_await _recovery_manager.replicate_update(
                      synced_term,
                      recovery_stage::failed,
                      ssx::sformat(
                        "Timed out waiting for cluster, {}/{} nodes...",
                        _members_table.node_count(),
                        nodes.size()));
                    co_return;
                }
                vlog(
                  clusterlog.info,
                  "Cluster only has reached {}/{} nodes, waiting...",
                  _members_table.node_count(),
                  nodes.size());
                co_await ss::sleep_abortable(permit.delay, term_as);
            }
            vlog(
              clusterlog.info,
              "Cluster has reached {}/{} nodes, proceeding...",
              _members_table.node_count(),
              nodes.size());
        }
        if (!co_await sync_in_term(term_as, synced_term)) {
            co_return;
        }

        // We may need to restore state from the controller snapshot.
        cloud_metadata::controller_snapshot_reconciler reconciler(
          _recovery_table.local(), _features, _creds, _topics);
        auto controller_actions = reconciler.get_actions(
          controller_snap.value());
        vlog(
          clusterlog.info,
          "Controller recovery will proceed in {} stages",
          controller_actions.stages.size());
        auto err = co_await apply_controller_actions_in_term(
          term_as, synced_term, std::move(controller_actions));
        if (err != cluster::errc::success) {
            co_return;
        }
    }
    if (!co_await sync_in_term(term_as, synced_term)) {
        co_return;
    }

    if (may_require_offsets_recovery(recovery_state.stage)) {
        auto& manifest_offsets
          = recovery_state.manifest.offsets_snapshots_by_partition;
        std::vector<std::vector<cloud_storage::remote_segment_path>>
          offsets_snapshot_paths(manifest_offsets.size());
        for (size_t i = 0; i < offsets_snapshot_paths.size(); i++) {
            std::transform(
              manifest_offsets[i].begin(),
              manifest_offsets[i].end(),
              std::back_inserter(offsets_snapshot_paths[i]),
              [](auto& p) { return cloud_storage::remote_segment_path{p}; });
        }
        retry_chain_node parent_retry(term_as, 3600s, 1s);
        auto err = co_await _offsets_recovery->recover(
          parent_retry,
          recovery_state.bucket,
          std::move(offsets_snapshot_paths));
        if (err != error_outcome::success) {
            if (
              err == error_outcome::term_has_changed
              || err == error_outcome::not_ready) {
                co_return;
            }
            co_await _recovery_manager.replicate_update(
              synced_term,
              recovery_stage::failed,
              ssx::sformat(
                "Failed to apply action for consumer offsets recovery: {}",
                err));
            co_return;
        }
        auto errc = co_await _recovery_manager.replicate_update(
          synced_term, recovery_stage::recovered_offsets_topic);
        if (errc != cluster::errc::success) {
            co_return;
        }
    }

    if (!co_await sync_in_term(term_as, synced_term)) {
        co_return;
    }

    if (may_require_producer_id_recovery(recovery_state.stage)) {
        auto err = co_await _producer_id_recovery->recover();
        if (err != error_outcome::success) {
            co_await _recovery_manager.replicate_update(
              synced_term,
              recovery_stage::failed,
              ssx::sformat(
                "Failed to apply action for producer_id recovery: {}", err));
            co_return;
        }
        auto errc = co_await _recovery_manager.replicate_update(
          synced_term, recovery_stage::recovered_tx_coordinator);
        if (errc != cluster::errc::success) {
            co_return;
        }
    }

    // All done! Record success.
    co_await _recovery_manager.replicate_update(
      synced_term, recovery_stage::complete);
}

} // namespace cluster::cloud_metadata
