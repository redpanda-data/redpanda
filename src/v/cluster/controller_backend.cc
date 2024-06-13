// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller_backend.h"

#include "cluster/cluster_utils.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/members_backend.h"
#include "cluster/members_table.h"
#include "cluster/partition.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/group_configuration.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/later.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <exception>
#include <optional>
#include <variant>

/// on every core, sharded
namespace cluster {
namespace {

std::optional<ss::shard_id>
find_shard_on_node(const replicas_t& replicas, model::node_id node) {
    for (const auto& bs : replicas) {
        if (bs.node_id == node) {
            return bs.shard;
        }
    }
    return std::nullopt;
}

model::broker
get_node_metadata(const members_table& members, model::node_id id) {
    auto nm = members.get_node_metadata_ref(id);
    if (!nm) {
        nm = members.get_removed_node_metadata_ref(id);
    }
    if (!nm) {
        throw std::logic_error(
          fmt::format("Replica node {} is not available", id));
    }
    return nm->get().broker;
}

std::vector<model::broker> create_brokers_set(
  const replicas_t& replicas, cluster::members_table& members) {
    std::vector<model::broker> brokers;
    brokers.reserve(replicas.size());
    std::transform(
      std::cbegin(replicas),
      std::cend(replicas),
      std::back_inserter(brokers),
      [&members](const model::broker_shard& bs) {
          return get_node_metadata(members, bs.node_id);
      });
    return brokers;
}

std::vector<raft::broker_revision> create_brokers_set(
  const replicas_t& replicas,
  const absl::flat_hash_map<model::node_id, model::revision_id>&
    replica_revisions,
  model::revision_id cmd_revision,
  cluster::members_table& members) {
    std::vector<raft::broker_revision> brokers;
    brokers.reserve(replicas.size());

    std::transform(
      std::cbegin(replicas),
      std::cend(replicas),
      std::back_inserter(brokers),
      [&](const model::broker_shard& bs) {
          auto broker = get_node_metadata(members, bs.node_id);
          model::revision_id rev;
          auto rev_it = replica_revisions.find(bs.node_id);
          if (rev_it != replica_revisions.end()) {
              rev = rev_it->second;
          } else {
              rev = cmd_revision;
          }
          return raft::broker_revision{.broker = std::move(broker), .rev = rev};
      });
    return brokers;
}

static std::vector<raft::vnode> create_vnode_set(
  const replicas_t& replicas,
  const absl::flat_hash_map<model::node_id, model::revision_id>&
    old_replica_revisions,
  model::revision_id cmd_revision) {
    std::vector<raft::vnode> nodes;
    nodes.reserve(replicas.size());

    for (const auto& bs : replicas) {
        model::revision_id rev;
        auto rev_it = old_replica_revisions.find(bs.node_id);
        if (rev_it != old_replica_revisions.end()) {
            rev = rev_it->second;
        } else {
            rev = cmd_revision;
        }
        nodes.emplace_back(bs.node_id, rev);
    }

    return nodes;
}

bool are_configuration_replicas_up_to_date(
  const raft::group_configuration& cfg, const replicas_t& requested_replicas) {
    absl::flat_hash_set<model::node_id> all_ids;
    all_ids.reserve(requested_replicas.size());

    for (auto& id : cfg.current_config().voters) {
        all_ids.emplace(id.id());
    }

    // there is different number of brokers in group configuration
    if (all_ids.size() != requested_replicas.size()) {
        return false;
    }

    for (auto& b : requested_replicas) {
        all_ids.emplace(b.node_id);
    }

    return all_ids.size() == requested_replicas.size();
}

std::error_code check_configuration_update(
  model::node_id self,
  const ss::lw_shared_ptr<partition>& partition,
  const replicas_t& bs,
  model::revision_id cmd_revision) {
    auto group_cfg = partition->group_configuration();
    vlog(
      clusterlog.trace,
      "[{}] checking if configuration {} is up to date with {}, revision: {}",
      partition->ntp(),
      group_cfg,
      bs,
      cmd_revision);

    // group configuration revision is older than expected, this configuration
    // isn't up to date.
    if (group_cfg.revision_id() < cmd_revision) {
        vlog(
          clusterlog.trace,
          "[{}] configuration revision '{}' is smaller than requested "
          "update revision '{}'",
          partition->ntp(),
          group_cfg.revision_id(),
          cmd_revision);
        return errc::partition_configuration_revision_not_updated;
    }
    const bool includes_self = contains_node(bs, self);

    /*
     * if configuration includes current node, we expect configuration to be
     * fully up to date, as the node will continue to receive all the raft group
     * requests. This is why we claim configuration as not being up to date when
     * it is joint configuration type.
     *
     * NOTE: why include_self matters
     *
     * If the node is not included in current configuration there is no
     * guarantee that it will receive configuration that was moving consensus
     * from JOINT to SIMPLE. Also if the node is removed from replica set we
     * will only remove the partition after other node claim update as finished.
     *
     */
    if (
      includes_self
      && group_cfg.get_state() != raft::configuration_state::simple) {
        vlog(
          clusterlog.trace,
          "[{}] contains current node and consensus configuration is still "
          "in joint state",
          partition->ntp());
        return errc::partition_configuration_in_joint_mode;
    }

    if (includes_self && partition->raft()->has_configuration_override()) {
        vlog(
          clusterlog.trace,
          "[{}] contains current node and there is configuration override "
          "active",
          partition->ntp());
        return errc::partition_configuration_in_joint_mode;
    }
    /*
     * if replica set is a leader it must have configuration committed i.e. it
     * was successfully replicated to majority of followers.
     */
    auto configuration_committed = partition->get_latest_configuration_offset()
                                   <= partition->committed_offset();
    if (partition->is_elected_leader() && !configuration_committed) {
        vlog(
          clusterlog.trace,
          "[{}] current node is a leader, waiting for configuration to be "
          "committed",
          partition->ntp());
        return errc::partition_configuration_leader_config_not_committed;
    }

    /**
     * at this point we just compare the configuration broker ids if they are
     * the same as expected, we claim configuration as being up to date
     */
    if (!are_configuration_replicas_up_to_date(group_cfg, bs)) {
        vlog(
          clusterlog.trace,
          "[{}] requested replica set {} differs from partition replica set: "
          "{}",
          partition->ntp(),
          bs,
          group_cfg);
        return errc::partition_configuration_differs;
    }

    return errc::success;
}

} // namespace

controller_backend::controller_backend(
  ss::sharded<topic_table>& tp_state,
  ss::sharded<shard_table>& st,
  ss::sharded<partition_manager>& pm,
  ss::sharded<members_table>& members,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<topics_frontend>& frontend,
  ss::sharded<storage::api>& storage,
  ss::sharded<features::feature_table>& features,
  config::binding<std::optional<size_t>> initial_retention_local_target_bytes,
  config::binding<std::optional<std::chrono::milliseconds>>
    initial_retention_local_target_ms,
  config::binding<std::optional<size_t>> retention_local_target_bytes_default,
  config::binding<std::chrono::milliseconds> retention_local_target_ms_default,
  config::binding<bool> retention_local_strict,
  ss::sharded<ss::abort_source>& as)
  : _topics(tp_state)
  , _shard_table(st)
  , _partition_manager(pm)
  , _members_table(members)
  , _partition_leaders_table(leaders)
  , _topics_frontend(frontend)
  , _storage(storage)
  , _features(features)
  , _self(*config::node().node_id())
  , _data_directory(config::node().data_directory().as_sstring())
  , _housekeeping_timer_interval(
      config::shard_local_cfg().controller_backend_housekeeping_interval_ms())
  , _initial_retention_local_target_bytes(
      std::move(initial_retention_local_target_bytes))
  , _initial_retention_local_target_ms(
      std::move(initial_retention_local_target_ms))
  , _retention_local_target_bytes_default(
      std::move(retention_local_target_bytes_default))
  , _retention_local_target_ms_default(
      std::move(retention_local_target_ms_default))
  , _retention_local_strict(std::move(retention_local_strict))
  , _as(as) {}

bool controller_backend::command_based_membership_active() const {
    return _features.local().is_active(
      features::feature::membership_change_controller_cmds);
}

ss::future<> controller_backend::stop() {
    vlog(clusterlog.info, "Stopping Controller Backend...");
    _housekeeping_timer.cancel();
    return _gate.close();
}

std::optional<controller_backend::in_progress_operation>
controller_backend::get_current_op(const model::ntp& ntp) const {
    if (auto it = _states.find(ntp); it != _states.end()) {
        return it->second.cur_operation;
    }
    return {};
}

void controller_backend::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:controller"),
      {
        sm::make_gauge(
          "pending_partition_operations",
          [this] { return _states.size(); },
          sm::description(
            "Number of partitions with ongoing/requested operations")),
      });
}
/**
 * Create snapshot of topic table that contains all ntp that
 * must be presented on local disk storage and their revision
 * This snapshot will help to define orphan topic files
 * If topic is not presented in this snapshot or its revision
 * its revision is less than in this snapshot then this topic
 * directory is orphan
 */
static absl::flat_hash_map<model::ntp, model::revision_id>
create_topic_table_snapshot(
  ss::sharded<cluster::topic_table>& topics, model::node_id current_node) {
    absl::flat_hash_map<model::ntp, model::revision_id> snapshot;

    for (const auto& nt : topics.local().all_topics()) {
        auto ntp_view = model::topic_namespace_view(nt);
        auto ntp_meta = topics.local().get_topic_metadata(ntp_view);
        if (!ntp_meta) {
            continue;
        }
        for (const auto& p : ntp_meta->get_assignments()) {
            auto ntp = model::ntp(nt.ns, nt.tp, p.id);
            auto revision_id = ntp_meta->get_revision();
            if (cluster::contains_node(p.replicas, current_node)) {
                snapshot.emplace(ntp, revision_id);
                continue;
            }
            auto target_replica_set = topics.local().get_target_replica_set(
              ntp);
            if (
              target_replica_set
              && cluster::contains_node(*target_replica_set, current_node)) {
                snapshot.emplace(ntp, revision_id);
                continue;
            }
            auto previous_replica_set = topics.local().get_previous_replica_set(
              ntp);
            if (
              previous_replica_set
              && cluster::contains_node(*previous_replica_set, current_node)) {
                snapshot.emplace(ntp, revision_id);
                continue;
            }
        }
    }
    return snapshot;
}

ss::future<> controller_backend::start() {
    setup_metrics();
    return bootstrap_controller_backend().then([this] {
        if (ss::this_shard_id() == cluster::controller_stm_shard) {
            auto bootstrap_revision = _topics.local().last_applied_revision();
            auto snapshot = create_topic_table_snapshot(_topics, _self);
            ssx::spawn_with_gate(
              _gate,
              [this, bootstrap_revision, snapshot = std::move(snapshot)] {
                  return clear_orphan_topic_files(
                           bootstrap_revision, std::move(snapshot))
                    .handle_exception([](const std::exception_ptr& err) {
                        vlog(
                          clusterlog.error,
                          "Exception while cleaning orphan files {}",
                          err);
                    });
              });
        }
        start_topics_reconciliation_loop();
        _housekeeping_timer.set_callback([this] { housekeeping(); });
        _housekeeping_timer.arm(_housekeeping_timer_interval);
    });
}

ss::future<> controller_backend::bootstrap_controller_backend() {
    if (!_topics.local().has_pending_changes()) {
        vlog(clusterlog.trace, "no pending changes, skipping bootstrap");
        co_return;
    }

    co_await fetch_deltas();
    co_await bootstrap_partition_claims();
    co_await reconcile_topics();
}

namespace {

ss::future<std::error_code> do_update_replica_set(
  ss::lw_shared_ptr<partition> p,
  const replicas_t& replicas,
  const replicas_revision_map& replica_revisions,
  model::revision_id cmd_revision,
  members_table& members,
  bool command_based_members_update,
  std::optional<model::offset> learner_initial_offset) {
    vlog(
      clusterlog.debug,
      "[{}] updating partition replicas. revision: {}, replicas: {}, using "
      "vnodes: {}, learner initial offset: {}",
      p->ntp(),
      cmd_revision,
      replicas,
      command_based_members_update,
      learner_initial_offset);

    // when cluster membership updates are driven by controller commands, use
    // only vnodes to update raft replica set
    if (likely(command_based_members_update)) {
        auto nodes = create_vnode_set(
          replicas, replica_revisions, cmd_revision);
        co_return co_await p->update_replica_set(
          std::move(nodes), cmd_revision, learner_initial_offset);
    }

    auto brokers = create_brokers_set(
      replicas, replica_revisions, cmd_revision, members);
    co_return co_await p->update_replica_set(std::move(brokers), cmd_revision);
}

ss::future<std::error_code> revert_configuration_update(
  ss::lw_shared_ptr<partition> p,
  const replicas_t& replicas,
  const replicas_revision_map& replica_revisions,
  model::revision_id cmd_revision,
  members_table& members,
  bool command_based_members_update) {
    vlog(
      clusterlog.debug,
      "[{}] reverting already finished reconfiguration. Revision: {}, replica "
      "set: {}",
      p->ntp(),
      cmd_revision,
      replicas);
    return do_update_replica_set(
      std::move(p),
      replicas,
      replica_revisions,
      cmd_revision,
      members,
      command_based_members_update,
      std::nullopt);
}

/**
 * Retrieve topic property based on the following logic
 *
 *
 * +---------------------------------+---------------+----------+-------------+
 * |Cluster(optional)\Topic(tristate)|     Empty     | Disabled |    Value    |
 * +---------------------------------+---------------+----------+-------------+
 * |Empty                            | OFF           | OFF      | Topic Value |
 * |Value                            | Cluster Value | OFF      | Topic Value |
 * +---------------------------------+---------------+----------+-------------+
 *
 */
template<typename T>
std::optional<T> get_topic_property(
  std::optional<T> cluster_level_property, tristate<T> topic_property) {
    // disabled
    if (topic_property.is_disabled()) {
        return std::nullopt;
    }
    // has value
    if (topic_property.has_optional_value()) {
        return *topic_property;
    }
    return cluster_level_property;
}

} // namespace

std::optional<model::offset>
controller_backend::calculate_learner_initial_offset(
  reconfiguration_policy policy, const ss::lw_shared_ptr<partition>& p) const {
    /**
     * Initial learner start offset only makes sense for partitions with cloud
     * storage data
     */
    if (!p->cloud_data_available()) {
        vlog(clusterlog.trace, "no cloud data available for: {}", p->ntp());
        return std::nullopt;
    }

    auto log = p->log();
    /**
     * Calculate retention targets based on cluster and topic configuration
     */
    auto initial_retention_bytes = get_topic_property(
      _initial_retention_local_target_bytes(),
      log->config().has_overrides()
        ? log->config().get_overrides().initial_retention_local_target_bytes
        : tristate<size_t>{std::nullopt});

    auto initial_retention_ms = get_topic_property(
      _initial_retention_local_target_ms(),
      log->config().has_overrides()
        ? log->config().get_overrides().initial_retention_local_target_ms
        : tristate<std::chrono::milliseconds>{std::nullopt});

    /**
     * There are two possibilities for learner start offset calculation:
     *
     * >>> fast partition movement <<<
     * - the reconfiguration policy is set to use target_initial_retention and
     *   initial retention is configured, in this case the initial learner
     *   offset will be calculated based on the initial target retention
     *   settings
     *
     * >>> full local retention move <<<
     * - with non strict local retention the storage manager may allow
     *   partitions to grow beyond their configured local retention target. In
     *   this case the controller backend will use the local retention target
     *   properties and will schedule move delivering only the data that would
     *   be retained if local retention was working in strict mode regardless of
     *   initial retention settings and configured move policy.
     */
    const bool no_initial_retention_settings = !(
      initial_retention_bytes.has_value()
      || initial_retention_bytes.has_value());

    bool full_move = policy == reconfiguration_policy::full_local_retention
                     || no_initial_retention_settings;
    // full local retention move
    if (full_move) {
        // strict local retention, no need to override learner start
        if (_retention_local_strict()) {
            return std::nullopt;
        }

        // use default target local retention settings
        initial_retention_bytes = get_topic_property(
          _retention_local_target_bytes_default(),
          log->config().has_overrides()
            ? log->config().get_overrides().retention_local_target_bytes
            : tristate<size_t>{std::nullopt});

        initial_retention_ms = get_topic_property(
          {_retention_local_target_ms_default()},
          log->config().has_overrides()
            ? log->config().get_overrides().retention_local_target_ms
            : tristate<std::chrono::milliseconds>{std::nullopt});

        vlog(
          clusterlog.trace,
          "[{}] full partition move requested. Using default target local "
          "retention settings for the topic - target bytes: {}, target ms: {}",
          p->ntp(),
          initial_retention_bytes,
          initial_retention_ms->count());
    }

    model::timestamp retention_timestamp_threshold(0);
    if (initial_retention_ms) {
        retention_timestamp_threshold = model::timestamp(
          model::timestamp::now().value() - initial_retention_ms->count());
    }

    auto retention_offset = log->retention_offset(storage::gc_config(
      retention_timestamp_threshold, initial_retention_bytes));

    if (!retention_offset) {
        return std::nullopt;
    }

    auto const cloud_storage_safe_offset
      = p->archival_meta_stm()->max_collectible_offset();
    /**
     * Last offset uploaded to the cloud is target learner retention upper
     * bound. We can not start retention recover from the point which is not yet
     * uploaded to Cloud Storage.
     */
    vlog(
      clusterlog.info,
      "[{}] calculated retention offset: {}, last uploaded to cloud: {}, "
      "manifest clean offset: {}, max_collectible_offset: {}",
      p->ntp(),
      *retention_offset,
      p->archival_meta_stm()->manifest().get_last_offset(),
      p->archival_meta_stm()->get_last_clean_at(),
      cloud_storage_safe_offset);

    return model::next_offset(
      std::min(cloud_storage_safe_offset, *retention_offset));
}

ss::future<> controller_backend::fetch_deltas() {
    return _topics.local()
      .wait_for_changes(_as.local())
      .then([this](fragmented_vector<topic_table::delta> deltas) {
          return ss::with_semaphore(
            _topics_sem, 1, [this, deltas = std::move(deltas)]() mutable {
                for (auto& d : deltas) {
                    vlog(clusterlog.trace, "got delta: {}", d);

                    auto& rs = _states[d.ntp];

                    if (rs.changed_at) {
                        vassert(
                          *rs.changed_at <= d.revision,
                          "[{}]: delta revision unexpectedly decreased, "
                          "was: {}, update: {}",
                          d.ntp,
                          rs.changed_at,
                          d.revision);
                    }
                    rs.changed_at = d.revision;

                    if (d.type == topic_table_delta_type::added) {
                        rs.removed = false;
                    } else {
                        vassert(
                          !rs.removed,
                          "[{}] unexpected delta: {}, state: {}",
                          d.ntp,
                          d,
                          rs);
                        if (d.type == topic_table_delta_type::removed) {
                            rs.removed = true;
                        }
                    }

                    if (d.type == topic_table_delta_type::properties_updated) {
                        rs.properties_changed_at = d.revision;
                    }
                }
            });
      });
}

ss::future<> controller_backend::bootstrap_partition_claims() {
    for (const auto& [ntp, rs] : _states) {
        co_await ss::maybe_yield();

        if (rs.removed) {
            continue;
        }

        auto topic_it = _topics.local().topics_map().find(
          model::topic_namespace_view{ntp});
        if (topic_it == _topics.local().topics_map().end()) {
            continue;
        }
        auto p_it = topic_it->second.partitions.find(ntp.tp.partition);
        if (p_it == topic_it->second.partitions.end()) {
            continue;
        }
        auto as_it = topic_it->second.get_assignments().find(ntp.tp.partition);
        vassert(
          as_it != topic_it->second.get_assignments().end(),
          "[{}] expected assignment",
          ntp);

        auto update_it = _topics.local().updates_in_progress().find(ntp);

        const auto& orig_replicas
          = update_it != _topics.local().updates_in_progress().end()
              ? update_it->second.get_previous_replicas()
              : as_it->replicas;

        if (has_local_replicas(_self, orig_replicas)) {
            // We add an initial claim for the partition on the shard from the
            // original replica set (even in the case of cross-shard move). The
            // reason for this is that if there is an ongoing cross-shard move,
            // we can't be sure if it was done before the previous shutdown or
            // not, so during reconciliation we'll first look for kvstore state
            // on the original shard, and, if there is none (meaning that the
            // update was finished previously), use the state on the destination
            // shard.

            auto replica_it = p_it->second.replicas_revisions.find(_self);
            vassert(
              replica_it != p_it->second.replicas_revisions.end(),
              "[{}] expected to find {} in revisions map: {}",
              ntp,
              _self,
              p_it->second.replicas_revisions);

            vlog(
              clusterlog.info,
              "expecting partition {} with log revision {} on this shard",
              ntp,
              replica_it->second);

            _ntp_claims[ntp] = partition_claim{
              .log_revision = replica_it->second,
              .state = partition_claim_state::released,
              .hosting = true,
            };
        }
    }
}

ss::future<result<ss::stop_iteration>>
controller_backend::force_replica_set_update(
  ss::lw_shared_ptr<partition> partition,
  const replicas_t& previous_replicas,
  const replicas_t& new_replicas,
  const replicas_revision_map& initial_replicas_revisions,
  model::revision_id cmd_rev) {
    vlog(
      clusterlog.debug,
      "[{}] force-update replica set, "
      "revision: {}, new replicas: {}, initial revisions: {}",
      partition->ntp(),
      cmd_rev,
      new_replicas,
      initial_replicas_revisions);

    if (!has_local_replicas(_self, new_replicas)) {
        // This node will no longer be a part of the raft group,
        // will be cleaned up as a part of update_finished command.
        co_return ss::stop_iteration::yes;
    }

    auto [voters, learners] = split_voters_learners_for_force_reconfiguration(
      previous_replicas, new_replicas, initial_replicas_revisions, cmd_rev);

    // Force raft configuration update locally.
    co_return co_await partition->force_update_replica_set(
      std::move(voters), std::move(learners), cmd_rev);
}

/**
 * Topic files is qualified as orphan if we don't have it in topic table
 * or it's revision is less than revision it topic table
 * And it's revision is less than topic table snapshot revision
 */
static bool topic_files_are_orphan(
  const model::ntp& ntp,
  storage::partition_path::metadata ntp_directory_data,
  const absl::flat_hash_map<model::ntp, model::revision_id>&
    topic_table_snapshot,
  model::revision_id last_applied_revision) {
    vlog(clusterlog.debug, "Checking topic files for ntp {} are orphan", ntp);

    if (ntp_directory_data.revision_id > last_applied_revision) {
        return false;
    }
    if (
      topic_table_snapshot.contains(ntp)
      && ntp_directory_data.revision_id
           >= topic_table_snapshot.find(ntp)->second) {
        return false;
    }
    return true;
}

ss::future<> controller_backend::clear_orphan_topic_files(
  model::revision_id bootstrap_revision,
  absl::flat_hash_map<model::ntp, model::revision_id> topic_table_snapshot) {
    vlog(
      clusterlog.info,
      "Cleaning up orphan topic files. bootstrap_revision: {}",
      bootstrap_revision);
    // Init with default namespace to clean if there is no topics
    absl::flat_hash_set<model::ns> namespaces = {{model::kafka_namespace}};
    for (const auto& t : _topics.local().all_topics()) {
        namespaces.emplace(t.ns);
    }

    return _storage.local().log_mgr().remove_orphan_files(
      _data_directory,
      std::move(namespaces),
      [bootstrap_revision,
       topic_table_snapshot = std::move(topic_table_snapshot)](
        model::ntp ntp, storage::partition_path::metadata p) {
          return topic_files_are_orphan(
            ntp, p, topic_table_snapshot, bootstrap_revision);
      });
}

void controller_backend::start_topics_reconciliation_loop() {
    ssx::spawn_with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this] {
              return fetch_deltas()
                .then([this] { return reconcile_topics(); })
                .handle_exception_type(
                  [](const ss::abort_requested_exception&) {
                      // Shutting down: don't log this exception as an error
                      vlog(
                        clusterlog.debug,
                        "Abort requested while reconciling topics");
                  })
                .handle_exception([](const std::exception_ptr& e) {
                    vlog(
                      clusterlog.error,
                      "Error while reconciling topics - {}",
                      e);
                });
          });
    });
}

void controller_backend::housekeeping() {
    ssx::background
      = ssx::spawn_with_gate_then(_gate, [this] {
            auto f = ss::now();
            if (!_states.empty() && _topics_sem.available_units() > 0) {
                f = reconcile_topics();
            }
            return f.finally([this] {
                if (!_gate.is_closed()) {
                    _housekeeping_timer.arm(_housekeeping_timer_interval);
                }
            });
        }).handle_exception([](const std::exception_ptr& e) {
            // we ignore the exception as controller backend will retry in next
            // loop
            vlog(clusterlog.warn, "error during reconciliation - {}", e);
        });
}

ss::future<> controller_backend::reconcile_ntp(
  const model::ntp& ntp, ntp_reconciliation_state& rs) {
    if (should_skip(ntp)) {
        vlog(clusterlog.debug, "[{}] skipping reconcilation", ntp);
        rs.mark_reconciled(_topics.local().last_applied_revision());
        co_return;
    }

    while (rs.changed_at && !_as.local().abort_requested()) {
        model::revision_id changed_at = *rs.changed_at;
        cluster::errc last_error = errc::success;
        try {
            auto res = co_await reconcile_ntp_step(ntp, rs);
            if (res.has_value() && res.value() == ss::stop_iteration::yes) {
                rs.mark_reconciled(changed_at);
                vlog(
                  clusterlog.debug,
                  "[{}] reconciled at revision {}",
                  ntp,
                  changed_at);
                break;
            } else if (res.has_error() && res.error()) {
                if (res.error().category() == error_category()) {
                    last_error = static_cast<errc>(res.error().value());
                } else {
                    last_error = errc::partition_operation_failed;
                }
            }
        } catch (ss::gate_closed_exception const&) {
            break;
        } catch (ss::abort_requested_exception const&) {
            break;
        } catch (...) {
            vlog(
              clusterlog.warn,
              "[{}] exception occured during reconciliation: {}",
              ntp,
              std::current_exception());
            last_error = errc::partition_operation_failed;
        }

        if (last_error != errc::success) {
            vassert(rs.cur_operation, "[{}] expected current operation", ntp);
            rs.cur_operation->last_error = last_error;
            rs.cur_operation->retries += 1;
            vlog(
              clusterlog.trace,
              "[{}] reconciliation attempt, state: {}",
              ntp,
              rs);
            break;
        }
    }
}

// caller must hold _topics_sem lock
ss::future<> controller_backend::reconcile_topics() {
    return ss::with_semaphore(_topics_sem, 1, [this] {
        if (_states.empty()) {
            return ss::now();
        }
        // reconcile NTPs in parallel
        return ss::max_concurrent_for_each(
                 _states.begin(),
                 _states.end(),
                 1024,
                 [this](auto& rs) {
                     return reconcile_ntp(rs.first, rs.second);
                 })
          .then([this] {
              // cleanup reconciled NTP keys
              absl::erase_if(
                _states, [](const auto& rs) { return !rs.second.changed_at; });
          });
    });
}

ss::future<result<ss::stop_iteration>> controller_backend::reconcile_ntp_step(
  const model::ntp& ntp, ntp_reconciliation_state& rs) {
    // This function is the heart of the reconciliation process. It has the
    // following structure:
    //
    // 1. Check for the diff between topic table and local partition state.
    // 2. If there is no diff, stop iterating, wait for updates
    // 3. Do something to reconcile the diff
    // 4a. If an error occurred, wait for the next housekeeping timer to fire.
    // 4b. If the action was successful, and this was the final action, stop
    //     iterating. The return value in this case is ss::stop_iteration::yes
    // 4c. Otherwise proceed to 1 immediately. The return value in this case is
    //     either ss::stop_iteration::no or errc::success.
    //
    // As there is a risk that something will change between steps 3 and 4, we
    // calculate the diff in a single continuation, dispatch the appropriate
    // action and, after getting the result, return immediately. This way we
    // don't have to worry that something will change between scheduling points.
    // Also, because step 1 is a single continuation, is ok to query topic_table
    // even in the middle of a snapshot application process (each ntp state is
    // updated in a single continuation as well).

    model::revision_id tt_rev = _topics.local().last_applied_revision();
    vlog(
      clusterlog.debug,
      "[{}] reconcilation step, state: {}, topic_table revision: {}",
      ntp,
      rs,
      tt_rev);

    // Step 1. Determine if the partition object has to be created/shut
    // down/removed on this shard.

    auto delete_partition_globally = [&]() {
        if (!rs.removed) {
            // Even if the partition is not found in the topic table, just to be
            // on the safe side, we don't delete it until we receive the
            // corresponding delta (to protect against topic table
            // inconsistencies).
            return ss::now();
        }
        rs.set_cur_operation(*rs.changed_at, partition_operation_type::remove);
        return delete_partition(
          ntp, *rs.changed_at, partition_removal_mode::global);
    };

    auto topic_it = _topics.local().topics_map().find(
      model::topic_namespace_view{ntp});
    if (topic_it == _topics.local().topics_map().end()) {
        // topic was removed
        co_await delete_partition_globally();
        co_return ss::stop_iteration::yes;
    }
    auto p_it = topic_it->second.partitions.find(ntp.tp.partition);
    if (p_it == topic_it->second.partitions.end()) {
        // partition was removed (handling this case for completeness, not
        // really possible with current controller commands)
        co_await delete_partition_globally();
        co_return ss::stop_iteration::yes;
    }

    // The desired partition placement is the following: if there is no replicas
    // update, the partition object should exist only on the assigned nodes and
    // shards. If there is an update, the rules are more complicated: the
    // partition should exist on both new nodes (those not present in the
    // original assignment) and the original nodes for the whole duration of the
    // update. On original nodes, if there is a cross-shard move, the shard is
    // determined by the end state of the move.
    //
    // Example: (x/y means node/shard) suppose there is an update
    // {1/0, 2/0, 3/0} -> {2/1, 3/0, 4/0} in progress. Then the partition object
    // must be present only on 1/0, 2/1, 3/0 and 4/0. If the update is then
    // cancelled, the partition object must be present only on 1/0, 2/0, 3/0,
    // 4/0 (note that the desired shard for node 2 changed).
    //
    // Note also that the partition on new nodes is always created, even if the
    // update is cancelled. This is due to a possibility of "revert_cancel"
    // scenario (i.e. when we cancel a move, but the raft layer has already
    // completed it, in this case we end up with the "updated" replica set, not
    // the original one).

    auto as_it = topic_it->second.get_assignments().find(ntp.tp.partition);
    vassert(
      as_it != topic_it->second.get_assignments().end(),
      "[{}] expected assignment",
      ntp);
    const auto& assignment = *as_it;

    const replicas_t* orig_replicas = &assignment.replicas;
    const replicas_t* updated_replicas = nullptr;
    model::revision_id last_update_finished_revision
      = p_it->second.last_update_finished_revision;
    model::revision_id last_cmd_revision = last_update_finished_revision;

    auto update_it = _topics.local().updates_in_progress().find(ntp);
    if (update_it != _topics.local().updates_in_progress().end()) {
        vlog(clusterlog.trace, "[{}] update: {}", ntp, update_it->second);
        orig_replicas = &update_it->second.get_previous_replicas();
        updated_replicas = &update_it->second.get_target_replicas();
        last_cmd_revision = update_it->second.get_last_cmd_revision();
    }

    std::optional<ss::shard_id> orig_shard_on_cur_node = find_shard_on_node(
      *orig_replicas, _self);
    std::optional<ss::shard_id> updated_shard_on_cur_node
      = updated_replicas ? find_shard_on_node(*updated_replicas, _self)
                         : std::nullopt;

    // Has value if the partition is expected to exist on this node.
    std::optional<model::revision_id> expected_log_rev;
    if (orig_shard_on_cur_node) {
        // partition is originally on this node
        expected_log_rev = p_it->second.replicas_revisions.at(_self);
    } else if (updated_shard_on_cur_node) {
        // partition appears in this node in the updated assignment
        expected_log_rev = update_it->second.get_update_revision();
    }

    bool expected_on_cur_shard = false;
    if (orig_shard_on_cur_node) {
        // partition is originally on this node
        if (updated_shard_on_cur_node) {
            // partition stays on this node, possibly changing its shard.
            // expected shard is determined by the resulting assignment.
            expected_on_cur_shard = has_local_replicas(
              _self, update_it->second.get_resulting_replicas());
        } else {
            // there is no update or partition is moved from this node.
            // expected shard is determined by the original assignment.
            expected_on_cur_shard
              = (orig_shard_on_cur_node == ss::this_shard_id());
        }
    } else {
        // partition is not originally on this node.
        // expected shard is determined by the updated assignment (if present).
        expected_on_cur_shard
          = (updated_shard_on_cur_node == ss::this_shard_id());
    }

    const bool is_disabled = _topics.local().is_disabled(ntp);

    vlog(
      clusterlog.trace,
      "[{}] orig_shard_on_cur_node: {}, updated_shard_on_cur_node: {}, "
      "expected_log_rev: {}, expected_on_cur_shard: {}, is_disabled: {}",
      ntp,
      orig_shard_on_cur_node,
      updated_shard_on_cur_node,
      expected_log_rev,
      expected_on_cur_shard,
      is_disabled);

    auto partition = _partition_manager.local().get(ntp);
    auto claim_it = _ntp_claims.find(ntp);

    vlog(
      clusterlog.trace,
      "[{}] existing partition log_revision: {}, existing claim: {}",
      ntp,
      partition ? std::optional{partition->get_log_revision_id()}
                : std::nullopt,
      claim_it != _ntp_claims.end() ? std::optional{claim_it->second}
                                    : std::nullopt);

    // Cleanup obsolete revisions that should not exist on this node. This is
    // typically done after the replicas update is finished.

    auto is_obsolete = [&](model::revision_id log_revision) {
        if (expected_log_rev) {
            // If comparison is true, it means either: partition was moved from
            // this node and back in a previous update epochs, or the topic was
            // recreated.
            return log_revision < *expected_log_rev;
        } else {
            // Partition is not expected to exist on this node after a finished
            // update. Compare with last_update_finished_revision before
            // deleting to be on a safe side.
            return log_revision < last_update_finished_revision;
        }
    };

    if (
      claim_it != _ntp_claims.end()
      && is_obsolete(claim_it->second.log_revision)) {
        vlog(
          clusterlog.trace,
          "[{}] dropping obsolete partition claim {}",
          ntp,
          claim_it->second);
        // TODO: if hosting = true, delete persistent kvstore state.
        _ntp_claims.erase(claim_it);
        claim_it = _ntp_claims.end();
    }

    if (partition && is_obsolete(partition->get_log_revision_id())) {
        rs.set_cur_operation(
          last_update_finished_revision,
          partition_operation_type::finish_update,
          assignment);
        co_await delete_partition(
          ntp,
          last_update_finished_revision,
          partition_removal_mode::local_only);
        co_return ss::stop_iteration::no;
    }

    if (!expected_log_rev) {
        co_return ss::stop_iteration::yes;
    }

    if (partition) {
        vassert(
          partition->get_log_revision_id() == *expected_log_rev,
          "[{}] unexpected partition log_revision: {} (expected {})",
          ntp,
          partition->get_log_revision_id(),
          *expected_log_rev);
    }
    if (claim_it != _ntp_claims.end()) {
        vassert(
          claim_it->second.log_revision == *expected_log_rev,
          "[{}] unexpected partition claim log_revision: {} (expected {})",
          ntp,
          claim_it->second.log_revision,
          *expected_log_rev);
    }

    // After this point the partition is expected to exist on this node.

    if (!expected_on_cur_shard || is_disabled) {
        // Partition is disabled or expected to exist on some other shard. If we
        // still own it, shut it down and make it available for a possible
        // cross-shard move.

        if (partition) {
            rs.set_cur_operation(
              last_cmd_revision, partition_operation_type::reset, assignment);
            co_await shutdown_partition(
              std::move(partition), last_cmd_revision);
        } else if (claim_it != _ntp_claims.end()) {
            vlog(
              clusterlog.trace,
              "[{}] releasing partition claim {}",
              ntp,
              claim_it->second);

            if (claim_it->second.hosting) {
                claim_it->second.state = partition_claim_state::released;
            } else {
                _ntp_claims.erase(claim_it);
                claim_it = _ntp_claims.end();
            }
        }

        co_return ss::stop_iteration::yes;
    }

    // After this point the partition object is expected to exist on current
    // shard, create it if needed.

    if (!partition) {
        rs.set_cur_operation(
          last_cmd_revision, partition_operation_type::reset, assignment);

        replicas_t initial_replicas;
        if (orig_shard_on_cur_node) {
            initial_replicas = *orig_replicas;
        } else if (
          updated_replicas
          && update_it->second.get_state()
               == reconfiguration_state::force_update) {
            auto [voters, learners]
              = split_voters_learners_for_force_reconfiguration(
                *orig_replicas,
                *updated_replicas,
                p_it->second.replicas_revisions,
                last_cmd_revision);
            // Current nodes is a voter only if we do not
            // retain any of the original nodes. initial
            // replicas is populated only if the replica is voter
            // because for learners it automatically replicated
            // via configuration update at raft level.
            if (learners.empty()) {
                initial_replicas = *updated_replicas;
            }
        } else {
            // Configuration will be replicate to the new replica
            initial_replicas = {};
        }

        auto ec = co_await create_partition(
          ntp,
          assignment.group,
          expected_log_rev.value(),
          last_cmd_revision,
          std::move(initial_replicas));
        if (ec) {
            co_return ec;
        }

        // The partition that we just created uses topic properties queried from
        // topic_table at tt_rev. Thus all properties updates with revisions <=
        // tt_rev are already reconciled.
        rs.mark_properties_reconciled(tt_rev);

        co_return ss::stop_iteration::no;
    }

    // Step 2. Reconcile configuration properties if needed.

    if (rs.properties_changed_at) {
        rs.set_cur_operation(
          *rs.properties_changed_at,
          partition_operation_type::update_properties);

        auto cfg = topic_it->second.get_configuration().properties;
        vlog(
          clusterlog.trace,
          "[{}] updating configuration with properties: {}",
          ntp,
          cfg);
        co_await partition->update_configuration(std::move(cfg));

        rs.mark_properties_reconciled(tt_rev);
        co_return ss::stop_iteration::no;
    }

    // Step 3. Dispatch required raft reconfiguration and the update_finished
    // command.

    if (update_it != _topics.local().updates_in_progress().end()) {
        partition_operation_type op_type;
        switch (update_it->second.get_state()) {
        case reconfiguration_state::in_progress:
            op_type = partition_operation_type::update;
            break;
        case reconfiguration_state::force_update:
            op_type = partition_operation_type::force_update;
            break;
        case reconfiguration_state::cancelled:
            op_type = partition_operation_type::cancel_update;
            break;
        case reconfiguration_state::force_cancelled:
            op_type = partition_operation_type::force_cancel_update;
            break;
        default:
            __builtin_unreachable();
        }
        rs.set_cur_operation(last_cmd_revision, op_type, assignment);

        co_return co_await reconcile_partition_reconfiguration(
          rs,
          std::move(partition),
          update_it->second,
          p_it->second.replicas_revisions);
    }

    co_return ss::stop_iteration::yes;
}

ss::future<result<ss::stop_iteration>>
controller_backend::reconcile_partition_reconfiguration(
  ntp_reconciliation_state& rs,
  ss::lw_shared_ptr<partition> partition,
  const topic_table::in_progress_update& update,
  const replicas_revision_map& replicas_revisions) {
    model::revision_id cmd_revision = update.get_last_cmd_revision();

    if (partition->get_revision_id() > cmd_revision) {
        vlog(
          clusterlog.trace,
          "[{}] found newer revision, finishing reconfiguration to: {}",
          partition->ntp(),
          update.get_target_replicas());
        // other replicas replicated a configuration with a newer revision,
        // probably our topic_table is out of date? Anyway, there is nothing for
        // us to do except to wait for a topic table update.
        co_return ss::stop_iteration::yes;
    }

    // Check if configuration is reconciled. If it is, try finishing the update.

    auto update_ec = check_configuration_update(
      _self, partition, update.get_resulting_replicas(), cmd_revision);
    if (!update_ec) {
        auto leader = partition->get_leader_id();
        size_t retries = (rs.cur_operation ? rs.cur_operation->retries : 0);
        vlog(
          clusterlog.trace,
          "[{}] update complete, checking if our node can finish it "
          "(leader: {}, retry: {})",
          partition->ntp(),
          leader,
          retries);
        if (can_finish_update(
              leader,
              retries,
              update.get_state(),
              update.get_resulting_replicas())) {
            auto ec = co_await dispatch_update_finished(
              partition->ntp(), update.get_resulting_replicas());
            if (ec) {
                co_return ec;
            } else {
                co_return ss::stop_iteration::yes;
            }
        }
        // Wait for the operation to be finished on one of the nodes
        co_return errc::waiting_for_reconfiguration_finish;
    }

    if (partition->get_revision_id() == cmd_revision) {
        // Requested raft configuration update has already been dispatched.
        // Just wait for recovery to finish.
        co_return errc::waiting_for_recovery;
    }

    // We need to dispatch the requested raft configuration update.
    switch (update.get_state()) {
    case reconfiguration_state::in_progress:
        co_return co_await update_partition_replica_set(
          std::move(partition),
          update.get_target_replicas(),
          replicas_revisions,
          cmd_revision,
          update.get_reconfiguration_policy());
    case reconfiguration_state::force_update:
        co_return co_await force_replica_set_update(
          std::move(partition),
          update.get_previous_replicas(),
          update.get_target_replicas(),
          replicas_revisions,
          cmd_revision);
    case reconfiguration_state::cancelled:
        co_return co_await cancel_replica_set_update(
          std::move(partition),
          update.get_previous_replicas(),
          replicas_revisions,
          update.get_target_replicas(),
          cmd_revision);
    case reconfiguration_state::force_cancelled:
        co_return co_await force_abort_replica_set_update(
          std::move(partition),
          update.get_previous_replicas(),
          replicas_revisions,
          update.get_target_replicas(),
          cmd_revision);
    }
    __builtin_unreachable();
}

bool controller_backend::can_finish_update(
  std::optional<model::node_id> current_leader,
  uint64_t current_retry,
  reconfiguration_state state,
  const replicas_t& current_replicas) {
    if (
      state == reconfiguration_state::force_update
      || state == reconfiguration_state::force_cancelled) {
        // Wait for the leader to be elected in the new replica set.
        return current_leader == _self;
    }
    /**
     * If the revert feature is active we use current leader to dispatch
     * partition move
     */
    if (_features.local().is_active(
          features::feature::partition_move_revert_cancel)) {
        return current_leader == _self
               && has_local_replicas(_self, current_replicas);
    }
    /**
     * Use retry count to determine which node is eligible to dispatch update
     * finished. Using modulo division allow us to round robin between
     * candidates
     */
    const model::broker_shard& candidate
      = current_replicas[current_retry % current_replicas.size()];

    return candidate.node_id == _self && candidate.shard == ss::this_shard_id();
}

ss::future<std::error_code> controller_backend::create_partition(
  model::ntp ntp,
  raft::group_id group,
  model::revision_id log_revision,
  model::revision_id cmd_revision,
  replicas_t initial_replicas) {
    vlog(
      clusterlog.debug,
      "[{}] creating partition, log_revision: {}, cmd_revision: {}, "
      "initial_replicas: {}",
      ntp,
      log_revision,
      cmd_revision,
      initial_replicas);

    auto& my_claim
      = _ntp_claims.emplace(ntp, partition_claim{.log_revision = log_revision})
          .first->second;
    vlog(clusterlog.trace, "[{}] my claim: {}", ntp, my_claim);

    std::optional<ss::shard_id> source_shard;
    if (
      my_claim.state != partition_claim_state::acquired || !my_claim.hosting) {
        // We need to find the shard that hosts the partition and make sure that
        // no other shard is claiming it.

        my_claim.state = partition_claim_state::acquiring;

        auto all_claims = co_await container().map(
          [&ntp, log_revision](const controller_backend& peer) {
              auto it = peer._ntp_claims.find(ntp);
              if (it != peer._ntp_claims.end()) {
                  return it->second;
              }
              auto partition = peer._partition_manager.local().get(ntp);
              if (partition) {
                  return partition_claim{
                    .log_revision = partition->get_log_revision_id(),
                    .state = partition_claim_state::acquired,
                    .hosting = true,
                  };
              }
              return partition_claim{
                .log_revision = log_revision,
              };
          });

        vassert(
          all_claims.size() == ss::smp::count,
          "[{}] unexpected backend instances count: {} (expected {})",
          ntp,
          all_claims.size(),
          ss::smp::count);
        for (ss::shard_id s = 0; s < all_claims.size(); ++s) {
            if (s == ss::this_shard_id()) {
                continue;
            }

            vlog(
              clusterlog.trace,
              "[{}] claim on shard {}: {}",
              ntp,
              s,
              all_claims[s]);

            if (all_claims[s].log_revision != log_revision) {
                // Two shards claim the partition with the same ntp but
                // different update epochs. This looks unlikely but in any case
                // wait until one of the shards updates the topic_table and
                // drops the claim.
                co_return errc::waiting_for_reconfiguration_finish;
            }

            if (all_claims[s].state != partition_claim_state::released) {
                // Note that two shards can block each other but that is okay
                // because one will eventually yield after it gets a fresh topic
                // table update.
                co_return errc::waiting_for_partition_shutdown;
            }

            if (all_claims[s].hosting) {
                source_shard = s;
            }
        }
    }

    // At this point we are sure that our shard is the only one claiming the
    // partition.

    my_claim.state = partition_claim_state::acquired;
    if (!source_shard) {
        source_shard = ss::this_shard_id();
        my_claim.hosting = true;
    }

    vlog(
      clusterlog.trace,
      "[{}] creating partition from shard {}",
      ntp,
      source_shard.value());

    if (!my_claim.hosting) {
        co_await raft::details::move_persistent_state(
          group, source_shard.value(), ss::this_shard_id(), _storage);
        co_await raft::offset_translator::move_persistent_state(
          group, source_shard.value(), ss::this_shard_id(), _storage);
        co_await raft::move_persistent_stm_state(
          ntp, source_shard.value(), ss::this_shard_id(), _storage);

        my_claim.hosting = true;

        co_await container().invoke_on(
          source_shard.value(),
          [&ntp, log_revision](controller_backend& source) {
              auto it = source._ntp_claims.find(ntp);
              if (
                it != source._ntp_claims.end()
                && it->second.log_revision == log_revision) {
                  source._ntp_claims.erase(it);
              }
          });
    }

    std::vector<model::broker> initial_brokers = create_brokers_set(
      initial_replicas, _members_table.local());
    co_return co_await do_create_partition(
      ntp, group, log_revision, cmd_revision, std::move(initial_brokers));
}

/**
 * notifies the topics frontend that partition update has been finished, all
 * the interested nodes can now safely remove unnecessary partition
 * replicas.
 */
ss::future<std::error_code> controller_backend::dispatch_update_finished(
  model::ntp ntp, replicas_t replicas) {
    vlog(
      clusterlog.trace,
      "[{}] dispatching update finished event with replicas: {}",
      ntp,
      replicas);

    return ss::with_gate(
      _gate,
      [this, ntp = std::move(ntp), replicas = std::move(replicas)]() mutable {
          return _topics_frontend.local().finish_moving_partition_replicas(
            std::move(ntp),
            std::move(replicas),
            model::timeout_clock::now()
              + config::shard_local_cfg().replicate_append_timeout_ms());
      });
}

template<typename Func>
ss::future<result<ss::stop_iteration>>
controller_backend::apply_configuration_change_on_leader(
  ss::lw_shared_ptr<partition> partition,
  const replicas_t& expected_replicas,
  model::revision_id cmd_rev,
  Func&& func) {
    if (partition->is_leader()) {
        // we are the leader, update configuration
        auto f = func(partition);
        try {
            // TODO: use configurable timeout here
            co_return co_await ss::with_timeout(
              model::timeout_clock::now() + std::chrono::seconds(5),
              std::move(f));
        } catch (const ss::timed_out_error& e) {
            co_return make_error_code(errc::timeout);
        }
    }

    co_return errc::not_leader;
}

ss::future<result<ss::stop_iteration>>
controller_backend::cancel_replica_set_update(
  ss::lw_shared_ptr<partition> p,
  const replicas_t& replicas,
  const replicas_revision_map& replicas_revisions,
  const replicas_t& previous_replicas,
  model::revision_id cmd_revision) {
    // If the node is the current leader, cancel the configuration change if it
    // is has been dispatched and not yet finished.
    return apply_configuration_change_on_leader(
      std::move(p),
      replicas,
      cmd_revision,
      [this, cmd_revision, &replicas, &previous_replicas, &replicas_revisions](
        ss::lw_shared_ptr<partition> p) {
          const auto current_cfg = p->group_configuration();

          const auto raft_not_reconfiguring
            = current_cfg.get_state() == raft::configuration_state::simple;
          const auto not_yet_moved = are_configuration_replicas_up_to_date(
            current_cfg, replicas);
          const auto already_moved = are_configuration_replicas_up_to_date(
            current_cfg, previous_replicas);
          vlog(
            clusterlog.debug,
            "[{}] cancelling replica set update (cmd_revision: {}), "
            "not reconfiguring: {}, not yet moved: {}, already_moved: {}",
            p->ntp(),
            cmd_revision,
            raft_not_reconfiguring,
            not_yet_moved,
            already_moved);
          // raft already finished its part, we need to move replica back
          if (raft_not_reconfiguring) {
              // move hasn't yet requested
              if (not_yet_moved) {
                  // just update configuration revision
                  return do_update_replica_set(
                           std::move(p),
                           replicas,
                           replicas_revisions,
                           cmd_revision,
                           _members_table.local(),
                           command_based_membership_active(),
                           std::nullopt)
                    .then([](std::error_code ec) {
                        return result<ss::stop_iteration>{ec};
                    });
              } else if (already_moved) {
                  if (likely(_features.local().is_active(
                        features::feature::partition_move_revert_cancel))) {
                      return dispatch_revert_cancel_move(p->ntp()).then(
                        [](std::error_code ec) -> result<ss::stop_iteration> {
                            if (ec) {
                                return ec;
                            }
                            // revert_cancel is dispatched, nothing else to do,
                            // but wait for the topic table update.
                            return ss::stop_iteration::yes;
                        });
                  }

                  return revert_configuration_update(
                           std::move(p),
                           replicas,
                           replicas_revisions,
                           cmd_revision,
                           _members_table.local(),
                           command_based_membership_active())
                    .then([](std::error_code ec) {
                        return result<ss::stop_iteration>{ec};
                    });
              }
              return ss::make_ready_future<result<ss::stop_iteration>>(
                errc::waiting_for_recovery);

          } else {
              vlog(
                clusterlog.debug, "[{}] cancelling reconfiguration", p->ntp());
              return p->cancel_replica_set_update(cmd_revision)
                .then([](std::error_code ec) {
                    return result<ss::stop_iteration>{ec};
                });
          }
      });
}

ss::future<std::error_code>
controller_backend::dispatch_revert_cancel_move(model::ntp ntp) {
    vlog(clusterlog.trace, "[{}] dispatching revert cancel move command", ntp);
    auto holder = _gate.hold();

    co_return co_await _topics_frontend.local().revert_cancel_partition_move(
      std::move(ntp),
      model::timeout_clock::now()
        + config::shard_local_cfg().replicate_append_timeout_ms());
}

ss::future<result<ss::stop_iteration>>
controller_backend::force_abort_replica_set_update(
  ss::lw_shared_ptr<partition> partition,
  const replicas_t& replicas,
  const replicas_revision_map& replicas_revisions,
  const replicas_t& previous_replicas,
  model::revision_id cmd_revision) {
    /**
     * Force abort configuration change for each of the partition replicas.
     */
    const auto current_cfg = partition->group_configuration();

    const auto raft_not_reconfiguring = current_cfg.get_state()
                                        == raft::configuration_state::simple;
    const auto not_yet_moved = are_configuration_replicas_up_to_date(
      current_cfg, replicas);
    const auto already_moved = are_configuration_replicas_up_to_date(
      current_cfg, previous_replicas);

    vlog(
      clusterlog.debug,
      "[{}] force-aborting replica set update (cmd_revision: {}), "
      "not reconfiguring: {}, not yet moved: {}, already_moved: {}",
      partition->ntp(),
      cmd_revision,
      raft_not_reconfiguring,
      not_yet_moved,
      already_moved);

    // raft already finished its part, we need to move replica back
    if (raft_not_reconfiguring) {
        // move hasn't yet requested
        if (not_yet_moved) {
            // just update configuration revision

            co_return co_await update_partition_replica_set(
              partition,
              replicas,
              replicas_revisions,
              cmd_revision,
              reconfiguration_policy::full_local_retention);
        } else if (already_moved) {
            if (likely(_features.local().is_active(
                  features::feature::partition_move_revert_cancel))) {
                std::error_code ec = co_await dispatch_revert_cancel_move(
                  partition->ntp());
                if (ec) {
                    co_return ec;
                }
                co_return ss::stop_iteration::yes;
            }

            co_return co_await apply_configuration_change_on_leader(
              std::move(partition),
              replicas,
              cmd_revision,
              [&](ss::lw_shared_ptr<cluster::partition> p) {
                  return revert_configuration_update(
                    std::move(p),
                    replicas,
                    replicas_revisions,
                    cmd_revision,
                    _members_table.local(),
                    command_based_membership_active());
              });
        }
        co_return errc::waiting_for_recovery;
    } else {
        auto leader_id = partition->get_leader_id();
        if (leader_id && leader_id != _self) {
            // The leader is alive and we are a follower. Wait for the leader to
            // replicate the aborting configuration, but don't append it
            // ourselves to minimize the chance of log inconsistency.
            co_return errc::not_leader;
        }

        vlog(
          clusterlog.debug,
          "[{}] force-aborting reconfiguration",
          partition->ntp());
        co_return co_await partition->force_abort_replica_set_update(
          cmd_revision);
    }
}

ss::future<result<ss::stop_iteration>>
controller_backend::update_partition_replica_set(
  ss::lw_shared_ptr<partition> partition,
  const replicas_t& replicas,
  const replicas_revision_map& replicas_revisions,
  model::revision_id cmd_revision,
  reconfiguration_policy policy) {
    // Dispatch the requested raft configuration change if we are the leader.
    return apply_configuration_change_on_leader(
      std::move(partition),
      replicas,
      cmd_revision,
      [&](ss::lw_shared_ptr<cluster::partition> p) {
          /**
           * We want to keep full local retention on the learner, do not return
           * initial offset override
           */
          auto learner_initial_offset = calculate_learner_initial_offset(
            policy, p);

          return do_update_replica_set(
            std::move(p),
            replicas,
            replicas_revisions,
            cmd_revision,
            _members_table.local(),
            command_based_membership_active(),
            learner_initial_offset);
      });
}

ss::future<> controller_backend::add_to_shard_table(
  model::ntp ntp,
  raft::group_id raft_group,
  uint32_t shard,
  model::revision_id revision) {
    // update shard_table: broadcast
    vlog(
      clusterlog.trace,
      "[{}] adding to shard table at shard {} with revision {}",
      ntp,
      shard,
      revision);
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), raft_group, shard, revision](
        shard_table& s) mutable {
          s.update(ntp, raft_group, shard, revision);
      });
}

ss::future<> controller_backend::remove_from_shard_table(
  model::ntp ntp, raft::group_id raft_group, model::revision_id revision) {
    // update shard_table: broadcast

    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), raft_group, revision](shard_table& s) mutable {
          s.erase(ntp, raft_group, revision);
      });
}

ss::future<std::error_code> controller_backend::do_create_partition(
  model::ntp ntp,
  raft::group_id group_id,
  model::revision_id log_revision,
  model::revision_id command_revision,
  std::vector<model::broker> members) {
    auto cfg = _topics.local().get_topic_cfg(model::topic_namespace_view(ntp));

    if (!cfg) {
        // partition was already removed, do nothing
        co_return errc::success;
    }

    // handle partially created topic
    auto partition = _partition_manager.local().get(ntp);

    // initial revision of the partition on the moment when it was created
    // the value is used by shadow indexing
    // if topic is read replica, the value from remote topic manifest is
    // used
    auto initial_rev = _topics.local().get_initial_revision(ntp);
    if (!initial_rev) {
        co_return errc::topic_not_exists;
    }
    // no partition exists, create one
    if (likely(!partition)) {
        std::optional<cloud_storage_clients::bucket_name> read_replica_bucket;
        if (cfg->is_read_replica()) {
            read_replica_bucket = cloud_storage_clients::bucket_name(
              cfg->properties.read_replica_bucket.value());
        }
        // we use offset as an rev as it is always increasing and it
        // increases while ntp is being created again
        try {
            co_await _partition_manager.local().manage(
              cfg->make_ntp_config(
                _data_directory,
                ntp.tp.partition,
                log_revision,
                initial_rev.value()),
              group_id,
              std::move(members),
              *cfg,
              cfg->properties.remote_topic_properties,
              read_replica_bucket);

            // partition object acts as a claim as well so we don't need to keep
            // ntp in the _ntp_claims map.
            _ntp_claims.erase(ntp);

            co_await add_to_shard_table(
              ntp, group_id, ss::this_shard_id(), command_revision);

        } catch (...) {
            vlog(
              clusterlog.warn,
              "[{}] failed to create partition with revision {} - {}",
              ntp,
              log_revision,
              std::current_exception());
            co_return errc::failed_to_create_partition;
        }
    } else {
        // old partition still exists, wait for it to be removed
        if (partition->get_revision_id() < log_revision) {
            co_return errc::partition_already_exists;
        }
    }

    if (ntp.tp.partition == 0) {
        auto partition = _partition_manager.local().get(ntp);
        if (partition) {
            partition->set_topic_config(
              std::make_unique<topic_configuration>(std::move(*cfg)));
        }
    }

    co_return errc::success;
}

ss::future<> controller_backend::shutdown_partition(
  ss::lw_shared_ptr<partition> partition, model::revision_id cmd_revision) {
    vlog(
      clusterlog.debug,
      "[{}] shutting down, cmd_revision: {}",
      partition->ntp(),
      cmd_revision);

    auto ntp = partition->ntp();
    auto gr = partition->group();
    auto log_revision = partition->get_log_revision_id();

    // Insert a short-lived claim (that we then release) for the duration of the
    // shutdown process to notify other shards that it is ongoing.
    _ntp_claims[ntp] = partition_claim{
      .log_revision = log_revision,
      .state = partition_claim_state::acquired,
      .hosting = true,
    };

    try {
        // remove from shard table
        co_await remove_from_shard_table(ntp, gr, cmd_revision);
        // shutdown partition
        co_await _partition_manager.local().shutdown(ntp);

        _ntp_claims[ntp].state = partition_claim_state::released;
    } catch (...) {
        /**
         * If partition shutdown failed we should crash, this error is
         * unrecoverable
         */
        vassert(
          false,
          "error shutting down {} partition at cmd revision {}, error: {}, "
          "terminating",
          ntp,
          cmd_revision,
          std::current_exception());
    }
}

ss::future<> controller_backend::delete_partition(
  model::ntp ntp, model::revision_id rev, partition_removal_mode mode) {
    vlog(
      clusterlog.debug,
      "[{}] removing partition, cmd_revision: {}, global: {}",
      ntp,
      rev,
      mode == partition_removal_mode::global);

    // The partition leaders table contains partition leaders for all
    // partitions accross the cluster. For this reason, when deleting a
    // partition (i.e. removal mode is global), we need to delete from the table
    // regardless of whether a replica of 'ntp' is present on the node.
    if (mode == partition_removal_mode::global) {
        co_await _partition_leaders_table.invoke_on_all(
          [ntp, rev](partition_leaders_table& leaders) {
              leaders.remove_leader(ntp, rev);
          });
    }

    auto part = _partition_manager.local().get(ntp);
    // partition is not replicated locally or it was already recreated with
    // greater rev, do nothing
    if (part.get() == nullptr || part->get_revision_id() > rev) {
        co_return;
    }

    // Insert a short-lived claim (that we then delete) for the duration of the
    // deletion process to notify other shards that it is ongoing.
    _ntp_claims[ntp] = partition_claim{
      .log_revision = part->get_log_revision_id(),
      .state = partition_claim_state::acquired,
      .hosting = true,
    };

    auto group_id = part->group();

    co_await _shard_table.invoke_on_all(
      [ntp, group_id, rev](shard_table& st) mutable {
          st.erase(ntp, group_id, rev);
      });

    co_await _partition_manager.local().remove(ntp, mode);

    _ntp_claims.erase(ntp);
}

bool controller_backend::should_skip(const model::ntp& ntp) const {
    return config::node().recovery_mode_enabled() && model::is_user_topic(ntp);
}

std::pair<controller_backend::vnodes, controller_backend::vnodes>
controller_backend::split_voters_learners_for_force_reconfiguration(
  const replicas_t& original,
  const replicas_t& new_replicas,
  const replicas_revision_map& replicas_revision_map,
  model::revision_id command_revision) {
    auto original_vnodes = create_vnode_set(
      original, replicas_revision_map, command_revision);
    auto new_vnodes = create_vnode_set(
      new_replicas, replicas_revision_map, command_revision);

    auto enhanced_force_reconfiguration_enabled = _features.local().is_active(
      features::feature::enhanced_force_reconfiguration);

    vnodes voters;
    vnodes learners;
    if (unlikely(!enhanced_force_reconfiguration_enabled)) {
        voters = std::move(new_vnodes);
    } else {
        auto remaining_original_nodes = intersect(original_vnodes, new_vnodes);
        if (remaining_original_nodes.size() == 0) {
            // we do not retain any of the original replicas, so
            // the new replica set begins with every replica as a
            // voter.
            voters = std::move(new_vnodes);
        } else {
            // Here we do retain some original nodes, so making them
            // as voters and the rest as learners ensures that the learners
            // are first caught up before they form a majority.
            voters = std::move(remaining_original_nodes);
            learners = subtract(new_vnodes, original_vnodes);
        }
    }
    vassert(
      voters.size() + learners.size() == new_replicas.size(),
      "Incorrect computation of voters {} and learners {} during force "
      "reconfiguration, previous: {}, new replicas: {}, revision: {}. This is "
      "most likely a logic error / bug.",
      voters,
      learners,
      original,
      new_replicas,
      command_revision);
    return std::make_pair(std::move(voters), std::move(learners));
}

std::ostream& operator<<(
  std::ostream& o, const controller_backend::in_progress_operation& op) {
    fmt::print(
      o,
      "{{revision: {}, type: {}, assignment: {}, retries: {}, "
      "last_error: {} ({})}}",
      op.revision,
      op.type,
      op.assignment,
      op.retries,
      op.last_error,
      std::error_code{op.last_error}.message());
    return o;
}

} // namespace cluster
