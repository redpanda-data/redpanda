// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller_backend.h"

#include "base/outcome.h"
#include "base/vassert.h"
#include "cloud_storage/remote_path_provider.h"
#include "cluster/archival/archival_metadata_stm.h"
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
#include "metrics/prometheus_sanitize.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "raft/fundamental.h"
#include "raft/group_configuration.h"
#include "ssx/event.h"
#include "ssx/future-util.h"
#include "storage/offset_translator.h"
#include "types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/switch_to.hh>
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

struct controller_backend::ntp_reconciliation_state {
    // A counter to avoid losing notifications that arrived concurrently with
    // reconciliation.
    size_t pending_notifies = 0;

    std::optional<model::revision_id> properties_changed_at;
    std::optional<model::revision_id> removed_at;

    ssx::event wakeup_event{"c/cb/rfwe"};
    ss::lowres_clock::time_point last_retried_at = ss::lowres_clock::now();
    std::optional<in_progress_operation> cur_operation;

    bool is_reconciled() const { return pending_notifies == 0; }

    void mark_reconciled(size_t notifies) {
        vassert(
          pending_notifies >= notifies,
          "unexpected pending_notifies: {}",
          pending_notifies);
        pending_notifies -= notifies;

        cur_operation = std::nullopt;
    }

    void mark_properties_reconciled(model::revision_id rev) {
        if (properties_changed_at && *properties_changed_at <= rev) {
            properties_changed_at = std::nullopt;
        }
    }

    void set_cur_operation(
      model::revision_id rev,
      partition_operation_type type,
      partition_assignment p_as = partition_assignment{}) {
        if (!cur_operation) {
            cur_operation = in_progress_operation{};
        }
        cur_operation->revision = rev;
        cur_operation->type = type;
        cur_operation->assignment = std::move(p_as);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const ntp_reconciliation_state& rs) {
        fmt::print(
          o,
          "{{pending_notifies: {},  properties_changed_at: {}, removed_at: {}, "
          "cur_operation: {}}}",
          rs.pending_notifies,
          rs.properties_changed_at,
          rs.removed_at,
          rs.cur_operation);
        return o;
    }
};

controller_backend::controller_backend(
  ss::sharded<topic_table>& tp_state,
  ss::sharded<shard_placement_table>& shard_placement,
  ss::sharded<shard_table>& st,
  ss::sharded<partition_manager>& pm,
  ss::sharded<members_table>& members,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<topics_frontend>& frontend,
  ss::sharded<storage::api>& storage,
  ss::sharded<features::feature_table>& features,
  config::binding<std::chrono::milliseconds> housekeeping_interval,
  config::binding<std::optional<size_t>> initial_retention_local_target_bytes,
  config::binding<std::optional<std::chrono::milliseconds>>
    initial_retention_local_target_ms,
  config::binding<std::optional<size_t>> retention_local_target_bytes_default,
  config::binding<std::chrono::milliseconds> retention_local_target_ms_default,
  config::binding<bool> retention_local_strict,
  ss::sharded<ss::abort_source>& as)
  : _topics(tp_state)
  , _shard_placement(shard_placement.local())
  , _shard_table(st)
  , _partition_manager(pm)
  , _members_table(members)
  , _partition_leaders_table(leaders)
  , _topics_frontend(frontend)
  , _storage(storage)
  , _features(features)
  , _self(*config::node().node_id())
  , _data_directory(config::node().data_directory().as_sstring())
  , _housekeeping_interval(std::move(housekeeping_interval))
  , _housekeeping_jitter(_housekeeping_interval())
  , _initial_retention_local_target_bytes(
      std::move(initial_retention_local_target_bytes))
  , _initial_retention_local_target_ms(
      std::move(initial_retention_local_target_ms))
  , _retention_local_target_bytes_default(
      std::move(retention_local_target_bytes_default))
  , _retention_local_target_ms_default(
      std::move(retention_local_target_ms_default))
  , _retention_local_strict(std::move(retention_local_strict))
  , _as(as) {
    _housekeeping_interval.watch([this] {
        _housekeeping_jitter = simple_time_jitter<ss::lowres_clock>(
          _housekeeping_interval());
    });
}

controller_backend::~controller_backend() = default;

bool controller_backend::command_based_membership_active() const {
    return _features.local().is_active(
      features::feature::membership_change_controller_cmds);
}

ss::future<> controller_backend::stop() {
    vlog(clusterlog.info, "Stopping Controller Backend...");

    _topics.local().unregister_ntp_delta_notification(
      _topic_table_notify_handle);

    for (auto& [_, rs] : _states) {
        rs->wakeup_event.set();
    }
    _reconciliation_sem.broken();
    co_await _gate.close();
}

std::optional<controller_backend::in_progress_operation>
controller_backend::get_current_op(const model::ntp& ntp) const {
    if (auto it = _states.find(ntp); it != _states.end()) {
        return it->second->cur_operation;
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
        for (const auto& [_, p] : ntp_meta->get_assignments()) {
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

        // unblock reconciliation fibers
        constexpr size_t max_reconciliation_concurrency = 1024;
        _reconciliation_sem.signal(max_reconciliation_concurrency);

        ssx::background = stuck_ntp_watchdog_fiber();
    });
}

ss::future<> controller_backend::bootstrap_controller_backend() {
    for (const auto& [ntp, _] : _shard_placement.shard_local_states()) {
        notify_reconciliation(ntp);
    }

    _topic_table_notify_handle
      = _topics.local().register_ntp_delta_notification(
        [this](topic_table::ntp_delta_range_t deltas_range) {
            for (const auto& d : deltas_range) {
                process_delta(d);
            }
        });

    co_return;
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

    const auto cloud_storage_safe_offset
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

void controller_backend::process_delta(const topic_table::ntp_delta& d) {
    vlog(clusterlog.trace, "got delta: {}", d);

    // update partition_leaders_table if needed

    if (d.type == topic_table_ntp_delta_type::removed) {
        _partition_leaders_table.local().remove_leader(d.ntp, d.revision);
    }

    // notify reconciliation fiber

    auto [rs_it, inserted] = _states.try_emplace(d.ntp);
    if (inserted) {
        rs_it->second = ss::make_lw_shared<ntp_reconciliation_state>();
    }
    auto& rs = *rs_it->second;
    rs.pending_notifies += 1;

    if (d.type == topic_table_ntp_delta_type::added) {
        rs.removed_at.reset();
    } else {
        vassert(
          !rs.removed_at, "[{}] unexpected delta: {}, state: {}", d.ntp, d, rs);
        if (d.type == topic_table_ntp_delta_type::removed) {
            rs.removed_at = d.revision;
        }
    }

    if (d.type == topic_table_ntp_delta_type::properties_updated) {
        rs.properties_changed_at = d.revision;
    }

    rs.wakeup_event.set();
    if (inserted) {
        ssx::background = reconcile_ntp_fiber(d.ntp, rs_it->second);
    }
}

void controller_backend::notify_reconciliation(const model::ntp& ntp) {
    auto [rs_it, inserted] = _states.try_emplace(ntp);
    if (inserted) {
        rs_it->second = ss::make_lw_shared<ntp_reconciliation_state>();
    }
    auto& rs = *rs_it->second;

    rs.pending_notifies += 1;
    vlog(
      clusterlog.trace,
      "[{}] notify reconciliation fiber, current state: {}",
      ntp,
      rs);
    rs.wakeup_event.set();
    if (inserted) {
        ssx::background = reconcile_ntp_fiber(ntp, rs_it->second);
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

    if (!contains_node(new_replicas, _self)) {
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

ss::future<> controller_backend::reconcile_ntp_fiber(
  model::ntp ntp, ss::lw_shared_ptr<ntp_reconciliation_state> rs) {
    if (_gate.is_closed()) {
        co_return;
    }
    auto gate_holder = _gate.hold();

    // If we don't switch here, reconciliation will inherit the scheduling group
    // of whoever triggered it (could be e.g. the admin SG).
    co_await ss::coroutine::switch_to(ss::default_scheduling_group());

    while (true) {
        co_await rs->wakeup_event.wait(_housekeeping_jitter.next_duration());
        if (_as.local().abort_requested()) {
            break;
        }

        try {
            auto sem_units = co_await ss::get_units(_reconciliation_sem, 1);
            rs->last_retried_at = ss::lowres_clock::now();
            co_await try_reconcile_ntp(ntp, *rs);
            if (rs->is_reconciled()) {
                _states.erase(ntp);
                break;
            }
        } catch (...) {
            auto ex = std::current_exception();
            if (!ssx::is_shutdown_exception(ex)) {
                vlog(
                  clusterlog.error,
                  "[{}] unexpected exception during reconciliation: {}",
                  ntp,
                  ex);
            }
        }
    }
}

ss::future<> controller_backend::try_reconcile_ntp(
  const model::ntp& ntp, ntp_reconciliation_state& rs) {
    // Run the reconciliation process until an error occurs that will force us
    // to sleep before retrying.

    if (should_skip(ntp)) {
        vlog(clusterlog.debug, "[{}] skipping reconcilation", ntp);
        rs.mark_reconciled(rs.pending_notifies);
        co_return;
    }

    while (!rs.is_reconciled() && !_as.local().abort_requested()) {
        size_t notifies = rs.pending_notifies;
        cluster::errc last_error = errc::success;
        try {
            auto res = co_await reconcile_ntp_step(ntp, rs);
            if (res.has_value() && res.value() == ss::stop_iteration::yes) {
                rs.mark_reconciled(notifies);
                vlog(
                  clusterlog.debug,
                  "[{}] reconciled, notify count: {}",
                  ntp,
                  notifies);
            } else if (res.has_error() && res.error()) {
                if (res.error().category() == error_category()) {
                    last_error = static_cast<errc>(res.error().value());
                } else {
                    last_error = errc::partition_operation_failed;
                }
            }
        } catch (const ss::gate_closed_exception&) {
            break;
        } catch (const ss::abort_requested_exception&) {
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
            if (rs.cur_operation) {
                rs.cur_operation->last_error = last_error;
                rs.cur_operation->retries += 1;
            }
            vlog(
              clusterlog.trace,
              "[{}] reconciliation attempt finished, state: {}",
              ntp,
              rs);
            break;
        }
    }
}

ss::future<> controller_backend::stuck_ntp_watchdog_fiber() {
    const auto sleep_interval = 10 * _housekeeping_interval();
    const auto stuck_timeout = 60 * _housekeeping_interval();
    const size_t max_reported = 50;

    if (_gate.is_closed()) {
        co_return;
    }
    auto gate_holder = _gate.hold();

    while (!_as.local().abort_requested()) {
        try {
            co_await ss::sleep_abortable(sleep_interval, _as.local());
        } catch (const ss::sleep_aborted&) {
            break;
        }

        auto now = ss::lowres_clock::now();
        size_t num_stuck = 0;
        for (const auto& [ntp, rs] : _states) {
            if (now > rs->last_retried_at + stuck_timeout) {
                ++num_stuck;
                if (num_stuck > max_reported) {
                    vlog(
                      clusterlog.error,
                      "more than {} stuck partitions, won't report more",
                      max_reported);
                    break;
                }

                vlog(
                  clusterlog.error,
                  "[{}] reconciliation seems stuck "
                  "(last retried {}s. ago), state: {}",
                  ntp,
                  (now - rs->last_retried_at) / 1s,
                  *rs);
            }
        }
    }
}

ss::future<result<ss::stop_iteration>> controller_backend::reconcile_ntp_step(
  const model::ntp& ntp, ntp_reconciliation_state& rs) {
    // This function is the heart of the reconciliation process. It has the
    // following structure:
    //
    // 1. Check for the diff between topic table and shard placement table on
    //    the one hand and local partition state on the other.
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

    vlog(
      clusterlog.debug,
      "[{}] reconciliation step, reconciliation state: {}",
      ntp,
      rs);

    // Step 1. Determine if the partition object has to be removed/shut
    // down/transferred/created on this shard.

    std::optional<shard_placement_table::placement_state> maybe_placement
      = _shard_placement.state_on_this_shard(ntp);

    auto partition = _partition_manager.local().get(ntp);
    if (partition) {
        vlog(
          clusterlog.trace,
          "[{}] existing partition log_revision: {}",
          ntp,
          partition->get_log_revision_id());
        vassert(
          maybe_placement,
          "[{}] placement must be present if partition is",
          ntp);
        vassert(
          maybe_placement->current()
            && partition->get_log_revision_id()
                 == maybe_placement->current()->log_revision
            && maybe_placement->current()->status
                 == shard_placement_table::hosted_status::hosted,
          "[{}] unexpected local state: {} (partition log revision: {})",
          ntp,
          maybe_placement->current(),
          partition->get_log_revision_id());
    }

    std::optional<topic_table::partition_replicas_view> maybe_replicas_view
      = _topics.local().get_replicas_view(ntp);
    if (!maybe_replicas_view) {
        if (!rs.removed_at) {
            // Even if the partition is not found in the topic table, just to be
            // on the safe side, we don't delete it until we receive the
            // corresponding delta (to protect against topic table
            // inconsistencies).
            co_return ss::stop_iteration::yes;
        }

        rs.set_cur_operation(*rs.removed_at, partition_operation_type::remove);
        auto ec = co_await delete_partition(
          ntp, maybe_placement, *rs.removed_at, partition_removal_mode::global);
        if (ec) {
            co_return ec;
        }
        co_return ss::stop_iteration::yes;
    }

    if (!maybe_placement) {
        // this shard is unrelated to this ntp
        co_return ss::stop_iteration::yes;
    }

    auto replicas_view = maybe_replicas_view.value();
    raft::group_id group_id = replicas_view.assignment.group;
    vlog(clusterlog.trace, "[{}] replicas view: {}", ntp, maybe_replicas_view);

    auto placement = *maybe_placement;
    vlog(
      clusterlog.trace,
      "[{}] placement state on this shard: {}",
      ntp,
      placement);

    std::optional<model::revision_id> expected_log_revision
      = log_revision_on_node(replicas_view, _self);

    switch (placement.get_reconciliation_action(expected_log_revision)) {
    case shard_placement_table::reconciliation_action::remove_partition: {
        // Cleanup obsolete revisions that should not exist on this node. This
        // is typically done after the replicas update is finished.
        rs.set_cur_operation(
          replicas_view.last_update_finished_revision(),
          partition_operation_type::finish_update,
          replicas_view.assignment);
        auto ec = co_await delete_partition(
          ntp,
          placement,
          replicas_view.last_update_finished_revision(),
          partition_removal_mode::local_only);
        if (ec) {
            co_return ec;
        }
        co_return ss::stop_iteration::no;
    }
    case shard_placement_table::reconciliation_action::remove_kvstore_state:
        // Cleanup obsolete kvstore state (without removing the partition data
        // itself). This can be required after a cross-shard transfer is
        // retried.
        rs.set_cur_operation(
          replicas_view.last_update_finished_revision(),
          partition_operation_type::finish_update,
          replicas_view.assignment);
        co_await remove_partition_kvstore_state(
          ntp,
          placement.current().value().group,
          expected_log_revision.value());
        co_return ss::stop_iteration::no;
    case shard_placement_table::reconciliation_action::wait_for_target_update:
        co_return errc::waiting_for_shard_placement_update;
    case shard_placement_table::reconciliation_action::transfer: {
        rs.set_cur_operation(
          replicas_view.last_cmd_revision(),
          partition_operation_type::reset,
          replicas_view.assignment);
        auto ec = co_await transfer_partition(
          ntp, group_id, expected_log_revision.value());
        if (ec) {
            co_return ec;
        }
        co_return ss::stop_iteration::no;
    }
    case shard_placement_table::reconciliation_action::create:
        // After this point the partition object is expected to exist on current
        // shard, it will be created below.
        break;
    }

    // Shut down disabled partitions

    if (_topics.local().is_disabled(ntp)) {
        vlog(clusterlog.trace, "[{}] partition disabled", ntp);
        if (partition) {
            rs.set_cur_operation(
              replicas_view.last_cmd_revision(),
              partition_operation_type::reset,
              replicas_view.assignment);
            co_await shutdown_partition(std::move(partition));
        }
        co_return ss::stop_iteration::yes;
    }

    if (!partition) {
        rs.set_cur_operation(
          replicas_view.last_cmd_revision(),
          partition_operation_type::reset,
          replicas_view.assignment);

        replicas_t initial_replicas;
        if (contains_node(replicas_view.orig_replicas(), _self)) {
            initial_replicas = replicas_view.orig_replicas();
        } else if (
          replicas_view.update
          && replicas_view.update->get_state()
               == reconfiguration_state::force_update) {
            auto [voters, learners]
              = split_voters_learners_for_force_reconfiguration(
                replicas_view.orig_replicas(),
                replicas_view.update->get_target_replicas(),
                replicas_view.revisions(),
                replicas_view.last_cmd_revision());
            // Current nodes is a voter only if we do not
            // retain any of the original nodes. initial
            // replicas is populated only if the replica is voter
            // because for learners it automatically replicated
            // via configuration update at raft level.
            if (learners.empty()) {
                initial_replicas = replicas_view.update->get_target_replicas();
            }
        } else {
            // Configuration will be replicate to the new replica
            initial_replicas = {};
        }
        auto ec = co_await create_partition(
          ntp,
          group_id,
          expected_log_revision.value(),
          std::move(initial_replicas),
          force_reconfiguration{
            replicas_view.update
            && replicas_view.update->is_force_reconfiguration()});
        if (ec) {
            co_return ec;
        }

        // The partition that we just created uses topic properties queried from
        // topic_table at last_applied_revision(). Thus all properties updates
        // with revisions <= last_applied_revision() are already reconciled.
        rs.mark_properties_reconciled(_topics.local().last_applied_revision());

        co_return ss::stop_iteration::no;
    }

    // Step 2. Reconcile configuration properties if needed.

    if (rs.properties_changed_at) {
        rs.set_cur_operation(
          *rs.properties_changed_at,
          partition_operation_type::update_properties);

        auto cfg = _topics.local().get_topic_cfg(
          model::topic_namespace_view{ntp});
        vassert(cfg, "[{}] expected topic cfg to be present", ntp);
        vlog(
          clusterlog.trace,
          "[{}] updating configuration with properties: {}",
          ntp,
          cfg.value());
        co_await partition->update_configuration(
          std::move(cfg).value().properties);

        rs.mark_properties_reconciled(_topics.local().last_applied_revision());
        co_return ss::stop_iteration::no;
    }

    // Step 3. Dispatch required raft reconfiguration and the update_finished
    // command.

    if (replicas_view.update) {
        partition_operation_type op_type;
        switch (replicas_view.update->get_state()) {
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
        rs.set_cur_operation(
          replicas_view.last_cmd_revision(), op_type, replicas_view.assignment);

        co_return co_await reconcile_partition_reconfiguration(
          rs,
          std::move(partition),
          *replicas_view.update,
          replicas_view.revisions());
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
               && contains_node(current_replicas, _self);
    }
    /**
     * Use retry count to determine which node is eligible to dispatch update
     * finished. Using modulo division allow us to round robin between
     * candidates
     */
    const model::broker_shard& candidate
      = current_replicas[current_retry % current_replicas.size()];

    return candidate.node_id == _self;
}

ss::future<std::error_code> controller_backend::create_partition(
  model::ntp ntp,
  raft::group_id group_id,
  model::revision_id log_revision,
  replicas_t initial_replicas,
  force_reconfiguration is_force_reconfigured) {
    vlog(
      clusterlog.debug,
      "[{}] creating partition, log revision: {}, initial_replicas: {}",
      ntp,
      log_revision,
      initial_replicas);

    auto cfg = _topics.local().get_topic_cfg(model::topic_namespace_view(ntp));
    if (!cfg) {
        // partition was already removed, do nothing
        co_return errc::success;
    }

    auto ec = co_await _shard_placement.prepare_create(ntp, log_revision);
    if (ec) {
        co_return ec;
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
        std::vector<model::broker> initial_brokers = create_brokers_set(
          initial_replicas, _members_table.local());

        std::optional<cloud_storage_clients::bucket_name> read_replica_bucket;
        if (cfg->is_read_replica()) {
            read_replica_bucket = cloud_storage_clients::bucket_name(
              cfg->properties.read_replica_bucket.value());
        }

        std::optional<xshard_transfer_state> xst_state;
        if (auto it = _xst_states.find(ntp); it != _xst_states.end()) {
            xst_state = it->second;
        }
        auto ntp_config = cfg->make_ntp_config(
          _data_directory, ntp.tp.partition, log_revision, initial_rev.value());
        auto rtp = cfg->properties.remote_topic_properties;
        const bool is_cloud_topic = ntp_config.is_archival_enabled()
                                    || ntp_config.is_remote_fetch_enabled();
        const bool is_internal = ntp.ns == model::kafka_internal_namespace;
        /**
         * Here we decide if a partition needs recovery from tiered storage, it
         * may be the case if partition was force reconfigured. In this case we
         * simply set the remote topic properties to initialize recovery of data
         * from the tiered storage.
         */
        if (
          is_force_reconfigured && is_cloud_topic && !is_internal
          && !ntp_config.get_overrides().recovery_enabled) {
            // topic being cloud enabled implies existence of overrides
            ntp_config.get_overrides().recovery_enabled
              = storage::topic_recovery_enabled::yes;
            rtp.emplace(*initial_rev, cfg->partition_count);
        }
        // we use offset as an rev as it is always increasing and it
        // increases while ntp is being created again
        try {
            co_await _partition_manager.local().manage(
              std::move(ntp_config),
              group_id,
              std::move(initial_brokers),
              raft::with_learner_recovery_throttle::yes,
              raft::keep_snapshotted_log::no,
              std::move(xst_state),
              rtp,
              read_replica_bucket,
              cfg->properties.remote_label,
              cfg->properties.remote_topic_namespace_override);

            _xst_states.erase(ntp);

            co_await add_to_shard_table(
              ntp, group_id, ss::this_shard_id(), log_revision);
        } catch (...) {
            vlog(
              clusterlog.warn,
              "[{}] failed to create partition with log revision {} - {}",
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
  const replicas_t&,
  model::revision_id,
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
  model::revision_id log_revision) {
    // update shard_table: broadcast
    vlog(
      clusterlog.trace,
      "[{}] adding to shard table at shard {} with log revision {}",
      ntp,
      shard,
      log_revision);
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), raft_group, shard, log_revision](
        shard_table& s) mutable {
          s.update(ntp, raft_group, shard, log_revision);
      });
}

ss::future<> controller_backend::remove_from_shard_table(
  model::ntp ntp, raft::group_id raft_group, model::revision_id log_revision) {
    // update shard_table: broadcast
    vlog(
      clusterlog.trace,
      "[{}] removing from shard table, log revision {}",
      ntp,
      log_revision);
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), raft_group, log_revision](shard_table& s) mutable {
          s.erase(ntp, raft_group, log_revision);
      });
}

ss::future<std::error_code> controller_backend::transfer_partition(
  model::ntp ntp, raft::group_id group, model::revision_id log_revision) {
    vlog(
      clusterlog.debug,
      "[{}] transferring partition, log_revision: {}",
      ntp,
      log_revision);

    auto transfer_info = co_await _shard_placement.prepare_transfer(
      ntp, log_revision, _shard_placement.container());
    if (transfer_info.source_error != errc::success) {
        co_return transfer_info.source_error;
    } else if (transfer_info.dest_error != errc::success) {
        co_return transfer_info.dest_error;
    } else if (transfer_info.is_finished) {
        co_return errc::success;
    }
    ss::shard_id destination = transfer_info.destination.value();

    std::optional<xshard_transfer_state> xst_state;

    auto partition = _partition_manager.local().get(ntp);
    if (partition) {
        xst_state = co_await shutdown_partition(std::move(partition));
    } else if (auto it = _xst_states.find(ntp); it != _xst_states.end()) {
        // We didn't get to start the partition before it was transferred again.
        xst_state = std::move(it->second);
        _xst_states.erase(it);
    }

    if (xst_state) {
        co_await container().invoke_on(
          destination, [&ntp, &xst_state](controller_backend& dest) {
              dest._xst_states[ntp] = *xst_state;
          });
    }

    co_await copy_persistent_state(
      ntp, group, _storage.local().kvs(), destination, _storage);

    auto shard_callback = [this](const model::ntp& ntp) {
        auto& dest = container().local();
        auto it = dest._states.find(ntp);
        if (it != dest._states.end()) {
            it->second->wakeup_event.set();
        }
    };

    co_await _shard_placement.finish_transfer(
      ntp, log_revision, _shard_placement.container(), shard_callback);
    co_return errc::success;
}

ss::future<> controller_backend::transfer_partitions_from_extra_shard(
  storage::kvstore& extra_kvs, shard_placement_table& extra_spt) {
    vassert(
      _topic_table_notify_handle == notification_id_type_invalid,
      "method is expected to be called before controller_backend is started");

    co_await ss::max_concurrent_for_each(
      extra_spt.shard_local_states(),
      256,
      [&](const shard_placement_table::ntp2state_t::value_type& kv) {
          const auto& [ntp, state] = kv;
          return transfer_partition_from_extra_shard(
            ntp, state, extra_kvs, extra_spt);
      });
}

ss::future<> controller_backend::transfer_partition_from_extra_shard(
  const model::ntp& ntp,
  shard_placement_table::placement_state placement,
  storage::kvstore& extra_kvs,
  shard_placement_table& extra_spt) {
    vlog(
      clusterlog.debug,
      "[{}] transferring partition from extra shard, placement: {}",
      ntp,
      placement);

    auto target = _shard_placement.get_target(ntp);
    if (!target) {
        co_return;
    }
    model::revision_id log_rev = target->log_revision;

    using reconciliation_action = shard_placement_table::reconciliation_action;

    if (
      placement.get_reconciliation_action(log_rev)
      != reconciliation_action::transfer) {
        // this can happen if the partition is already superceded by a partition
        // with greater log revision.
        co_return;
    }

    auto transfer_info = co_await extra_spt.prepare_transfer(
      ntp, log_rev, _shard_placement.container());
    if (transfer_info.source_error != errc::success) {
        // This can happen if this extra shard was the destination of an
        // unfinished x-shard transfer. We can ignore this partition as we
        // already have a valid copy of kvstore data on one of the valid shards.
        co_return;
    }

    if (transfer_info.dest_error != errc::success) {
        // clear kvstore state on destination
        co_await container().invoke_on(
          transfer_info.destination.value(),
          [&ntp, log_rev](controller_backend& dest) {
              auto dest_placement = dest._shard_placement.state_on_this_shard(
                ntp);
              vassert(dest_placement, "[{}] expected placement", ntp);
              switch (dest_placement->get_reconciliation_action(log_rev)) {
              case reconciliation_action::create:
              case reconciliation_action::transfer:
              case reconciliation_action::wait_for_target_update:
                  vassert(
                    false,
                    "[{}] unexpected reconciliation action, placement: {}",
                    ntp,
                    *dest_placement);
              case reconciliation_action::remove_partition:
                  // TODO: remove obsolete log directory
              case reconciliation_action::remove_kvstore_state:
                  break;
              }
              return remove_persistent_state(
                       ntp,
                       dest_placement->current().value().group,
                       dest._storage.local().kvs())
                .then([&dest, &ntp, log_rev] {
                    return dest._shard_placement.finish_delete(ntp, log_rev);
                });
          });

        transfer_info = co_await extra_spt.prepare_transfer(
          ntp, log_rev, _shard_placement.container());
    }

    vassert(
      transfer_info.destination && transfer_info.dest_error == errc::success,
      "[{}] expected successful prepare_transfer, destination error: {}",
      ntp,
      transfer_info.dest_error);

    if (transfer_info.is_finished) {
        co_return;
    }

    ss::shard_id destination = transfer_info.destination.value();
    co_await copy_persistent_state(
      ntp, target->group, extra_kvs, destination, _storage);

    co_await extra_spt.finish_transfer(
      ntp, log_rev, _shard_placement.container(), [](const model::ntp&) {});
}

ss::future<xshard_transfer_state>
controller_backend::shutdown_partition(ss::lw_shared_ptr<partition> partition) {
    vlog(
      clusterlog.debug,
      "[{}] shutting down, log revision: {}",
      partition->ntp(),
      partition->get_log_revision_id());

    auto ntp = partition->ntp();
    auto gr = partition->group();

    try {
        // remove from shard table
        co_await remove_from_shard_table(
          ntp, gr, partition->get_log_revision_id());
        // shutdown partition
        co_return co_await _partition_manager.local().shutdown(ntp);
    } catch (...) {
        /**
         * If partition shutdown failed we should crash, this error is
         * unrecoverable
         */
        vassert(
          false,
          "error shutting down {} partition, error: {}, terminating",
          ntp,
          std::current_exception());
    }
}

ss::future<std::error_code> controller_backend::delete_partition(
  model::ntp ntp,
  std::optional<shard_placement_table::placement_state> placement,
  model::revision_id cmd_revision,
  partition_removal_mode mode) {
    vlog(
      clusterlog.debug,
      "[{}] removing partition, placement state: {}, cmd_revision: {}, global: "
      "{}",
      ntp,
      placement,
      cmd_revision,
      mode == partition_removal_mode::global);

    if (!placement) {
        // nothing to delete
        co_return errc::success;
    }

    auto ec = co_await _shard_placement.prepare_delete(ntp, cmd_revision);
    if (ec) {
        co_return ec;
    }

    if (!placement->current()) {
        // nothing to delete
        co_return errc::success;
    }

    auto log_revision = placement->current()->log_revision;
    if (log_revision >= cmd_revision) {
        // Perform an extra revision check to be on the safe side, if the
        // partition has already been re-created with greater revision, do
        // nothing.
        co_return errc::waiting_for_reconfiguration_finish;
    }

    auto part = _partition_manager.local().get(ntp);
    if (part) {
        co_await remove_from_shard_table(ntp, part->group(), log_revision);
        co_await _partition_manager.local().remove(ntp, mode);
    } else {
        // TODO: delete log directory even when there is no partition object
        _xst_states.erase(ntp);
        co_await remove_persistent_state(
          ntp, placement->current()->group, _storage.local().kvs());
    }

    co_await _shard_placement.finish_delete(ntp, log_revision);
    co_return errc::success;
}

ss::future<> controller_backend::remove_partition_kvstore_state(
  model::ntp ntp, raft::group_id group, model::revision_id log_revision) {
    vlog(
      clusterlog.debug,
      "[{}] removing obsolete partition kvstore state, log_revision: {}",
      ntp,
      log_revision);

    _xst_states.erase(ntp);
    co_await remove_persistent_state(ntp, group, _storage.local().kvs());
    co_await _shard_placement.finish_delete(ntp, log_revision);
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
