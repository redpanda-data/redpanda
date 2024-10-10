/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "cluster/notification.h"
#include "cluster/topic_table_probe.h"
#include "container/chunked_hash_map.h"
#include "container/contiguous_range_map.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/stable_iterator_adaptor.h"

#include <absl/container/node_hash_map.h>

#include <type_traits>

namespace cluster {
/**
 * ## Overview
 *
 * Topic table represent all topics configuration and partition assignments.
 * The topic table is copied on each core to minimize cross core communication
 * when requesting topic information. The topics table provides an API for
 * Kafka requests and delta API for controller backend. The delta API allows
 * backend to wait for changes in topics table. Topics table is update directly
 * from controller_stm. The table is always updated before any actions related
 * with topic creation or deletion are executed.
 *
 * Topic table is updated through topic_updates_dispatcher.
 *
 *
 * ## Partition movement state machine
 *
 * When partition is being moved from replica set A to replica set B it may
 * either be finished or cancelled. The cancellation on the other hand may be
 * reverted. The partition movement state transitions are explained in the
 * following diagram:
 *
 * A - replica set A
 * B - replica set B
 *
 * ┌──────────┐   move           ┌──────────┐       finish         ┌──────────┐
 * │  static  │                  │  moving  │                      │  static  │
 * │          ├─────────────────►│          ├─────────────────────►│          │
 * │    A     │                  │  A -> B  │                      │    B     │
 * └──────────┘                  └────┬─────┘                      └──────────┘
 *      ▲                             │                                  ▲
 *      │                             │                                  │
 *      │                             │ cancel/force_cancel              │
 *      │                             │                                  │
 *      │                             │                                  │
 *      │                             ▼                                  │
 *      │                        ┌──────────┐                            │
 *      │        finish          │  moving  │     revert_cancel          │
 *      └────────────────────────┤          ├────────────────────────────┘
 *                               │  B -> A  │
 *                               └──────────┴─────┐
 *                                    ▲           │
 *                                    │           │
 *                                    │           │
 *                                    │           │
 *                                    └───────────┘
 *                                        force_cancel
 */

class topic_table {
public:
    enum class topic_state { exists, not_exists, indeterminate };

    // Guide to various partition revisions (i.e. offsets of the corresponding
    // commands in the controller log), presented in the chronological order:
    // * topic creation revision
    // * revision of the command (topic creation or partition movement) that
    //   caused a partition replica to appear on a given node (note that the
    //   partition can move away from a node and come back again and each time
    //   revision will be different). This revision appears in:
    //   * replicas_revision_map
    //   * partition::get_ntp_config().get_revision()
    //   * the partition directory name
    // * revision of the last applied command that caused raft reconfiguration
    //   (can be reconfiguration itself or cancellation). This revision appears
    //   in:
    //   * in_progress_update::get_last_cmd_revision()
    //   * partition::get_revision_id()
    //   * raft::group_configuration::revision_id()

    class concurrent_modification_error final
      : public ::concurrent_modification_error {
    public:
        concurrent_modification_error(
          model::revision_id initial_revision,
          model::revision_id current_revision)
          : ::concurrent_modification_error(ssx::sformat(
              "Topic table was modified by concurrent fiber. "
              "(initial_revision: {}, current_revision: {}) ",
              initial_revision,
              current_revision)) {}
    };

    class in_progress_update {
    public:
        explicit in_progress_update(
          replicas_t previous_replicas,
          replicas_t target_replicas,
          reconfiguration_state state,
          model::revision_id update_revision,
          reconfiguration_policy policy,
          topic_table_probe* probe)
          : _previous_replicas(std::move(previous_replicas))
          , _target_replicas(std::move(target_replicas))
          , _state(state)
          , _update_revision(update_revision)
          , _last_cmd_revision(update_revision)
          , _policy(policy)
          , _probe(probe) {
            if (_probe) {
                _probe->handle_update(_previous_replicas, _target_replicas);
            }
        }

        ~in_progress_update() {
            if (_probe) {
                _probe->handle_update_finish(
                  _previous_replicas, _target_replicas);
                if (is_cancelled_state(_state)) {
                    _probe->handle_update_cancel_finish(
                      _previous_replicas, _target_replicas);
                }
            }
        }

        in_progress_update(const in_progress_update&) = delete;
        in_progress_update(in_progress_update&&) = default;
        in_progress_update& operator=(const in_progress_update&) = delete;
        in_progress_update& operator=(in_progress_update&&) = delete;

        const reconfiguration_state& get_state() const { return _state; }

        void set_state(reconfiguration_state state, model::revision_id rev) {
            if (
              _probe && !is_cancelled_state(_state)
              && is_cancelled_state(state)) {
                _probe->handle_update_cancel(
                  _previous_replicas, _target_replicas);
            }
            _state = state;
            _last_cmd_revision = rev;
        }

        const replicas_t& get_previous_replicas() const {
            return _previous_replicas;
        }
        const replicas_t& get_target_replicas() const {
            return _target_replicas;
        }

        const replicas_t& get_resulting_replicas() const {
            if (is_cancelled_state(_state)) {
                return _previous_replicas;
            } else {
                return _target_replicas;
            }
        }

        const model::revision_id& get_update_revision() const {
            return _update_revision;
        }

        const model::revision_id& get_last_cmd_revision() const {
            return _last_cmd_revision;
        }

        reconfiguration_policy get_reconfiguration_policy() const {
            return _policy;
        }

        bool is_force_reconfiguration() const {
            return _state == reconfiguration_state::force_cancelled
                   || _state == reconfiguration_state::force_update;
        }

        friend std::ostream&
        operator<<(std::ostream&, const in_progress_update&);

    private:
        replicas_t _previous_replicas;
        replicas_t _target_replicas;
        reconfiguration_state _state;
        model::revision_id _update_revision;
        model::revision_id _last_cmd_revision;
        reconfiguration_policy _policy;
        // _probe can be nullptr, in this case metrics are not updated.
        topic_table_probe* _probe;
    };

    struct partition_meta {
        /// Replica revision map reflecting *only the finished partition
        /// updates* (i.e. it only gets updated when the update_finished command
        /// is processed).
        replicas_revision_map replicas_revisions;
        /// Revision id of the last applied update_finished controller command
        /// (or of addition command if none)
        model::revision_id last_update_finished_revision;
    };

    struct topic_metadata_item {
        topic_metadata metadata;
        contiguous_range_map<model::partition_id::type, partition_meta>
          partitions;

        assignments_set& get_assignments() {
            return metadata.get_assignments();
        }

        const assignments_set& get_assignments() const {
            return metadata.get_assignments();
        }
        model::revision_id get_revision() const {
            return metadata.get_revision();
        }
        std::optional<model::initial_revision_id> get_remote_revision() const {
            return metadata.get_remote_revision();
        }

        const topic_configuration& get_configuration() const {
            return metadata.get_configuration();
        }
        topic_configuration& get_configuration() {
            return metadata.get_configuration();
        }

        replication_factor get_replication_factor() const {
            return metadata.get_replication_factor();
        }
    };

    using underlying_t = chunked_hash_map<
      model::topic_namespace,
      topic_metadata_item,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    using lifecycle_markers_t = absl::node_hash_map<
      nt_revision,
      nt_lifecycle_marker,
      nt_revision_hash,
      nt_revision_eq>;

    using disabled_partitions_t = chunked_hash_map<
      model::topic_namespace,
      topic_disabled_partitions_set,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    using topic_delta = topic_table_topic_delta;

    using topic_delta_cb_t
      = ss::noncopyable_function<void(const chunked_vector<topic_delta>&)>;

    using ntp_delta = topic_table_ntp_delta;

    using ntp_delta_range_t
      = boost::iterator_range<fragmented_vector<ntp_delta>::const_iterator>;
    using ntp_delta_cb_t = ss::noncopyable_function<void(ntp_delta_range_t)>;
    using lw_ntp_cb_t = ss::noncopyable_function<void()>;

    /// A helper struct that has various replica-related metadata all in one
    /// place, so that the API user doesn't have to query several maps manually.
    ///
    /// Note that it contains references to current topic_table state and
    /// therefore cannot be used across scheduling points.
    struct partition_replicas_view {
        const replicas_t& orig_replicas() const {
            return update ? update->get_previous_replicas()
                          : assignment.replicas;
        }

        const replicas_t& resulting_replicas() const {
            return assignment.replicas;
        }

        const replicas_revision_map& revisions() const {
            return partition_meta.replicas_revisions;
        }

        model::revision_id last_update_finished_revision() const {
            return partition_meta.last_update_finished_revision;
        }

        model::revision_id last_cmd_revision() const {
            return update ? update->get_last_cmd_revision()
                          : last_update_finished_revision();
        }

        friend std::ostream&
        operator<<(std::ostream&, const partition_replicas_view&);

        const partition_meta& partition_meta;
        const partition_assignment& assignment;
        const in_progress_update* update = nullptr;
    };

    explicit topic_table(data_migrations::migrated_resources&);

    cluster::notification_id_type
    register_topic_delta_notification(topic_delta_cb_t cb) {
        auto id = _topic_notification_id++;
        _topic_notifications.emplace_back(id, std::move(cb));
        return id;
    }

    void unregister_topic_delta_notification(cluster::notification_id_type id) {
        std::erase_if(
          _topic_notifications,
          [id](const std::pair<cluster::notification_id_type, topic_delta_cb_t>&
                 n) { return n.first == id; });
    }

    cluster::notification_id_type
    register_ntp_delta_notification(ntp_delta_cb_t cb) {
        auto id = _ntp_notification_id++;
        _ntp_notifications.emplace_back(id, std::move(cb));
        return id;
    }

    void unregister_ntp_delta_notification(cluster::notification_id_type id) {
        std::erase_if(
          _ntp_notifications,
          [id](
            const std::pair<cluster::notification_id_type, ntp_delta_cb_t>& n) {
              return n.first == id;
          });
    }

    /// similar to delta notifications but lightweight because a copy of delta
    /// is not included in the notification.
    cluster::notification_id_type register_lw_ntp_notification(lw_ntp_cb_t cb) {
        auto id = _lw_ntp_notification_id++;
        _lw_ntp_notifications.emplace_back(id, std::move(cb));
        return id;
    }

    void unregister_lw_ntp_notification(cluster::notification_id_type id) {
        std::erase_if(
          _lw_ntp_notifications,
          [id](const std::pair<cluster::notification_id_type, lw_ntp_cb_t>& n) {
              return n.first == id;
          });
    }

    using updates_t = absl::node_hash_map<model::ntp, in_progress_update>;

    bool is_batch_applicable(const model::record_batch& b) const {
        return b.header().type
               == model::record_batch_type::topic_management_cmd;
    }

    /// State machine applies
    ss::future<std::error_code> apply(create_topic_cmd, model::offset);
    ss::future<std::error_code> apply(delete_topic_cmd, model::offset);
    ss::future<std::error_code>
      apply(topic_lifecycle_transition, model::offset);
    ss::future<std::error_code>
      apply(move_partition_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(finish_moving_partition_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(update_topic_properties_cmd, model::offset);
    ss::future<std::error_code> apply(create_partition_cmd, model::offset);
    ss::future<std::error_code>
      apply(cancel_moving_partition_replicas_cmd, model::offset);
    ss::future<std::error_code> apply(move_topic_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(revert_cancel_partition_move_cmd, model::offset);
    ss::future<std::error_code>
      apply(force_partition_reconfiguration_cmd, model::offset);
    ss::future<std::error_code>
      apply(update_partition_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(set_topic_partitions_disabled_cmd, model::offset);
    ss::future<std::error_code>
      apply(bulk_force_reconfiguration_cmd, model::offset);

    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<>
    apply_snapshot(model::offset snapshot_offset, const controller_snapshot&);

    ss::future<> stop();

    /// Query API

    /// Returns list of all topics that exists in the cluster.
    std::vector<model::topic_namespace> all_topics() const;

    // Returns the number of topics that exist in the cluster.
    size_t all_topics_count() const;

    ///\brief Returns metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<topic_metadata>
      get_topic_metadata(model::topic_namespace_view) const;

    ///\brief Returns reference to metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    ///
    /// IMPORTANT: remember not to use the reference when its lifetime has to
    /// span across multiple scheduling point. Reference returning method is
    /// provided not to copy metadata object when used by synchronous parts of
    /// code
    std::optional<std::reference_wrapper<const topic_metadata>>
      get_topic_metadata_ref(model::topic_namespace_view) const;

    ///\brief Returns configuration of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<topic_configuration>
      get_topic_cfg(model::topic_namespace_view) const;

    ///\brief Returns configuration of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<replication_factor>
      get_topic_replication_factor(model::topic_namespace_view) const;

    ///\brief Returns partition assignments of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<assignments_set>
      get_topic_assignments(model::topic_namespace_view) const;

    ///\brief Returns topics timestamp type
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<model::timestamp_type>
      get_topic_timestamp_type(model::topic_namespace_view) const;

    /// Returns metadata of all topics.
    const underlying_t& all_topics_metadata() const;

    /// Get corresponding partition_replicas_view for an ntp (if present).
    std::optional<partition_replicas_view>
    get_replicas_view(const model::ntp& ntp) const;

    /// Get corresponding partition_replicas_view for an ntp, assumes that the
    /// partition exists. Useful for iterating over partitions of a single
    /// topic (as it avoids repeatedly querying topic-wise maps).
    partition_replicas_view get_replicas_view(
      const model::ntp& ntp,
      const topic_metadata_item& md_item,
      const partition_assignment& assignment) const;

    // use this pair of methods to check if the topics map has changed (so that
    // it is not safe to continue iterating over it).
    model::revision_id topics_map_revision() const {
        return _topics_map_revision;
    }
    void check_topics_map_stable(model::revision_id) const;

    /// Checks if it has given partition
    bool contains(model::topic_namespace_view, model::partition_id) const;
    /// Checks if it has given topic
    bool contains(model::topic_namespace_view tp) const {
        return _topics.contains(tp);
    }
    /// contains() check with stronger validation on the topic revision/offset.
    /// Just looking up in the cache can yield false negatives if the cache is
    /// not warmed up because controller replay can be in progress. This variant
    /// of contains factors in such false negatives by incorporating a check on
    /// the topic revision. Topic revision is the topic creation command offset
    /// in
    //  the controller log.
    ///
    /// There are 3 possibilities.
    /// 1. id > last applied offset, not enough information
    /// 2. id <= last_applied offset && not in cache
    /// 3. id <= last_applied offset && in cache.
    ///
    /// Callers must note that false positives are still possible because of any
    /// inflight pending deltas (yet to be applied) that delete the topic.
    topic_state
    get_topic_state(model::topic_namespace_view, model::revision_id id) const;

    std::optional<partition_assignment>
    get_partition_assignment(const model::ntp&) const;

    const underlying_t& topics_map() const { return _topics; }

    bool is_update_in_progress(const model::ntp&) const;

    bool has_updates_in_progress() const {
        return !_updates_in_progress.empty();
    }

    const updates_t& updates_in_progress() const {
        return _updates_in_progress;
    }

    ///\brief Returns initial revision id of the topic
    std::optional<model::initial_revision_id>
    get_initial_revision(model::topic_namespace_view tp) const;

    ///\brief Returns initial revision id of the partition
    std::optional<model::initial_revision_id>
    get_initial_revision(const model::ntp& ntp) const;

    /**
     * returns previous replica set of partition if partition is currently being
     * reconfigured. For reconfiguration from [1,2,3] to [2,3,4] this method
     * will return [1,2,3].
     */
    std::optional<replicas_t> get_previous_replica_set(const model::ntp&) const;
    /**
     * returns target replica set of partition if partition is currently being
     * reconfigured. For reconfiguration from [1,2,3] to [2,3,4] this method
     * will return [2,3,4].
     */
    std::optional<replicas_t> get_target_replica_set(const model::ntp&) const;

    /**
     * Lists all NTPs that replicas are being move to a node
     */
    std::vector<model::ntp> ntps_moving_to_node(model::node_id) const;

    /**
     * Lists all NTPs that replicas are being move from a node
     */
    std::vector<model::ntp> ntps_moving_from_node(model::node_id) const;

    /**
     * Lists all ntps moving either from or to a node
     */
    std::vector<model::ntp> all_ntps_moving_per_node(model::node_id) const;

    std::vector<model::ntp> all_updates_in_progress() const;

    model::revision_id last_applied_revision() const {
        return _last_applied_revision_id;
    }

    // does not include non-replicable partitions
    size_t partition_count() const { return _partition_count; }

    /**
     * Returns number of partitions allocated on given node
     */
    size_t get_node_partition_count(model::node_id) const;

    /**
     * See which topics have pending deletion work
     */
    const lifecycle_markers_t& get_lifecycle_markers() const {
        return _lifecycle_markers;
    }

    const disabled_partitions_t& get_disabled_partitions() const {
        return _disabled_partitions;
    }

    const topic_disabled_partitions_set*
    get_topic_disabled_set(model::topic_namespace_view ns_tp) const {
        auto it = _disabled_partitions.find(ns_tp);
        if (it == _disabled_partitions.end()) {
            return nullptr;
        }
        return &it->second;
    }

    bool is_fully_disabled(model::topic_namespace_view ns_tp) const {
        auto it = _disabled_partitions.find(ns_tp);
        if (it == _disabled_partitions.end()) {
            return false;
        }
        return it->second.is_fully_disabled();
    }

    bool is_fully_enabled(model::topic_namespace_view ns_tp) const {
        auto it = _disabled_partitions.find(ns_tp);
        if (it == _disabled_partitions.end()) {
            return true;
        }
        return it->second.is_fully_enabled();
    }

    bool is_disabled(
      model::topic_namespace_view ns_tp, model::partition_id p_id) const {
        auto it = _disabled_partitions.find(ns_tp);
        if (it == _disabled_partitions.end()) {
            return false;
        }
        return it->second.is_disabled(p_id);
    }

    bool is_disabled(const model::ntp& ntp) const {
        return is_disabled(model::topic_namespace_view{ntp}, ntp.tp.partition);
    }

    auto topics_iterator_begin() const {
        return stable_iterator<
          underlying_t::const_iterator,
          model::revision_id>(
          [this] { return _topics_map_revision; }, _topics.begin());
    }

    auto topics_iterator_end() const {
        return stable_iterator<
          underlying_t::const_iterator,
          model::revision_id>(
          [this] { return _topics_map_revision; }, _topics.end());
    }

    const force_recoverable_partitions_t& partitions_to_force_recover() const {
        return _partitions_to_force_reconfigure;
    }

    std::error_code validate_force_reconfigurable_partitions(
      const fragmented_vector<ntp_with_majority_loss>&) const;

    auto partitions_to_force_recover_it_begin() const {
        return stable_iterator<
          force_recoverable_partitions_t::const_iterator,
          model::revision_id>(
          [this]() { return _partitions_to_force_reconfigure_revision; },
          _partitions_to_force_reconfigure.begin());
    }

    auto partitions_to_force_recover_it_end() const {
        return stable_iterator<
          force_recoverable_partitions_t::const_iterator,
          model::revision_id>(
          [this]() { return _partitions_to_force_reconfigure_revision; },
          _partitions_to_force_reconfigure.end());
    }

private:
    friend topic_table_probe;

    struct waiter {
        explicit waiter(uint64_t id)
          : id(id) {}
        ss::promise<fragmented_vector<ntp_delta>> promise;
        ss::abort_source::subscription sub;
        uint64_t id;
    };

    void notify_waiters();

    void change_partition_replicas(
      model::ntp ntp,
      const replicas_t& new_assignment,
      partition_assignment& current_assignment,
      model::offset o,
      bool is_forced,
      reconfiguration_policy policy);

    class snapshot_applier;

    std::error_code do_local_delete(
      model::topic_namespace nt, model::offset offset, bool ignore_migration);
    ss::future<std::error_code>
      do_apply(update_partition_replicas_cmd_data, model::offset);

    void add_partition_to_force_reconfigure(ntp_with_majority_loss);
    void reset_partitions_to_force_reconfigure(
      const force_recoverable_partitions_t&);

    void on_partition_deletion(const model::ntp&);
    void on_partition_move_finish(
      const model::ntp&, const std::vector<model::broker_shard>& replicas);
    std::error_code validate_force_reconfigurable_partition(
      const ntp_with_majority_loss&) const;

    underlying_t _topics;
    lifecycle_markers_t _lifecycle_markers;
    disabled_partitions_t _disabled_partitions;
    size_t _partition_count{0};

    updates_t _updates_in_progress;
    model::revision_id _last_applied_revision_id;

    // Monotonic counter that is bumped each time _topics, _disabled_partitions,
    // or _updates_in_progress are modified in a way that makes iteration over
    // them unsafe (i.e. invalidates iterators or references, including
    // for nested collections like partition sets and replica sets).
    //
    // Unlike other revisions this does not correspond to the command
    // revision that updated the map.
    model::revision_id _topics_map_revision{0};

    chunked_vector<topic_delta> _pending_topic_deltas;
    cluster::notification_id_type _topic_notification_id{0};
    std::vector<std::pair<cluster::notification_id_type, topic_delta_cb_t>>
      _topic_notifications;

    fragmented_vector<ntp_delta> _pending_ntp_deltas;
    cluster::notification_id_type _ntp_notification_id{0};
    cluster::notification_id_type _lw_ntp_notification_id{0};
    std::vector<std::pair<cluster::notification_id_type, ntp_delta_cb_t>>
      _ntp_notifications;
    std::vector<std::pair<cluster::notification_id_type, lw_ntp_cb_t>>
      _lw_ntp_notifications;

    topic_table_probe _probe;
    force_recoverable_partitions_t _partitions_to_force_reconfigure;
    model::revision_id _partitions_to_force_reconfigure_revision{0};
    data_migrations::migrated_resources& _migrated_resources;
    friend class topic_table_partition_generator;
};

} // namespace cluster
